# Responsável por: lógica comum de extração, retry, paginação, comparação JSON

import requests
import time
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert
from config.settings import headers
from config.database import Session

# =======================================================
# 1. FUNÇÃO DE COMPARAÇÃO DE JSON 
# =======================================================

def comparar_jsons(json1, json2):
    """
    Compara dois JSONs de forma inteligente
    Retorna True se são diferentes, False se são iguais
    """
    try:
        # Converter ambos para string normalizada para comparação
        # Isso garante que diferenças na ordem dos campos sejam ignoradas
        if isinstance(json1, dict) and isinstance(json2, dict):
            # Ordenar as chaves recursivamente para comparação consistente
            def ordenar_dict(obj):
                if isinstance(obj, dict):
                    return {k: ordenar_dict(v) for k, v in sorted(obj.items())} # Sorted é para ordenar as chaves do dicionário
                elif isinstance(obj, list):
                    return [ordenar_dict(item) for item in obj]
                return obj
            
            json1_ordenado = ordenar_dict(json1)
            json2_ordenado = ordenar_dict(json2)
            
            return json1_ordenado != json2_ordenado
        else:
            return json1 != json2
    except Exception as e:
        print(f"Erro ao comparar JSONs: {e}")
        # Em caso de erro, assume que são diferentes para atualizar
        return True

# =======================================================
# 2. CLASSE BASE PARA EXTRATORES
# =======================================================

class BaseExtractor:
    """
    Classe base que contém toda a lógica comum de extração
    Outros extractors vão herdar desta classe e só mudar o que é específico
    """
    
    def __init__(self, base_url, model_class): # Self é para acessar as variáveis da classe
        """
        Inicializa o extractor base
        Args:
            base_url: URL da API para este endpoint
            model_class: Classe do modelo SQLAlchemy (ex: ContatoRaw)
        """
        self.base_url = base_url 
        self.headers = headers
        self.model_class = model_class

# =======================================================
# 3. FUNÇÃO DE EXTRAÇÃO DOS DADOS (COM RETRY E PARADA)
# =======================================================  

    # Definindo a função de extração e fazendo a requisição
    def extract_dados_bling_paginado(self, limite_por_pagina=100, delay_entre_requests=0.35, max_paginas=1000, max_tentativas=3): # Extrai todos os registros da API Bling usando paginação
        """
        Extrai todos os dados de qualquer endpoint da API Bling usando paginação
        PARA COMPLETAMENTE se não conseguir obter uma página após 3 tentativas
        
        Args   
            limite_por_pagina (int): Número máximo de registros por página (máx 100)
            delay_entre_requests (float): Tempo de espera entre requests em segundos
            max_paginas (int): Limite máximo de páginas para evitar loops infinitos
            max_tentativas (int): Número de tentativas por página antes de parar tudo

        Returns:
            list: Lista com todos os dados de cada endpoint extraídos
        """
        todos_registros = []      # Lista genérica para armazenar todos os registros
        pagina_atual = 1          # Começamos da página 1
        total_paginas = None      # Vamos descobrir isso na primeira requisição
        registros_unicos = set()   # Para evitar duplicatas
        
        print(f"Iniciando extração paginada...")
        print(f"Configurações: delay={delay_entre_requests}s, max_tentativas={max_tentativas}")

        while pagina_atual <= max_paginas: # Proteção contra loop infinito
            # Parâmetros para requisição
            params = {
                "limite": limite_por_pagina,
                "pagina": pagina_atual
            }

            print(f"Processando página {pagina_atual}{'/' + str(total_paginas) if total_paginas else ''}...")
        
            # Sistema de retry para cada página
            sucesso = False
            for tentativa in range(max_tentativas):
                try:
                    # Fazendo requisição para a API com timeout
                    response = requests.get(
                        self.base_url,
                        headers=self.headers,
                        params=params,
                        timeout=30  # Timeout de 30 segundos
                    )
                    
                    # Verificando se a requisição foi bem sucedida
                    if response.status_code != 200:
                        print(f"Erro HTTP {response.status_code} na página {pagina_atual} (tentativa {tentativa + 1}/{max_tentativas})")
                        print(f"Resposta: {response.text}")
                        
                        if tentativa < max_tentativas - 1:
                            delay_erro = delay_entre_requests * 2
                            print(f"Aguardando {delay_erro}s antes de tentar novamente...")
                            time.sleep(delay_erro)
                            continue
                        else:
                            print(f"ERRO CRÍTICO: Falha HTTP após {max_tentativas} tentativas na página {pagina_atual}")
                            print("INTERROMPENDO EXTRAÇÃO para evitar perda de dados")
                            raise Exception(f"Falha HTTP {response.status_code} após {max_tentativas} tentativas")

                    # Convertendo a resposta para JSON
                    dados = response.json()
                    sucesso = True
                    break

                except (requests.exceptions.ConnectionError, 
                        requests.exceptions.Timeout,
                        requests.exceptions.RequestException) as e:
                    
                    print(f"Erro de conexão na página {pagina_atual} (tentativa {tentativa + 1}/{max_tentativas}): {e}")
                    
                    if tentativa < max_tentativas - 1:
                        # Delay progressivo: 0.35s → 0.7s → 1.4s
                        delay_progressivo = delay_entre_requests * (2 ** tentativa)
                        print(f"Aguardando {delay_progressivo:.1f}s antes de tentar novamente...")
                        time.sleep(delay_progressivo)
                    else:
                        print(f"ERRO CRÍTICO: Falha de conexão após {max_tentativas} tentativas na página {pagina_atual}")
                        print("INTERROMPENDO EXTRAÇÃO para evitar perda de dados")
                        print("Verifique sua conexão de internet e tente novamente")
                        raise Exception(f"Falha de conexão após {max_tentativas} tentativas: {e}")
                except Exception as e:
                    print(f"Erro inesperado na página {pagina_atual} (tentativa {tentativa + 1}/{max_tentativas}): {e}")
                    
                    if tentativa < max_tentativas - 1:
                        time.sleep(delay_entre_requests)
                    else:
                        print(f"ERRO CRÍTICO: Erro não recuperável na página {pagina_atual}")
                        print("INTERROMPENDO EXTRAÇÃO para análise do erro")
                        raise Exception(f"Erro não recuperável após {max_tentativas} tentativas: {e}")
            
            # Se chegou aqui, obteve sucesso na requisição
            if not sucesso:
                # Esta condição não deveria ser alcançada devido aos raises acima
                print(f"ERRO INTERNO: Lógica de retry falhou")
                raise Exception("Falha interna no sistema de retry")
                
            # Debug: mostrar estrutura da resposta na primeira página
            if pagina_atual == 1:
                print(f"Total informado pela API: {dados.get('total', 'N/A')}")
                print(f"Total de páginas informado: {dados.get('total_pages', 'N/A')}")

            # Na primeira requisição, capturamos o total de páginas
            if total_paginas is None:
                total_paginas = dados.get("total_pages", 1)
                total_registros = dados.get("total", 0)
                print(f"Total de páginas: {total_paginas}")
                print(f"Total de registros: {total_registros}")

            # Extraindo os registros da página atual
            registros_pagina = dados.get("data", [])
            
            # Se não há mais registros, paramos o loop
            if not registros_pagina:
                print(f"Página {pagina_atual} vazia. Finalizando extração.")
                break

            # Verificar se temos registro novos ou se estamos vendo repetidos
            registros_novos = 0
            for registro in registros_pagina:
                if registro['id'] not in registros_unicos:
                    registros_unicos.add(registro['id'])
                    todos_registros.append(registro)
                    registros_novos += 1

            print(f"Extraídos {len(registros_pagina)} registro da página {pagina_atual} ({registros_novos} novos)")
            
            # Se não encontramos registro novos, provavelmente chegamos ao fim
            if registros_novos == 0:
                print(f"Nenhum registro novo na página {pagina_atual}. Finalizando.")
                break
            
            # Se chegamos na última página OFICIAL, mas ainda há dados, continuamos
            if pagina_atual >= total_paginas and len(registros_pagina) < limite_por_pagina:
                print(f"Última página oficial ({total_paginas}) processada e com menos que {limite_por_pagina} registros. Finalizando.")
                break

            # Incrementamos para a próxima página
            pagina_atual += 1
            
            # Pausa entre requests para não sobrecarregar a API
            if delay_entre_requests > 0:
                time.sleep(delay_entre_requests)
        
        print(f"Extração finalizada com sucesso. Total de registro coletados: {len(todos_registros)}")
        print(f"Páginas processadas: {pagina_atual - 1}")
        return todos_registros   

# =============================================================
# 4. FUNÇÃO PARA SALVAR NO POSTGRES (COMPARAR ANTES DE SALVAR)
# =============================================================

    def salvar_dados_postgres_bulk(self, lista_dados): # Salva múltiplos registros no Postgres de forma eficiente usando bulk insert
        """
        Salva dados usando comparação inteligente:
        - Novos registros: INSERT
        - Registros existentes idênticos: SKIP
        - Registros existentes diferentes: UPDATE
        """
        if not lista_dados:
            print("Nenhum dado para salvar.")
            return {"inseridos": 0, "atualizados": 0, "ignorados": 0, "total": 0}
        
        session = Session()
        stats = {"inseridos": 0, "atualizados": 0, "ignorados": 0, "total": len(lista_dados)}

        try:
            print(f"🔍 Buscando registros existentes para comparação...")
            inicio_busca = datetime.now()

            # Buscar TODOS os registros existentes com seus JSONs
            # Isso é mais eficiente que buscar um por um
            registros_existentes = {}
            existing_records = session.query(
                self.model_class.bling_id,
                self.model_class.dados_json
            ).all()

            # Carregando os registros existentes
            for record in existing_records:
                registros_existentes[record.bling_id] = record.dados_json
            
            fim_busca = datetime.now()
            print(f"📋 {len(registros_existentes)} registros existentes carregados em {fim_busca - inicio_busca}")

            # Classificar os dados em: novos, diferentes, idênticos
            registros_novos = []
            registros_para_atualizar = []
            
            print(f"🔍 Comparando {len(lista_dados)} registros...")
            inicio_comparacao = datetime.now()
            
            for i, dados in enumerate(lista_dados): # Enumerates ajuda com loops que exigem um contador, adicionando um índice a cada item em um iterável
                bling_id = dados['bling_id']
                novo_json = dados['dados_json']
                
                # Mostrar progresso a cada 1000 registros
                if (i + 1) % 1000 == 0:
                    print(f"Processados {i + 1}/{len(lista_dados)} registros...")
                
                if bling_id not in registros_existentes:
                    # Registro novo → INSERT
                    registros_novos.append({
                        'bling_id': bling_id,
                        'dados_json': novo_json,
                        'data_ingestao': datetime.now(),
                        'status_processamento': 'pendente'
                    })
                    stats["inseridos"] += 1
                    
                else:
                    # Registro existe → comparar conteúdo
                    json_existente = registros_existentes[bling_id]
                    
                    if comparar_jsons(json_existente, novo_json):
                        # Conteúdo diferente → UPDATE
                        registros_para_atualizar.append(dados)
                        stats["atualizados"] += 1
                    else:
                        # Conteúdo idêntico → SKIP
                        stats["ignorados"] += 1
            
            fim_comparacao = datetime.now()
            print(f"✅ Comparação concluída em {fim_comparacao - inicio_comparacao}")
            
            # Relatório da classificação
            print(f"\n📊 CLASSIFICAÇÃO DOS REGISTROS:")
            print(f"   • 🆕 Novos (inserir): {stats['inseridos']}")
            print(f"   • 🔄 Diferentes (atualizar): {stats['atualizados']}")
            print(f"   • ⏭️ Idênticos (ignorar): {stats['ignorados']}")
            
            # BULK INSERT dos registros novos (mais rápido)
            if registros_novos:
                print(f"\n💾 Inserindo {len(registros_novos)} registros novos...")
                inicio_insert = datetime.now()
                session.bulk_insert_mappings(self.model_class, registros_novos)
                fim_insert = datetime.now()
                print(f"✅ Inserções concluídas em {fim_insert - inicio_insert}")

            # UPDATE dos registros diferentes (um por um, mas só os necessários)
            if registros_para_atualizar:
                print(f"\n🔄 Atualizando {len(registros_para_atualizar)} registros diferentes...")
                inicio_update = datetime.now()
                
                for i, dados in enumerate(registros_para_atualizar):
                    if (i + 1) % 100 == 0:
                        print(f"Atualizados {i + 1}/{len(registros_para_atualizar)} registros...")
                    
                    stmt = insert(self.model_class).values( # stmt = statement 
                        bling_id=dados['bling_id'],
                        dados_json=dados['dados_json'],
                        data_ingestao=datetime.now(),
                        status_processamento='pendente'
                    )
                    
                    stmt = stmt.on_conflict_do_update( # On conflict do update é para atualizar o registro se já existir
                        index_elements=['bling_id'],
                        set_={
                            'dados_json': stmt.excluded.dados_json, 
                            'data_ingestao': stmt.excluded.data_ingestao,
                            'status_processamento': 'pendente'
                        }
                    )
                    
                    session.execute(stmt)
                
                fim_update = datetime.now()
                print(f"✅ Atualizações concluídas em {fim_update - inicio_update}")

            # Se não há nada para fazer, só reportar
            if not registros_novos and not registros_para_atualizar:
                print(f"\n✨ Nenhum registro novo ou alterado! Banco já está atualizado.")

            session.commit()

            # Relatório final detalhado
            print(f"\n🎉 SALVAMENTO CONCLUÍDO!")
            print(f"📊 Estatísticas detalhadas:")
            print(f"   • 🆕 Registros inseridos: {stats['inseridos']}")
            print(f"   • 🔄 Registros atualizados: {stats['atualizados']}")
            print(f"   • ⏭️ Registros ignorados (idênticos): {stats['ignorados']}")
            print(f"   • 📈 Total processado: {stats['total']}")
            print(f"   • 💾 Operações de escrita: {stats['inseridos'] + stats['atualizados']}")
            print(f"   • ⚡  Economia: {stats['ignorados']} escritas desnecessárias evitadas!")
            
            return stats
            
        except Exception as e:
            session.rollback() # Rollback é para desfazer as operações se ocorrer um erro
            print(f"❌ Erro ao salvar dados: {e}")
            raise
        finally:
            session.close()  # Sempre fechar a sessão do banco de dados