# Responsável por: lógica comum de extração, retry, paginação, comparação JSON

import requests
import time
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert
from config.settings import headers
from config.database import Session

# =======================================================
# 1. FUNÇÃO DE COMPARAÇÃO DE JSON - VERSÃO FINAL
# =======================================================

def comparar_jsons(json1, json2):
    """
    Compara dois JSONs de forma inteligente
    Compara APENAS campos que existem em AMBOS os JSONs
    Isso resolve o problema de endpoints diferentes (resumo vs detalhes)
    
    Retorna True se são diferentes, False se são iguais
    """
    try:
        # Se não são dicts, comparar direto
        if not isinstance(json1, dict) or not isinstance(json2, dict):
            return json1 != json2
        
        # Pegar apenas campos que existem em AMBOS
        campos_comuns = set(json1.keys()) & set(json2.keys())
        
        # Se não tem campos em comum, são diferentes
        if not campos_comuns:
            return True
        
        # Comparar cada campo comum
        for campo in campos_comuns:
            val1 = json1[campo]
            val2 = json2[campo]
            
            # Comparar recursivamente se for dict
            if isinstance(val1, dict) and isinstance(val2, dict):
                if comparar_jsons(val1, val2):
                    return True
            
            # Comparar listas
            elif isinstance(val1, list) and isinstance(val2, list):
                if len(val1) != len(val2):
                    return True
                # Tentar ordenar e comparar
                try:
                    if sorted(val1) != sorted(val2):
                        return True
                except TypeError:
                    # Se não conseguir ordenar, comparar item a item
                    for i, item in enumerate(val1):
                        if i >= len(val2):
                            return True
                        if isinstance(item, dict):
                            if comparar_jsons(item, val2[i]):
                                return True
                        elif item != val2[i]:
                            return True
            
            # Comparar floats com arredondamento
            elif isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                if round(float(val1), 2) != round(float(val2), 2):
                    return True
            
            # Comparar valores simples
            elif val1 != val2:
                return True
        
        # Se chegou aqui, são iguais
        return False
        
    except Exception as e:
        print(f"⚠️  Erro ao comparar JSONs: {e}")
        # Em caso de erro, assume que são diferentes
        return True

# =======================================================
# 2. CLASSE BASE PARA EXTRATORES
# =======================================================

class BaseExtractor:
    """
    Classe base que contém toda a lógica comum de extração
    Outros extractors vão herdar desta classe e só mudar o que é específico
    """
    
    def __init__(self, base_url, model_class):
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

    def extract_dados_bling_paginado(self, limite_por_pagina=100, delay_entre_requests=0.35, max_paginas=1000, max_tentativas=3):
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
        todos_registros = []
        pagina_atual = 1
        total_paginas = None
        registros_unicos = set()
        
        print(f"Iniciando extração paginada...")
        print(f"Configurações: delay={delay_entre_requests}s, max_tentativas={max_tentativas}")

        while pagina_atual <= max_paginas:
            params = {
                "limite": limite_por_pagina,
                "pagina": pagina_atual
            }

            print(f"Processando página {pagina_atual}{'/' + str(total_paginas) if total_paginas else ''}...")
        
            sucesso = False
            for tentativa in range(max_tentativas):
                try:
                    response = requests.get(
                        self.base_url,
                        headers=self.headers,
                        params=params,
                        timeout=30
                    )
                    
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

                    dados = response.json()
                    sucesso = True
                    break

                except (requests.exceptions.ConnectionError, 
                        requests.exceptions.Timeout,
                        requests.exceptions.RequestException) as e:
                    
                    print(f"Erro de conexão na página {pagina_atual} (tentativa {tentativa + 1}/{max_tentativas}): {e}")
                    
                    if tentativa < max_tentativas - 1:
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
            
            if not sucesso:
                print(f"ERRO INTERNO: Lógica de retry falhou")
                raise Exception("Falha interna no sistema de retry")
                
            if pagina_atual == 1:
                print(f"Total informado pela API: {dados.get('total', 'N/A')}")
                print(f"Total de páginas informado: {dados.get('total_pages', 'N/A')}")

            if total_paginas is None:
                total_paginas = dados.get("total_pages", 1)
                total_registros = dados.get("total", 0)
                print(f"Total de páginas: {total_paginas}")
                print(f"Total de registros: {total_registros}")

            registros_pagina = dados.get("data", [])
            
            if not registros_pagina:
                print(f"Página {pagina_atual} vazia. Finalizando extração.")
                break

            registros_novos = 0
            for registro in registros_pagina:
                if registro['id'] not in registros_unicos:
                    registros_unicos.add(registro['id'])
                    todos_registros.append(registro)
                    registros_novos += 1

            print(f"Extraídos {len(registros_pagina)} registro da página {pagina_atual} ({registros_novos} novos)")
            
            if registros_novos == 0:
                print(f"Nenhum registro novo na página {pagina_atual}. Finalizando.")
                break
            
            if pagina_atual >= total_paginas and len(registros_pagina) < limite_por_pagina:
                print(f"Última página oficial ({total_paginas}) processada e com menos que {limite_por_pagina} registros. Finalizando.")
                break

            pagina_atual += 1
            
            if delay_entre_requests > 0:
                time.sleep(delay_entre_requests)
        
        print(f"Extração finalizada com sucesso. Total de registro coletados: {len(todos_registros)}")
        print(f"Páginas processadas: {pagina_atual - 1}")
        return todos_registros   

# =============================================================
# 4. FUNÇÃO PARA SALVAR NO POSTGRES (COMPARAR ANTES DE SALVAR)
# =============================================================

    def salvar_dados_postgres_bulk(self, lista_dados):
        """
        Salva dados usando comparação inteligente OTIMIZADA:
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
            registros_existentes = {}
            existing_records = session.query(
                self.model_class.bling_id,
                self.model_class.dados_json
            ).all()

            for record in existing_records:
                registros_existentes[record.bling_id] = record.dados_json
            
            fim_busca = datetime.now()
            print(f"📋 {len(registros_existentes)} registros existentes carregados em {fim_busca - inicio_busca}")

            # Classificar os dados
            registros_novos = []
            registros_para_atualizar = []
            
            print(f"🔍 Comparando {len(lista_dados)} registros...")
            inicio_comparacao = datetime.now()
            
            for i, dados in enumerate(lista_dados):
                bling_id = dados['bling_id']
                novo_json = dados['dados_json']
                
                if (i + 1) % 1000 == 0:
                    print(f"Processados {i + 1}/{len(lista_dados)} registros...")
                
                if bling_id not in registros_existentes:
                    # Registro novo → INSERT
                    registros_novos.append({
                        'bling_id': bling_id,
                        'empresa_id': dados.get('empresa_id'),  # Adicionado empresa_id
                        'dados_json': novo_json,
                        'data_ingestao': datetime.now(),
                        'status_processamento': 'pendente'
                    })
                    stats["inseridos"] += 1
                    
                else:
                    # Registro existe → comparar conteúdo
                    json_existente = registros_existentes[bling_id]
                    
                    # USAR A FUNÇÃO OTIMIZADA (compara apenas campos comuns)
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
            
            # BULK INSERT
            if registros_novos:
                print(f"\n💾 Inserindo {len(registros_novos)} registros novos...")
                inicio_insert = datetime.now()
                session.bulk_insert_mappings(self.model_class, registros_novos)
                fim_insert = datetime.now()
                print(f"✅ Inserções concluídas em {fim_insert - inicio_insert}")

            # UPDATE
            if registros_para_atualizar:
                print(f"\n🔄 Atualizando {len(registros_para_atualizar)} registros diferentes...")
                inicio_update = datetime.now()
                
                for i, dados in enumerate(registros_para_atualizar):
                    if (i + 1) % 100 == 0:
                        print(f"Atualizados {i + 1}/{len(registros_para_atualizar)} registros...")
                    
                    stmt = insert(self.model_class).values(
                        bling_id=dados['bling_id'],
                        empresa_id=dados.get('empresa_id'),
                        dados_json=dados['dados_json'],
                        data_ingestao=datetime.now(),
                        status_processamento='pendente'
                    )
                    
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['bling_id', 'empresa_id'],  # CORREÇÃO: chave composta!
                        set_={
                            'dados_json': stmt.excluded.dados_json, 
                            'data_ingestao': stmt.excluded.data_ingestao,
                            'status_processamento': 'pendente'
                        }
                    )
                    
                    session.execute(stmt)
                
                fim_update = datetime.now()
                print(f"✅ Atualizações concluídas em {fim_update - inicio_update}")

            if not registros_novos and not registros_para_atualizar:
                print(f"\n✨ Nenhum registro novo ou alterado! Banco já está atualizado.")

            session.commit()

            # Relatório final
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
            session.rollback()
            print(f"❌ Erro ao salvar dados: {e}")
            raise
        finally:
            session.close()