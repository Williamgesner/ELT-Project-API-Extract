# Responsável por: lógica comum de extração, retry, paginação, comparação JSON

import requests
import time
from datetime import datetime
import json
from decimal import Decimal
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text
from config.settings import headers
from config.database import Session

# =======================================================
# 1. FUNÇÃO DE COMPARAÇÃO DE JSON - ✅ VERSÃO CORRIGIDA
# =======================================================

def _normalizar_json_para_comparacao(obj):
    """
    Normaliza estruturas JSON (dict/list) para comparação determinística.

    Objetivo:
    - Ignorar ordem de listas (ex.: categorias, imagens, etc.), evitando falsos positivos.
    - Ignorar ordem das chaves em dicts (já é ignorada por padrão em dict equality, mas
      aqui garantimos também uma representação canônica para ordenação de listas).
    - Tratar números (int/float/Decimal) com arredondamento para evitar 100.0 != 100.00.

    Observação:
    - A normalização assume que a ordem das listas NÃO é semanticamente relevante para
      os payloads do Bling usados aqui (se houver campo onde a ordem importa, devemos
      tratar esse campo explicitamente).
    """
    # Dict: ordenar chaves e normalizar recursivamente valores
    if isinstance(obj, dict):
        return {k: _normalizar_json_para_comparacao(obj[k]) for k in sorted(obj.keys())}

    # Lista: normalizar itens e ordenar por uma assinatura canônica (preserva duplicatas)
    if isinstance(obj, list):
        normalizados = [_normalizar_json_para_comparacao(x) for x in obj]

        def assinatura(item):
            # item já está normalizado (somente tipos serializáveis)
            try:
                return json.dumps(
                    item,
                    sort_keys=True,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            except TypeError:
                # Fallback conservador: string do objeto
                return str(item)

        return sorted(normalizados, key=assinatura)

    # Números: normalizar para float com 2 casas
    if isinstance(obj, (int, float, Decimal)):
        return round(float(obj), 2)

    # Outros tipos simples (str/bool/None) ou valores inesperados
    return obj


def comparar_jsons(json1, json2):
    """
    Compara dois JSONs de forma inteligente
    ✅ CORREÇÃO: Ignora ordem de listas (incluindo listas de dicts ANINHADAS).
    
    Retorna True se são diferentes, False se são iguais
    """
    try:
        # Se não são dicts, comparar já normalizado
        if not isinstance(json1, dict) or not isinstance(json2, dict):
            return _normalizar_json_para_comparacao(json1) != _normalizar_json_para_comparacao(json2)

        # Pegar apenas campos que existem em AMBOS (mantém o comportamento atual do projeto)
        campos_comuns = set(json1.keys()) & set(json2.keys())

        # Se não tem campos em comum, são diferentes
        if not campos_comuns:
            return True

        # Comparar cada campo comum, mas usando normalização profunda
        for campo in campos_comuns:
            val1 = json1.get(campo)
            val2 = json2.get(campo)

            if _normalizar_json_para_comparacao(val1) != _normalizar_json_para_comparacao(val2):
                return True

        return False
        
    except Exception as e:
        print(f"⚠️  Erro ao comparar JSONs: {e}")
        # Em caso de erro, assume que são diferentes (conservador)
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

    def extract_dados_bling_paginado(
            self, 
            limite_por_pagina=100, 
            delay_entre_requests=0.35,
            max_paginas=1000, 
            max_tentativas=3,
            filtros_adicionais=None # Inserido para conseguir fazer filtros de data de extração
        ):
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

            if filtros_adicionais: # True (Se existe filtro)
                params.update(filtros_adicionais) # Adiciona ao dicionário. Agora fica "limite": 100,"pagina": 1,"dataInicial": "2024-01-01". Se não existe, não faz nada

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
# SEMPRE atualiza data_ingestao - VERSÃO DEFINITIVA CORRIGIDA
# =============================================================

    def salvar_dados_postgres_bulk(self, lista_dados, limpar_orfaos=True):
        """
        Salva dados usando comparação inteligente OTIMIZADA:
        - Novos registros: INSERT
        - Registros existentes idênticos: UPDATE data_ingestao
        - Registros existentes diferentes: UPDATE completo
        
        ✅ CORREÇÃO APLICADA: Suporta campo de data customizado por endpoint
        Configure self.campo_data_filtro no extrator para definir qual campo usar
        """
        if not lista_dados:
            print("Nenhum dado para salvar.")
            return {"inseridos": 0, "atualizados": 0, "ignorados": 0, "total": 0}
        
        session = Session()
        stats = {"inseridos": 0, "atualizados": 0, "ignorados": 0, "total": len(lista_dados)}

        try:
            # ⚠️ CORREÇÃO: Obter nome da tabela DINAMICAMENTE
            schema = self.model_class.__table__.schema
            table_name = self.model_class.__tablename__
            full_table_name = f"{schema}.{table_name}"
            
            print(f"🔍 Buscando registros existentes para comparação...")
            print(f"   📋 Tabela: {full_table_name}")
            inicio_busca = datetime.now()

            # Obter empresa_id e data_inicial_extracao
            empresa_id = lista_dados[0].get('empresa_id') if lista_dados else None
            data_inicial_extracao = getattr(self, 'data_inicial_extracao', None)
            
            registros_existentes = {}
            if empresa_id is not None:
                # 🆕 Se tem filtro de data e limpeza ativa, buscar apenas do período
                if data_inicial_extracao is not None and limpar_orfaos:
                    # ✅ CORREÇÃO CRÍTICA: Usar campo de data customizado por endpoint
                    campo_data = getattr(self, 'campo_data_filtro', 'dataInclusao')
                    print(f"   📅 Filtrando registros >= {data_inicial_extracao.strftime('%Y-%m-%d')} (período de extração)")
                    
                    # ✅ NOVO: Se campo_data é None, busca TODOS (sem filtro de data)
                    # Usado por extractors que não têm campo de data confiável no JSONB (ex: produtos)
                    if campo_data is None:
                        print(f"   ⚠️  Campo de data: None (buscando TODOS os registros da empresa)")
                        existing_records = session.query(
                            self.model_class.bling_id,
                            self.model_class.dados_json
                        ).filter(
                            self.model_class.empresa_id == empresa_id
                        ).all()
                    else:
                        print(f"   🏷️  Campo de data usado: '{campo_data}'")
                        query = text(f"""
                            SELECT bling_id, dados_json
                            FROM {full_table_name}
                            WHERE empresa_id = :empresa_id
                            AND (dados_json->>'{campo_data}')::timestamp >= :data_inicial
                        """)
                        result = session.execute(query, {
                            "empresa_id": empresa_id,
                            "data_inicial": data_inicial_extracao
                        })
                        existing_records = [(r.bling_id, r.dados_json) for r in result]
                else:
                    # Buscar APENAS desta empresa (comportamento original)
                    existing_records = session.query(
                        self.model_class.bling_id,
                        self.model_class.dados_json
                    ).filter(
                        self.model_class.empresa_id == empresa_id
                    ).all()
            else:
                # Fallback: buscar todos (comportamento original)
                existing_records = session.query(
                    self.model_class.bling_id,
                    self.model_class.dados_json
                ).all()

            for record in existing_records:
                registros_existentes[record.bling_id if hasattr(record, 'bling_id') else record[0]] = record.dados_json if hasattr(record, 'dados_json') else record[1]
            
            fim_busca = datetime.now()
            if data_inicial_extracao is not None and limpar_orfaos:
                print(f"📋 {len(registros_existentes)} registros existentes (período filtrado) carregados em {fim_busca - inicio_busca}")
            else:
                print(f"📋 {len(registros_existentes)} registros existentes carregados em {fim_busca - inicio_busca}")

            # Classificar os dados
            registros_novos = []
            registros_para_atualizar = []
            registros_para_tocar_data = []  # ⚠️ NOVO
            
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
                        'empresa_id': dados.get('empresa_id'),
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
                        # Conteúdo diferente → UPDATE completo
                        registros_para_atualizar.append(dados)
                        stats["atualizados"] += 1
                    else:
                        # ⚠️ CORREÇÃO: Conteúdo idêntico → UPDATE apenas data_ingestao
                        registros_para_tocar_data.append({
                            'bling_id': bling_id,
                            'empresa_id': dados.get('empresa_id')
                        })
                        stats["ignorados"] += 1
            
            fim_comparacao = datetime.now()
            print(f"✅ Comparação concluída em {fim_comparacao - inicio_comparacao}")
            
            # Relatório da classificação
            print(f"\n📊 CLASSIFICAÇÃO DOS REGISTROS:")
            print(f"   • 🆕 Novos (inserir): {stats['inseridos']}")
            print(f"   • 🔄 Diferentes (atualizar): {stats['atualizados']}")
            print(f"   • ⏭️ Idênticos (tocar data): {stats['ignorados']}")
            
            # BULK INSERT
            if registros_novos:
                print(f"\n💾 Inserindo {len(registros_novos)} registros novos...")
                inicio_insert = datetime.now()
                session.bulk_insert_mappings(self.model_class, registros_novos)
                fim_insert = datetime.now()
                print(f"✅ Inserções concluídas em {fim_insert - inicio_insert}")

            # UPDATE COMPLETO
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

                    # Regra especial para vendas:
                    # - Não substituir o JSON completo pelo payload "lista"
                    # - Fazer merge (preserva campos de detalhes já enriquecidos)
                    if table_name == 'vendas_raw':
                        dados_json_update = self.model_class.dados_json.op('||')(stmt.excluded.dados_json)
                    else:
                        dados_json_update = stmt.excluded.dados_json
                    
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['bling_id', 'empresa_id'],
                        set_={
                            'dados_json': dados_json_update,
                            'data_ingestao': stmt.excluded.data_ingestao,
                            'status_processamento': 'pendente'
                        }
                    )
                    
                    session.execute(stmt)
                
                fim_update = datetime.now()
                print(f"✅ Atualizações concluídas em {fim_update - inicio_update}")

            # ✅ CORREÇÃO APLICADA: UPDATE apenas data_ingestao
            if registros_para_tocar_data:
                print(f"\n🕐 Atualizando data_ingestao em {len(registros_para_tocar_data)} registros idênticos...")
                print(f"   📋 Tabela alvo: {full_table_name}")
                inicio_touch = datetime.now()
                
                # Agrupar por empresa_id
                por_empresa = {}
                for r in registros_para_tocar_data:
                    emp_id = r['empresa_id']
                    if emp_id not in por_empresa:
                        por_empresa[emp_id] = []
                    por_empresa[emp_id].append(r['bling_id'])
                
                # Update por empresa em lote
                for empresa_id, bling_ids in por_empresa.items():
                    # Tabelas que NÃO têm status_processamento
                    tabelas_sem_status = [
                        'formas_pagamentos_raw',
                        'categorias_contas_pagar_raw',
                        'natureza_operacao_raw'
                    ]
                    
                    if table_name in tabelas_sem_status:
                        # ✅ CORRETO: SÓ atualizar data_ingestao
                        query_update = f"""
                            UPDATE {full_table_name}
                            SET data_ingestao = CURRENT_TIMESTAMP
                            WHERE bling_id = ANY(:bling_ids)
                            AND empresa_id = :empresa_id
                        """
                    else:
                        # ✅ CORREÇÃO: SÓ atualizar data_ingestao (SEM status_processamento!)
                        query_update = f"""
                            UPDATE {full_table_name}
                            SET data_ingestao = CURRENT_TIMESTAMP
                            WHERE bling_id = ANY(:bling_ids)
                            AND empresa_id = :empresa_id
                        """
                    
                    session.execute(text(query_update), {
                        "bling_ids": bling_ids,
                        "empresa_id": empresa_id
                    })
                
                fim_touch = datetime.now()
                print(f"✅ Datas atualizadas em {fim_touch - inicio_touch}")
                print(f"   ✅ CORREÇÃO APLICADA: Registros idênticos NÃO são marcados como 'pendente'!")

            if not registros_novos and not registros_para_atualizar and not registros_para_tocar_data:
                print(f"\n✨ Nenhum registro novo ou alterado! Banco já está atualizado.")

            session.commit()

            # Relatório final
            print(f"\n🎉 SALVAMENTO CONCLUÍDO!")
            print(f"📊 Estatísticas detalhadas:")
            print(f"   • 🆕 Registros inseridos: {stats['inseridos']}")
            print(f"   • 🔄 Registros atualizados: {stats['atualizados']}")
            print(f"   • ⏭️ Registros idênticos (data atualizada): {stats['ignorados']}")
            print(f"   • 📈 Total processado: {stats['total']}")
            print(f"   • 💾 Operações de escrita: {stats['inseridos'] + stats['atualizados'] + len(registros_para_tocar_data)}")
            if len(registros_para_tocar_data) > 0:
                print(f"   • ✅ CORREÇÃO ATIVA: data_ingestao sempre atualizada na tabela {full_table_name}!")
            
            # ⚠️ LIMPEZA DE ÓRFÃOS (APENAS EM MODO FULL)
            if limpar_orfaos:
                # Remove registros que não vieram da API (foram deletados no Bling)
                print(f"\n🧹 LIMPANDO REGISTROS ÓRFÃOS DA RAW...")
                if data_inicial_extracao is not None:
                    print(f"   📅 Período de limpeza: {data_inicial_extracao.strftime('%Y-%m-%d')} → hoje")
                    print(f"   📋 Registros na RAW (período filtrado): {len(registros_existentes)}")
                else:
                    print(f"   📋 Registros na RAW: {len(registros_existentes)}")
                print(f"   📥 Registros da API: {len(lista_dados)}")
                
                # IDs que vieram da API
                ids_da_api = {dados['bling_id'] for dados in lista_dados}
                
                # IDs que estão na RAW mas NÃO vieram da API = órfãos (deletados no Bling)
                ids_orfaos = set(registros_existentes.keys()) - ids_da_api
                
                if len(ids_orfaos) > 0:
                    print(f"   ⚠️  {len(ids_orfaos)} registro(s) órfão(s) detectado(s)")
                    if data_inicial_extracao is not None:
                        print(f"   🗑️  Estes registros foram DELETADOS no Bling (dentro do período {data_inicial_extracao.strftime('%Y-%m-%d')}+)")
                    else:
                        print(f"   🗑️  Estes registros foram DELETADOS no Bling")
                    
                    # Agrupar por empresa_id para deletar
                    # Primeiro, buscar empresa_id dos órfãos
                    query_orfaos = f"""
                        SELECT bling_id, empresa_id
                        FROM {full_table_name}
                        WHERE bling_id = ANY(:bling_ids)
                    """
                    orfaos_result = session.execute(text(query_orfaos), {
                        "bling_ids": list(ids_orfaos)
                    }).fetchall()
                    
                    # Agrupar por empresa
                    orfaos_por_empresa = {}
                    for bling_id, empresa_id in orfaos_result:
                        if empresa_id not in orfaos_por_empresa:
                            orfaos_por_empresa[empresa_id] = []
                        orfaos_por_empresa[empresa_id].append(bling_id)
                    
                    # Deletar por empresa
                    total_deletados = 0
                    for empresa_id, ids in orfaos_por_empresa.items():
                        query_delete = f"""
                            DELETE FROM {full_table_name}
                            WHERE bling_id = ANY(:bling_ids)
                            AND empresa_id = :empresa_id
                        """
                        resultado = session.execute(text(query_delete), {
                            "bling_ids": ids,
                            "empresa_id": empresa_id
                        })
                        total_deletados += resultado.rowcount
                        print(f"   🗑️  Empresa {empresa_id}: {resultado.rowcount} órfãos removidos")
                    
                    session.commit()
                    print(f"   ✅ Total removido: {total_deletados} registros órfãos")
                    
                    # Mostrar alguns IDs removidos (auditoria)
                    if len(ids_orfaos) <= 10:
                        print(f"   📋 IDs removidos: {sorted(list(ids_orfaos))}")
                    else:
                        print(f"   📋 Primeiros 10 IDs: {sorted(list(ids_orfaos))[:10]}")
                else:
                    print(f"   ✅ Nenhum registro órfão detectado")
            else:
                print(f"\n🛡️  LIMPEZA DE ÓRFÃOS DESABILITADA (Modo Incremental)")
                print(f"   📋 Registros na RAW: {len(registros_existentes)}")
                print(f"   📥 Registros da API: {len(lista_dados)}")
                print(f"   ✅ Segurança: Dados históricos preservados")
            
            return stats
            
        except Exception as e:
            session.rollback()
            print(f"❌ Erro ao salvar dados: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            session.close()