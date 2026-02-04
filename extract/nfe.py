# Responsável por: extrair NFe (entrada E saída) da API Bling
# VERSÃO OTIMIZADA: Suporta modo FULL e INCREMENTAL
# Comparação inteligente: evita falsos positivos entre JSON resumido vs completo

from datetime import datetime, timedelta
import requests
import time
from core.base_extractor import BaseExtractor
from models.nfe_raw import NFeRaw
from config.settings import endpoints
from config.extraction_mode import ExtractionMode
from config.database import Session
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text

# =====================================================
# 1. EXTRATOR DE NFe COM COMPARAÇÃO INTELIGENTE
# =====================================================

class NFeExtractor(BaseExtractor):
    
    """
    Extrator de NFe com suporte a tipos e comparação inteligente
    
    MODOS DE EXTRAÇÃO:
    - FULL: Extrai desde 2024-01-01 + Limpeza de órfãos ATIVA
    - INCREMENTAL: Extrai últimos 120 dias + Limpeza DESABILITADA
    
    ESTRATÉGIA DE COMPARAÇÃO:
    - Compara apenas campos-chave (id, numero, tipo, situacao, dataEmissao)
    - Ignora diferenças entre JSON resumido (lista) vs completo (detalhes)
    - Detecta enriquecimento (novos campos como valorNota)
    
    OBSERVAÇÃO: NFe NÃO possui filtro de dataAlteração na API Bling,
    por isso usamos janela de 120 dias no incremental para cobertura.
    """
    
    def __init__(self, api_key, empresa_id):
        """
        Inicializa o extrator de NFe
        
        Args:
            api_key: Token de autenticação da API Bling
            empresa_id: ID da empresa na tabela dim_empresas
        """
        super().__init__(endpoints['nfe'], NFeRaw)
        self.empresa_id = empresa_id
        
        # Sobrescrever headers do base_extractor com a API key específica
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def extract_dados_bling_paginado_com_tipo(
        self,
        tipo,
        limite_por_pagina=100,
        delay_entre_requests=0.35,
        max_paginas=1000,
        max_tentativas=3,
        data_emissao_inicial=None,
        data_emissao_final=None,
    ):
        """
        Extrai NFe de um tipo específico da API Bling
        
        Args:
            tipo: 0 para ENTRADA, 1 para SAÍDA
            data_emissao_inicial: Filtro de data inicial (formato: "YYYY-MM-DD HH:MM:SS")
            data_emissao_final: Filtro de data final (formato: "YYYY-MM-DD HH:MM:SS")
        """
        todos_registros = []
        pagina_atual = 1
        total_paginas = None
        registros_unicos = set()
        
        tipo_nome = "ENTRADA" if tipo == 0 else "SAÍDA"
        print(f"Extraindo NFe de {tipo_nome} (tipo={tipo})...")

        while pagina_atual <= max_paginas:
            params = {
                "limite": limite_por_pagina,
                "pagina": pagina_atual,
                "tipo": tipo
            }

            # Filtro conforme OpenAPI do Bling (GET /nfe):
            # - dataEmissaoInicial / dataEmissaoFinal
            if data_emissao_inicial:
                params["dataEmissaoInicial"] = data_emissao_inicial
            if data_emissao_final:
                params["dataEmissaoFinal"] = data_emissao_final

            print(f"   Processando página {pagina_atual}{'/' + str(total_paginas) if total_paginas else ''}...")
            
            sucesso = False
            for tentativa in range(max_tentativas):
                try:
                    response = requests.get(
                        self.base_url,
                        headers=self.headers,
                        params=params,
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        registros = data.get('data', [])
                        
                        if pagina_atual == 1:
                            if 'totalPages' in data:
                                total_paginas = data['totalPages']
                            elif registros and isinstance(registros, list) and len(registros) > 0:
                                if isinstance(registros[0], dict) and 'totalPages' in registros[0]:
                                    total_paginas = registros[0]['totalPages']
                            
                            if total_paginas:
                                print(f"   Total de páginas: {total_paginas}")
                        
                        novos = 0
                        for registro in registros:
                            registro_id = registro.get('id')
                            if registro_id and registro_id not in registros_unicos:
                                registros_unicos.add(registro_id)
                                todos_registros.append(registro)
                                novos += 1
                        
                        print(f"   ✅ Página {pagina_atual}: {len(registros)} registros ({novos} novos)")
                        sucesso = True
                        break
                    
                    elif response.status_code == 401:
                        print(f"   ❌ Erro 401: Token expirado ou inválido")
                        raise Exception("API Key expirada - atualize e reinicie")
                    
                    elif response.status_code == 429:
                        print(f"   ⚠️ Rate limit atingido - aguardando...")
                        time.sleep(2)
                        continue
                    
                    else:
                        print(f"   ❌ Erro HTTP {response.status_code}")
                        if tentativa < max_tentativas - 1:
                            time.sleep(0.5 * (tentativa + 1))
                            continue
                        
                except requests.exceptions.Timeout:
                    print(f"   ⏱️ Timeout na requisição")
                    if tentativa < max_tentativas - 1:
                        time.sleep(1)
                        continue
                    
                except requests.exceptions.RequestException as e:
                    print(f"   ❌ Erro de conexão: {e}")
                    if tentativa < max_tentativas - 1:
                        time.sleep(0.5 * (tentativa + 1))
                    else:
                        print(f"   ⚠️ FALHA TOTAL na página {pagina_atual}")
                        return todos_registros
                
                except Exception as e:
                    if "API Key expirada" in str(e):
                        raise
                    print(f"   ❌ Erro inesperado: {e}")
                    return todos_registros
            
            if not sucesso:
                print(f"   ⚠️ Parando extração de {tipo_nome} - falha na página {pagina_atual}")
                break
            
            if not registros or (total_paginas and pagina_atual >= total_paginas):
                break
            
            pagina_atual += 1
            time.sleep(delay_entre_requests)
        
        print(f"   🎉 {tipo_nome}: {len(todos_registros)} notas extraídas")
        return todos_registros
    
    def executar_extracao_completa(self, extraction_mode=ExtractionMode.FULL, debug=False):
        """
        Executa o processo completo de extração de NFe
        
        Args:
            extraction_mode: ExtractionMode.FULL ou ExtractionMode.INCREMENTAL
                - FULL: dataEmissao desde 2024-01-01 + Remove órfãos
                - INCREMENTAL: dataEmissao últimos 120 dias (sem remoção)
            debug: Se True, mostra detalhes das comparações
        """
        try:
            inicio_extracao = datetime.now()
            
            # =====================================================
            # DEFINIR JANELA DE EXTRAÇÃO BASEADA NO MODO
            # =====================================================
            if extraction_mode == ExtractionMode.INCREMENTAL:
                # MODO INCREMENTAL: Últimos 120 dias
                print(f"\n⚡ MODO INCREMENTAL: NFe")
                print(f"📅 Período: Últimos 120 dias")
                print(f"🔍 Filtro API: dataEmissao")
                print(f"🛡️ Limpeza de órfãos: DESABILITADA")
                
                data_emissao_inicial = datetime.now() - timedelta(days=120)
                limpar_orfaos = False
                
            else:
                # MODO FULL: Desde 2024 (sincronização completa)
                print(f"\n📊 MODO FULL: NFe")
                print(f"📅 Período: Desde 2024-01-01")
                print(f"🔍 Filtro API: dataEmissao")
                print(f"🧹 Limpeza de órfãos: ATIVA")
                
                data_emissao_inicial = datetime(2024, 1, 1, 0, 0, 0)
                limpar_orfaos = True
            
            # ⚠️ CRÍTICO: Informar período de extração ao base_extractor
            # Usado para limpeza de órfãos APENAS dentro do escopo de data
            self.data_inicial_extracao = data_emissao_inicial if limpar_orfaos else None

            print(f"\n📄 EXTRAÇÃO: NOTAS FISCAIS ELETRÔNICAS (NFe) (Empresa ID: {self.empresa_id})")
            print("=" * 60)
            print("⚡ Comparação inteligente: apenas campos-chave")

            # =====================================================
            # EXTRAÇÃO COM JANELAS DE DATAS (SEGURANÇA API)
            # =====================================================
            # API Bling usa: dataEmissaoInicial / dataEmissaoFinal
            # Formato: "YYYY-MM-DD HH:MM:SS"
            # Janelas de 360 dias evitam erro de intervalo (máx 365 dias)
            
            data_emissao_final = datetime.now()
            janela_dias = 360  # Margem de segurança (< 365)

            # ===== EXTRAIR NOTAS DE SAÍDA (tipo=1) =====
            print("\n📤 EXTRAINDO NOTAS DE SAÍDA...")
            print("-" * 60)
            
            nfe_saida = []
            ids_saida = set()
            inicio_janela = data_emissao_inicial

            while inicio_janela <= data_emissao_final:
                fim_janela = min(
                    inicio_janela + timedelta(days=janela_dias) - timedelta(seconds=1),
                    data_emissao_final,
                )

                filtro_ini = inicio_janela.strftime("%Y-%m-%d %H:%M:%S")
                filtro_fim = fim_janela.strftime("%Y-%m-%d %H:%M:%S")

                print(f"\n📅 Janela SAÍDA: {filtro_ini} → {filtro_fim}")

                nfe_janela = self.extract_dados_bling_paginado_com_tipo(
                    tipo=1,
                    limite_por_pagina=100,
                    delay_entre_requests=0.35,
                    max_paginas=1000,
                    max_tentativas=3,
                    data_emissao_inicial=filtro_ini,
                    data_emissao_final=filtro_fim,
                )

                # Deduplica entre janelas
                for nfe in nfe_janela:
                    nfe_id = nfe.get("id")
                    if nfe_id is None or nfe_id in ids_saida:
                        continue
                    ids_saida.add(nfe_id)
                    nfe_saida.append(nfe)

                inicio_janela = fim_janela + timedelta(seconds=1)

            # ===== EXTRAIR NOTAS DE ENTRADA (tipo=0) =====
            print("\n📥 EXTRAINDO NOTAS DE ENTRADA...")
            print("-" * 60)
            
            nfe_entrada = []
            ids_entrada = set()
            inicio_janela = data_emissao_inicial

            while inicio_janela <= data_emissao_final:
                fim_janela = min(
                    inicio_janela + timedelta(days=janela_dias) - timedelta(seconds=1),
                    data_emissao_final,
                )

                filtro_ini = inicio_janela.strftime("%Y-%m-%d %H:%M:%S")
                filtro_fim = fim_janela.strftime("%Y-%m-%d %H:%M:%S")

                print(f"\n📅 Janela ENTRADA: {filtro_ini} → {filtro_fim}")

                nfe_janela = self.extract_dados_bling_paginado_com_tipo(
                    tipo=0,
                    limite_por_pagina=100,
                    delay_entre_requests=0.35,
                    max_paginas=1000,
                    max_tentativas=3,
                    data_emissao_inicial=filtro_ini,
                    data_emissao_final=filtro_fim,
                )

                # Deduplica entre janelas
                for nfe in nfe_janela:
                    nfe_id = nfe.get("id")
                    if nfe_id is None or nfe_id in ids_entrada:
                        continue
                    ids_entrada.add(nfe_id)
                    nfe_entrada.append(nfe)

                inicio_janela = fim_janela + timedelta(seconds=1)

            # ===== COMBINAR ENTRADA + SAÍDA =====
            todas_nfe = nfe_saida + nfe_entrada
            
            fim_extracao = datetime.now()
            tempo_extracao = fim_extracao - inicio_extracao

            # =====================================================
            # VALIDAÇÃO DOS DADOS EXTRAÍDOS
            # =====================================================
            if not todas_nfe:
                if extraction_mode == ExtractionMode.INCREMENTAL:
                    print("\n✨ Nenhuma NFe nos últimos 120 dias.")
                    print("   Isso pode ser normal no modo incremental.")
                else:
                    print("\n⚠️ Nenhuma NFe foi extraída.")
                    print("   Possíveis causas:")
                    print("   • Não há NFe no período especificado")
                    print("   • Problema de conectividade com a API")
                return
            
            print("\n" + "=" * 60)
            print("📊 EXTRAÇÃO CONCLUÍDA")
            print("=" * 60)
            print(f"⏱️  Tempo de extração: {tempo_extracao}")
            print(f"📤 NFe SAÍDA: {len(nfe_saida)}")
            print(f"📥 NFe ENTRADA: {len(nfe_entrada)}")
            print(f"📈 TOTAL: {len(todas_nfe)}")
            print(f"🚀 Velocidade: {len(todas_nfe)/tempo_extracao.total_seconds():.1f} notas/segundo")

            # =====================================================
            # PREPARAR DADOS PARA SALVAMENTO
            # =====================================================
            print("\n📝 Preparando dados para salvamento...")
            dados_para_salvar = []
            
            for nfe in todas_nfe:
                dados_formatados = {
                    'bling_id': nfe['id'],
                    'empresa_id': self.empresa_id,
                    'dados_json': nfe
                }
                dados_para_salvar.append(dados_formatados)

            # =====================================================
            # SALVAMENTO COM COMPARAÇÃO INTELIGENTE
            # =====================================================
            # NFe usa método customizado para evitar falsos positivos
            # entre JSON resumido (lista) vs completo (detalhes)
            
            print(f"\n💾 Iniciando salvamento inteligente...")
            print(f"🔄 Modo: {'FULL (com limpeza de órfãos)' if limpar_orfaos else 'INCREMENTAL (sem limpeza)'}")
            
            inicio_salvamento = datetime.now()
            
            stats = self.salvar_dados_postgres_bulk_nfe(dados_para_salvar, debug=debug)
            
            fim_salvamento = datetime.now()
            
            # =====================================================
            # LIMPEZA DE ÓRFÃOS (APENAS MODO FULL)
            # =====================================================
            if limpar_orfaos:
                print(f"\n🧹 LIMPEZA DE ÓRFÃOS (MODO FULL)...")
                self._limpar_orfaos_nfe(
                    ids_da_api=set(nfe['id'] for nfe in todas_nfe),
                    data_inicial=data_emissao_inicial
                )
            
            fim_total = datetime.now()
            tempo_salvamento = fim_salvamento - inicio_salvamento
            tempo_total = fim_total - inicio_extracao

            # =====================================================
            # RELATÓRIO FINAL DE PERFORMANCE
            # =====================================================
            print(f"\n🏁 EXECUÇÃO COMPLETA!")
            print(f"⏱️  Tempo total: {tempo_total}")
            print(f"⏱️  Tempo de extração: {tempo_extracao}")
            print(f"⏱️  Tempo de salvamento: {tempo_salvamento}")
            print(f"🚀 Performance: {len(todas_nfe)/tempo_total.total_seconds():.1f} notas/segundo")
            
            if stats['total'] > 0:
                eficiencia = (stats['ignorados'] / stats['total']) * 100
                print(f"⚡ Eficiência: {eficiencia:.1f}% idênticos")
                print(f"   (Evitou {stats['ignorados']} reprocessamentos)")

            self._exibir_estatisticas_nfe(todas_nfe)

            print("\n✅ NFe extraídas e salvas com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️  Execução interrompida pelo usuário")
            print("💾 Dados processados até este ponto foram preservados")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def _limpar_orfaos_nfe(self, ids_da_api, data_inicial):
        """
        Remove NFe órfãs (existem no banco mas não vieram da API)
        CRÍTICO: Aplica APENAS no escopo de data definido (ex: 2024+)
        
        Args:
            ids_da_api: Set com IDs que vieram da API
            data_inicial: Data inicial do período de extração (para filtro)
        """
        session = Session()
        
        try:
            print(f"\n🧹 LIMPANDO REGISTROS ÓRFÃOS...")
            print(f"   📅 Período de limpeza: {data_inicial.strftime('%Y-%m-%d')} → hoje")
            
            # Buscar IDs que existem no banco DENTRO DO ESCOPO DE DATA
            # NFe usa 'dataEmissao' como campo de data
            query = text("""
                SELECT bling_id
                FROM raw.nfe_raw
                WHERE empresa_id = :empresa_id
                AND (dados_json->>'dataEmissao')::timestamp >= :data_inicial
            """)
            
            resultado = session.execute(query, {
                "empresa_id": self.empresa_id,
                "data_inicial": data_inicial
            })
            ids_no_banco = set(row.bling_id for row in resultado)
            
            print(f"   📋 Registros na RAW (período filtrado): {len(ids_no_banco)}")
            print(f"   📥 Registros da API: {len(ids_da_api)}")
            
            # IDs que estão no banco mas NÃO vieram da API = órfãos
            ids_orfaos = ids_no_banco - ids_da_api
            
            if len(ids_orfaos) > 0:
                print(f"   ⚠️  {len(ids_orfaos)} NFe órfã(s) detectada(s)")
                print(f"   🗑️  Estas NFe foram CANCELADAS/DELETADAS no Bling")
                
                # Deletar órfãos
                query_delete = text("""
                    DELETE FROM raw.nfe_raw
                    WHERE bling_id = ANY(:bling_ids)
                    AND empresa_id = :empresa_id
                """)
                
                resultado_delete = session.execute(query_delete, {
                    "bling_ids": list(ids_orfaos),
                    "empresa_id": self.empresa_id
                })
                
                session.commit()
                print(f"   ✅ Total removido: {resultado_delete.rowcount} registros órfãos")
                
                # Mostrar alguns IDs removidos (auditoria)
                if len(ids_orfaos) <= 10:
                    print(f"   📋 IDs removidos: {sorted(list(ids_orfaos))}")
                else:
                    print(f"   📋 Primeiros 10 IDs: {sorted(list(ids_orfaos))[:10]}")
            else:
                print(f"   ✅ Nenhum registro órfão detectado")
                
        except Exception as e:
            session.rollback()
            print(f"❌ Erro ao limpar órfãos: {e}")
            raise
        finally:
            session.close()
    
    def _exibir_estatisticas_nfe(self, notas):
        """
        Exibe estatísticas das NFe extraídas
        """
        print(f"\n📊 ESTATÍSTICAS DAS NFe:")
        print("-" * 60)
        
        tipos = {}
        for nfe in notas:
            tipo = nfe.get('tipo')
            if tipo == 0:
                tipo_nome = "Entrada"
            elif tipo == 1:
                tipo_nome = "Saída"
            else:
                tipo_nome = f"Tipo {tipo}"
            tipos[tipo_nome] = tipos.get(tipo_nome, 0) + 1
        
        print(f"   • Total de NFe: {len(notas)}")
        print(f"\n   • Distribuição por tipo:")
        for tipo, qtd in sorted(tipos.items()):
            percentual = (qtd / len(notas)) * 100
            print(f"      - {tipo}: {qtd} ({percentual:.1f}%)")
        
        situacoes = {}
        for nfe in notas:
            situacao = nfe.get('situacao', 'Desconhecida')
            situacoes[situacao] = situacoes.get(situacao, 0) + 1
        
        if situacoes:
            print(f"\n   • Distribuição por situação:")
            for situacao, qtd in sorted(situacoes.items()):
                print(f"      - Situação {situacao}: {qtd}")
        
        print(f"\n💡 RELACIONAMENTO:")
        print(f"   • NFe → Pedidos via vendas_raw.notaFiscal.id")
    
    # =====================================================
    # MÉTODO ESPECIALIZADO PARA NFe
    # =====================================================
    def salvar_dados_postgres_bulk_nfe(self, lista_dados, debug=False):
        """
        COMPARAÇÃO ESPECIALIZADA para NFe
        Resolve problema de JSON resumido vs completo
        
        Compara apenas campos-chave:
        - numero, tipo, situacao, dataEmissao (essenciais)
        - valorNota, valorFrete (enriquecimento)
        
        Ignora diferenças estruturais entre lista e detalhes
        
        Args:
            lista_dados: Lista de NFe para salvar
            debug: Se True, mostra detalhes das comparações
            
        Returns:
            dict: Estatísticas de salvamento
        """
        if not lista_dados:
            print("Nenhum dado para salvar.")
            return {"inseridos": 0, "atualizados": 0, "ignorados": 0, "total": 0}
        
        session = Session()
        stats = {"inseridos": 0, "atualizados": 0, "ignorados": 0, "total": len(lista_dados)}

        try:
            print(f"🔍 Buscando registros existentes...")
            inicio = datetime.now()

            registros_existentes = {}
            existing_records = session.query(
                self.model_class.bling_id,
                self.model_class.dados_json
            ).filter(
                self.model_class.empresa_id == self.empresa_id
            ).all()

            for record in existing_records:
                registros_existentes[record.bling_id] = record.dados_json
            
            print(f"📋 {len(registros_existentes)} registros carregados em {datetime.now() - inicio}")

            registros_novos = []
            registros_para_atualizar = []
            registros_para_tocar_data = []
            
            print(f"🔍 Comparando {len(lista_dados)} registros...")
            
            for i, dados in enumerate(lista_dados):
                bling_id = dados['bling_id']
                novo_json = dados['dados_json']
                
                if (i + 1) % 1000 == 0:
                    print(f"   Processados {i + 1}/{len(lista_dados)}...")
                
                if bling_id not in registros_existentes:
                    # NOVO - INSERT
                    registros_novos.append({
                        'bling_id': bling_id,
                        'empresa_id': self.empresa_id,
                        'dados_json': novo_json,
                        'data_ingestao': datetime.now(),
                        'status_processamento': 'pendente'
                    })
                    stats['inseridos'] += 1
                    
                else:
                    # EXISTE - comparar apenas campos-chave
                    json_existente = registros_existentes[bling_id]
                    
                    # Campos-chave para comparação
                    campos_chave = ['numero', 'tipo', 'situacao', 'dataEmissao']
                    
                    mudou = False
                    for campo in campos_chave:
                        if campo in novo_json and campo in json_existente:
                            if str(novo_json[campo]) != str(json_existente[campo]):
                                mudou = True
                                if debug:
                                    print(f"   Campo '{campo}' mudou: {json_existente[campo]} → {novo_json[campo]}")
                                break
                    
                    # Verificar enriquecimento (novos campos importantes)
                    if not mudou:
                        campos_enriquecimento = ['valorNota', 'valorFrete', 'contato']
                        for campo in campos_enriquecimento:
                            if campo in novo_json and campo not in json_existente:
                                mudou = True
                                if debug:
                                    print(f"   Novo campo detectado: '{campo}'")
                                break
                    
                    if mudou:
                        # Conteúdo diferente → UPDATE completo
                        registros_para_atualizar.append({
                            'bling_id': bling_id,
                            'empresa_id': self.empresa_id,
                            'dados_json': novo_json,
                            'data_ingestao': datetime.now()
                        })
                        stats['atualizados'] += 1
                    else:
                        # Conteúdo idêntico → apenas toca data_ingestao
                        registros_para_tocar_data.append({
                            'bling_id': bling_id,
                            'empresa_id': self.empresa_id
                        })
                        stats['ignorados'] += 1
            
            print(f"\n📊 CLASSIFICAÇÃO:")
            print(f"   • 🆕 Novos: {stats['inseridos']}")
            print(f"   • 🔄 Alterados: {stats['atualizados']}")
            print(f"   • ⏭️ Idênticos: {stats['ignorados']}")
            
            # INSERIR NOVOS
            if registros_novos:
                print(f"\n💾 Inserindo {len(registros_novos)} novos...")
                session.bulk_insert_mappings(self.model_class, registros_novos)
                session.commit()
                print(f"✅ Inserções concluídas")
            
            # ATUALIZAR DIFERENTES
            if registros_para_atualizar:
                print(f"\n🔄 Atualizando {len(registros_para_atualizar)}...")
                for registro in registros_para_atualizar:
                    stmt = insert(self.model_class.__table__).values(registro)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['bling_id', 'empresa_id'],
                        set_={
                            'dados_json': stmt.excluded.dados_json,
                            'data_ingestao': stmt.excluded.data_ingestao,
                            'status_processamento': 'pendente'
                        }
                    )
                    session.execute(stmt)
                session.commit()
                print(f"✅ Atualizações concluídas")
            
            # ATUALIZAR APENAS DATA_INGESTAO
            if registros_para_tocar_data:
                print(f"\n🕐 Atualizando data_ingestao em {len(registros_para_tocar_data)} idênticos...")
                
                # Agrupar por empresa
                por_empresa = {}
                for r in registros_para_tocar_data:
                    emp_id = r['empresa_id']
                    if emp_id not in por_empresa:
                        por_empresa[emp_id] = []
                    por_empresa[emp_id].append(r['bling_id'])
                
                # Update por empresa em lote
                for empresa_id, bling_ids in por_empresa.items():
                    query_update = text("""
                        UPDATE raw.nfe_raw
                        SET data_ingestao = CURRENT_TIMESTAMP
                        WHERE bling_id = ANY(:bling_ids)
                        AND empresa_id = :empresa_id
                    """)
                    
                    session.execute(query_update, {
                        "bling_ids": bling_ids,
                        "empresa_id": empresa_id
                    })
                
                session.commit()
                print(f"✅ Datas atualizadas")
            
            return stats
            
        except Exception as e:
            session.rollback()
            print(f"\n❌ Erro ao salvar: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            session.close()