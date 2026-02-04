# Responsável por: extrair contatos completos da API Bling
# VERSÃO OTIMIZADA: Suporta modo FULL e INCREMENTAL
# ESTRATÉGIA: Inserir apenas novos (SEM comparação de JSON)

from datetime import datetime, timedelta
import time
import requests
from core.base_extractor import BaseExtractor
from models.contact_raw import ContatoRaw
from config.settings import endpoints
from config.database import Session
from sqlalchemy import text
from config.extraction_mode import ExtractionMode

# =============================================================
# 1. CRIANDO A CLASSE PARA EXTRAÇÃO DE CONTATOS COMPLETOS
# =============================================================

class ContatosCompletoExtractor(BaseExtractor):
    """
    Extrator otimizado para contatos da API Bling
    
    MODOS DE EXTRAÇÃO:
    - FULL: Extrai desde 2024-01-01 usando dataInclusao + Limpeza de órfãos ATIVA
    - INCREMENTAL: Extrai últimos 7 dias usando dataAlteracao + Limpeza DESABILITADA
    
    ESTRATÉGIA OTIMIZADA:
    - Filtra apenas novos ANTES de buscar detalhes (economia de requests)
    - Inserção direta SEM comparação de JSON (mais rápido)
    - Busca detalhes completos incluindo endereços
    
    IMPORTANTE: Contatos possui filtro de dataAlteração na API Bling!
    Isso permite capturar mudanças em contatos antigos (ex: contato de 2023 
    com telefone alterado hoje será capturado no incremental).
    """
    
    def __init__(self, api_key, empresa_id):
        """
        Inicializa o extrator de contatos
        
        Args:
            api_key: Token de autenticação da API Bling
            empresa_id: ID da empresa na tabela dim_empresas
        """
        super().__init__(endpoints['contatos'], ContatoRaw)
        self.empresa_id = empresa_id
        
        # Sobrescrever headers do base_extractor com a API key específica
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def executar_extracao_completa(self, extraction_mode=ExtractionMode.FULL):
        """
        Processo otimizado: inserir apenas novos (SEM COMPARAÇÃO)
        
        Args:
            extraction_mode: ExtractionMode.FULL ou ExtractionMode.INCREMENTAL
                - FULL: dataInclusao desde 2024-01-01 + Remove órfãos
                - INCREMENTAL: dataAlteracao últimos 7 dias (sem remoção)
        """
        try:
            inicio_total = datetime.now()
            
            # =====================================================
            # DEFINIR JANELA DE EXTRAÇÃO BASEADA NO MODO
            # =====================================================
            if extraction_mode == ExtractionMode.INCREMENTAL:
                # MODO INCREMENTAL: Contatos ALTERADOS nos últimos 7 dias
                print(f"\n⚡ MODO INCREMENTAL: Contatos")
                print(f"📅 Período: Alterados nos últimos 7 dias")
                print(f"🔍 Filtro API: dataAlteracao")
                print(f"🛡️ Limpeza de órfãos: DESABILITADA")
                print(f"⚡ Estratégia: Inserir apenas novos (sem comparação)")
                
                data_inicial = datetime.now() - timedelta(days=7)
                usar_filtro_alteracao = True
                limpar_orfaos = False
                
            else:
                # MODO FULL: Todos os contatos desde 2024 (sincronização completa)
                print(f"\n📊 MODO FULL: Contatos")
                print(f"📅 Período: Desde 2024-01-01")
                print(f"🔍 Filtro API: dataInclusao")
                print(f"🧹 Limpeza de órfãos: ATIVA")
                print(f"⚡ Estratégia: Inserir apenas novos (sem comparação)")
                
                data_inicial = datetime(2024, 1, 1, 0, 0, 0)
                usar_filtro_alteracao = False
                limpar_orfaos = True
            
            print(f"\n👥 EXTRAÇÃO: CONTATOS COMPLETOS (Empresa ID: {self.empresa_id})")
            print("=" * 60)
            
            # =====================================================
            # ETAPA 1: EXTRAIR LISTA BÁSICA DE CONTATOS
            # =====================================================
            print("\n1️⃣ EXTRAINDO LISTA BÁSICA DE CONTATOS...")
            inicio_lista = datetime.now()

            # API Bling aceita dois filtros de data:
            # - dataInclusaoInicial/Final: quando o contato foi criado
            # - dataAlteracaoInicial/Final: quando o contato foi modificado
            # Formato: "YYYY-MM-DD HH:MM:SS"
            #
            # Janelas de 360 dias evitam erro de intervalo (máx 365 dias)
            
            data_final = datetime.now()
            janela_dias = 360  # Margem de segurança (< 365)

            lista_contatos = []
            ids_vistos = set()

            inicio_janela = data_inicial
            
            while inicio_janela <= data_final:
                # Calcula fim da janela (máximo 360 dias ou até data_final)
                fim_janela = min(
                    inicio_janela + timedelta(days=janela_dias) - timedelta(seconds=1),
                    data_final,
                )

                # =====================================================
                # FILTROS DIFERENTES POR MODO
                # =====================================================
                if usar_filtro_alteracao:
                    # INCREMENTAL: Contatos ALTERADOS (pega mudanças em contatos antigos)
                    filtros_adicionais = {
                        "criterio": 1,  # "Todos" (mais seguro com filtros de data)
                        "dataAlteracaoInicial": inicio_janela.strftime("%Y-%m-%d %H:%M:%S"),
                        "dataAlteracaoFinal": fim_janela.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    print(
                        f"\n📅 Janela ALTERAÇÃO: "
                        f"{filtros_adicionais['dataAlteracaoInicial']} → "
                        f"{filtros_adicionais['dataAlteracaoFinal']}"
                    )
                else:
                    # FULL: Contatos INCLUÍDOS (extração completa do período)
                    filtros_adicionais = {
                        "criterio": 1,  # "Todos" (mais seguro com filtros de data)
                        "dataInclusaoInicial": inicio_janela.strftime("%Y-%m-%d %H:%M:%S"),
                        "dataInclusaoFinal": fim_janela.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    print(
                        f"\n📅 Janela INCLUSÃO: "
                        f"{filtros_adicionais['dataInclusaoInicial']} → "
                        f"{filtros_adicionais['dataInclusaoFinal']}"
                    )

                # Extração paginada desta janela
                contatos_janela = self.extract_dados_bling_paginado(
                    limite_por_pagina=100,
                    delay_entre_requests=0.35,
                    max_paginas=1000,
                    max_tentativas=3,
                    filtros_adicionais=filtros_adicionais,
                )

                # Deduplica registros (evita duplicatas entre janelas)
                for contato in contatos_janela:
                    contato_id = contato.get("id")
                    if contato_id is None or contato_id in ids_vistos:
                        continue
                    ids_vistos.add(contato_id)
                    lista_contatos.append(contato)

                # Próxima janela (evita sobreposição - +1 segundo)
                inicio_janela = fim_janela + timedelta(seconds=1)
            
            fim_lista = datetime.now()
            tempo_lista = fim_lista - inicio_lista
            
            # =====================================================
            # VALIDAÇÃO DOS DADOS EXTRAÍDOS
            # =====================================================
            if not lista_contatos:
                if extraction_mode == ExtractionMode.INCREMENTAL:
                    print("✨ Nenhum contato alterado nos últimos 7 dias.")
                    print("   Isso é normal no modo incremental.")
                else:
                    print("⚠️ Nenhum contato foi extraído.")
                    print("   Possíveis causas:")
                    print("   • Não há contatos no período especificado")
                    print("   • Problema de conectividade com a API")
                return
            
            print(f"✅ {len(lista_contatos)} contatos extraídos em {tempo_lista}")
            
            # =====================================================
            # ETAPA 2: IDENTIFICAR APENAS NOVOS (OTIMIZAÇÃO)
            # =====================================================
            print(f"\n2️⃣ IDENTIFICANDO CONTATOS NOVOS...")
            inicio_filtro = datetime.now()
            
            contatos_novos = self._filtrar_apenas_novos(lista_contatos)
            
            fim_filtro = datetime.now()
            
            if not contatos_novos:
                print(f"✅ Nenhum contato novo encontrado. Base já atualizada!")
                print(f"⏱️  Tempo de verificação: {fim_filtro - inicio_filtro}")
                
                # ⚠️ CRÍTICO: Mesmo sem novos, precisa limpar órfãos no FULL
                if limpar_orfaos:
                    print(f"\n🧹 EXECUTANDO LIMPEZA DE ÓRFÃOS (MODO FULL)...")
                    self._limpar_orfaos_contatos(
                        ids_da_api=set(c['id'] for c in lista_contatos),
                        data_inicial=data_inicial
                    )
                
                return
            
            print(f"✅ {len(contatos_novos)} contatos novos identificados em {fim_filtro - inicio_filtro}")
            
            # =====================================================
            # ETAPA 3: BUSCAR DETALHES APENAS DOS NOVOS
            # =====================================================
            print(f"\n3️⃣ BUSCANDO DETALHES DOS NOVOS CONTATOS...")
            inicio_detalhes = datetime.now()
            
            contatos_completos = self._buscar_detalhes_otimizado(contatos_novos)
            
            fim_detalhes = datetime.now()
            tempo_detalhes = fim_detalhes - inicio_detalhes
            
            print(f"✅ Detalhes coletados em {tempo_detalhes}")
            
            # =====================================================
            # ETAPA 4: SALVAR APENAS NOVOS (INSERT DIRETO)
            # =====================================================
            print(f"\n4️⃣ SALVANDO NOVOS CONTATOS (INSERT DIRETO)...")
            inicio_salvamento = datetime.now()
            
            # Função otimizada - NÃO usa salvar_dados_postgres_bulk()
            stats = self._salvar_novos_direto(contatos_completos)
            
            fim_salvamento = datetime.now()
            
            # =====================================================
            # ETAPA 5: LIMPEZA DE ÓRFÃOS (APENAS MODO FULL)
            # =====================================================
            if limpar_orfaos:
                print(f"\n5️⃣ LIMPEZA DE ÓRFÃOS (MODO FULL)...")
                self._limpar_orfaos_contatos(
                    ids_da_api=set(c['id'] for c in lista_contatos),
                    data_inicial=data_inicial
                )
            
            fim_total = datetime.now()
            
            # =====================================================
            # RELATÓRIO FINAL
            # =====================================================
            print(f"\n🎉 EXTRAÇÃO CONCLUÍDA COM SUCESSO!")
            print(f"=" * 60)
            print(f"\n⏱️  TEMPOS:")
            print(f"   • Extração da API: {tempo_lista}")
            print(f"   • Filtro de novos: {fim_filtro - inicio_filtro}")
            print(f"   • Busca detalhes: {tempo_detalhes}")
            print(f"   • Salvamento: {fim_salvamento - inicio_salvamento}")
            print(f"   • TOTAL: {fim_total - inicio_total}")
            
            print(f"\n📊 ESTATÍSTICAS GERAIS:")
            print(f"   • Total de contatos na API: {len(lista_contatos)}")
            print(f"   • Contatos já existentes no banco: {len(lista_contatos) - len(contatos_novos)}")
            print(f"   • Contatos novos encontrados: {len(contatos_novos)}")
            
            print(f"\n💾 OPERAÇÕES NO BANCO:")
            print(f"   • Inseridos com sucesso: {stats['inseridos']}")
            print(f"   • Erros durante inserção: {stats['erros']}")
            
            print(f"\n📈 RESUMO DO BANCO:")
            total_no_banco = self._contar_total_no_banco()
            print(f"   • Total de contatos no banco agora: {total_no_banco}")
            
            # Estatísticas de endereços
            self._calcular_estatisticas_enderecos(contatos_completos, len(contatos_novos))
            
            # Resumo de economia
            economia_operacoes = len(lista_contatos) - len(contatos_novos)
            if economia_operacoes > 0:
                print(f"\n⚡ ECONOMIA:")
                print(f"   • {economia_operacoes} inserções duplicadas evitadas")
            
            print(f"\n✅ Contatos extraídos e salvos com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️  Execução interrompida pelo usuário")
            print("💾 Dados processados até este ponto foram preservados")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def _filtrar_apenas_novos(self, lista_contatos):
        """
        Filtra apenas contatos que NÃO existem no banco
        OTIMIZAÇÃO: Uma única query SQL para verificar todos os IDs de uma vez
        
        Args:
            lista_contatos: Lista de contatos da API
            
        Returns:
            list: Apenas contatos que não existem no banco
        """
        session = Session()
        
        try:
            # Extrai todos os IDs da API
            ids_api = [c['id'] for c in lista_contatos]
            
            if not ids_api:
                return []
            
            # Query única para buscar todos os IDs que já existem
            # Muito mais rápido que 1000 queries individuais
            query = text("""
                SELECT bling_id 
                FROM raw.contatos_raw 
                WHERE bling_id = ANY(:ids)
                  AND empresa_id = :empresa_id
            """)
            
            resultado = session.execute(query, {
                "ids": ids_api,
                "empresa_id": self.empresa_id
            })
            ids_existentes = set(row.bling_id for row in resultado)
            
            # Filtra apenas os que NÃO existem
            contatos_novos = [c for c in lista_contatos if c['id'] not in ids_existentes]
            
            print(f"   📊 {len(ids_existentes)} já existem no banco")
            print(f"   🆕 {len(contatos_novos)} são novos")
            
            return contatos_novos
            
        except Exception as e:
            print(f"❌ Erro ao filtrar contatos: {e}")
            raise
        finally:
            session.close()
    
    def _limpar_orfaos_contatos(self, ids_da_api, data_inicial):
        """
        Remove contatos órfãos (existem no banco mas não vieram da API)
        CRÍTICO: Aplica APENAS no escopo de data definido (ex: 2024+)
        
        Isso garante que se você mudar o filtro de 2024+ para 2025+,
        os dados de 2024 NÃO serão apagados.
        
        Args:
            ids_da_api: Set com IDs que vieram da API
            data_inicial: Data inicial do período de extração (para filtro)
        """
        session = Session()
        
        try:
            print(f"\n🧹 LIMPANDO REGISTROS ÓRFÃOS...")
            print(f"   📅 Período de limpeza: {data_inicial.strftime('%Y-%m-%d')} → hoje")
            
            # Buscar IDs que existem no banco DENTRO DO ESCOPO DE DATA
            query = text("""
                SELECT bling_id
                FROM raw.contatos_raw
                WHERE empresa_id = :empresa_id
                AND (dados_json->>'dataInclusao')::timestamp >= :data_inicial
            """)
            
            resultado = session.execute(query, {
                "empresa_id": self.empresa_id,
                "data_inicial": data_inicial
            })
            ids_no_banco = set(row.bling_id for row in resultado)
            
            print(f"   📋 Registros na RAW (período filtrado): {len(ids_no_banco)}")
            print(f"   📥 Registros da API: {len(ids_da_api)}")
            
            # IDs que estão no banco mas NÃO vieram da API = órfãos (deletados no Bling)
            ids_orfaos = ids_no_banco - ids_da_api
            
            if len(ids_orfaos) > 0:
                print(f"   ⚠️  {len(ids_orfaos)} contato(s) órfão(s) detectado(s)")
                print(f"   🗑️  Estes contatos foram DELETADOS no Bling")
                
                # Deletar órfãos
                query_delete = text("""
                    DELETE FROM raw.contatos_raw
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
    
    def _salvar_novos_direto(self, contatos_completos):
        """
        Salva novos contatos direto no banco (sem comparação de JSON)
        
        - Sem SELECT de registros existentes
        - Sem comparação de JSON  
        - Apenas INSERT direto
        - Commits em lote para performance
        
        Args:
            contatos_completos: Lista de contatos processados
            
        Returns:
            dict: Estatísticas de inserção
        """
        if not contatos_completos:
            return {'inseridos': 0, 'erros': 0}
        
        session = Session()
        stats = {'inseridos': 0, 'erros': 0}
        
        try:
            print(f"   💾 Preparando {len(contatos_completos)} registros para inserção...")
            
            for i, contato in enumerate(contatos_completos):
                try:
                    # Cria objeto diretamente (sem verificações)
                    novo_registro = ContatoRaw(
                        bling_id=contato['id'],
                        empresa_id=self.empresa_id, 
                        dados_json=contato,
                        data_ingestao=datetime.now(),
                        status_processamento='pendente'
                    )
                    
                    session.add(novo_registro)
                    stats['inseridos'] += 1
                    
                    # Commit em lotes de 100 para performance
                    if (i + 1) % 100 == 0:
                        session.commit()
                        print(f"   ✅ {i + 1}/{len(contatos_completos)} registros inseridos...")
                    
                except Exception as e:
                    session.rollback()
                    stats['erros'] += 1
                    erro_msg = str(e)
                    
                    # Log apenas se não for duplicata (que seria estranho aqui)
                    if "duplicate key" not in erro_msg and "already exists" not in erro_msg:
                        print(f"   ❌ Erro no contato {contato.get('id')}: {erro_msg[:100]}")
            
            # Commit final para registros restantes
            session.commit()
            print(f"   ✅ Commit final realizado")
            
        except Exception as e:
            session.rollback()
            print(f"❌ Erro crítico no salvamento: {e}")
            raise
        finally:
            session.close()
        
        return stats
    
    def _buscar_detalhes_otimizado(self, lista_contatos):
        """
        Busca detalhes completos dos contatos (incluindo endereços)
        com delay otimizado entre requisições
        
        Args:
            lista_contatos: Lista de contatos básicos
            
        Returns:
            list: Contatos com detalhes completos e endereços estruturados
        """
        contatos_completos = []
        total = len(lista_contatos)
        
        print(f"   📡 Buscando detalhes de {total} contatos...")
        
        for i, contato in enumerate(lista_contatos):
            # Progresso a cada 50 contatos
            if (i + 1) % 50 == 0:
                print(f"   Processando {i + 1}/{total}...")
            
            # Busca detalhes do contato
            detalhes = self._buscar_detalhes_contato(contato['id'])
            
            if detalhes:
                # Processa e estrutura o endereço
                processado = self._processar_contato_detalhado(detalhes)
                contatos_completos.append(processado)
            else:
                # Se não conseguir detalhes, usa dados básicos
                contatos_completos.append(contato)
            
            # Delay pequeno para não estourar rate limit
            time.sleep(0.05)
        
        return contatos_completos
    
    def _buscar_detalhes_contato(self, contato_id):
        """
        Busca detalhes completos de um contato específico
        
        Args:
            contato_id: ID do contato no Bling
            
        Returns:
            dict: Dados completos do contato ou None se falhar
        """
        try:
            url = f"{endpoints['contatos']}/{contato_id}"
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                return response.json().get('data', {})
            else:
                return None
                
        except Exception:
            return None
    
    def _processar_contato_detalhado(self, contato_detalhado):
        """
        Processa contato adicionando estrutura de endereço formatada
        
        Args:
            contato_detalhado: Dados completos do contato da API
            
        Returns:
            dict: Contato com endereço estruturado
        """
        endereco_geral = contato_detalhado.get("endereco", {}).get("geral", {})
        contato_processado = contato_detalhado.copy()
        
        if endereco_geral:
            contato_processado['endereco_estruturado'] = {
                'tem_endereco': True,
                'endereco_completo_formatado': self._formatar_endereco_contato(endereco_geral),
                'endereco_detalhado': {
                    'logradouro': endereco_geral.get('endereco'),
                    'numero': endereco_geral.get('numero'),
                    'complemento': endereco_geral.get('complemento'),
                    'bairro': endereco_geral.get('bairro'),
                    'cidade': endereco_geral.get('municipio'),
                    'estado': endereco_geral.get('uf'),
                    'cep': endereco_geral.get('cep'),
                    'pais': endereco_geral.get('pais', 'Brasil')
                },
                'data_processamento': datetime.now().isoformat()
            }
        else:
            contato_processado['endereco_estruturado'] = {
                'tem_endereco': False,
                'endereco_completo_formatado': None,
                'endereco_detalhado': None,
                'data_processamento': datetime.now().isoformat()
            }
        
        return contato_processado
    
    def _formatar_endereco_contato(self, endereco_geral):
        """
        Formata endereço em string legível
        
        Args:
            endereco_geral: Dicionário com dados do endereço
            
        Returns:
            str: Endereço formatado ou None se vazio
        """
        partes = [
            endereco_geral.get('endereco', ''),
            endereco_geral.get('numero', ''),
            endereco_geral.get('complemento', ''),
            endereco_geral.get('bairro', ''),
            endereco_geral.get('municipio', ''),
            endereco_geral.get('uf', ''),
            endereco_geral.get('cep', '')
        ]
        
        partes_validas = [p.strip() for p in partes if p and p.strip()]
        return ', '.join(partes_validas) if partes_validas else None
    
    def _calcular_estatisticas_enderecos(self, contatos_completos, total_contatos):
        """
        Calcula e exibe estatísticas sobre endereços
        
        Args:
            contatos_completos: Lista de contatos processados
            total_contatos: Total de contatos
        """
        contatos_com_endereco = sum(
            1 for c in contatos_completos
            if c.get('endereco_estruturado', {}).get('tem_endereco', False)
        )
        
        print(f"\n🏠 ESTATÍSTICAS DE ENDEREÇOS:")
        print(f"   • Com endereços completos: {contatos_com_endereco}/{total_contatos}")
        
        if total_contatos > 0:
            taxa = (contatos_com_endereco / total_contatos) * 100
            print(f"   • Taxa de cobertura: {taxa:.1f}%")
            
            if taxa < 50:
                print(f"   ⚠️  Atenção: Mais de 50% dos contatos sem endereço")
    
    def _contar_total_no_banco(self):
        """
        Conta o total de contatos no banco após inserção
        """
        session = Session()
        try:
            query = text("""
                SELECT COUNT(*) 
                FROM raw.contatos_raw 
                WHERE empresa_id = :empresa_id
            """)
            resultado = session.execute(query, {"empresa_id": self.empresa_id})
            total = resultado.scalar()
            return total
        finally:
            session.close()