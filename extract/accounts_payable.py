# Responsável por: extrair contas a pagar da API Bling
# VERSÃO OTIMIZADA: Suporta modo FULL e INCREMENTAL

from datetime import datetime, timedelta
from core.base_extractor import BaseExtractor
from models.accounts_payable_raw import ContasPagarRaw
from config.settings import endpoints
from config.extraction_mode import ExtractionMode

# =====================================================
# 1. CRIANDO A CLASSE PARA EXTRAÇÃO DE CONTAS A PAGAR
# =====================================================

class ContasPagarExtractor(BaseExtractor):
    
    """
    Extrator específico para contas a pagar da API Bling
    Herda toda a lógica comum da BaseExtractor e adiciona só o que é específico
    
    MODOS DE EXTRAÇÃO:
    - FULL: Extrai desde 2024-01-01 + Limpeza de órfãos ATIVA
    - INCREMENTAL: Extrai últimos 90 dias + Limpeza de órfãos DESABILITADA
    
    OBSERVAÇÃO: Contas a Pagar NÃO possui filtro de dataAlteração na API Bling,
    por isso usamos janela de 90 dias no incremental para garantir cobertura.
    """
    
    def __init__(self, api_key, empresa_id):
        """
        Inicializa o extrator de contas a pagar
        Passa para a classe pai (BaseExtractor) a URL e modelo específicos
        
        Args:
            api_key: Token de autenticação da API Bling
            empresa_id: ID da empresa na tabela dim_empresas
        """
        super().__init__(endpoints['contas_pagar'], ContasPagarRaw)
        self.empresa_id = empresa_id
        
        # Sobrescrever headers do base_extractor com a API key específica
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def executar_extracao_completa(self, extraction_mode=ExtractionMode.FULL):
        """
        Executa o processo completo de extração de contas a pagar
        
        Args:
            extraction_mode: ExtractionMode.FULL ou ExtractionMode.INCREMENTAL
                - FULL: Extrai desde 2024-01-01 + Remove órfãos
                - INCREMENTAL: Extrai últimos 90 dias (sem remoção)
        """
        try:
            inicio_extracao = datetime.now()
            
            # =====================================================
            # DEFINIR JANELA DE EXTRAÇÃO BASEADA NO MODO
            # =====================================================
            if extraction_mode == ExtractionMode.INCREMENTAL:
                # MODO INCREMENTAL: Últimos 90 dias
                print(f"\n⚡ MODO INCREMENTAL: Contas a Pagar")
                print(f"📅 Período: Últimos 90 dias")
                print(f"🛡️ Limpeza de órfãos: DESABILITADA")
                
                data_vencimento_inicial = (datetime.now() - timedelta(days=90)).date()
                limpar_orfaos = False
                
            else:
                # MODO FULL: Desde 2024 (sincronização completa)
                print(f"\n📊 MODO FULL: Contas a Pagar")
                print(f"📅 Período: 2024-01-01 até hoje+365 dias")
                print(f"🧹 Limpeza de órfãos: ATIVA")
                
                data_vencimento_inicial = datetime(2024, 1, 1).date()
                limpar_orfaos = True
            
            # ⚠️ CRÍTICO: Informar período de extração ao base_extractor
            # Usado para limpeza de órfãos APENAS dentro do escopo de data
            self.data_inicial_extracao = datetime.combine(data_vencimento_inicial, datetime.min.time()) if limpar_orfaos else None
            
            # ✅ CORREÇÃO CRÍTICA: Campo de data no JSONB usado para filtrar registros existentes
            # Contas a Pagar usa 'vencimento' (NÃO 'dataInclusao')
            self.campo_data_filtro = 'vencimento'

            print(f"\n💰 EXTRAÇÃO: CONTAS A PAGAR (Empresa ID: {self.empresa_id})")
            print("=" * 60)

            # =====================================================
            # EXTRAÇÃO COM JANELAS DE DATAS (SEGURANÇA API)
            # =====================================================
            # API Bling usa: dataVencimentoInicial / dataVencimentoFinal
            # Janelas de 360 dias evitam erro de intervalo (máx 365 dias)
            
            print("Extraindo contas a pagar da API...")
            
            data_vencimento_final = (datetime.now() + timedelta(days=365)).date()
            janela_dias = 360  # Margem de segurança (< 365)

            todas_contas = []
            ids_vistos = set()

            inicio_janela = data_vencimento_inicial
            
            while inicio_janela <= data_vencimento_final:
                fim_janela = min(inicio_janela + timedelta(days=janela_dias), data_vencimento_final)

                filtros_adicionais = {
                    "dataVencimentoInicial": inicio_janela.strftime("%Y-%m-%d"),
                    "dataVencimentoFinal": fim_janela.strftime("%Y-%m-%d"),
                }

                print(
                    f"\n📅 Janela: "
                    f"{filtros_adicionais['dataVencimentoInicial']} → "
                    f"{filtros_adicionais['dataVencimentoFinal']}"
                )

                # Extração paginada desta janela
                contas_janela = self.extract_dados_bling_paginado(
                    limite_por_pagina=100,       # Máximo permitido pela API
                    delay_entre_requests=0.35,   # Delay mínimo, com margem de segurança
                    max_paginas=1000,            # Limite de segurança
                    max_tentativas=3,            # 3 tentativas antes de parar tudo
                    filtros_adicionais=filtros_adicionais,
                )

                # Deduplica registros (evita duplicatas entre janelas)
                for conta in contas_janela:
                    conta_id = conta.get("id")
                    if conta_id is None or conta_id in ids_vistos:
                        continue
                    ids_vistos.add(conta_id)
                    todas_contas.append(conta)

                # Próxima janela (evita sobreposição)
                inicio_janela = fim_janela + timedelta(days=1)

            fim_extracao = datetime.now()
            tempo_extracao = fim_extracao - inicio_extracao

            # =====================================================
            # VALIDAÇÃO DOS DADOS EXTRAÍDOS
            # =====================================================
            if not todas_contas:
                print("⚠️ Nenhuma conta a pagar foi extraída.")
                print("   Possíveis causas:")
                print("   • Não há contas no período especificado")
                print("   • Problema de conectividade com a API")
                print("   • Filtros muito restritivos")
                return
            
            print(f"\n📊 EXTRAÇÃO CONCLUÍDA:")
            print(f"⏱️  Tempo de extração: {tempo_extracao}")
            print(f"📈 Contas extraídas: {len(todas_contas)}")
            print(f"🚀 Velocidade: {len(todas_contas)/tempo_extracao.total_seconds():.1f} contas/segundo")

            # =====================================================
            # PREPARAR DADOS PARA SALVAMENTO
            # =====================================================
            print("\n📝 Preparando dados para salvamento...")
            dados_para_salvar = []
            
            for conta in todas_contas:
                dados_formatados = {
                    'bling_id': conta['id'],
                    'empresa_id': self.empresa_id, 
                    'dados_json': conta  # JSON completo e puro (sem alterações)
                }
                dados_para_salvar.append(dados_formatados)

            # =====================================================
            # SALVAMENTO INTELIGENTE COM COMPARAÇÃO
            # =====================================================
            # A função salvar_dados_postgres_bulk já possui:
            # - Comparação inteligente (ignora ordem de listas, normaliza números)
            # - UPDATE apenas data_ingestao para registros idênticos
            # - UPDATE completo para registros diferentes
            # - INSERT para registros novos
            # - Limpeza de órfãos (se limpar_orfaos=True)
            
            print(f"\n💾 Iniciando salvamento inteligente...")
            print(f"🔄 Modo: {'FULL (com limpeza de órfãos)' if limpar_orfaos else 'INCREMENTAL (sem limpeza)'}")
            
            inicio_salvamento = datetime.now()
            
            # ⚠️ CRÍTICO: Passar flag limpar_orfaos corretamente
            stats = self.salvar_dados_postgres_bulk(
                dados_para_salvar, 
                limpar_orfaos=limpar_orfaos
            )
            
            fim_salvamento = datetime.now()
            tempo_salvamento = fim_salvamento - inicio_salvamento
            tempo_total = fim_salvamento - inicio_extracao

            # =====================================================
            # RELATÓRIO FINAL DE PERFORMANCE
            # =====================================================
            print(f"\n🏁 EXECUÇÃO COMPLETA!")
            print(f"⏱️  Tempo total: {tempo_total}")
            print(f"⏱️  Tempo de extração: {tempo_extracao}")
            print(f"⏱️  Tempo de salvamento: {tempo_salvamento}")
            print(f"🚀 Performance geral: {len(todas_contas)/tempo_total.total_seconds():.1f} contas/segundo")
            
            # Eficiência do algoritmo de comparação
            if stats['total'] > 0:
                eficiencia = (stats['ignorados'] / stats['total']) * 100
                print(f"⚡ Eficiência: {eficiencia:.1f}% dos registros eram idênticos")
                print(f"   (Evitou {stats['ignorados']} escritas desnecessárias)")

            print(f"\n✅ Contas a Pagar extraídas e salvas com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️  Execução interrompida pelo usuário")
            print("💾 Dados processados até este ponto foram preservados")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
            print("Script interrompido para análise do erro")
            print("Todos os dados extraídos até este ponto foram preservados")
            raise