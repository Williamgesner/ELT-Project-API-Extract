# Responsável por: extrair contas a receber com ESTRATÉGIA HÍBRIDA (120 dias)

from datetime import datetime, timedelta
from core.base_extractor import BaseExtractor
from models.accounts_receivable_raw import ContasReceberRaw
from config.settings import endpoints
from config.extraction_mode import ExtractionMode

class ContasReceberExtractorV2(BaseExtractor):
    """
    Extrator otimizado de contas a receber
    
    ESTRATÉGIA:
    - FULL: Extrai tudo + limpa órfãos
    - INCREMENTAL: Últimos 120 dias + SEMPRE compara (detecta mudanças de status)
    """
    
    def __init__(self, api_key, empresa_id, extraction_mode=ExtractionMode.INCREMENTAL):
        super().__init__(endpoints['contas_receber'], ContasReceberRaw)
        self.empresa_id = empresa_id
        self.extraction_mode = extraction_mode
        
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def executar_extracao_completa(self):
        """Executa extração baseada no modo configurado"""
        try:
            print(f"\n💵 EXTRAÇÃO: CONTAS A RECEBER (Empresa ID: {self.empresa_id})")
            print(f"🔧 Modo: {self.extraction_mode.value.upper()}")
            print("=" * 60)
            inicio_extracao = datetime.now()

            # Determinar janela de extração
            if self.extraction_mode == ExtractionMode.FULL:
                data_inicial = datetime(2024, 1, 1).date()
                print(f"   📅 Período: 2024-01-01 até HOJE (FULL)")
                print(f"   🛡️  Limpeza de órfãos: HABILITADA")
            else:
                # INCREMENTAL: Últimos 120 dias
                data_inicial = (datetime.now() - timedelta(days=120)).date()
                print(f"   📅 Período: Últimos 120 dias (INCREMENTAL)")
                print(f"   🔍 Comparação inteligente: HABILITADA")
                print(f"   🛡️  Limpeza de órfãos: DESABILITADA")
            
            data_final = datetime.now().date()
            janela_dias = 360
            
            # Extrair dados
            print(f"\n📥 Extraindo contas a receber da API...")
            todas_contas = []
            ids_vistos = set()
            
            inicio_janela = data_inicial
            while inicio_janela <= data_final:
                fim_janela = min(inicio_janela + timedelta(days=janela_dias), data_final)
                
                filtros_adicionais = {
                    "tipoFiltroData": "E",
                    "dataInicial": inicio_janela.strftime("%Y-%m-%d"),
                    "dataFinal": fim_janela.strftime("%Y-%m-%d"),
                }
                
                print(f"\n   📅 Janela: {filtros_adicionais['dataInicial']} → {filtros_adicionais['dataFinal']}")
                
                contas_janela = self.extract_dados_bling_paginado(
                    limite_por_pagina=100,
                    delay_entre_requests=0.35,
                    max_paginas=1000,
                    max_tentativas=3,
                    filtros_adicionais=filtros_adicionais,
                )
                
                for conta in contas_janela:
                    conta_id = conta.get("id")
                    if conta_id is None or conta_id in ids_vistos:
                        continue
                    ids_vistos.add(conta_id)
                    todas_contas.append(conta)
                
                inicio_janela = fim_janela + timedelta(days=1)
            
            fim_extracao = datetime.now()
            tempo_extracao = fim_extracao - inicio_extracao
            
            if not todas_contas:
                print("❌ Nenhuma conta a receber extraída")
                return
            
            print(f"\n📊 EXTRAÇÃO CONCLUÍDA:")
            print(f"⏱️ Tempo: {tempo_extracao}")
            print(f"📈 Contas extraídas: {len(todas_contas)}")
            
            # Preparar dados
            print(f"\n📝 Preparando dados para salvamento...")
            dados_para_salvar = []
            
            for conta in todas_contas:
                dados_formatados = {
                    'bling_id': conta['id'],
                    'empresa_id': self.empresa_id, 
                    'dados_json': conta
                }
                dados_para_salvar.append(dados_formatados)
            
            # SALVAMENTO COM COMPARAÇÃO INTELIGENTE
            print(f"\n💾 Iniciando salvamento inteligente...")
            print(f"   🔍 Comparação: SEMPRE ATIVA (detecta mudanças de status)")
            
            inicio_salvamento = datetime.now()
            
            # Limpar órfãos apenas em modo FULL
            limpar_orfaos = (self.extraction_mode == ExtractionMode.FULL)
            
            stats = self.salvar_dados_postgres_bulk(dados_para_salvar, limpar_orfaos=limpar_orfaos)
            
            fim_salvamento = datetime.now()
            tempo_salvamento = fim_salvamento - inicio_salvamento
            tempo_total = fim_salvamento - inicio_extracao
            
            # RELATÓRIO FINAL
            print(f"\n🏁 EXECUÇÃO COMPLETA!")
            print(f"⏱️ Tempo total: {tempo_total}")
            print(f"⏱️ Tempo de salvamento: {tempo_salvamento}")
            
            if stats['total'] > 0:
                print(f"\n📊 ESTATÍSTICAS:")
                print(f"   • 🆕 Novos: {stats['inseridos']}")
                print(f"   • 🔄 Atualizados: {stats['atualizados']}")
                print(f"   • ✓ Idênticos: {stats['ignorados']}")
                
                if stats['atualizados'] > 0:
                    taxa_mudanca = (stats['atualizados'] / stats['total']) * 100
                    print(f"\n⚠️  MUDANÇAS DETECTADAS:")
                    print(f"   • {stats['atualizados']} contas alteradas ({taxa_mudanca:.1f}%)")
                    print(f"   • Status: Marcadas como 'pendente' para reprocessamento")
            
            if self.extraction_mode == ExtractionMode.INCREMENTAL:
                print(f"\n⚡ MODO INCREMENTAL:")
                print(f"   • Janela: 120 dias (100% de cobertura)")
                print(f"   • Segurança: Dados históricos preservados")
            
            print("\n🎉 Script de contas a receber executado com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️ Execução interrompida pelo usuário")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO: {e}")
            raise
