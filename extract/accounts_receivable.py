# Responsável por: extrair contas a receber da API Bling - VERSÃO OTIMIZADA

from datetime import datetime, timedelta
from core.base_extractor import BaseExtractor
from models.accounts_receivable_raw import ContasReceberRaw
from config.settings import endpoints
from config.extraction_mode import ExtractionMode

class ContasReceberExtractor(BaseExtractor):
    """
    Extrator específico para contas a receber da API Bling - VERSÃO OTIMIZADA
    Suporta modo FULL e INCREMENTAL
    """
    
    def __init__(self, api_key, empresa_id):
        """
        Inicializa o extrator de contas a receber
        
        Args:
            api_key: Token de autenticação da API Bling
            empresa_id: ID da empresa na tabela dim_empresas
        """
        super().__init__(endpoints['contas_receber'], ContasReceberRaw)
        self.empresa_id = empresa_id
        
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def executar_extracao_completa(self, extraction_mode=ExtractionMode.INCREMENTAL):
        """
        Executa o processo completo de extração de contas a receber
        
        Args:
            extraction_mode: ExtractionMode.FULL ou ExtractionMode.INCREMENTAL
        """
        try:
            inicio_extracao = datetime.now()
            
            # Definir janela de extração baseada no modo
            if extraction_mode == ExtractionMode.INCREMENTAL:
                print(f"🔄 MODO INCREMENTAL: Extraindo contas dos últimos 90 dias (Empresa ID: {self.empresa_id})...")
                data_inicial = (datetime.now() - timedelta(days=90)).date()
                limpar_orfaos = False
            else:
                print(f"📊 MODO FULL: Extraindo todas as contas desde 2024 (Empresa ID: {self.empresa_id})...")
                data_inicial = datetime(2024, 1, 1).date()
                limpar_orfaos = True

            # 🆕 CRÍTICO: Informar período de extração ao base_extractor
            self.data_inicial_extracao = datetime.combine(data_inicial, datetime.min.time()) if limpar_orfaos else None
            
            # ✅ CORREÇÃO CRÍTICA: Campo de data no JSONB usado para filtrar registros existentes
            # Contas a Receber usa 'vencimento' (NÃO 'dataInclusao')
            self.campo_data_filtro=None

            data_final = datetime.now().date()
            janela_dias = 360

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

                print(
                    f"\n📅 Janela (contas a receber): "
                    f"{filtros_adicionais['dataInicial']} → {filtros_adicionais['dataFinal']}"
                )

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
                print("❌ Nenhuma conta a receber foi extraída. Verificar API ou configurações.")
                return
            
            print(f"\n📊 EXTRAÇÃO CONCLUÍDA:")
            print(f"⏱️ Tempo de extração: {tempo_extracao}")
            print(f"📈 Contas extraídas: {len(todas_contas)}")
            print(f"🚀 Velocidade: {len(todas_contas)/tempo_extracao.total_seconds():.1f} contas/segundo")

            print("\n📝 Preparando dados para salvamento...")
            dados_para_salvar = []
            
            for conta in todas_contas:
                dados_formatados = {
                    'bling_id': conta['id'],
                    'empresa_id': self.empresa_id, 
                    'dados_json': conta
                }
                dados_para_salvar.append(dados_formatados)

            print(f"\n💾 Iniciando salvamento inteligente...")
            inicio_salvamento = datetime.now()
            
            stats = self.salvar_dados_postgres_bulk(dados_para_salvar, limpar_orfaos=limpar_orfaos)
            
            fim_salvamento = datetime.now()
            tempo_salvamento = fim_salvamento - inicio_salvamento
            tempo_total = fim_salvamento - inicio_extracao

            print(f"\n🏁 EXECUÇÃO COMPLETA!")
            print(f"⏱️ Tempo total: {tempo_total}")
            print(f"⏱️ Tempo de salvamento: {tempo_salvamento}")
            print(f"🚀 Performance geral: {len(todas_contas)/tempo_total.total_seconds():.1f} contas/segundo")
            
            if stats['total'] > 0:
                eficiencia = (stats['ignorados'] / stats['total']) * 100
                print(f"⚡ Eficiência: {eficiencia:.1f}% dos registros eram idênticos (evitou escritas desnecessárias)")

            print("\n🎉 Script de contas a receber executado com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️ Execução interrompida pelo usuário")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
            print("Script interrompido para análise do erro")
            print("Todos os dados extraídos até este ponto foram preservados")
            raise