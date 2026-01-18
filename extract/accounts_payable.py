# Responsável por: extrair contas a pagar da API Bling

from datetime import datetime, timedelta
from core.base_extractor import BaseExtractor
from models.accounts_payable_raw import ContasPagarRaw
from config.settings import endpoints

# =====================================================
# 1. CRIANDO A CLASSE PARA EXTRAÇÃO DE CONTAS A PAGAR
# =====================================================

class ContasPagarExtractor(BaseExtractor):
    
    """
    Extrator específico para contas a pagar da API Bling
    Herda toda a lógica comum da BaseExtractor e adiciona só o que é específico
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
    
    def executar_extracao_completa(self):
        """
        Executa o processo completo de extração de contas a pagar
        """
        try:
            print(f"\n💰 EXTRAÇÃO: CONTAS A PAGAR (Empresa ID: {self.empresa_id})")
            print("=" * 60)
            inicio_extracao = datetime.now()

            # Extrai TODOS os dados da API usando paginação
            print("Extraindo todas as contas a pagar da API...")
            # Filtro correto conforme OpenAPI do Bling (GET /contas/pagar):
            # - dataEmissaoInicial / dataEmissaoFinal (format: "YYYY-MM-DD")
            #
            # Observação: para evitar erro de intervalo (ex.: > 1 ano),
            # extraímos em janelas de datas.
            data_inicial = datetime(2024, 1, 1).date()
            data_final = datetime.now().date()
            janela_dias = 360  # margem de segurança (< 365)

            todas_contas = []
            ids_vistos = set()

            inicio_janela = data_inicial
            while inicio_janela <= data_final:
                fim_janela = min(inicio_janela + timedelta(days=janela_dias), data_final)

                filtros_adicionais = {
                    "dataEmissaoInicial": inicio_janela.strftime("%Y-%m-%d"),
                    "dataEmissaoFinal": fim_janela.strftime("%Y-%m-%d"),
                }

                print(
                    f"\n📅 Janela (contas a pagar): "
                    f"{filtros_adicionais['dataEmissaoInicial']} → {filtros_adicionais['dataEmissaoFinal']}"
                )

                contas_janela = self.extract_dados_bling_paginado(
                    limite_por_pagina=100,       # Máximo permitido pela API
                    delay_entre_requests=0.35,   # Delay mínimo, com margem de segurança
                    max_paginas=1000,            # Limite de segurança
                    max_tentativas=3,            # 3 tentativas antes de parar tudo
                    filtros_adicionais=filtros_adicionais,
                )

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

            if not todas_contas:
                print("❌ Nenhuma conta a pagar foi extraída. Verificar API ou configurações.")
                return
            
            print(f"\n📊 EXTRAÇÃO CONCLUÍDA:")
            print(f"⏱️ Tempo de extração: {tempo_extracao}")
            print(f"📈 Contas extraídas: {len(todas_contas)}")
            print(f"🚀 Velocidade: {len(todas_contas)/tempo_extracao.total_seconds():.1f} contas/segundo")

            # Preparar dados - APENAS JSON PURO
            print("\n📝 Preparando dados para salvamento...")
            dados_para_salvar = []
            
            for conta in todas_contas:
                dados_formatados = {
                    'bling_id': conta['id'],
                    'empresa_id': self.empresa_id, 
                    'dados_json': conta  # JSON completo e puro
                }
                dados_para_salvar.append(dados_formatados)

            # Salvamento inteligente
            print(f"\n💾 Iniciando salvamento inteligente...")
            inicio_salvamento = datetime.now()
            
            stats = self.salvar_dados_postgres_bulk(dados_para_salvar)
            
            fim_salvamento = datetime.now()
            tempo_salvamento = fim_salvamento - inicio_salvamento
            tempo_total = fim_salvamento - inicio_extracao

            # Relatório final de performance
            print(f"\n🏁 EXECUÇÃO COMPLETA!")
            print(f"⏱️ Tempo total: {tempo_total}")
            print(f"⏱️ Tempo de salvamento: {tempo_salvamento}")
            print(f"🚀 Performance geral: {len(todas_contas)/tempo_total.total_seconds():.1f} contas/segundo")
            
            # Eficiência do algoritmo
            if stats['total'] > 0:
                eficiencia = (stats['ignorados'] / stats['total']) * 100
                print(f"⚡ Eficiência: {eficiencia:.1f}% dos registros eram idênticos (evitou escritas desnecessárias)")

            print("\n🎉 Script de contas a pagar executado com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️ Execução interrompida pelo usuário")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
            print("Script interrompido para análise do erro")
            print("Todos os dados extraídos até este ponto foram preservados")
            raise