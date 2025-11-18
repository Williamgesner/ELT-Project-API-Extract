# Responsável por: extrair contas a receber da API Bling

from datetime import datetime
from core.base_extractor import BaseExtractor
from models.accounts_receivable_raw import ContasReceberRaw
from config.settings import endpoints

# =====================================================
# 1. CRIANDO A CLASSE PARA EXTRAÇÃO DE CONTAS A RECEBER
# =====================================================

class ContasReceberExtractor(BaseExtractor):
    
    """
    Extrator específico para contas a receber da API Bling
    Herda toda a lógica comum da BaseExtractor e adiciona só o que é específico
    """
    
    def __init__(self, api_key, empresa_id):
        """
        Inicializa o extrator de contas a receber
        Passa para a classe pai (BaseExtractor) a URL e modelo específicos
        
        Args:
            api_key: Token de autenticação da API Bling
            empresa_id: ID da empresa na tabela dim_empresas
        """
        super().__init__(endpoints['contas_receber'], ContasReceberRaw)
        self.empresa_id = empresa_id
        
        # Sobrescrever headers do base_extractor com a API key específica
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def executar_extracao_completa(self):
        """
        Executa o processo completo de extração de contas a receber
        """
        try:
            print(f"\n💵 EXTRAÇÃO: CONTAS A RECEBER (Empresa ID: {self.empresa_id})")
            print("=" * 60)
            inicio_extracao = datetime.now()

            # Extrai TODOS os dados da API usando paginação
            print("Extraindo todas as contas a receber da API...")
            todas_contas = self.extract_dados_bling_paginado(
                limite_por_pagina=100,       # Máximo permitido pela API
                delay_entre_requests=0.35,   # Delay mínimo, com margem de segurança
                max_paginas=1000,            # Limite de segurança
                max_tentativas=3             # 3 tentativas antes de parar tudo
            )

            fim_extracao = datetime.now()
            tempo_extracao = fim_extracao - inicio_extracao

            if not todas_contas:
                print("❌ Nenhuma conta a receber foi extraída. Verificar API ou configurações.")
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

            print("\n🎉 Script de contas a receber executado com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️ Execução interrompida pelo usuário")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
            print("Script interrompido para análise do erro")
            print("Todos os dados extraídos até este ponto foram preservados")
            raise