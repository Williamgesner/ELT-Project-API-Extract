# Responsável por: orquestrar a extração de contatos especificamente

from datetime import datetime
from core.base_extractor import BaseExtractor
from models.sales_raw import VendasRaw
from config.settings import endpoints

# =====================================================
# 1. CRIANDO A CLASSE PARA EXTRAÇÃO DE VENDAS
# =====================================================

class VendasExtractor(BaseExtractor):
    
    """
    Extrator específico para vendas da API Bling
    Herda toda a lógica comum da BaseExtractor e adiciona só o que é específico de vendas
    """
    
    def __init__(self, api_key, empresa_id): # Essa é a função que inicializa a classe
        """
        Inicializa o extrator de vendas
        Passa para a classe pai (BaseExtractor) a URL e modelo específicos de vendas
        
        Args:
            api_key: Token de autenticação da API Bling
            empresa_id: ID da empresa na tabela dim_empresas
        """
        super().__init__(endpoints['vendas'], VendasRaw)
        self.empresa_id = empresa_id
        
        # Sobrescrever headers do base_extractor com a API key específica
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def executar_extracao_completa(self):
        """
        Executa o processo completo de extração de vendas
        """
        try:
            # Extrai TODOS os dados da API usando paginação
            print(f"Extraindo todos as vendas da API (Empresa ID: {self.empresa_id})...")
            inicio_extracao = datetime.now()

            todas_vendas = self.extract_dados_bling_paginado(
                limite_por_pagina=100,       # Máximo permitido pela API
                delay_entre_requests=0.35,   # Deley mínimo, com margem de segurança. Segundo documentação da API são 3 requisições por segundo
                max_paginas=1000,            # Limite de segurança
                max_tentativas=3             # 3 tentativas antes de parar tudo
            )

            fim_extracao = datetime.now()
            tempo_extracao = fim_extracao - inicio_extracao

            if not todas_vendas:
                print("❌ Nenhuma venda foi extraído. Verificar API ou configurações.")
                exit()
            
            print(f"\n📊 EXTRAÇÃO CONCLUÍDA:")
            print(f"⏱️ Tempo de extração: {tempo_extracao}")
            print(f"📈 Contatos extraídos: {len(todas_vendas)}")
            print(f"🚀 Velocidade: {len(todas_vendas)/tempo_extracao.total_seconds():.1f} vendas/segundo")

            # Preparar dados
            print("\n📝 Preparando dados para salvamento...")
            dados_para_salvar = []
            
            for venda in todas_vendas:
                dados_formatados = {
                    'bling_id': venda['id'],
                    'empresa_id': self.empresa_id,  
                    'dados_json': venda
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
            print(f"🚀 Performance geral: {len(todas_vendas)/tempo_total.total_seconds():.1f} vendas/segundo")
            
            # Eficiência do algoritmo
            if stats['total'] > 0:
                eficiencia = (stats['ignorados'] / stats['total']) * 100
                print(f"⚡ Eficiência: {eficiencia:.1f}% dos registros eram idênticos (evitou escritas desnecessárias)")

            print("\n🎉 Script de vendas executado com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️ Execução interrompida pelo usuário")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
            print("Script interrompido para análise do erro")
            print("Todos os dados extraídos até este ponto foram preservados")
            raise