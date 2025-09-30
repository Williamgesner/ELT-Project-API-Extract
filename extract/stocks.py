# Responsável por: orquestrar a extração de produtos especificamente

from datetime import datetime
from core.base_extractor import BaseExtractor
from models.stocks_raw import EstoqueRaw
from config.settings import endpoints

# =====================================================
# 1. CRIANDO A CLASSE PARA EXTRAÇÃO DE ESTOQUE
# =====================================================

class EstoqueExtractor(BaseExtractor):
    
    """
    Extrator específico para estoque da API Bling
    Herda toda a lógica comum da BaseExtractor e adiciona só o que é específico de estoque
    """
    
    def __init__(self): # Essa é a função que inicializa a classe
        """
        Inicializa o extrator de estoque
        Passa para a classe pai (BaseExtractor) a URL e modelo específicos de estoque
        """
        super().__init__(endpoints['estoque'], EstoqueRaw)
    
    def executar_extracao_completa(self):
        """
        Executa o processo completo de extração de estoque
        """
        try:
            # Extrai TODOS os dados da API usando paginação
            print("Extraindo todo o estoque da API...")
            inicio_extracao = datetime.now()

            todo_estoque = self.extract_dados_bling_paginado(
                limite_por_pagina=100,       # Máximo permitido pela API
                delay_entre_requests=0.35,   # Delay mínimo, com margem de segurança
                max_paginas=1000,            # Limite de segurança
                max_tentativas=3             # 3 tentativas antes de parar tudo
            )

            fim_extracao = datetime.now()
            tempo_extracao = fim_extracao - inicio_extracao

            if not todo_estoque:
                print("❌ Nenhum estoque foi extraído. Verificar API ou configurações.")
                exit()
            
            print(f"\n📊 EXTRAÇÃO CONCLUÍDA:")
            print(f"⏱️ Tempo de extração: {tempo_extracao}")
            print(f"📈 Produtos extraídos: {len(todo_estoque)}")
            print(f"🚀 Velocidade: {len(todo_estoque)/tempo_extracao.total_seconds():.1f} estoque/segundo")

            # Preparar dados
            print("\n📝 Preparando dados para salvamento...")
            dados_para_salvar = []
            
            for estoque in todo_estoque:
                dados_formatados = {
                    'bling_id': estoque['id'],
                    'dados_json': estoque
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
            print(f"🚀 Performance geral: {len(todo_estoque)/tempo_total.total_seconds():.1f} estoque/segundo")
            
            # Eficiência do algoritmo
            if stats['total'] > 0:
                eficiencia = (stats['ignorados'] / stats['total']) * 100
                print(f"⚡ Eficiência: {eficiencia:.1f}% dos registros eram idênticos (evitou escritas desnecessárias)")

            print("\n🎉 Script de estoque executado com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️ Execução interrompida pelo usuário")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
            print("Script interrompido para análise do erro")
            print("Todos os dados extraídos até este ponto foram preservados")
            raise