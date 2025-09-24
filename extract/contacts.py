# Responsável por: orquestrar a extração de contatos especificamente

from datetime import datetime
from core.base_extractor import BaseExtractor
from models.endpoint_contatos import ContatoRaw
from config.settings import base_url


# =====================================================
# 1. CRIANDO A CLASSE PARA EXTRAÇÃO 
# =====================================================

class ContatosExtractor(BaseExtractor):
    
    """
    Extrator específico para contatos da API Bling
    Herda toda a lógica comum da BaseExtractor e adiciona só o que é específico de contatos
    """
    
    def __init__(self):
        """
        Inicializa o extrator de contatos
        Passa para a classe pai (BaseExtractor) a URL e modelo específicos de contatos
        """
        super().__init__(base_url, ContatoRaw)
    
    def executar_extracao_completa(self):
        """
        Executa o processo completo de extração de contatos
        """
        try:
            # Extrai TODOS os dados da API usando paginação
            print("Extraindo todos os contatos da API...")
            inicio_extracao = datetime.now()

            todos_contatos = self.extract_dados_bling_paginado(
                limite_por_pagina=100,       # Máximo permitido pela API
                delay_entre_requests=0.35,   # Deley mínimo, com margem de segurança. Segundo documentação da API são 3 requisições por segundo
                max_paginas=1000,            # Limite de segurança
                max_tentativas=3             # 3 tentativas antes de parar tudo
            )

            fim_extracao = datetime.now()
            tempo_extracao = fim_extracao - inicio_extracao

            if not todos_contatos:
                print("❌ Nenhum contato foi extraído. Verificar API ou configurações.")
                exit()
            
            print(f"\n📊 EXTRAÇÃO CONCLUÍDA:")
            print(f"⏱️ Tempo de extração: {tempo_extracao}")
            print(f"📈 Contatos extraídos: {len(todos_contatos)}")
            print(f"🚀 Velocidade: {len(todos_contatos)/tempo_extracao.total_seconds():.1f} contatos/segundo")

            # Preparar dados
            print("\n📝 Preparando dados para salvamento...")
            dados_para_salvar = []
            
            for contato in todos_contatos:
                dados_formatados = {
                    'bling_id': contato['id'],
                    'dados_json': contato
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
            print(f"🚀 Performance geral: {len(todos_contatos)/tempo_total.total_seconds():.1f} contatos/segundo")
            
            # Eficiência do algoritmo
            if stats['total'] > 0:
                eficiencia = (stats['ignorados'] / stats['total']) * 100
                print(f"⚡ Eficiência: {eficiencia:.1f}% dos registros eram idênticos (evitou escritas desnecessárias)")

            print("\n🎉 Script executado com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️ Execução interrompida pelo usuário")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
            print("Script interrompido para análise do erro")
            print("Todos os dados extraídos até este ponto foram preservados")
            raise

