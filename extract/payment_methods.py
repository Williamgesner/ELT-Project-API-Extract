# Responsável por: extrair formas de pagamento da API Bling

from datetime import datetime
from core.base_extractor import BaseExtractor
from models.payment_methods_raw import FormasPagamentosRaw
from config.settings import endpoints

# =====================================================
# 1. CRIANDO A CLASSE PARA EXTRAÇÃO DE FORMAS DE PAGAMENTO
# =====================================================

class FormasPagamentosExtractor(BaseExtractor):
    
    """
    Extrator específico para formas de pagamento da API Bling
    Herda toda a lógica comum da BaseExtractor e adiciona só o que é específico
    """
    
    def __init__(self, api_key, empresa_id):
        """
        Inicializa o extrator de formas de pagamento
        Passa para a classe pai (BaseExtractor) a URL e modelo específicos
        
        Args:
            api_key: Token de autenticação da API Bling
            empresa_id: ID da empresa na tabela dim_empresas
        """
        super().__init__(endpoints['formas_pagamentos'], FormasPagamentosRaw)
        self.empresa_id = empresa_id
        
        # Sobrescrever headers do base_extractor com a API key específica
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def executar_extracao_completa(self):
        """
        Executa o processo completo de extração de formas de pagamento
        """
        try:
            print(f"\n💳 EXTRAÇÃO: FORMAS DE PAGAMENTO (Empresa ID: {self.empresa_id})")
            print("=" * 60)
            inicio_extracao = datetime.now()

            # Extrai TODOS os dados da API usando paginação
            print("Extraindo todas as formas de pagamento da API...")
            todas_formas = self.extract_dados_bling_paginado(
                limite_por_pagina=100,       # Máximo permitido pela API
                delay_entre_requests=0.35,   # Delay mínimo, com margem de segurança
                max_paginas=1000,            # Limite de segurança
                max_tentativas=3             # 3 tentativas antes de parar tudo
            )

            fim_extracao = datetime.now()
            tempo_extracao = fim_extracao - inicio_extracao

            if not todas_formas:
                print("❌ Nenhuma forma de pagamento foi extraída. Verificar API ou configurações.")
                return
            
            print(f"\n📊 EXTRAÇÃO CONCLUÍDA:")
            print(f"⏱️ Tempo de extração: {tempo_extracao}")
            print(f"📈 Formas de pagamento extraídas: {len(todas_formas)}")
            print(f"🚀 Velocidade: {len(todas_formas)/tempo_extracao.total_seconds():.1f} formas/segundo")

            # Preparar dados - APENAS JSON PURO
            print("\n📝 Preparando dados para salvamento...")
            dados_para_salvar = []
            
            for forma in todas_formas:
                dados_formatados = {
                    'bling_id': forma['id'],
                    'empresa_id': self.empresa_id, 
                    'dados_json': forma  # JSON completo e puro
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
            print(f"🚀 Performance geral: {len(todas_formas)/tempo_total.total_seconds():.1f} formas/segundo")
            
            # Eficiência do algoritmo
            if stats['total'] > 0:
                eficiencia = (stats['ignorados'] / stats['total']) * 100
                print(f"⚡ Eficiência: {eficiencia:.1f}% dos registros eram idênticos (evitou escritas desnecessárias)")

            # Estatísticas de formas de pagamento
            self._exibir_estatisticas_formas(todas_formas)

            print("\n🎉 Script de formas de pagamento executado com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️ Execução interrompida pelo usuário")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
            print("Script interrompido para análise do erro")
            print("Todos os dados extraídos até este ponto foram preservados")
            raise
    
    def _exibir_estatisticas_formas(self, formas):
        """
        Exibe estatísticas das formas de pagamento extraídas
        """
        print(f"\n📊 ESTATÍSTICAS DAS FORMAS DE PAGAMENTO:")
        
        # Separar por tipo
        tipos = {}
        ativas = 0
        fixas = 0
        
        for forma in formas:
            # Tipo de pagamento
            tipo = forma.get('tipoPagamento', 'Desconhecido')
            tipos[tipo] = tipos.get(tipo, 0) + 1
            
            # Situação (ativa?)
            if forma.get('situacao') == 1:
                ativas += 1
            
            # Fixa?
            if forma.get('fixa'):
                fixas += 1
        
        print(f"   • Total de formas: {len(formas)}")
        print(f"   • Ativas: {ativas}")
        print(f"   • Fixas: {fixas}")
        
        if tipos:
            print(f"   • Por tipo de pagamento:")
            for tipo, qtd in sorted(tipos.items()):
                print(f"      - Tipo {tipo}: {qtd}")
        
        # Exemplos
        print(f"\n📋 Exemplos de formas cadastradas:")
        for forma in formas[:5]:
            descricao = forma.get('descricao', 'Sem descrição')
            situacao = '✅ Ativa' if forma.get('situacao') == 1 else '❌ Inativa'
            print(f"   • {descricao} ({situacao})")