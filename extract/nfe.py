# Responsável por: extrair NFe (apenas lista, sem detalhes)

from datetime import datetime
from core.base_extractor import BaseExtractor
from models.nfe_raw import NFeRaw
from config.settings import endpoints

# =====================================================
# 1. EXTRATOR SIMPLES DE NFe (SEM ENRIQUECIMENTO)
# =====================================================

class NFeExtractor(BaseExtractor):
    
    """
    Extrator simples de NFe
    - Extrai apenas lista da API /nfe
    - NÃO busca detalhes individuais
    - Relacionamento com pedidos via vendas_raw.notaFiscal.id
    """
    
    def __init__(self):
        """
        Inicializa o extrator de NFe
        """
        super().__init__(endpoints['nfe'], NFeRaw)
    
    def executar_extracao_completa(self):
        """
        Executa o processo completo de extração de NFe (lista apenas)
        """
        try:
            print("\n📄 EXTRAÇÃO: NOTAS FISCAIS ELETRÔNICAS (NFe)")
            print("=" * 60)
            inicio_extracao = datetime.now()

            # Extrai TODOS os dados da API usando paginação
            print("Extraindo todas as NFe da API...")
            todas_nfe = self.extract_dados_bling_paginado(
                limite_por_pagina=100,       # Máximo permitido pela API
                delay_entre_requests=0.35,   # Delay mínimo, com margem de segurança
                max_paginas=1000,            # Limite de segurança
                max_tentativas=3             # 3 tentativas antes de parar tudo
            )

            fim_extracao = datetime.now()
            tempo_extracao = fim_extracao - inicio_extracao

            if not todas_nfe:
                print("❌ Nenhuma NFe foi extraída. Verificar API ou configurações.")
                return
            
            print(f"\n📊 EXTRAÇÃO CONCLUÍDA:")
            print(f"⏱️ Tempo de extração: {tempo_extracao}")
            print(f"📈 NFe extraídas: {len(todas_nfe)}")
            print(f"🚀 Velocidade: {len(todas_nfe)/tempo_extracao.total_seconds():.1f} notas/segundo")

            # Preparar dados - APENAS JSON PURO (da lista)
            print("\n📝 Preparando dados para salvamento...")
            dados_para_salvar = []
            
            for nfe in todas_nfe:
                dados_formatados = {
                    'bling_id': nfe['id'],
                    'dados_json': nfe  # JSON completo da lista (sem enriquecimento)
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
            print(f"🚀 Performance geral: {len(todas_nfe)/tempo_total.total_seconds():.1f} notas/segundo")
            
            # Eficiência do algoritmo
            if stats['total'] > 0:
                eficiencia = (stats['ignorados'] / stats['total']) * 100
                print(f"⚡ Eficiência: {eficiencia:.1f}% dos registros eram idênticos (evitou escritas desnecessárias)")

            # Estatísticas de NFe
            self._exibir_estatisticas_nfe(todas_nfe)

            print("\n🎉 Script de NFe executado com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️ Execução interrompida pelo usuário")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
            print("Script interrompido para análise do erro")
            print("Todos os dados extraídos até este ponto foram preservados")
            raise
    
    def _exibir_estatisticas_nfe(self, notas):
        """
        Exibe estatísticas das NFe extraídas
        """
        print(f"\n📊 ESTATÍSTICAS DAS NFe:")
        
        # Separar por situação (se campo existir na lista)
        situacoes = {}
        for nfe in notas:
            situacao = nfe.get('situacao', 'Desconhecida')
            situacoes[situacao] = situacoes.get(situacao, 0) + 1
        
        print(f"   • Total de NFe: {len(notas)}")
        
        # Mostrar distribuição por situação
        if situacoes:
            print(f"   • Distribuição por situação:")
            for situacao, qtd in sorted(situacoes.items()):
                print(f"      - Situação {situacao}: {qtd}")
        
        # Informação importante sobre relacionamento
        print(f"\n💡 RELACIONAMENTO:")
        print(f"   • NFe relaciona com Pedidos via vendas_raw.notaFiscal.id")
        print(f"   • Use: vendas_raw.dados_json->'notaFiscal'->>'id' = nfe_raw.bling_id")