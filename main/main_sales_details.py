# Responsável por: executar extração de detalhes completos de vendas (incluindo itens)

from extract.sales_details import VendasDetalhesExtractor

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - DETALHES DE VENDAS
# =====================================================

if __name__ == "__main__":
    try:
        print("\n💰 INICIANDO EXTRAÇÃO DE DETALHES DE VENDAS")
        print("=" * 70)
        print("Este processo busca os itens de cada pedido individualmente")
        print("=" * 70)
        
        # Criar o extrator de detalhes e executar
        extrator = VendasDetalhesExtractor()
        extrator.executar_extracao_detalhes(
            delay_entre_requests=0.35,  # Respeitar rate limit
            batch_size=100             # Commit a cada 100 vendas
        )
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
        print("💾 Os dados processados foram salvos automaticamente")
        print("Você pode continuar executando novamente este script")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        raise