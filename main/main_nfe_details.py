# Responsável por: executar enriquecimento de NFe (adicionar valorNota)

from extract.nfe_details import NFeDetalhesExtractor

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - ENRIQUECIMENTO DE NFe
# =====================================================

if __name__ == "__main__":
    try:
        print("\n💎 INICIANDO ENRIQUECIMENTO DE NFe")
        print("=" * 70)
        print("Este processo adiciona 'valorNota' em todas as NFe")
        print("=" * 70)
        
        # Criar o extrator de detalhes e executar
        extrator = NFeDetalhesExtractor()
        extrator.executar_enriquecimento_completo(
            delay_entre_requests=0.35,  # Respeitar rate limit (2.5 req/s)
            batch_size=100              # Commit a cada 100 NFe
        )
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
        print("💾 Os dados processados foram salvos automaticamente")
        print("Você pode continuar executando novamente este script")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        raise