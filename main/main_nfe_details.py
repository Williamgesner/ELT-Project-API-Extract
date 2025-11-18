# Responsável por: executar enriquecimento de NFe (adicionar valorNota)

import time
from config.settings import empresas
from extract.nfe_details import NFeDetalhesExtractor

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - ENRIQUECIMENTO DE NFe
# =====================================================

if __name__ == "__main__":
    inicio = time.time()
    
    try:
        print("\n💎 INICIANDO ENRIQUECIMENTO DE NFe")
        print("=" * 70)
        print("Este processo adiciona 'valorNota' em todas as NFe")
        print("=" * 70)
        
        # Loop para processar cada empresa
        for empresa_config in empresas:
            empresa_id = empresa_config['empresa_id']
            api_key = empresa_config['api_key']
            nome = empresa_config['nome']
            
            print(f"\n🏢 Processando: {nome} (ID: {empresa_id})")
            print("-" * 70)
            
            # Criar o extrator de detalhes e executar
            extrator = NFeDetalhesExtractor(api_key, empresa_id)
            extrator.executar_enriquecimento_completo(
                delay_entre_requests=0.35,  # Respeitar rate limit (2.5 req/s)
                batch_size=100              # Commit a cada 100 NFe
            )
        
        fim = time.time()
        tempo_total = fim - inicio
        print(f"\n✅ Enriquecimento concluído em {tempo_total:.2f} segundos")
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
        print("💾 Os dados processados foram salvos automaticamente")
        print("Você pode continuar executando novamente este script")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        raise