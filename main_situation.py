# Responsável por: executar todo o processo, criar schema, chamar o extrator
# ⚠️ Só executar esse Scrip depois que o Scrip de vendas_raw for executado ! 
# ⚠️ Só roda esse Script uma vez, ou quando alterar ou incluir novas situações !

from config.database import create_schema_raw, create_all_tables
from extract.situation import SituacoesExtractor

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - SITUAÇÃO
# =====================================================

if __name__ == "__main__":
    try:
         # Criar schema se não existir
        create_schema_raw()
    
        # Criar tabelas
        create_all_tables()
    
        # Executar extração
        extrator_situacao = SituacoesExtractor()
        extrator_situacao.executar_extracao_completa()
    
        print("\n✅ Script executado com sucesso!")
    
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO: {e}")
        raise
