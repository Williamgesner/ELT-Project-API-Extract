# Responsável por: executar todo o processo, criar schema, chamar o extrator
# ⚠️ Só executar esse Script depois que o Script de vendas_raw for executado ! 
# ⚠️ Só roda esse Script uma vez, ou quando alterar ou incluir novos canais !

from config.database import create_schema_raw, create_all_tables
from extract.channels import CanaisExtractor

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - CANAIS DE VENDA
# =====================================================

if __name__ == "__main__":
    try:
        # Criar schema se não existir
        create_schema_raw()
    
        # Criar tabelas
        create_all_tables()
    
        # Executar extração
        print("\n🏪 INICIANDO EXTRAÇÃO DE CANAIS DE VENDA")
        print("=" * 70)
        
        extrator_canais = CanaisExtractor()
        extrator_canais.executar_extracao_completa()
    
        print("\n✅ Script executado com sucesso!")
    
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO: {e}")
        raise