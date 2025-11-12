# Responsável por: executar transformação de natureza_operacao_raw → dim_natureza_operacao

from config.database import create_schema_processed, create_all_tables
from transform.nature_operation_dw import NaturezaOperacaoTransformer

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - TRANSFORMAÇÃO
# =====================================================

if __name__ == "__main__":
    try:
        # Cria o schema processed se não existir
        create_schema_processed()

        # Cria as tabelas
        create_all_tables()
        
        print("\n🔄 INICIANDO TRANSFORMAÇÃO DE NATUREZA DE OPERAÇÃO")
        print("=" * 70)
        print("Este processo transforma dados RAW → DW")
        print("=" * 70)
        
        # Criar o transformer e executar
        transformer = NaturezaOperacaoTransformer()
        transformer.executar_transformacao_completa()
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        raise