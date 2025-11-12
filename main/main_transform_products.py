# Responsável por: Orquestrar a transformação de produtos de raw → dim_produtos

from datetime import datetime
from config.database import create_schema_processed, create_all_tables
from transform.products_dw import ProdutosTransformer

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - TRANSFORMAÇÃO DE PRODUTOS
# =====================================================

if __name__ == "__main__":
    try:
        print("\n" + "=" * 70)
        print("🔄 TRANSFORMAÇÃO: PRODUTOS RAW → DIM_PRODUTOS")
        print("=" * 70)
        
        inicio = datetime.now()
        
        # Criar schema processed se não existir
        print("\n📂 Verificando schema processed...")
        create_schema_processed()
        
        # Criar tabelas (se não existirem)
        print("📋 Verificando tabelas...")
        create_all_tables()
        
        # Criar e executar o transformer
        print("\n🚀 Iniciando transformação...")
        transformer = ProdutosTransformer()
        transformer.executar_transformacao_completa()
        
        fim = datetime.now()
        tempo_total = fim - inicio
        
        print(f"\n{'='*70}")
        print(f"✅ TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO!")
        print(f"⏱️  Tempo total: {tempo_total}")
        print(f"{'='*70}")
        
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. Validar dados: SELECT * FROM processed.dim_produtos LIMIT 10;")
        print(f"   2. Verificar bicicletas: SELECT * FROM processed.dim_produtos WHERE aro IS NOT NULL;")
        print(f"   3. Executar novamente para processar novos registros")
        
    except KeyboardInterrupt:
        print("\n⚠️ Transformação interrompida pelo usuário")
        print("💾 Dados processados até este ponto foram preservados")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante transformação: {e}")
        print("Script interrompido para análise do erro")
        import traceback
        traceback.print_exc()
        raise