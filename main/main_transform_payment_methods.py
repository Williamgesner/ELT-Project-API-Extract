# Responsável por: Orquestrar a transformação de formas de pagamento de raw → dim_formas_pagamento

from datetime import datetime
from config.database import create_schema_processed, create_all_tables
from transform.methods_accounts_payable_dw import FormasPagamentoTransformer

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - TRANSFORMAÇÃO DE FORMAS DE PAGAMENTO
# =====================================================

if __name__ == "__main__":
    try:
        print("\n" + "=" * 70)
        print("🔄 TRANSFORMAÇÃO: FORMAS DE PAGAMENTO RAW → DIM_FORMAS_PAGAMENTO")
        print("=" * 70)
        print("\n⚠️  IMPORTANTE:")
        print("   Este script deve ser executado ANTES de processar contas a pagar!")
        print("   Motivo: fato_contas_pagar depende de dim_formas_pagamento")
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
        transformer = FormasPagamentoTransformer()
        transformer.executar_transformacao_completa()
        
        fim = datetime.now()
        tempo_total = fim - inicio
        
        print(f"\n{'='*70}")
        print(f"✅ TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO!")
        print(f"⏱️  Tempo total: {tempo_total}")
        print(f"{'='*70}")
        
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. Validar dados:")
        print(f"      SELECT * FROM processed.dim_formas_pagamento;")
        print(f"")
        print(f"   2. Verificar formas de pagamento mais usadas:")
        print(f"      SELECT forma_pagamento, COUNT(*) as qtd")
        print(f"      FROM processed.dim_formas_pagamento")
        print(f"      GROUP BY forma_pagamento;")
        print(f"")
        print(f"   3. ✅ Agora você pode processar contas a pagar:")
        print(f"      python main_transform_accounts_payable.py")
        
    except KeyboardInterrupt:
        print("\n⚠️ Transformação interrompida pelo usuário")
        print("💾 Dados processados até este ponto foram preservados")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante transformação: {e}")
        print("Script interrompido para análise do erro")
        import traceback
        traceback.print_exc()
        raise