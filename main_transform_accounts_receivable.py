# Responsável por: Orquestrar a transformação de contas a receber de raw → fato_contas_receber
# ⚠️ Antes de rodar esse, precisa garantir que a tabela de formas de pagamento está populada! 

from datetime import datetime
from config.database import create_schema_processed, create_all_tables
from transform.accounts_receivable_dw import ContasReceberTransformer

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - TRANSFORMAÇÃO DE CONTAS A RECEBER
# =====================================================

if __name__ == "__main__":
    try:
        print("\n" + "=" * 70)
        print("🔄 TRANSFORMAÇÃO: CONTAS A RECEBER RAW → FATO_CONTAS_RECEBER")
        print("=" * 70)
        print("\n📋 PRÉ-REQUISITOS:")
        print("   ✓ raw.contas_receber_raw deve estar populada")
        print("   ✓ processed.dim_tempo deve estar populada")
        print("   ✓ processed.dim_formas_pagamento deve estar populada (opcional)")
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
        transformer = ContasReceberTransformer()
        transformer.executar_transformacao_completa()
        
        fim = datetime.now()
        tempo_total = fim - inicio
        
        print(f"\n{'='*70}")
        print(f"✅ TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO!")
        print(f"⏱️  Tempo total: {tempo_total}")
        print(f"{'='*70}")
        
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. Validar dados:")
        print(f"      SELECT * FROM processed.fato_contas_receber LIMIT 10;")
        print(f"")
        print(f"   2. Executar novamente para processar novos registros")
        
    except KeyboardInterrupt:
        print("\n⚠️ Transformação interrompida pelo usuário")
        print("💾 Dados processados até este ponto foram preservados")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante transformação: {e}")
        print("Script interrompido para análise do erro")
        import traceback
        traceback.print_exc()
        raise