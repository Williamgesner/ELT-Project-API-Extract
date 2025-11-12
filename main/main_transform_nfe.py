# Responsável por: Orquestrar a transformação de NFe de raw.nfe_raw para processed.fato_nfe

from datetime import datetime
from config.database import create_schema_processed, create_all_tables
from transform.nfe_dw import NFeTransformer

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - TRANSFORMAÇÃO DE NFe
# =====================================================

if __name__ == "__main__":
    try:
        print("\n" + "=" * 70)
        print("🔄 TRANSFORMAÇÃO: NFe RAW → FATO_NFe")
        print("=" * 70)
        print("\n📋 PRÉ-REQUISITOS:")
        print("   ✓ raw.nfe_raw deve estar populada (com entrada E saída)")
        print("   ✓ raw.vendas_raw deve estar populada (para relacionamento)")
        print("   ✓ NFe devem estar enriquecidas (com valorNota e valorFrete)")
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
        transformer = NFeTransformer()
        transformer.executar_transformacao_completa()
        
        fim = datetime.now()
        tempo_total = fim - inicio
        
        print(f"\n{'='*70}")
        print(f"✅ TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO!")
        print(f"⏱️  Tempo total: {tempo_total}")
        print(f"{'='*70}")
        
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. Validar dados:")
        print(f"      SELECT * FROM processed.fato_nfe LIMIT 10;")
        print(f"")
        print(f"   2. Verificar distribuição por tipo:")
        print(f"      SELECT ")
        print(f"        tipo,")
        print(f"        COUNT(*) as qtd,")
        print(f"        SUM(valor_nf) as valor_total")
        print(f"      FROM processed.fato_nfe")
        print(f"      GROUP BY tipo;")
        print(f"")
        print(f"   3. Verificar NFe com pedido:")
        print(f"      SELECT ")
        print(f"        tipo,")
        print(f"        COUNT(*) as total,")
        print(f"        SUM(CASE WHEN numero_pedido IS NOT NULL THEN 1 ELSE 0 END) as com_pedido")
        print(f"      FROM processed.fato_nfe")
        print(f"      GROUP BY tipo;")
        print(f"")
        print(f"   4. Análise financeira:")
        print(f"      SELECT ")
        print(f"        tipo,")
        print(f"        situacao,")
        print(f"        COUNT(*) as qtd,")
        print(f"        AVG(valor_nf) as ticket_medio,")
        print(f"        SUM(valor_nf) as valor_total")
        print(f"      FROM processed.fato_nfe")
        print(f"      WHERE valor_nf IS NOT NULL")
        print(f"      GROUP BY tipo, situacao")
        print(f"      ORDER BY tipo, valor_total DESC;")
        
    except KeyboardInterrupt:
        print("\n⚠️ Transformação interrompida pelo usuário")
        print("💾 Dados processados até este ponto foram preservados")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante transformação: {e}")
        print("Script interrompido para análise do erro")
        import traceback
        traceback.print_exc()
        raise