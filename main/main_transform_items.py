# Responsável por: Orquestrar a transformação de itens de pedidos para fato_itens_pedidos

import time
from datetime import datetime
from config.database import create_schema_processed, create_all_tables
from config.settings import empresas
from transform.items_dw import ItensTransformer

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - TRANSFORMAÇÃO DE ITENS
# =====================================================

if __name__ == "__main__":
    inicio = time.time()
    
    try:
        print("\n" + "=" * 70)
        print("🔄 TRANSFORMAÇÃO: ITENS DE PEDIDOS → FATO_ITENS_PEDIDOS")
        print("=" * 70)
        print("\n📋 PRÉ-REQUISITOS:")
        print("   ✓ raw.vendas_raw deve estar populada")
        print("   ✓ processed.fato_pedidos deve estar populada")
        print("   ✓ processed.dim_produtos deve estar populada")
        print("=" * 70)
        
        # Criar schema processed se não existir
        print("\n📂 Verificando schema processed...")
        create_schema_processed()
        
        # Criar tabelas (se não existirem)
        print("📋 Verificando tabelas...")
        create_all_tables()
        
        # Loop para processar cada empresa
        for empresa_config in empresas:
            empresa_id = empresa_config['empresa_id']
            nome = empresa_config['nome']
            
            print(f"\n{'='*70}")
            print(f"🏢 Transformando: {nome} (ID: {empresa_id})")
            print(f"{'='*70}")
            
            # Criar e executar o transformer
            transformer = ItensTransformer(empresa_id)
            transformer.executar_transformacao_completa()
        
        fim = time.time()
        tempo_total = fim - inicio
        
        print(f"\n{'='*70}")
        print(f"✅ TRANSFORMAÇÃO DE TODAS AS EMPRESAS CONCLUÍDA!")
        print(f"⏱️  Tempo total: {tempo_total:.2f} segundos")
        print(f"{'='*70}")
        
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. Validar dados:")
        print(f"      SELECT * FROM processed.fato_itens_pedidos LIMIT 10;")
        print(f"")
        print(f"   2. Verificar relacionamentos:")
        print(f"      SELECT ")
        print(f"        fp.numero_pedido,")
        print(f"        dp.descricao_produto,")
        print(f"        fi.quantidade,")
        print(f"        fi.preco_total")
        print(f"      FROM processed.fato_itens_pedidos fi")
        print(f"      JOIN processed.fato_pedidos fp ON fi.pedido_id = fp.pedido_id")
        print(f"      LEFT JOIN processed.dim_produtos dp ON fi.produto_id = dp.produto_id")
        print(f"      LIMIT 10;")
        print(f"")
        print(f"   3. Verificar totais:")
        print(f"      -- Total de itens por pedido deve bater")
        print(f"      SELECT ")
        print(f"        fp.pedido_id,")
        print(f"        fp.valor_total as valor_pedido,")
        print(f"        SUM(fi.preco_total) as soma_itens")
        print(f"      FROM processed.fato_pedidos fp")
        print(f"      JOIN processed.fato_itens_pedidos fi ON fp.pedido_id = fi.pedido_id")
        print(f"      GROUP BY fp.pedido_id, fp.valor_total")
        print(f"      HAVING ABS(fp.valor_total - SUM(fi.preco_total)) > 0.01")
        print(f"      LIMIT 5;")
        print(f"")
        print(f"   4. Análises possíveis:")
        print(f"      • Produtos mais vendidos (quantidade)")
        print(f"      • Produtos mais vendidos (faturamento)")
        print(f"      • Ticket médio por produto")
        print(f"      • Mix de produtos nos pedidos")
        
    except KeyboardInterrupt:
        print("\n⚠️ Transformação interrompida pelo usuário")
        print("💾 Dados processados até este ponto foram preservados")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante transformação: {e}")
        print("Script interrompido para análise do erro")
        print("\n🔍 DICAS DE TROUBLESHOOTING:")
        print("   • Verifique se fato_pedidos está populada:")
        print("     SELECT COUNT(*) FROM processed.fato_pedidos;")
        print("   • Verifique se vendas_raw tem itens:")
        print("     SELECT COUNT(*) FROM raw.vendas_raw")
        print("     WHERE jsonb_array_length(dados_json->'itens') > 0;")
        print("   • Verifique se dim_produtos está populada:")
        print("     SELECT COUNT(*) FROM processed.dim_produtos;")
        import traceback
        traceback.print_exc()
        raise