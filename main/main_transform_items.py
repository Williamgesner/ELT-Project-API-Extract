# Responsável por: Orquestrar a transformação de itens de pedidos para fato_itens_pedidos

import argparse # Para eu poder rodar somente da uma empresa específica 
import time
from datetime import datetime
from config.database import create_schema_processed, create_all_tables
from config.settings import empresas
from transform.items_dw import ItensTransformer

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - TRANSFORMAÇÃO DE ITENS
# =====================================================

if __name__ == "__main__":
    # Configurar argumentos de linha de comando
    parser = argparse.ArgumentParser(
        description='Transformar itens de pedidos para fato',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Processar todas as empresas
  python3 -m main.main_transform_items
  
  # Processar apenas a empresa 01
  python3 -m main.main_transform_items --empresa 1
  
  # Processar apenas a empresa 02
  python3 -m main.main_transform_items --empresa 2
        """
    )
    parser.add_argument(
        '--empresa',
        type=int,
        help='ID da empresa para processar (ex: 1 para empresa 01). Se não informado, processa todas as empresas.'
    )
    
    args = parser.parse_args()
    
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
        
        # Filtrar empresas se empresa_id foi especificado
        empresas_para_processar = empresas
        if args.empresa is not None:
            empresas_para_processar = [
                emp for emp in empresas 
                if emp['empresa_id'] == args.empresa
            ]
            
            if not empresas_para_processar:
                print(f"\n❌ ERRO: Empresa ID {args.empresa} não encontrada!")
                print(f"Empresas disponíveis:")
                for emp in empresas:
                    print(f"  • ID {emp['empresa_id']}: {emp['nome']}")
                raise ValueError(f"Empresa ID {args.empresa} não encontrada")
            
            print(f"\n🎯 Modo de teste: Processando apenas empresa ID {args.empresa}")
        
        # Loop para processar cada empresa
        for empresa_config in empresas_para_processar:
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
        if args.empresa is not None:
            print(f"✅ TRANSFORMAÇÃO DA EMPRESA {args.empresa} CONCLUÍDA!")
        else:
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