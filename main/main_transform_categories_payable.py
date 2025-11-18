# Responsável por: Orquestrar a transformação de categorias de contas a pagar de raw → dim_categorias_contas_pagar

import time
from datetime import datetime
from config.database import create_schema_processed, create_all_tables
from config.settings import empresas
from transform.categories_payable_dw import CategoriasContasPagarTransformer

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - TRANSFORMAÇÃO DE CATEGORIAS
# =====================================================

if __name__ == "__main__":
    inicio = time.time()
    
    try:
        print("\n" + "=" * 70)
        print("🔄 TRANSFORMAÇÃO: CATEGORIAS CONTAS A PAGAR RAW → DIM_CATEGORIAS_CONTAS_PAGAR")
        print("=" * 70)
        print("\n⚠️  IMPORTANTE:")
        print("   Este script deve ser executado ANTES de processar contas a pagar!")
        print("   Motivo: fato_contas_pagar pode depender de dim_categorias_contas_pagar")
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
            transformer = CategoriasContasPagarTransformer(empresa_id)
            transformer.executar_transformacao_completa()
        
        fim = time.time()
        tempo_total = fim - inicio
        
        print(f"\n{'='*70}")
        print(f"✅ TRANSFORMAÇÃO DE TODAS AS EMPRESAS CONCLUÍDA!")
        print(f"⏱️  Tempo total: {tempo_total:.2f} segundos")
        print(f"{'='*70}")
        
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. Validar dados:")
        print(f"      SELECT * FROM processed.dim_categorias_contas_pagar;")
        print(f"")
        print(f"   2. Verificar categorias por tipo:")
        print(f"      SELECT tipo_categoria, COUNT(*) as qtd")
        print(f"      FROM processed.dim_categorias_contas_pagar")
        print(f"      GROUP BY tipo_categoria;")
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