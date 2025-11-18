# Responsável por: Orquestrar a transformação de contas a pagar de raw → fato_contas_pagar
# ⚠️ Antes de rodar esse, precisa garantir que a tabela de formas de pagamento esta populada! 

import time
from datetime import datetime
from config.database import create_schema_processed, create_all_tables
from config.settings import empresas
from transform.accounts_payable_dw import ContasPagarTransformer

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - TRANSFORMAÇÃO DE CONTAS A PAGAR
# =====================================================

if __name__ == "__main__":
    inicio = time.time()
    
    try:
        print("\n" + "=" * 70)
        print("🔄 TRANSFORMAÇÃO: CONTAS A PAGAR RAW → FATO_CONTAS_PAGAR")
        print("=" * 70)
        print("\n📋 PRÉ-REQUISITOS:")
        print("   ✓ raw.contas_pagar_raw deve estar populada")
        print("   ✓ processed.dim_tempo deve estar populada")
        print("   ✓ processed.dim_formas_pagamento deve estar populada (opcional)")
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
            transformer = ContasPagarTransformer(empresa_id)
            transformer.executar_transformacao_completa()
        
        fim = time.time()
        tempo_total = fim - inicio
        
        print(f"\n{'='*70}")
        print(f"✅ TRANSFORMAÇÃO DE TODAS AS EMPRESAS CONCLUÍDA!")
        print(f"⏱️  Tempo total: {tempo_total:.2f} segundos")
        print(f"{'='*70}")
        
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. Validar dados:")
        print(f"      SELECT * FROM processed.fato_contas_pagar LIMIT 10;")
        print(f"")
        print(f"   2. Verificar relacionamentos:")
        print(f"      SELECT ")
        print(f"        fcp.contas_pagar_id,")
        print(f"        fcp.valor,")
        print(f"        fcp.situacao,")
        print(f"        fcp.data_vencimento,")
        print(f"        dfp.descricao as forma_pagamento")
        print(f"      FROM processed.fato_contas_pagar fcp")
        print(f"      LEFT JOIN processed.dim_formas_pagamento dfp ON fcp.forma_pagamento_id = dfp.forma_pagamento_id")
        print(f"      LIMIT 10;")
        print(f"")
        print(f"   3. Análises financeiras:")
        print(f"      -- Total a pagar por situação")
        print(f"      SELECT situacao, COUNT(*) as qtd, SUM(valor) as total")
        print(f"      FROM processed.fato_contas_pagar")
        print(f"      GROUP BY situacao;")
        print(f"")
        print(f"   4. Executar novamente para processar novos registros")
        
    except KeyboardInterrupt:
        print("\n⚠️ Transformação interrompida pelo usuário")
        print("💾 Dados processados até este ponto foram preservados")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante transformação: {e}")
        print("Script interrompido para análise do erro")
        import traceback
        traceback.print_exc()
        raise