# Responsável por: Orquestrar a transformação de contas a receber de raw → fato_contas_receber
# ⚠️ Antes de rodar esse, precisa garantir que a tabela de formas de pagamento está populada! 

import argparse # Para eu poder rodar somente da uma empresa específica 
import time
from datetime import datetime
from config.database import create_schema_processed, create_all_tables
from config.settings import empresas
from transform.accounts_receivable_dw import ContasReceberTransformer

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - TRANSFORMAÇÃO DE CONTAS A RECEBER
# =====================================================

if __name__ == "__main__":
    # Configurar argumentos de linha de comando
    parser = argparse.ArgumentParser(
        description='Transformar contas a receber de raw para fato',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Processar todas as empresas
  python3 -m main.main_transform_accounts_receivable
  
  # Processar apenas a empresa 01
  python3 -m main.main_transform_accounts_receivable --empresa 1
  
  # Processar apenas a empresa 02
  python3 -m main.main_transform_accounts_receivable --empresa 2
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
        print("🔄 TRANSFORMAÇÃO: CONTAS A RECEBER RAW → FATO_CONTAS_RECEBER")
        print("=" * 70)
        print("\n📋 PRÉ-REQUISITOS:")
        print("   ✓ raw.contas_receber_raw deve estar populada")
        print("   ✓ processed.dim_tempo deve estar populada")
        print("   ✓ processed.dim_formas_pagamento deve estar populada (opcional)")
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
            transformer = ContasReceberTransformer(empresa_id)
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