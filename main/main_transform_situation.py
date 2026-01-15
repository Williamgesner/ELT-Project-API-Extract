# Responsável por: Orquestrar a transformação de situações de raw.situacoes_raw para processed.dim_situacao

import argparse # Para eu poder rodar somente da uma empresa específica 
import time
from datetime import datetime
from config.database import create_schema_processed, create_all_tables
from config.settings import empresas
from transform.situation_dw import SituacoesTransformer

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - TRANSFORMAÇÃO DE SITUAÇÕES
# =====================================================

if __name__ == "__main__":
    # Configurar argumentos de linha de comando
    parser = argparse.ArgumentParser(
        description='Transformar situações de raw para dimensão',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Processar todas as empresas
  python3 -m main.main_transform_situation
  
  # Processar apenas a empresa 01
  python3 -m main.main_transform_situation --empresa 1
  
  # Processar apenas a empresa 02
  python3 -m main.main_transform_situation --empresa 2
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
        print("🔄 TRANSFORMAÇÃO: SITUAÇÕES RAW → DIM_SITUACAO")
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
            transformer = SituacoesTransformer(empresa_id)
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
        print(f"   1. Validar dados: SELECT * FROM processed.dim_situacao LIMIT 10;")
        print(f"   2. Verificar qualidade dos dados transformados")
        print(f"   3. Verificar relacionamento com fato_pedidos")
        print(f"   4. Testar conexão no Power BI")
        
    except KeyboardInterrupt:
        print("\n⚠️ Transformação interrompida pelo usuário")
        print("💾 Dados processados até este ponto foram preservados")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante transformação: {e}")
        print("Script interrompido para análise do erro")
        import traceback
        traceback.print_exc()
        raise