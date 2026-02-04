# Responsável por: executar transformação de natureza_operacao_raw → dim_natureza_operacao

import argparse # Para eu poder rodar somente da uma empresa específica 
import time
from config.database import create_schema_processed, create_all_tables
from config.settings import empresas
from transform.nature_operation_dw import NaturezaOperacaoTransformer

# =============================================================
# 1. EXECUÇÃO DO SCRIPT - TRANSFORMAÇÃO NATUREZA DE OPERAÇÃO
# =============================================================

if __name__ == "__main__":
    # Configurar argumentos de linha de comando
    parser = argparse.ArgumentParser(
        description='Transformar natureza de operação de raw para dimensão',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Processar todas as empresas
  python3 -m main.main_transform_nature_operation
  
  # Processar apenas a empresa 01
  python3 -m main.main_transform_nature_operation --empresa 1
  
  # Processar apenas a empresa 02
  python3 -m main.main_transform_nature_operation --empresa 2
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
        # Cria o schema processed se não existir
        create_schema_processed()

        # Cria as tabelas
        create_all_tables()
        
        print("\n🔄 INICIANDO TRANSFORMAÇÃO DE NATUREZA DE OPERAÇÃO")
        print("=" * 70)
        print("Este processo transforma dados RAW → DW")
        print("=" * 70)
        
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
            
            print(f"🎯 Modo de teste: Processando apenas empresa ID {args.empresa}")
        
        # Loop para processar cada empresa
        for empresa_config in empresas_para_processar:
            empresa_id = empresa_config['empresa_id']
            nome = empresa_config['nome']
            
            print(f"\n🏢 Transformando: {nome} (ID: {empresa_id})")
            print("-" * 70)
            
            # Criar o transformer e executar
            transformer = NaturezaOperacaoTransformer(empresa_id)
            transformer.executar_transformacao_completa()
        
        fim = time.time()
        tempo_total = fim - inicio
        print(f"\n✅ Transformação concluída em {tempo_total:.2f} segundos")
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        raise