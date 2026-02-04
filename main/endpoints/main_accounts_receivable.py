# Responsável por: executar extração de contas a receber

import argparse # Para eu poder rodar somente da uma empresa específica 
import time
from config.database import create_schema_raw, create_all_tables
from config.settings import empresas
from extract.accounts_receivable import ContasReceberExtractor

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - CONTAS A RECEBER
# =====================================================

if __name__ == "__main__":
    # Configurar argumentos de linha de comando
    parser = argparse.ArgumentParser(
        description='Extrair contas a receber do Bling',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Processar todas as empresas
  python3 -m main.main_accounts_receivable
  
  # Processar apenas a empresa 01
  python3 -m main.main_accounts_receivable --empresa 1
  
  # Processar apenas a empresa 02
  python3 -m main.main_accounts_receivable --empresa 2
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
        # Cria o schema se não existir
        create_schema_raw()

        # Cria as tabelas
        create_all_tables()

        print("\n💵 INICIANDO EXTRAÇÃO DE CONTAS A RECEBER")
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
            api_key = empresa_config['api_key']
            nome = empresa_config['nome']
            
            print(f"\n🏢 Processando: {nome} (ID: {empresa_id})")
            print("-" * 70)
            
            # Criar o extrator de contas a receber e executar
            extrator = ContasReceberExtractor(api_key, empresa_id)
            extrator.executar_extracao_completa()
            
        fim = time.time()
        tempo_total = fim - inicio
        print(f"\n✅ Extração de todas as empresas concluída em {tempo_total:.2f} segundos")
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        print("Todos os dados extraídos até este ponto foram preservados")
        raise