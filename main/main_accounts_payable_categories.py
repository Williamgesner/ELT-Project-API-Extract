# Responsável por: executar extração de categorias

import argparse # Para eu poder rodar somente da uma empresa específica 
from config.database import create_schema_raw, create_all_tables
from extract.accounts_payable_categories import CategoriasExtractor
from config.settings import empresas  # Importa lista de empresas

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - CATEGORIAS
# =====================================================

if __name__ == "__main__":
    # Configurar argumentos de linha de comando
    parser = argparse.ArgumentParser(
        description='Extrair categorias do Bling',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Processar todas as empresas
  python3 -m main.main_categories
  
  # Processar apenas a empresa 01
  python3 -m main.main_categories --empresa 1
  
  # Processar apenas a empresa 02
  python3 -m main.main_categories --empresa 2
        """
    )
    parser.add_argument(
        '--empresa',
        type=int,
        help='ID da empresa para processar (ex: 1 para empresa 01). Se não informado, processa todas as empresas.'
    )
    
    args = parser.parse_args()
    
    try:
        # Cria o schema se não existir
        create_schema_raw()

        # Cria as tabelas
        create_all_tables()

        # Criar o extrator de categorias e executar
        print("\n📂 INICIANDO EXTRAÇÃO DE CATEGORIAS")
        print("=" * 50)

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

        for empresa_config in empresas_para_processar:                     # Loop pelas que TÊM API key
            empresa_id = empresa_config['empresa_id']
            api_key = empresa_config['api_key']
            nome = empresa_config['nome']
            
            print(f"\n{'='*50}")
            print(f"🏢 Processando: {nome} (ID: {empresa_id})")
            print(f"{'='*50}")
            
            # Criar extrator com api_key e empresa_id específicos
            extrator = CategoriasExtractor(api_key, empresa_id)
            extrator.executar_extracao_completa()
            
            print(f"\n✅ {nome} concluído!")

        print(f"\n{'='*50}")
        if args.empresa is not None:
            print(f"🎉 EMPRESA {args.empresa} PROCESSADA!")
        else:
            print("🎉 TODAS AS EMPRESAS PROCESSADAS!")
        print(f"{'='*50}")

        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        print("Todos os dados extraídos até este ponto foram preservados")
        raise