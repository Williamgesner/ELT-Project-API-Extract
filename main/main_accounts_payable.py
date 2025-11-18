# Responsável por: executar extração de contas a pagar

from config.database import create_schema_raw, create_all_tables
from extract.accounts_payable import ContasPagarExtractor
from config.settings import empresas

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - CONTAS A PAGAR
# =====================================================

if __name__ == "__main__":
    try:
        # Cria o schema se não existir
        create_schema_raw()

        # Cria as tabelas
        create_all_tables()

        # Criar o extrator de contas a pagar e executar
        print("\n💰 INICIANDO EXTRAÇÃO DE CONTAS A PAGAR")
        print("=" * 50)
        
        for empresa_config in empresas:
            empresa_id = empresa_config['empresa_id']
            api_key = empresa_config['api_key']
            nome = empresa_config['nome']
            
            print(f"\n{'='*50}")
            print(f"🏢 Processando: {nome} (ID: {empresa_id})")
            print(f"{'='*50}")
            
            extrator = ContasPagarExtractor(api_key, empresa_id)
            extrator.executar_extracao_completa()
            
            print(f"\n✅ {nome} concluído!")
        
        print(f"\n{'='*50}")
        print("🎉 TODAS AS EMPRESAS PROCESSADAS!")
        print(f"{'='*50}")
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        print("Todos os dados extraídos até este ponto foram preservados")
        raise