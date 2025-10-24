# Responsável por: executar extração de contas a pagar

from config.database import create_schema_raw, create_all_tables
from extract.accounts_payable import ContasPagarExtractor

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
        extrator = ContasPagarExtractor()
        extrator.executar_extracao_completa()
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        print("Todos os dados extraídos até este ponto foram preservados")
        raise