# Responsável por: executar extração de formas de pagamento

from config.database import create_schema_raw, create_all_tables
from extract.payment_methods import FormasPagamentosExtractor

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - FORMAS DE PAGAMENTO
# =====================================================

if __name__ == "__main__":
    try:
        # Cria o schema se não existir
        create_schema_raw()

        # Cria as tabelas
        create_all_tables()

        # Criar o extrator de formas de pagamento e executar
        print("\n💳 INICIANDO EXTRAÇÃO DE FORMAS DE PAGAMENTO")
        print("=" * 50)
        print("ℹ️ Usado para relacionar com contas a pagar/receber")
        print("💡 Relacionamento: contas.formaPagamento.id = payment_methods.bling_id")
        print("=" * 50)
        
        extrator = FormasPagamentosExtractor()
        extrator.executar_extracao_completa()
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        print("Todos os dados extraídos até este ponto foram preservados")
        raise