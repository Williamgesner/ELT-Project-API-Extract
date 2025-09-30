# Responsável por: executar todo o processo, criar schema, chamar o extrator

from config.database import create_schema_raw, create_all_tables
from extract.sales import VendasExtractor

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - VENDAS
# =====================================================

if __name__ == "__main__":  # Se o arquivo for executado diretamente, não execute o código abaixo. Isso é importante para evitar que o código seja executado quando o arquivo for importado.
    try:
        # Cria o schema se não existir
        create_schema_raw()

        # Cria as tabelas
        create_all_tables()

        # Criar o extrator de vendas e executar
        extrator_vendas = VendasExtractor()
        extrator_vendas.executar_extracao_completa()
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        print("Todos os dados extraídos até este ponto foram preservados")
        raise