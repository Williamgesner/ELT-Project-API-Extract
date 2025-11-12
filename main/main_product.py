# Responsável por: executar extração de produtos

from config.database import create_schema_raw, create_all_tables
from extract.products import ProdutosExtractor

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - PRODUTOS
# =====================================================

if __name__ == "__main__":
    try:
        # Cria o schema se não existir
        create_schema_raw()

        # Cria as tabelas
        create_all_tables()

        # Criar o extrator de produtos e executar
        print("\n🏭 INICIANDO EXTRAÇÃO DE PRODUTOS")
        print("=" * 50)
        extrator_produtos = ProdutosExtractor()
        extrator_produtos.executar_extracao_completa()
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        print("Todos os dados extraídos até este ponto foram preservados")
        raise