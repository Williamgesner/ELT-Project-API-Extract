# Responsável por: executar extração de NFe (VERSÃO SIMPLES - sem enriquecimento)

from config.database import create_schema_raw, create_all_tables
from extract.nfe import NFeExtractor

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - NFe SIMPLES
# =====================================================

if __name__ == "__main__":
    try:
        # Cria o schema se não existir
        create_schema_raw()

        # Cria as tabelas
        create_all_tables()

        # Criar o extrator de NFe e executar
        print("\n📄 INICIANDO EXTRAÇÃO DE NFe (VERSÃO SIMPLES)")
        print("=" * 50)
        
        extrator = NFeExtractor()
        extrator.executar_extracao_completa()
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        print("Todos os dados extraídos até este ponto foram preservados")
        raise