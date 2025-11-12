# Responsável por: executar extração final otimizada de contatos completos

from config.database import create_schema_raw, create_all_tables
from extract.contacts import ContatosCompletoExtractor

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - CONTATOS + ENDEREÇOS
# =====================================================

if __name__ == "__main__":
    try:
        # Cria o schema se não existir
        create_schema_raw()

        # Cria as tabelas
        create_all_tables()
        
        # Executar extração otimizada
        extrator = ContatosCompletoExtractor()
        extrator.executar_extracao_completa()
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
        print("💾 Dados processados até este ponto foram preservados")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        print("Todos os dados extraídos até este ponto foram preservados")
        raise