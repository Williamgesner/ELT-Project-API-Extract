# Responsável por: executar extração de naturezas de operação
from config.database import create_schema_raw, create_all_tables
from extract.nature_operation import NaturezaOperacaoExtractor

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - NATUREZAS DE OPERAÇÃO
# =====================================================

if __name__ == "__main__":
    try:
        # Criar schema se não existir
        create_schema_raw()
    
        # Criar tabelas
        create_all_tables()

        print("\n🌿 INICIANDO EXTRAÇÃO DE NATUREZAS DE OPERAÇÃO")
        print("=" * 70)
        print("Este processo busca as naturezas de operação das NFe")
        print("=" * 70)
        
        # Executar extração
        extrator = NaturezaOperacaoExtractor()
        extrator.executar_extracao_completa()
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        raise