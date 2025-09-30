# Responsável por: executar todo o processo, criar schema, chamar o extrator
# Obs.: O Endpoint do estoque só será pissível rogar no Bling do G4, que é onde tem os dados atualizados. Os demais bling, não tem info de estoque!
# Por hora esse scrip não irá ser usado ! 

from config.database import create_schema_raw, create_all_tables
from extract.stocks import EstoqueExtractor

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - ESTOQUE
# =====================================================

if __name__ == "__main__":  # Se o arquivo for executado diretamente, não execute o código abaixo. Isso é importante para evitar que o código seja executado quando o arquivo for importado.
    try:
        # Cria o schema se não existir
        create_schema_raw()

        # Cria as tabelas
        create_all_tables()

        # Criar o extrator de estoque e executar
        extrator_estoque = EstoqueExtractor()
        extrator_estoque.executar_extracao_completa()
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        print("Todos os dados extraídos até este ponto foram preservados")
        raise