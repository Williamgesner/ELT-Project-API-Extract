# Responsável por: executar extração de NFe (VERSÃO SIMPLES - sem enriquecimento)

import time
from config.database import create_schema_raw, create_all_tables
from config.settings import empresas
from extract.nfe import NFeExtractor

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - NFe SIMPLES
# =====================================================

if __name__ == "__main__":
    inicio = time.time()
    
    try:
        # Cria o schema se não existir
        create_schema_raw()

        # Cria as tabelas
        create_all_tables()

        # Criar o extrator de NFe e executar
        print("\n📄 INICIANDO EXTRAÇÃO DE NFe (VERSÃO SIMPLES)")
        print("=" * 70)
        
        # Loop para processar cada empresa
        for empresa_config in empresas:
            empresa_id = empresa_config['empresa_id']
            api_key = empresa_config['api_key']
            nome = empresa_config['nome']
            
            print(f"\n🏢 Processando: {nome} (ID: {empresa_id})")
            print("-" * 70)
            
            extrator = NFeExtractor(api_key, empresa_id)
            extrator.executar_extracao_completa()
        
        fim = time.time()
        tempo_total = fim - inicio
        print(f"\n✅ Extração concluída em {tempo_total:.2f} segundos")
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        print("Todos os dados extraídos até este ponto foram preservados")
        raise