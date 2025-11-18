# Responsável por: executar todo o processo, criar schema, chamar o extrator
# ⚠️ Só executar esse Scrip depois que o Scrip de vendas_raw for executado ! 
# ⚠️ Só roda esse Script uma vez, ou quando alterar ou incluir novas situações !

import time
from config.database import create_schema_raw, create_all_tables
from config.settings import empresas
from extract.situation import SituacoesExtractor

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - SITUAÇÃO
# =====================================================

if __name__ == "__main__":
    inicio = time.time()
    
    try:
        # Criar schema se não existir
        create_schema_raw()
    
        # Criar tabelas
        create_all_tables()
    
        print("\n📊 INICIANDO EXTRAÇÃO DE SITUAÇÕES")
        print("=" * 70)
        
        # Loop para processar cada empresa
        for empresa_config in empresas:
            empresa_id = empresa_config['empresa_id']
            api_key = empresa_config['api_key']
            nome = empresa_config['nome']
            
            print(f"\n🏢 Processando: {nome} (ID: {empresa_id})")
            print("-" * 70)
            
            # Executar extração
            extrator_situacao = SituacoesExtractor(api_key, empresa_id)
            extrator_situacao.executar_extracao_completa()
        
        fim = time.time()
        tempo_total = fim - inicio
        print(f"\n✅ Script executado com sucesso em {tempo_total:.2f} segundos!")
    
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO: {e}")
        raise