# Responsável por: Orquestrar a transformação de contatos de raw.contatos_raw para processed.dim_contatos

import time
from datetime import datetime
from config.database import create_schema_processed, create_all_tables
from config.settings import empresas
from transform.contacts_dw import ContatosTransformer

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - TRANSFORMAÇÃO DE CONTATOS
# =====================================================

if __name__ == "__main__":
    inicio = time.time()
    
    try:
        print("\n" + "=" * 70)
        print("🔄 TRANSFORMAÇÃO: CONTATOS RAW → DIM_CONTATOS")
        print("=" * 70)
        
        # Criar schema processed se não existir
        print("\n📂 Verificando schema processed...")
        create_schema_processed()
        
        # Criar tabelas (se não existirem)
        print("📋 Verificando tabelas...")
        create_all_tables()
        
        # Loop para processar cada empresa
        for empresa_config in empresas:
            empresa_id = empresa_config['empresa_id']
            nome = empresa_config['nome']
            
            print(f"\n{'='*70}")
            print(f"🏢 Transformando: {nome} (ID: {empresa_id})")
            print(f"{'='*70}")
            
            # Criar e executar o transformer
            transformer = ContatosTransformer(empresa_id)
            transformer.executar_transformacao_completa()
        
        fim = time.time()
        tempo_total = fim - inicio
        
        print(f"\n{'='*70}")
        print(f"✅ TRANSFORMAÇÃO DE TODAS AS EMPRESAS CONCLUÍDA!")
        print(f"⏱️  Tempo total: {tempo_total:.2f} segundos")
        print(f"{'='*70}")
        
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. Validar dados: SELECT * FROM processed.dim_contatos LIMIT 10;")
        print(f"   2. Verificar qualidade dos dados transformados")
        print(f"   3. Executar novamente para processar novos registros")
        
    except KeyboardInterrupt:
        print("\n⚠️ Transformação interrompida pelo usuário")
        print("💾 Dados processados até este ponto foram preservados")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante transformação: {e}")
        print("Script interrompido para análise do erro")
        import traceback
        traceback.print_exc()
        raise