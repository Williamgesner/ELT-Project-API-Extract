# Responsável por: executar transformação de natureza_operacao_raw → dim_natureza_operacao

import time
from config.database import create_schema_processed, create_all_tables
from config.settings import empresas
from transform.nature_operation_dw import NaturezaOperacaoTransformer

# =====================================================
# 1. EXECUÇÃO DO SCRIPT - TRANSFORMAÇÃO
# =====================================================

if __name__ == "__main__":
    inicio = time.time()
    
    try:
        # Cria o schema processed se não existir
        create_schema_processed()

        # Cria as tabelas
        create_all_tables()
        
        print("\n🔄 INICIANDO TRANSFORMAÇÃO DE NATUREZA DE OPERAÇÃO")
        print("=" * 70)
        print("Este processo transforma dados RAW → DW")
        print("=" * 70)
        
        # Loop para processar cada empresa
        for empresa_config in empresas:
            empresa_id = empresa_config['empresa_id']
            nome = empresa_config['nome']
            
            print(f"\n🏢 Transformando: {nome} (ID: {empresa_id})")
            print("-" * 70)
            
            # Criar o transformer e executar
            transformer = NaturezaOperacaoTransformer(empresa_id)
            transformer.executar_transformacao_completa()
        
        fim = time.time()
        tempo_total = fim - inicio
        print(f"\n✅ Transformação concluída em {tempo_total:.2f} segundos")
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        raise