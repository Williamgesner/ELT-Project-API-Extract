"""
DIM_METAS_EMPRESAS
================================================================================
Script para subir metas no PostgreSQL

Execução: python upload_metas.py
"""

import pandas as pd
from datetime import datetime
from sqlalchemy import text
from config.database import engine, Session, create_schema_processed
import os

# =====================================================
# CONFIGURAÇÕES
# =====================================================

DATA_FOLDER = 'data_business'
METAS_FILE = f'{DATA_FOLDER}/metas_empresas.csv'

# =====================================================
# DIM_METAS_EMPRESAS
# =====================================================

def criar_tabela_metas():
    """Cria tabela dim_metas_empresas no PostgreSQL"""
    print("\n📋 Criando/verificando tabela dim_metas_empresas...")
    
    create_schema_processed()
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS processed.dim_metas_empresas (
        id SERIAL PRIMARY KEY,
        empresa_id INTEGER NOT NULL,
        ano INTEGER NOT NULL,
        mes INTEGER NOT NULL,
        data_referencia DATE NOT NULL,
        meta_faturamento NUMERIC(15,2) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(empresa_id, ano, mes),
        FOREIGN KEY (empresa_id) REFERENCES processed.dim_empresas(empresa_id)
    );
    
    CREATE INDEX IF NOT EXISTS idx_metas_empresa_periodo 
    ON processed.dim_metas_empresas(empresa_id, ano, mes);
    
    COMMENT ON TABLE processed.dim_metas_empresas IS 
    'Metas mensais de faturamento por empresa';
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
    
    print("✅ Tabela dim_metas_empresas OK!")


def upload_metas(replace=True):
    """
    Lê meta_empresas.csv e faz upload no PostgreSQL
    
    Args:
        replace: Se True, substitui dados existentes. Se False, apenas adiciona.
    """
    print("\n" + "="*70)
    print("📂 UPLOAD: DIM_METAS_EMPRESAS")
    print("="*70)
    
    try:
        # 1. Verificar arquivo
        if not os.path.exists(METAS_FILE):
            print(f"❌ ERRO: Arquivo não encontrado: {METAS_FILE}")
            return False
        
        # 2. Ler CSV
        print(f"📂 Lendo {METAS_FILE}...")
        df = pd.read_csv(METAS_FILE, sep=';')
        
        # 3. Validar estrutura
        required_cols = ['empresa_id', 'ano', 'mes', 'meta_faturamento']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"❌ ERRO: Colunas faltando no CSV: {missing_cols}")
            return False
        
        print(f"✅ CSV válido! Total de registros: {len(df)}")
        
        # 4. Validar dados
        print("🔍 Validando dados...")
        
        # Empresas inválidas
        empresas_invalidas = df[~df['empresa_id'].isin([1,2,3,4,5,6])]
        if not empresas_invalidas.empty:
            print(f"⚠️  AVISO: Empresas inválidas encontradas:")
            print(empresas_invalidas[['empresa_id', 'ano', 'mes']])
        
        # Valores negativos
        valores_negativos = df[df['meta_faturamento'] < 0]
        if not valores_negativos.empty:
            print(f"⚠️  AVISO: Metas negativas (convertidas para 0):")
            print(valores_negativos[['empresa_id', 'ano', 'mes', 'meta_faturamento']])
            df.loc[df['meta_faturamento'] < 0, 'meta_faturamento'] = 0
        
        # Duplicatas
        duplicatas = df[df.duplicated(subset=['empresa_id', 'ano', 'mes'], keep=False)]
        if not duplicatas.empty:
            print(f"⚠️  AVISO: Duplicatas encontradas (primeira mantida):")
            print(duplicatas[['empresa_id', 'ano', 'mes']])
            df = df.drop_duplicates(subset=['empresa_id', 'ano', 'mes'], keep='first')
        
        # 5. Preparar DataFrame
        df_final = df[required_cols].copy()
        df_final['meta_faturamento'] = df_final['meta_faturamento'].round(2)

        df_final['data_referencia'] = pd.to_datetime(
            df_final['ano'].astype(str) + '-' + 
            df_final['mes'].astype(str).str.zfill(2) + '-01'
        )
        
        print(f"\n📊 Resumo:")
        print(f"   • Registros: {len(df_final)}")
        print(f"   • Empresas: {sorted(df_final['empresa_id'].unique())}")
        print(f"   • Períodos: {df_final['ano'].min()}/{df_final['mes'].min()} até {df_final['ano'].max()}/{df_final['mes'].max()}")
        print(f"   • Meta total: R$ {df_final['meta_faturamento'].sum():,.2f}")
        
        # 6. Upload
        if replace:
            print("\n🗑️  Removendo dados antigos...")
            with engine.connect() as conn:
                conn.execute(text("DELETE FROM processed.dim_metas_empresas"))
                conn.commit()
        
        print(f"📤 Uploading {len(df_final)} registros...")
        
        df_final.to_sql(
            name='dim_metas_empresas',
            schema='processed',
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=100
        )
        
        print("✅ Upload concluído!")
        
        # 7. Verificar
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) FROM processed.dim_metas_empresas"
            ))
            total = result.fetchone()[0]
            print(f"✅ Verificação: {total} registros na tabela")
        
        return True
        
    except Exception as e:
        print(f"❌ ERRO: {str(e)}")
        return False


def validar_metas():
    """Exibe resumo das metas"""
    print("\n" + "="*70)
    print("🔍 VALIDAÇÃO")
    print("="*70)
    
    session = Session()
    
    try:
        query_metas = text("""
            SELECT 
                empresa_id,
                COUNT(*) as qtd_meses,
                TO_CHAR(SUM(meta_faturamento), 'FM999,999,999.00') as total
            FROM processed.dim_metas_empresas
            GROUP BY empresa_id
            ORDER BY empresa_id
        """)
        metas = session.execute(query_metas).fetchall()
        
        print(f"\n📊 DIM_METAS_EMPRESAS:")
        for meta in metas:
            print(f"   • Empresa {meta.empresa_id}: {meta.qtd_meses} mês(es) - Total: R$ {meta.total}")
        
        return True
        
    except Exception as e:
        print(f"❌ ERRO: {str(e)}")
        return False
    finally:
        session.close()


# =====================================================
# MAIN
# =====================================================

if __name__ == '__main__':
    print("\n" + "="*70)
    print("🎯 UPLOAD DE DIM_METAS_EMPRESAS")
    print("="*70)
    
    try:
        criar_tabela_metas()
        sucesso = upload_metas(replace=True)
        
        if sucesso:
            validar_metas()
            print("\n✅ Processo concluído com sucesso!")
            print("👉 Pronto para conectar no Power BI!")
        else:
            print("\n❌ Processo falhou!")
        
        print("="*70 + "\n")
        
    except KeyboardInterrupt:
        print("\n⚠️  Execução interrompida")
    except Exception as e:
        print(f"\n❌ ERRO: {str(e)}")