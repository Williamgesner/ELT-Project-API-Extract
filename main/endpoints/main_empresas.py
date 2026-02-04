"""
SCRIPT DE CRIAÇÃO E CARGA DA TABELA DIM_EMPRESAS
================================================================================
Responsável por: criar a tabela dim_empresas e popular com os dados do CSV
VERSÃO CORRIGIDA: Insere apenas empresas que NÃO existem no banco

Fluxo:
1. Importar o modelo DimEmpresas
2. Criar a tabela no banco (se não existir)
3. Ler o arquivo empresas.csv (de data_business/)
4. Verificar quais empresas já existem
5. Inserir APENAS as empresas novas
6. Validar os dados inseridos

"""

import pandas as pd
from datetime import datetime
from sqlalchemy import text
from config.database import engine, Session, create_schema_processed
from models.dim_fato.dim_empresas import DimEmpresas
import os  # ← ADICIONAR IMPORT

# =====================================================
# CONFIGURAÇÕES
# =====================================================

EMPRESAS_FILE = 'data_business/empresas.csv'  # ← NOVA CONSTANTE

# =====================================================
# 1. CRIAR SCHEMA E TABELA
# =====================================================

def criar_tabela_empresas():
    """
    Cria a tabela dim_empresas no schema processed
    """
    print("\n" + "="*70)
    print("📋 CRIANDO TABELA DIM_EMPRESAS")
    print("="*70)
    
    # Garantir que o schema existe
    create_schema_processed()
    
    # Criar tabela
    DimEmpresas.__table__.create(engine, checkfirst=True)
    
    print("✅ Tabela dim_empresas criada/verificada com sucesso!")
    print("="*70)

# =====================================================
# 2. IMPORTAR DADOS DO CSV
# =====================================================

def importar_empresas_do_csv():
    """
    Lê o arquivo empresas.csv e insere os dados no banco
    Insere apenas empresas que ainda não existem
    """
    print("\n" + "="*70)
    print("📂 IMPORTANDO DADOS DO CSV")
    print("="*70)
    
    session = Session()
    
    try:
        # Verificar se arquivo existe
        if not os.path.exists(EMPRESAS_FILE):
            raise FileNotFoundError(f"Arquivo não encontrado: {EMPRESAS_FILE}")
        
        # Ler CSV
        print(f"\n1️⃣ Lendo arquivo {EMPRESAS_FILE}...")  
        df = pd.read_csv(EMPRESAS_FILE)
        
        print(f"✅ Arquivo lido com sucesso!")
        print(f"   • Total de empresas no CSV: {len(df)}")
        
        # Validar estrutura
        colunas_esperadas = ['empresa_id', 'cnpj', 'razao_social']
        colunas_faltantes = [col for col in colunas_esperadas if col not in df.columns]
        
        if colunas_faltantes:
            raise Exception(f"Colunas faltantes no CSV: {colunas_faltantes}")
        
        print("✅ Estrutura do CSV validada!")
        
        # Buscar empresas que já existem no banco
        print("\n2️⃣ Verificando empresas existentes no banco...")
        empresas_existentes = session.query(DimEmpresas.empresa_id).all()
        ids_existentes = [emp.empresa_id for emp in empresas_existentes]
        
        print(f"   • Empresas já cadastradas: {len(ids_existentes)}")
        if ids_existentes:
            print(f"   • IDs existentes: {sorted(ids_existentes)}")
        
        # Filtrar apenas empresas novas
        df_novas = df[~df['empresa_id'].isin(ids_existentes)].copy()
        
        if len(df_novas) == 0:
            print("\n⚠️  Nenhuma empresa nova para inserir!")
            print("   Todas as empresas do CSV já estão cadastradas.")
            return 0
        
        print(f"\n📊 EMPRESAS NOVAS PARA INSERIR:")
        print("-"*70)
        for _, row in df_novas.iterrows():
            print(f"   • ID {row['empresa_id']}: {row['cnpj']} - {row['razao_social']}")
        print("-"*70)
        
        # Adicionar metadados
        df_novas['data_ingestao'] = datetime.now()
        df_novas['data_processamento'] = datetime.now()
        
        # Inserir apenas as novas
        print(f"\n3️⃣ Inserindo {len(df_novas)} empresa(s) nova(s)...")
        
        df_novas.to_sql(
            name='dim_empresas',
            con=engine,
            schema='processed',
            if_exists='append',
            index=False
        )
        
        print(f"✅ {len(df_novas)} empresa(s) inserida(s) com sucesso!")
        
        return len(df_novas)
        
    except FileNotFoundError as e:
        print(f"\n❌ ERRO: {str(e)}")
        print(f"💡 Crie o arquivo em: {EMPRESAS_FILE}")
        print("   Com as colunas: empresa_id, cnpj, razao_social")
        raise
    except Exception as e:
        print(f"\n❌ ERRO ao importar dados: {e}")
        raise
    finally:
        session.close()

# =====================================================
# 3. VALIDAR DADOS INSERIDOS
# =====================================================

def validar_dados_empresas():
    """
    Valida se os dados foram inseridos corretamente
    """
    print("\n" + "="*70)
    print("🔍 VALIDANDO DADOS INSERIDOS")
    print("="*70)
    
    session = Session()
    
    try:
        # Contar total de registros
        query = text("SELECT COUNT(*) FROM processed.dim_empresas")
        total = session.execute(query).scalar()
        
        print(f"\n✅ Total de empresas na tabela: {total}")
        
        # Buscar todas as empresas
        query = text("""
            SELECT empresa_id, cnpj, razao_social 
            FROM processed.dim_empresas 
            ORDER BY empresa_id
        """)
        
        empresas = session.execute(query).fetchall()
        
        print("\n📊 EMPRESAS CADASTRADAS:")
        print("-"*70)
        for empresa in empresas:
            print(f"   • ID {empresa.empresa_id}: {empresa.cnpj} - {empresa.razao_social}")
        print("-"*70)
        
        # Validar CNPJs únicos
        query = text("""
            SELECT cnpj, COUNT(*) as qtd
            FROM processed.dim_empresas
            GROUP BY cnpj
            HAVING COUNT(*) > 1
        """)
        
        duplicados = session.execute(query).fetchall()
        
        if duplicados:
            print("\n⚠️ ATENÇÃO: CNPJs duplicados encontrados!")
            for dup in duplicados:
                print(f"   • CNPJ {dup.cnpj}: {dup.qtd} ocorrências")
        else:
            print("\n✅ Nenhum CNPJ duplicado encontrado!")
        
        return total
        
    except Exception as e:
        print(f"\n❌ ERRO na validação: {e}")
        raise
    finally:
        session.close()

# =====================================================
# 4. EXECUTAR SCRIPT COMPLETO
# =====================================================

def executar_criacao_dim_empresas():
    """
    Executa o fluxo completo de criação da dim_empresas
    """
    print("\n" + "="*70)
    print("🚀 INICIANDO CRIAÇÃO DA DIM_EMPRESAS")
    print("="*70)
    print("Este script irá:")
    print("   1. Criar a tabela dim_empresas")
    print(f"   2. Importar dados do arquivo {EMPRESAS_FILE}")
    print("   3. Inserir APENAS empresas que não existem")
    print("   4. Validar os dados inseridos")
    print("="*70)
    
    try:
        # 1. Criar tabela
        criar_tabela_empresas()
        
        # 2. Importar dados
        total_importado = importar_empresas_do_csv()
        
        # 3. Validar
        total_validado = validar_dados_empresas()
        
        # Relatório final
        print("\n" + "="*70)
        print("🎉 PROCESSO CONCLUÍDO COM SUCESSO!")
        print("="*70)
        print(f"\n📊 RESUMO:")
        print(f"   • Empresas novas inseridas: {total_importado}")
        print(f"   • Total de empresas no banco: {total_validado}")
        print("="*70 + "\n")
        
    except Exception as e:
        print("\n" + "="*70)
        print("❌ ERRO NO PROCESSO")
        print("="*70)
        print(f"Erro: {e}")
        print("\n💡 Verifique:")
        print(f"   • O arquivo {EMPRESAS_FILE} existe")
        print("   • O arquivo tem as colunas corretas: empresa_id, cnpj, razao_social")
        print("   • A conexão com o banco está funcionando")
        print("="*70 + "\n")
        raise

# =====================================================
# 5. PONTO DE ENTRADA
# =====================================================

if __name__ == "__main__":
    try:
        executar_criacao_dim_empresas()
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
    except Exception:
        print("\n❌ Execução falhou. Verifique os erros acima.")