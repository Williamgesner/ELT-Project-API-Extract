"""
SCRIPT DE CRIAÇÃO E CARGA DA TABELA DIM_EMPRESAS
================================================================================
Responsável por: criar a tabela dim_empresas e popular com os dados do CSV
Este script deve ser executado UMA ÚNICA VEZ no início do projeto multi-CNPJ

Fluxo:
1. Importar o modelo DimEmpresas
2. Criar a tabela no banco (se não existir)
3. Ler o arquivo empresas.csv
4. Inserir os dados na tabela
5. Validar os dados inseridos

"""

import pandas as pd
from datetime import datetime
from sqlalchemy import text
from config.database import engine, Session, create_schema_processed
from models.dim_fato.dim_empresas import DimEmpresas

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
    """
    print("\n" + "="*70)
    print("📂 IMPORTANDO DADOS DO CSV")
    print("="*70)
    
    try:
        # Ler CSV
        print("\n1️⃣ Lendo arquivo empresas.csv...")
        df = pd.read_csv('empresas.csv')
        
        print(f"✅ Arquivo lido com sucesso!")
        print(f"   • Total de empresas: {len(df)}")
        
        # Validar estrutura
        colunas_esperadas = ['empresa_id', 'cnpj', 'razao_social']
        colunas_faltantes = [col for col in colunas_esperadas if col not in df.columns]
        
        if colunas_faltantes:
            raise Exception(f"Colunas faltantes no CSV: {colunas_faltantes}")
        
        print("✅ Estrutura do CSV validada!")
        
        # Exibir preview
        print("\n📊 PREVIEW DOS DADOS:")
        print("-"*70)
        for _, row in df.iterrows():
            print(f"   • ID {row['empresa_id']}: {row['cnpj']} - {row['razao_social']}")
        print("-"*70)
        
        # Adicionar metadados
        df['data_ingestao'] = datetime.now()
        df['data_processamento'] = datetime.now()
        
        # Inserir no banco
        print("\n2️⃣ Inserindo dados no banco...")
        
        df.to_sql(
            name='dim_empresas',
            con=engine,
            schema='processed',
            if_exists='append',  # append para não recriar a tabela
            index=False
        )
        
        print(f"✅ {len(df)} empresas inseridas com sucesso!")
        
        return len(df)
        
    except FileNotFoundError:
        print("\n❌ ERRO: Arquivo 'empresas.csv' não encontrado!")
        print("💡 Crie o arquivo empresas.csv na raiz do projeto com as colunas:")
        print("   empresa_id,cnpj,razao_social")
        raise
    except Exception as e:
        print(f"\n❌ ERRO ao importar dados: {e}")
        raise

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
    print("   2. Importar dados do arquivo empresas.csv")
    print("   3. Validar os dados inseridos")
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
        print(f"   • Empresas importadas: {total_importado}")
        print(f"   • Empresas na tabela: {total_validado}")
        print("="*70 + "\n")
        
    except Exception as e:
        print("\n" + "="*70)
        print("❌ ERRO NO PROCESSO")
        print("="*70)
        print(f"Erro: {e}")
        print("\n💡 Verifique:")
        print("   • O arquivo empresas.csv existe na raiz do projeto")
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