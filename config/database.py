# Responsável por: criar engine, sessão, base do SQLAlchemy, schemas e tabelas

from sqlalchemy import create_engine, text # Biblioteca para se comunicar com meu Banco de Dados Postgre SQL
from sqlalchemy.orm import declarative_base, sessionmaker
from config.settings import database_url 


# =====================================================
# 1. CONFIGURAÇÃO DO BANCO DE DADOS
# =====================================================

# Cria o engine e a sessão do banco de dados
engine = create_engine(database_url)
Session = sessionmaker(bind=engine)

# Cria a base para os modelos SQLAlchemy
Base = declarative_base()

print("Modelo de dados definido !")

# =====================================================
# 2. FUNÇÕES AUXILIARES - Criando schemas
# =====================================================

def create_schema_raw():
    """
    Cria o schema 'raw' se não existir
    Schema RAW: Armazena dados brutos extraídos da API Bling
    """
    print("Criando schema raw...")
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.commit()
    print("✅ Schema 'raw' criado/verificado com sucesso!")


def create_schema_processed():
    """
    Cria o schema 'processed' se não existir
    Schema PROCESSED: Armazena dados transformados e estruturados (Data Warehouse)
    """
    print("Criando schema processed...")
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS processed"))
        conn.commit()
    print("✅ Schema 'processed' criado/verificado com sucesso!")


def create_all_schemas():
    """
    Cria todos os schemas necessários para o projeto
    """
    print("\n📂 CRIANDO TODOS OS SCHEMAS")
    print("=" * 60)
    create_schema_raw()
    create_schema_processed()
    print("=" * 60)
    print("✅ Todos os schemas criados/verificados com sucesso!\n")

# =====================================================
# 3. FUNÇÕES AUXILIARES - Criando tabelas
# =====================================================

def create_all_tables():
    """
    Cria todas as tabelas no banco de dados
    Importa os modelos dinamicamente para evitar importação circular
    """
    print("\n📋 CRIANDO TODAS AS TABELAS")
    print("=" * 60)
    print("Importando modelos:")

    # =====================================================
    # 3.1. IMPORTAÇÃO DOS MODELOS RAW
    # =====================================================
    
    print("\n🗄️  MODELOS RAW (Dados Brutos):")
    
    # Importando aqui DENTRO da função, para evitar importação circular
    from models.contact_raw import ContatoRaw
    from models.product_raw import ProdutoRaw
    from models.sales_raw import VendasRaw
    from models.stocks_raw import EstoqueRaw
    from models.situation_raw import SituacoesRaw
    from models.channels_raw import CanaisRaw

    print("   ✓ ContatoRaw")
    print("   ✓ ProdutoRaw")
    print("   ✓ VendasRaw")
    print("   ✓ EstoqueRaw")
    print("   ✓ SituacoesRaw")
    print("   ✓ CanaisRaw")

    # =====================================================
    # 3.2. IMPORTAÇÃO DOS MODELOS PROCESSED (Dimensões)
    # =====================================================
    
    print("\n📊 MODELOS PROCESSED - DIMENSÕES:")
    
    from models.dim_fato.dim_contatos import DimContatos
    from models.dim_fato.dim_tempo import DimTempo
    #from models.dim_fato.dim_channels import DimCanais
    #from models.dim_fato.dim_produtos import DimProdutos
    
    print("   ✓ DimContatos")
    print("   ✓ DimTempo")
    print("   ✓ DimProdutos")
    print("   ✓ DimCanais")

    # =====================================================
    # 3.3. IMPORTAÇÃO DOS MODELOS PROCESSED (Fatos)
    # =====================================================
    
    print("\n📈 MODELOS PROCESSED - FATOS:")
    
    from models.dim_fato.fato_pedidos import FatoPedidos 
    #from models.dim_fato.fato_itens_pedidos import FatoItensPedidos
    #from models.dim_fato.fato_estoques import FatoEstoques
    
    print("   ✓ FatoPedidos")
    print("   ✓ FatoItensPedidos")
    print("   ✓ FatoEstoques")

    # =====================================================
    # 3.4. CRIAÇÃO DAS TABELAS
    # =====================================================
    
    print("\n🔨 Criando tabelas no banco de dados...")
    print("-" * 60)
    
    print("\nTabelas RAW que serão criadas:")
    print("   • raw.contatos_raw")
    print("   • raw.produtos_raw") 
    print("   • raw.vendas_raw")
    print("   • raw.estoque_raw")
    print("   • raw.situacoes_raw") 
    print("   • raw.canais_raw") 
    
    print("\nTabelas PROCESSED que serão criadas:")
    print("   • processed.dim_contatos")
    print("   • processed.dim_tempo") 
    print("   • processed.dim_produtos")
    print("   • processed.dim_canais")
    print("   • processed.fato_pedidos")
    print("   • processed.fato_itens_pedidos")
    print("   • processed.fato_estoques")
    
    # Cria todas as tabelas de uma vez
    Base.metadata.create_all(engine)
    
    print("\n" + "=" * 60)
    print("✅ Todas as tabelas foram criadas com sucesso!")
    print("=" * 60 + "\n")


# =====================================================
# 4. FUNÇÃO AUXILIAR - Verificar estrutura do banco
# =====================================================

def verificar_estrutura_banco():
    """
    Verifica e exibe a estrutura atual do banco de dados
    Útil para debug e validação
    """
    print("\n🔍 VERIFICANDO ESTRUTURA DO BANCO")
    print("=" * 60)
    
    with engine.connect() as conn:
        # Verificar schemas
        print("\n📂 SCHEMAS EXISTENTES:")
        result = conn.execute(text("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('raw', 'processed')
            ORDER BY schema_name
        """))
        
        schemas = [row[0] for row in result]
        for schema in schemas:
            print(f"   ✓ {schema}")
        
        if not schemas:
            print("   ⚠️  Nenhum schema encontrado")
        
        # Verificar tabelas por schema
        for schema in schemas:
            print(f"\n📋 TABELAS NO SCHEMA '{schema}':")
            result = conn.execute(text(f"""
                SELECT table_name, 
                       pg_size_pretty(pg_total_relation_size(quote_ident(table_schema)||'.'||quote_ident(table_name))) as size
                FROM information_schema.tables 
                WHERE table_schema = '{schema}'
                ORDER BY table_name
            """))
            
            tabelas = result.fetchall()
            if tabelas:
                for tabela, tamanho in tabelas:
                    print(f"   • {tabela} ({tamanho})")
            else:
                print(f"   ⚠️  Nenhuma tabela encontrada")
    
    print("\n" + "=" * 60)
    print("✅ Verificação concluída!")
    print("=" * 60 + "\n")