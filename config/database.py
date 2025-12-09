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
    from models.situation_raw import SituacoesRaw
    from models.channels_raw import CanaisRaw
    from models.accounts_payable_raw import ContasPagarRaw
    from models.accounts_payable_categories_raw import CategoriasRaw
    from models.payment_methods_raw import FormasPagamentosRaw
    from models.accounts_receivable_raw import ContasReceberRaw
    from models.nfe_raw import NFeRaw
    from models.nature_operation_raw import NaturezaOperacaoRaw
    

    print("   ✓ ContatoRaw")
    print("   ✓ ProdutoRaw")
    print("   ✓ VendasRaw")
    print("   ✓ SituacoesRaw")
    print("   ✓ CanaisRaw")
    print("   ✓ ContasPgarRaw")
    print("   ✓ CategoriasRaw")
    print("   ✓ FormasPagamentosRaw")
    print("   ✓ ContasReceberRaw")
    print("   ✓ NfeRaw")
    print("   ✓ NaturezaOperacaoRaw")

    # =====================================================
    # 3.2. IMPORTAÇÃO DOS MODELOS PROCESSED (Dimensões)
    # =====================================================
    
    print("\n📊 MODELOS PROCESSED - DIMENSÕES:")
    
    from models.dim_fato.dim_empresas import DimEmpresas
    from models.dim_fato.dim_contatos import DimContatos
    from models.dim_fato.dim_tempo import DimTempo
    from models.dim_fato.dim_produtos import DimProdutos
    from models.dim_fato.dim_canais import DimCanais
    from models.dim_fato.dim_categorias_contas_pagar import DimCategoriasContasPagar
    from models.dim_fato.dim_formas_pagamento import DimFormasPagamento
    from models.dim_fato.dim_natureza_operacao import DimNaturezaOperacao
    from models.dim_fato.dim_situacao import DimSituacao

    
    print("   ✓ DimEmpresas")
    print("   ✓ DimContatos")
    print("   ✓ DimTempo")
    print("   ✓ DimProdutos")
    print("   ✓ DimSituacao")
    print("   ✓ DimCanais")
    print("   ✓ DimCategoriasContasPagar")
    print("   ✓ DimFormasPagamento")
    print("   ✓ DimNaturezaOperacao")

    # =====================================================
    # 3.3. IMPORTAÇÃO DOS MODELOS PROCESSED (Fatos)
    # =====================================================
    
    print("\n📈 MODELOS PROCESSED - FATOS:")
    
    from models.dim_fato.fato_pedidos import FatoPedidos 
    from models.dim_fato.fato_itens_pedidos import FatoItensPedidos
    from models.dim_fato.fato_contas_pagar import FatoContasPagar
    from models.dim_fato.fato_contas_receber import FatoContasReceber
    from models.dim_fato.fato_nfe import FatoNFe
    
    print("   ✓ FatoPedidos")
    print("   ✓ FatoItensPedidos")
    print("   ✓ FatoContasPagar")
    print("   ✓ FatoContasReceber")
    print("   ✓ FatoNfe")

    # =====================================================
    # 3.4. CRIAÇÃO DAS TABELAS
    # =====================================================
    
    print("\n🔨 Criando tabelas no banco de dados...")
    print("-" * 60)
    
    print("\nTabelas RAW que serão criadas:")
    print("   • raw.contatos_raw")
    print("   • raw.produtos_raw") 
    print("   • raw.vendas_raw")
    print("   • raw.situacoes_raw") 
    print("   • raw.canais_raw") 
    print("   • raw.contas_pagar_raw") 
    print("   ✓ raw.categorias_contas_pagar_raw")
    print("   ✓ raw.formas_pagamentos_raw")
    print("   ✓ raw.contas_receber_raw")
    print("   ✓ raw.nfe_raw")
    print("   ✓ NaturezaOperacaoRaw")
    
    print("\nTabelas PROCESSED que serão criadas:")
    print("   • processed.dim_empresas")
    print("   • processed.dim_contatos")
    print("   • processed.dim_tempo") 
    print("   • processed.dim_produtos")
    print("   • processed.dim_canais")
    print("   • processed.dim_situacao") 
    print("   • processed.dim_categorias_contas_pagar")
    print("   • processed.dim_formas_pagamento")
    print("   • processed.dim_natureza_operacao")
    print("   • processed.fato_pedidos")
    print("   • processed.fato_itens_pedidos")
    print("   • processed.fato_contas_pagar")
    print("   • processed.fato_contas_receber")
    print("   • processed.fato_NFe")
    
    # Cria todas as tabelas de uma vez
    Base.metadata.create_all(engine)
    
    print("\n" + "=" * 60)
    print("✅ Todas as tabelas foram criadas com sucesso!")
    print("=" * 60 + "\n")


# =====================================================
# 4. FUNÇÃO PRINCIPAL
# =====================================================

if __name__ == "__main__":
    """
    Executa a criação de schemas e tabelas
    """
    print("\n" + "="*70)
    print("🗄️ INICIALIZANDO BANCO DE DADOS")
    print("="*70)
    
    # Criar schemas
    create_all_schemas()
    
    # Criar tabelas
    create_all_tables()
    
    print("\n" + "="*70)
    print("✅ BANCO DE DADOS INICIALIZADO COM SUCESSO!")
    print("="*70)
