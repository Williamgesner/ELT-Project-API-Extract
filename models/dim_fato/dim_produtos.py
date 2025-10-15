# Responsável por: definir a estrutura da tabela dim_produtos no schema processed

from datetime import datetime
from sqlalchemy import Column, Integer, String, BigInteger, Numeric, DateTime
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - DIM_PRODUTOS
# =====================================================

class DimProdutos(Base):
    __table_args__ = {"schema": "processed"}
    __tablename__ = "dim_produtos"

    # ============================
    # CHAVES
    # ============================
    
    # Chave primária
    produto_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Chave de negócio (ID da API Bling)
    bling_produto_id = Column(BigInteger, unique=True, nullable=False, index=True)
    
    # Código SKU
    sku = Column(String(50), index=True)
    
    # ============================
    # DADOS DO PRODUTO
    # ============================
    
    descricao_produto = Column(String(500), nullable=False)
    
    # ============================
    # PREÇOS
    # ============================
    
    preco_venda = Column(Numeric(15, 2))
    preco_custo = Column(Numeric(15, 2))
    
    # ============================
    # ATRIBUTOS (APENAS BICICLETAS)
    # ============================
    
    aro = Column(String(10))
    marca = Column(String(50))
    cor_principal = Column(String(150))
    cor_secundaria = Column(String(150))
    cor_terciaria = Column(String(150))
    tamanho = Column(String(10))
    marchas = Column(String(10))
    freio = Column(String(50))
    genero = Column(String(20))
    publico = Column(String(20))
    categoria = Column(String(50))
    
    # ============================
    # STATUS
    # ============================
    
    situacao = Column(String(1))  # A=Ativo, I=Inativo
    
    # ============================
    # METADADOS
    # ============================
    
    data_ingestao = Column(DateTime)
    data_processamento = Column(DateTime, default=datetime.now, nullable=False)

    def __repr__(self):
        return f"<DimProdutos(produto_id={self.produto_id}, sku='{self.sku}', descricao='{self.descricao_produto[:50]}...')>"