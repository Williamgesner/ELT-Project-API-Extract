# Responsável por: definir a estrutura da tabela dim_categorias_contas_pagar no schema processed

from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, String, DateTime
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - DIM_CATEGORIAS_CONTAS_PAGAR
# =====================================================

class DimCategoriasContasPagar(Base):
    __table_args__ = {"schema": "processed"}
    __tablename__ = "dim_categorias_contas_pagar"

    # ============================
    # CHAVES
    # ============================
    
    # Chave primária (mesmo ID da raw)
    categoria_id = Column(Integer, primary_key=True)
    
    # Chave de negócio (ID da API Bling)
    bling_categorias_id = Column(BigInteger, unique=True, nullable=False, index=True)
    
    # ============================
    # ATRIBUTOS DESCRITIVOS
    # ============================
    
    tipo_categoria = Column(String(50), nullable=True, index=True)  # Despesa, Receita, etc.
    descricao = Column(String(255), nullable=False)
    
    # ============================
    # METADADOS
    # ============================
    
    data_ingestao = Column(DateTime, nullable=True)
    data_processamento = Column(DateTime, default=datetime.now, nullable=False)

    def __repr__(self):
        return f"<DimCategoriasContasPagar(categoria_id={self.categoria_id}, descricao='{self.descricao}', tipo='{self.tipo_categoria}')>"