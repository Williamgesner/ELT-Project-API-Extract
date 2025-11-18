# Responsável por: definir a estrutura da tabela dim_canais no schema processed

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, UniqueConstraint
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - DIM_CANAIS
# =====================================================

class DimCanais(Base):
    __tablename__ = "dim_canais"
    __table_args__ = (
        UniqueConstraint('bling_canal_id', 'empresa_id', name='uq_dim_canais_bling_empresa'),
        {"schema": "processed"}
    )
    
    # Chave primária
    canal_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Chave de negócio
    bling_canal_id = Column(Integer, nullable=False, index=True)
    
    # Chave da empresa
    empresa_id = Column(Integer, ForeignKey('processed.dim_empresas.empresa_id'), nullable=False, index=True)
    
    # Atributos descritivos
    nome_canal = Column(String(200), nullable=False)
    
    # Metadados
    data_ingestao = Column(DateTime)
    data_processamento = Column(DateTime, default=datetime.now, nullable=False)

    def __repr__(self):
        return f"<DimCanais(canal_id={self.canal_id}, empresa_id={self.empresa_id}, nome_canal='{self.nome_canal}')>"