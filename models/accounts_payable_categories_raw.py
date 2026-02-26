# Responsável por: definir a estrutura da tabela categorias_raw

from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, DateTime, ForeignKey, UniqueConstraint, String
from sqlalchemy.dialects.postgresql import JSONB
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - CATEGORIAS (RAW)
# =====================================================

class CategoriasRaw(Base):
    __tablename__ = "categorias_contas_pagar_raw"
    __table_args__ = (
        UniqueConstraint('bling_id', 'empresa_id', name='uq_categorias_bling_empresa'),
        {"schema": "raw"}
    )

    # Chave primária
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Chave de negócio (ID da API Bling)
    bling_id = Column(BigInteger, nullable=False, index=True)
    
    # Chave da empresa
    empresa_id = Column(Integer, ForeignKey('processed.dim_empresas.empresa_id'), nullable=False, index=True)
    
    # JSON completo da API (dados brutos)
    dados_json = Column(JSONB, nullable=False)
    
    # Controle de processamento
    data_ingestao = Column(DateTime, default=datetime.now)

    # Controle de processamento
    status_processamento = Column(String, default='pendente', nullable=True)

    def __repr__(self):
        return f"<CategoriasRaw(bling_categoria_id={self.bling_categoria_id}, empresa_id={self.empresa_id}, data_ingestao={self.data_ingestao})>"