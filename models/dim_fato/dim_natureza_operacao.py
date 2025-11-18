# Responsável por: definir a estrutura da tabela dim_natureza_operacao

from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, ForeignKey, UniqueConstraint
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - DIMENSÃO NATUREZA DE OPERAÇÃO
# =====================================================

class DimNaturezaOperacao(Base):
    __tablename__ = "dim_natureza_operacao"
    __table_args__ = (
        UniqueConstraint('bling_natureza_operacao_id', 'empresa_id', name='uq_dim_natureza_bling_empresa'),
        {"schema": "processed"}
    )

    natureza_operacao_id = Column(Integer, primary_key=True, autoincrement=True)
    bling_natureza_operacao_id = Column(BigInteger, nullable=False, index=True)  # ID original da API
    empresa_id = Column(Integer, ForeignKey('processed.dim_empresas.empresa_id'), nullable=False, index=True)
    natureza_operacao = Column(String(255), nullable=False)  # Descrição
    data_ingestao = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<DimNaturezaOperacao(id={self.natureza_operacao_id}, empresa_id={self.empresa_id}, natureza='{self.natureza_operacao}')>"