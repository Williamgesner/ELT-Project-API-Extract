# Responsável por: definir a estrutura da tabela natureza_operacao_raw

from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - NATUREZA DE OPERAÇÃO
# =====================================================

# Definindo o modelo da tabela para dados brutos (raw)
class NaturezaOperacaoRaw(Base):
    __tablename__ = "natureza_operacao_raw"
    __table_args__ = (
        UniqueConstraint('bling_id', 'empresa_id', name='uq_natureza_bling_empresa'),
        {"schema": "raw"}
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    bling_id = Column(BigInteger, nullable=False, index=True)  # ID original da API
    empresa_id = Column(Integer, ForeignKey('processed.dim_empresas.empresa_id'), nullable=False, index=True)
    dados_json = Column(JSONB, nullable=False)  # JSON completo da API
    data_ingestao = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<NaturezaOperacaoRaw(id={self.id}, bling_id={self.bling_id}, empresa_id={self.empresa_id})>"