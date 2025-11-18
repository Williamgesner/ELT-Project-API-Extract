# Responsável por: definir a estrutura da tabela canais_raw

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - CANAIS DE VENDA
# =====================================================

# Definindo o modelo da tabela para dados brutos (raw)
class CanaisRaw(Base):
    __tablename__ = "canais_raw"
    __table_args__ = (
        UniqueConstraint('bling_canal_id', 'empresa_id', name='uq_canais_bling_empresa'),
        {"schema": "raw"}
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    bling_canal_id = Column(Integer, nullable=False, index=True)  # ID original da API
    empresa_id = Column(Integer, ForeignKey('processed.dim_empresas.empresa_id'), nullable=False, index=True)
    descricao = Column(String(200), nullable=False)  # "Loja Virtual", "Marketplace", etc.
    dados_json = Column(JSONB, nullable=False)  # JSON completo da API
    data_ingestao = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<CanaisRaw(id={self.bling_canal_id}, empresa_id={self.empresa_id}, descricao='{self.descricao}')>"