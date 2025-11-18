# Responsável por: definir a estrutura da tabela produtos_raw

from datetime import datetime
from sqlalchemy import Column, Integer, String, BigInteger, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB  # Importa JSONB (Mais rápido e ja convertido)
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - PROTUDOS
# =====================================================

# Definindo o modelo da tabela para dados brutos (raw)
class ProdutoRaw(Base):
    __tablename__ = "produtos_raw"
    __table_args__ = (
        UniqueConstraint('bling_id', 'empresa_id', name='uq_produtos_bling_empresa'),
        {"schema": "raw"}
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    bling_id = Column(BigInteger, nullable=False, index=True) # ID original da API
    empresa_id = Column(Integer, ForeignKey('processed.dim_empresas.empresa_id'), nullable=False, index=True)
    dados_json = Column(JSONB, nullable=False)  # JSONB é melhor que String para JSON. Nulllable é para dizer que a coluna não pode ser nula. Dados brutos do produto
    data_ingestao = Column(DateTime, default=datetime.now)  # Data de quando foi ingerido
    status_processamento = Column(String(20), default='pendente')  # Para controle de processamento - Saber o que ja virou dim_produtos (na hora de processar)

    def __repr__(self):
        return f"<ProdutoRaw(bling_id={self.bling_id}, empresa_id={self.empresa_id}, data_ingestao={self.data_ingestao})>"