# Responsável por: definir a estrutura da tabela contas_pagar_raw

from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, DateTime, String, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - CONTAS A PAGAR (RAW)
# =====================================================

class ContasPagarRaw(Base):
    __tablename__ = "contas_pagar_raw"
    __table_args__ = (
        UniqueConstraint('bling_id', 'empresa_id', name='uq_contas_pagar_bling_empresa'),
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
    status_processamento = Column(String(20), default='pendente')

    def __repr__(self):
        return f"<ContasPagarRaw(bling_id={self.bling_id}, empresa_id={self.empresa_id}, data_ingestao={self.data_ingestao})>"