# Responsável por: definir a estrutura da tabela fato_contas_receber no schema processed

from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, String, Numeric, Date, DateTime, ForeignKey, UniqueConstraint
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - FATO_CONTAS_RECEBER
# =====================================================

class FatoContasReceber(Base):
    __tablename__ = "fato_contas_receber"
    __table_args__ = (
        UniqueConstraint('bling_contas_receber_id', 'empresa_id', name='uq_fato_contas_receber_bling_empresa'),
        {"schema": "processed"}
    )

    # ============================
    # CHAVES
    # ============================
    
    # Chave primária (mesmo ID da raw)
    contas_receber_id = Column(Integer, primary_key=True)
    
    # Chave de negócio (ID da API Bling)
    bling_contas_receber_id = Column(BigInteger, nullable=False, index=True)
    
    # Chave da empresa
    empresa_id = Column(Integer, ForeignKey('processed.dim_empresas.empresa_id'), nullable=False, index=True)
    
    # ============================
    # MÉTRICAS
    # ============================
    
    valor = Column(Numeric(15, 2), nullable=False)
    
    # ============================
    # ATRIBUTOS DESCRITIVOS
    # ============================
    
    situacao = Column(String(50), nullable=True, index=True)
    origem = Column(String(50), nullable=True)
    conta_contabil = Column(String(255), nullable=True)
    numero_contas_receber = Column(BigInteger, nullable=True)
    
    # ============================
    # FOREIGN KEYS
    # ============================
    
    # FK para dim_tempo (através da data_vencimento)
    data_vencimento = Column(Date, ForeignKey('processed.dim_tempo.data_completa'), nullable=False, index=True)
    
    # FK para dim_contatos (cliente)
    bling_cliente_id = Column(BigInteger, ForeignKey("processed.dim_contatos.bling_cliente_id", ondelete="SET NULL"), nullable=True, index=True)
    
    # FK para dim_formas_pagamento
    forma_pagamento_id = Column(BigInteger, ForeignKey("processed.dim_formas_pagamento.forma_pagamento_id", ondelete="SET NULL"), nullable=True, index=True)
    
    # ============================
    # METADADOS
    # ============================
    
    data_ingestao = Column(DateTime, nullable=True)
    data_processamento = Column(DateTime, default=datetime.now, nullable=False)

    def __repr__(self):
        return f"<FatoContasReceber(contas_receber_id={self.contas_receber_id}, empresa_id={self.empresa_id}, valor={self.valor}, situacao='{self.situacao}')>"