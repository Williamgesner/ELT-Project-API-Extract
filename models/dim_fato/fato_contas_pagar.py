# Responsável por: definir a estrutura da tabela fato_contas_pagar no schema processed

from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, Numeric, Date, String, DateTime, ForeignKey
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - FATO_CONTAS_PAGAR
# =====================================================

class FatoContasPagar(Base):
    __table_args__ = {"schema": "processed"}
    __tablename__ = "fato_contas_pagar"

    # ============================
    # CHAVES
    # ============================
    
    # Chave primária (mesmo ID da raw)
    contas_pagar_id = Column(Integer, primary_key=True)
    
    # Chave de negócio (ID da API Bling)
    bling_contas_pagar_id = Column(BigInteger, unique=True, nullable=False, index=True)
    
    # ============================
    # MÉTRICAS FINANCEIRAS
    # ============================
    
    valor = Column(Numeric(15, 2), nullable=False)
    
    # ============================
    # ATRIBUTOS DESCRITIVOS
    # ============================
    
    situacao = Column(String(50), nullable=True, index=True)
    
    # ============================
    # CHAVES ESTRANGEIRAS
    # ============================
    
    # FK para dim_tempo (data de vencimento)
    data_vencimento = Column(Date, ForeignKey('processed.dim_tempo.data_completa'), nullable=False, index=True)
    
    # FK para dim_contatos (fornecedor/contato)
    bling_cliente_id = Column(BigInteger, index=True)  # Vai virar FK depois quando ligar com dim_contatos
    
    # FK para dim_formas_pagamento
    forma_pagamento_id = Column(Integer, ForeignKey('processed.dim_formas_pagamento.forma_pagamento_id'), nullable=True, index=True)
    
    # ============================
    # METADADOS
    # ============================
    
    data_ingestao = Column(DateTime, nullable=True)
    data_processamento = Column(DateTime, default=datetime.now, nullable=False)

    def __repr__(self):
        return f"<FatoContasPagar(contas_pagar_id={self.contas_pagar_id}, valor={self.valor}, situacao='{self.situacao}')>"