# Responsável por: definir a estrutura da tabela dim_contatos no schema processed

from datetime import datetime
from sqlalchemy import Column, Integer, String, BigInteger, DateTime, ForeignKey, UniqueConstraint
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - DIM_CONTATOS
# =====================================================

class DimContatos(Base):
    __tablename__ = "dim_contatos"
    __table_args__ = (
        UniqueConstraint('bling_cliente_id', 'empresa_id', name='uq_dim_contatos_bling_empresa'),
        {"schema": "processed"}
    )

    # Chave primária
    cliente_id = Column(Integer, primary_key=True)  # ID interno (mesmo do raw)
    
    # Chave de negócio
    bling_cliente_id = Column(BigInteger, nullable=False, index=True)  # ID da API Bling
    
    # Chave da empresa
    empresa_id = Column(Integer, ForeignKey('processed.dim_empresas.empresa_id'), nullable=False, index=True)
    
    # Dados do contato
    nome = Column(String(255), nullable=True)
    cpf_cnpj = Column(String(14), nullable=True, index=True)  # 11 (CPF) ou 14 (CNPJ) dígitos
    tipo_pessoa = Column(String(1), nullable=True)  # 'F' (Física) ou 'J' (Jurídica)
    telefone = Column(String(20), nullable=True)
    
    # Endereço
    cidade = Column(String(100), nullable=True)
    estado = Column(String(2), nullable=True)  # UF (ex: SP, RJ)
    cep = Column(String(10), nullable=True)  # Formato: xx.xxx-xx
    
    # Metadados
    data_ingestao = Column(DateTime, nullable=True)  # Data de quando foi extraído da API
    data_processamento = Column(DateTime, default=datetime.now, nullable=False)  # Data de quando foi processado

    def __repr__(self):
        return f"<DimContatos(bling_id={self.bling_cliente_id}, empresa_id={self.empresa_id}, nome='{self.nome}', tipo='{self.tipo_pessoa}')>"