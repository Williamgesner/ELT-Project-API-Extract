# Responsável por: definir a estrutura da tabela fato_nfe no schema processed

from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, Numeric, Date, String, DateTime, ForeignKey, UniqueConstraint
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - FATO_NFE
# =====================================================

class FatoNFe(Base):
    __tablename__ = "fato_nfe"
    __table_args__ = (
        UniqueConstraint('bling_nfe_id', 'empresa_id', name='uq_fato_nfe_bling_empresa'),
        {"schema": "processed"}
    )

    # ============================
    # CHAVES
    # ============================
    
    # Chave primária (mesmo ID da raw) - BIGINT para suportar IDs grandes
    nfe_id = Column(BigInteger, primary_key=True)
    
    # Chave de negócio (ID da API Bling)
    bling_nfe_id = Column(BigInteger, nullable=False, index=True)
    
    # Chave da empresa
    empresa_id = Column(Integer, ForeignKey('processed.dim_empresas.empresa_id'), nullable=False, index=True)
    
    # ============================
    # ATRIBUTOS DESCRITIVOS
    # ============================
    
    tipo = Column(String(10), nullable=False, index=True)  # 'Entrada' ou 'Saida'
    numero_nfe = Column(String(20), nullable=True)
    situacao = Column(String(50), nullable=True, index=True)
    
    # Relacionamento com pedido (apenas para NFe de saída)
    numero_pedido = Column(BigInteger, nullable=True, index=True)
    
    # ============================
    # MÉTRICAS FINANCEIRAS
    # ============================
    
    valor_nf = Column(Numeric(15, 2), nullable=True)
    valor_frete = Column(Numeric(15, 2), nullable=True)
    
    # ============================
    # CHAVES ESTRANGEIRAS
    # ============================
    
    # FK para dim_tempo (data de emissão)
    data_emissao = Column(Date, ForeignKey('processed.dim_tempo.data_completa'), nullable=True, index=True)
    
    # FK para dim_tempo (data de entrada/operação)
    data_entrada = Column(Date, ForeignKey('processed.dim_tempo.data_completa'), nullable=True, index=True)
    
    # FK para dim_canais
    bling_canal_id = Column(BigInteger, nullable=True, index=True)
    
    # FK para dim_contatos (cliente/fornecedor)
    bling_cliente_id = Column(BigInteger, nullable=True, index=True)
    
    # FK para natureza de operação (se criar dimensão futura)
    bling_natureza_operacao_id = Column(BigInteger, nullable=True, index=True)
    
    # ============================
    # METADADOS
    # ============================
    
    data_ingestao = Column(DateTime, nullable=True)
    data_processamento = Column(DateTime, default=datetime.now, nullable=False)

    def __repr__(self):
        return f"<FatoNFe(nfe_id={self.nfe_id}, empresa_id={self.empresa_id}, numero_nfe='{self.numero_nfe}', tipo='{self.tipo}', valor={self.valor_nf})>"