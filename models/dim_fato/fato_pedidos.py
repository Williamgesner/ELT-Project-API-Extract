# Responsável por: definir a estrutura da tabela fato_pedidos no schema processed

from datetime import datetime
from sqlalchemy import Column, Integer, String, BigInteger, Date, Numeric, DateTime, ForeignKey, UniqueConstraint, ForeignKeyConstraint
from config.database import Base

# =====================================================
# 1. MODELO DA TABELA - FATO_PEDIDOS
# =====================================================

class FatoPedidos(Base):
    __tablename__ = "fato_pedidos"
    __table_args__ = (
        UniqueConstraint('bling_pedido_id', 'empresa_id', name='uq_fato_pedidos_bling_empresa'),
        ForeignKeyConstraint(
            ['bling_canal_id', 'empresa_id'],
            ['processed.dim_canais.bling_canal_id', 'processed.dim_canais.empresa_id'],
            name='fk_pedidos_canal'
        ),
        ForeignKeyConstraint(
            ['situacao', 'empresa_id'],
            ['processed.dim_situacao.bling_situacao_id', 'processed.dim_situacao.empresa_id'],
            name='fk_pedidos_situacao'
        ),
        {"schema": "processed"}
    )

    # ============================
    # CHAVES
    # ============================
    
    # Chave primária
    pedido_id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Chave de negócio (ID da API Bling)
    bling_pedido_id = Column(BigInteger, nullable=False, index=True)
    
    # Chave da empresa
    empresa_id = Column(Integer, ForeignKey('processed.dim_empresas.empresa_id'), nullable=False, index=True)
    
    # Número do pedido (visível para usuários)
    numero_pedido_lv = Column(String(50), index=True)
    # Número do pedido que vai na NF
    numero_pedido_bling = Column(BigInteger, index=True)
    
    # ============================
    # CHAVES ESTRANGEIRAS
    # ============================
    
    # FK para dim_tempo (usando data como FK)
    data_pedido = Column(Date, ForeignKey('processed.dim_tempo.data_completa'), nullable=False, index=True)
    
    cliente_id = Column(Integer, ForeignKey('processed.dim_contatos.cliente_id'), index=True)
    
    bling_canal_id = Column(
        Integer,
        nullable=True,
        index=True,
        comment='Canal de venda - FK composta com empresa_id para dim_canais'
    )
    
    # ============================
    # MÉTRICAS FINANCEIRAS
    # ============================
    
    valor_total = Column(Numeric(15, 2), nullable=False)
    valor_frete = Column(Numeric(15, 2), default=0, nullable=False)
    
    # ============================
    # MÉTRICAS DE QUANTIDADE
    # ============================
    
    # Quantos tipos de produto diferentes no pedido
    quantidade_itens_total = Column(Integer, default=0)
    # Quantas unidades totais (soma das quantidades)
    quantidade_produtos_total = Column(Integer, default=0)
    
    # ============================
    # ATRIBUTOS DESCRITIVOS
    # ============================
    
    situacao = Column(Integer, nullable=True, index=True, comment='Código da situação (FK composta com empresa_id para dim_situacao)')
    
    # ============================
    # METADADOS
    # ============================
    
    # Data de quando foi extraído da API
    data_ingestao = Column(DateTime)
    # Data de quando foi processado para o DW
    data_processamento = Column(DateTime, default=datetime.now, nullable=False)

    def __repr__(self):
        return f"<FatoPedidos(pedido_id={self.pedido_id}, empresa_id={self.empresa_id}, numero={self.numero_pedido_bling}, valor={self.valor_total})>"