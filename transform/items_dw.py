# =====================================================
# TRANSFORMADOR DE ITENS DE PEDIDOS - MULTI-CNPJ
# =====================================================
# Responsável por: Extrair itens do array dentro de vendas_raw
# e transformar para fato_itens_pedidos

import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from config.database import Session, engine

# =====================================================
# 1. CLASSE TRANSFORMADORA
# =====================================================

class ItensTransformer:
    """
    Transformador específico para itens de pedidos
    Extrai itens do JSON de vendas_raw e cria registros individuais
    """

    def __init__(self, empresa_id):
        self.empresa_id = empresa_id
        self.engine = engine

    # =====================================================
    # 2. EXTRAIR DADOS DE VENDAS_RAW
    # =====================================================

    def extrair_vendas_com_itens(self):
        """
        Extrai vendas que já foram processadas em fato_pedidos
        e que possuem itens no JSON
        """
        print("\n1️⃣ EXTRAINDO VENDAS COM ITENS DE RAW.VENDAS_RAW...")

        query = text("""
            SELECT 
                vr.bling_id as bling_pedido_id,
                vr.dados_json->'itens' as itens_json,
                fp.pedido_id
            FROM raw.vendas_raw vr
            INNER JOIN processed.fato_pedidos fp 
                ON vr.bling_id = fp.bling_pedido_id
                AND vr.empresa_id = fp.empresa_id
            WHERE vr.empresa_id = :empresa_id
            AND vr.dados_json->'itens' IS NOT NULL
            AND jsonb_array_length(vr.dados_json->'itens') > 0
            ORDER BY fp.pedido_id
        """)

        df_vendas = pd.read_sql(query, self.engine, params={"empresa_id": self.empresa_id})
        print(f"✅ {len(df_vendas)} pedidos com itens encontrados (empresa_id = {self.empresa_id})")

        return df_vendas

    # =====================================================
    # 3. EXPLODIR ARRAY DE ITENS
    # =====================================================

    def explodir_itens(self, df_vendas):
        """
        Transforma cada item do array em uma linha separada
        
        Exemplo:
        Pedido 1 com 3 itens → 3 linhas na tabela fato_itens_pedidos
        """
        print("\n2️⃣ EXPLODINDO ARRAY DE ITENS...")

        # Lista para armazenar todos os itens
        itens_processados = []

        for idx, row in df_vendas.iterrows():
            pedido_id = row['pedido_id']
            itens_json = row['itens_json']

            # Iterar sobre cada item do array
            if isinstance(itens_json, list):
                for item in itens_json:
                    item_processado = {
                        'pedido_id': pedido_id,
                        'empresa_id': self.empresa_id,
                        'bling_item_id': item.get('id'),
                        'bling_produto_id': item.get('produto', {}).get('id'),
                        'descricao_item': item.get('descricao'),
                        'codigo_produto': item.get('codigo'),
                        'quantidade': item.get('quantidade', 0),
                        'preco_unitario': item.get('valor', 0),
                        'desconto_valor': item.get('desconto', 0),
                    }
                    itens_processados.append(item_processado)

        # Criar DataFrame com todos os itens
        df_itens = pd.DataFrame(itens_processados)
        
        print(f"✅ {len(df_itens)} itens extraídos de {len(df_vendas)} pedidos")
        print(f"   Média de {len(df_itens)/len(df_vendas):.1f} itens por pedido")

        return df_itens

    # =====================================================
    # 4. MAPEAR PRODUTO_ID
    # =====================================================

    def mapear_produto_id(self, df_itens):
        """
        Busca o produto_id na dim_produtos usando o bling_produto_id
        """
        print("\n3️⃣ MAPEANDO PRODUTO_ID NA DIM_PRODUTOS...")

        session = Session()

        try:
            # Buscar mapeamento de produtos PARA ESTA EMPRESA
            query = text("""
                SELECT bling_produto_id, produto_id
                FROM processed.dim_produtos
                WHERE empresa_id = :empresa_id
            """)

            resultado = session.execute(query, {"empresa_id": self.empresa_id})
            mapa_produtos = {row.bling_produto_id: row.produto_id for row in resultado}

            if mapa_produtos:
                df_itens['produto_id'] = df_itens['bling_produto_id'].map(mapa_produtos)

                produtos_mapeados = df_itens['produto_id'].notna().sum()
                produtos_nao_mapeados = df_itens['produto_id'].isna().sum()

                print(f"   ✅ {produtos_mapeados} produtos mapeados")

                if produtos_nao_mapeados > 0:
                    print(f"   ⚠️  {produtos_nao_mapeados} produtos não encontrados na dim_produtos")
                    print(f"      Estes itens ficarão com produto_id = NULL")
            else:
                print("   ⚠️  Nenhum produto encontrado na dim_produtos")
                df_itens['produto_id'] = None

            # Remover coluna auxiliar
            df_itens = df_itens.drop(columns=['bling_produto_id'])

        except Exception as e:
            print(f"   ⚠️  Erro ao mapear produtos: {e}")
            df_itens['produto_id'] = None
        finally:
            session.close()

        return df_itens

    # =====================================================
    # 5. CALCULAR PRECO_TOTAL
    # =====================================================

    def calcular_metricas(self, df_itens):
        """
        Calcula preco_total = (quantidade * preco_unitario) - desconto
        """
        print("\n4️⃣ CALCULANDO MÉTRICAS FINANCEIRAS...")

        # Converter para float
        df_itens['quantidade'] = pd.to_numeric(df_itens['quantidade'], errors='coerce').fillna(0)
        df_itens['preco_unitario'] = pd.to_numeric(df_itens['preco_unitario'], errors='coerce').fillna(0)
        df_itens['desconto_valor'] = pd.to_numeric(df_itens['desconto_valor'], errors='coerce').fillna(0)

        # Calcular preço total
        df_itens['preco_total'] = (df_itens['quantidade'] * df_itens['preco_unitario']) - df_itens['desconto_valor']

        # Arredondar valores
        df_itens['preco_unitario'] = df_itens['preco_unitario'].round(2)
        df_itens['preco_total'] = df_itens['preco_total'].round(2)
        df_itens['desconto_valor'] = df_itens['desconto_valor'].round(2)
        df_itens['quantidade'] = df_itens['quantidade'].round(3)

        print(f"✅ Métricas calculadas")
        print(f"   Valor total dos itens: R$ {df_itens['preco_total'].sum():,.2f}")

        return df_itens

    # =====================================================
    # 6. PREPARAR PARA EXPORTAÇÃO
    # =====================================================

    def preparar_para_exportacao(self, df_itens):
        """
        Seleciona e ordena as colunas finais
        """
        print("\n5️⃣ PREPARANDO DADOS PARA EXPORTAÇÃO...")

        # Adicionar metadados
        df_itens['data_processamento'] = datetime.now()

        # Colunas finais (sem item_id que é autoincremental)
        colunas_finais = [
            'pedido_id',
            'empresa_id',
            'produto_id',
            'bling_item_id',
            'quantidade',
            'preco_unitario',
            'preco_total',
            'desconto_valor',
            'descricao_item',
            'data_processamento'
        ]

        df_itens = df_itens[[col for col in colunas_finais if col in df_itens.columns]]

        print(f"✅ {len(df_itens)} itens prontos para exportação")
        return df_itens

    # =====================================================
    # 7. VALIDAR DADOS
    # =====================================================

    def validar_dados(self, df_itens):
        """
        Executa validações de qualidade
        """
        print("\n6️⃣ VALIDANDO DADOS...")

        total = len(df_itens)
        com_produto = df_itens['produto_id'].notna().sum()
        com_quantidade = (df_itens['quantidade'] > 0).sum()
        com_preco = (df_itens['preco_unitario'] > 0).sum()

        print(f"\n   📊 ESTATÍSTICAS DE QUALIDADE:")
        print(f"      • Total de itens: {total}")
        print(f"      • Com produto_id: {com_produto} ({com_produto/total*100:.1f}%)")
        print(f"      • Com quantidade > 0: {com_quantidade} ({com_quantidade/total*100:.1f}%)")
        print(f"      • Com preço > 0: {com_preco} ({com_preco/total*100:.1f}%)")

        # Alertas
        if com_produto < total * 0.9:
            print(f"\n   ⚠️  ATENÇÃO: Menos de 90% dos itens têm produto_id!")
            print(f"      Verifique se dim_produtos está completa")

        if com_quantidade < total:
            print(f"\n   ⚠️  {total - com_quantidade} itens com quantidade zero ou inválida")

        return df_itens

    # =====================================================
    # 8. EXPORTAR COM COMPARAÇÃO INTELIGENTE
    # =====================================================

    def exportar_para_processed(self, df_itens):
        """
        Exporta para processed.fato_itens_pedidos usando UPSERT
        Compara antes de inserir para evitar duplicatas
        """
        print("\n7️⃣ EXPORTANDO PARA PROCESSED.FATO_ITENS_PEDIDOS...")

        if len(df_itens) == 0:
            print("⚠️  Nenhum item para exportar")
            return 0

        session = Session()

        try:
            # === BUSCAR ITENS EXISTENTES ===
            print("🔍 Buscando itens existentes para comparação...")
            inicio_busca = datetime.now()

            query = text("""
                SELECT 
                    pedido_id,
                    empresa_id,
                    bling_item_id
                FROM processed.fato_itens_pedidos
                WHERE empresa_id = :empresa_id
            """)

            df_existentes = pd.read_sql(query, self.engine, params={"empresa_id": self.empresa_id})
            fim_busca = datetime.now()

            print(f"📋 {len(df_existentes)} itens existentes carregados em {fim_busca - inicio_busca}")

            # === IDENTIFICAR NOVOS E DUPLICATAS ===
            print("🔍 Identificando itens novos...")

            if len(df_existentes) > 0:
                # Criar chave composta para identificar duplicatas
                df_existentes['chave'] = df_existentes['pedido_id'].astype(str) + '_' + df_existentes['bling_item_id'].astype(str)
                df_itens_temp = df_itens.copy()
                df_itens_temp['chave'] = df_itens_temp['pedido_id'].astype(str) + '_' + df_itens_temp['bling_item_id'].astype(str)

                # Filtrar apenas novos
                chaves_existentes = set(df_existentes['chave'])
                df_novos = df_itens_temp[~df_itens_temp['chave'].isin(chaves_existentes)].copy()
                
                # Remover coluna auxiliar
                if 'chave' in df_novos.columns:
                    df_novos = df_novos.drop(columns=['chave'])

                itens_ignorados = len(df_itens) - len(df_novos)
            else:
                df_novos = df_itens.copy()
                itens_ignorados = 0

            print(f"\n📊 CLASSIFICAÇÃO:")
            print(f"   • 🆕 Novos (inserir): {len(df_novos)}")
            print(f"   • ⏭️  Já existentes (ignorar): {itens_ignorados}")

            # === INSERIR NOVOS ===
            if len(df_novos) > 0:
                print(f"\n💾 Inserindo {len(df_novos)} itens novos...")
                
                # IMPORTANTE: Reset do index para evitar problemas
                df_novos = df_novos.reset_index(drop=True)
                
                # Garantir que as colunas estão na ordem correta
                colunas_finais = [
                    'pedido_id',
                    'empresa_id',
                    'produto_id', 
                    'bling_item_id',
                    'quantidade',
                    'preco_unitario',
                    'preco_total',
                    'desconto_valor',
                    'descricao_item',
                    'data_processamento'
                ]
                
                df_novos = df_novos[colunas_finais]

                # Inserir em lotes menores para evitar problemas
                batch_size = 500
                total_inserido = 0
                
                for i in range(0, len(df_novos), batch_size):
                    batch = df_novos.iloc[i:i+batch_size]
                    
                    batch.to_sql(
                        name='fato_itens_pedidos',
                        con=self.engine,
                        schema='processed',
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    
                    total_inserido += len(batch)
                    print(f"   ✅ {total_inserido}/{len(df_novos)} itens inseridos...")

                print(f"✅ Todas as inserções concluídas")
            else:
                print(f"\n✨ Nenhum item novo! Tabela já está atualizada.")

            # === VERIFICAR TOTAL ===
            query = text("SELECT COUNT(*) FROM processed.fato_itens_pedidos WHERE empresa_id = :empresa_id")
            total = session.execute(query, {"empresa_id": self.empresa_id}).scalar()

            print(f"\n🎉 EXPORTAÇÃO CONCLUÍDA!")
            print(f"   • Total na tabela (empresa_id={self.empresa_id}): {total}")
            if itens_ignorados > 0:
                print(f"   • Economia: {itens_ignorados} duplicatas evitadas!")

            return len(df_novos) if len(df_novos) > 0 else 0

        except Exception as e:
            session.rollback()
            print(f"❌ ERRO ao exportar: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            session.close()

    # =====================================================
    # 9. EXECUTAR TRANSFORMAÇÃO COMPLETA
    # =====================================================

    def executar_transformacao_completa(self):
        """
        Executa o pipeline completo de transformação de itens
        """
        try:
            print("\n" + "=" * 70)
            print("🔄 TRANSFORMAÇÃO: ITENS DE PEDIDOS → FATO_ITENS_PEDIDOS")
            print("=" * 70)

            inicio = datetime.now()

            # 1. Extrair vendas com itens
            df_vendas = self.extrair_vendas_com_itens()

            if len(df_vendas) == 0:
                print(f"\n⚠️  Nenhuma venda com itens encontrada para empresa_id = {self.empresa_id}")
                return

            # 2. Explodir array de itens
            df_itens = self.explodir_itens(df_vendas)

            # 3. Mapear produto_id
            df_itens = self.mapear_produto_id(df_itens)

            # 4. Calcular métricas
            df_itens = self.calcular_metricas(df_itens)

            # 5. Preparar para exportação
            df_itens = self.preparar_para_exportacao(df_itens)

            # 6. Validar
            df_itens = self.validar_dados(df_itens)

            # 7. Exportar
            total_exportado = self.exportar_para_processed(df_itens)

            # Relatório final
            fim = datetime.now()
            tempo_total = fim - inicio

            print(f"\n{'='*70}")
            print(f"🎉 TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO!")
            print(f"⏱️  Tempo total: {tempo_total}")
            print(f"{'='*70}")

            print(f"\n📊 RESUMO FINAL:")
            print(f"   • Pedidos processados: {len(df_vendas)}")
            print(f"   • Itens extraídos: {len(df_itens)}")
            print(f"   • Itens inseridos: {total_exportado}")

        except Exception as e:
            print(f"\n❌ ERRO na transformação: {e}")
            raise