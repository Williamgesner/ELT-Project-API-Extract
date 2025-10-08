# =====================================================
# TRANSFORMADOR DE VENDAS
# =====================================================
# Responsável por: Limpar e transformar dados de vendas_raw
# para fato_pedidos no schema processed
# ESTRATÉGIA: Comparar antes de salvar (igual extratores)

import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from config.database import Session, engine
from extract.situation import obter_mapeamento_situacoes

# =====================================================
# 1. CLASSE TRANSFORMADORA
# =====================================================

class VendasTransformer:
    """
    Transformador específico para vendas
    Aplica todas as limpezas e padronizações necessárias
    """

    def __init__(self):
        self.engine = engine

    # =====================================================
    # 2. EXTRAIR DADOS RAW
    # =====================================================

    def extrair_dados_raw(self):
        """
        Extrai dados da tabela raw.vendas_raw
        """
        print("\n1️⃣ EXTRAINDO DADOS DE RAW.VENDAS_RAW...")

        query = """
            SELECT 
                id,
                bling_id,
                dados_json,
                data_ingestao
            FROM raw.vendas_raw
            WHERE status_processamento = 'pendente'
            ORDER BY bling_id
        """

        df_raw = pd.read_sql(query, self.engine)
        print(f"✅ {len(df_raw)} registros extraídos (status = 'pendente')")

        return df_raw

    # =====================================================
    # 3. EXPANDIR JSON
    # =====================================================

    def expandir_json(self, df_raw):
        """
        Expande o JSON em colunas
        """
        print("\n2️⃣ EXPANDINDO JSON EM COLUNAS...")

        # Normalizar o JSON principal
        df_json = pd.json_normalize(df_raw["dados_json"])

        # Renomear 'id' do JSON para 'id_bling'
        if "id" in df_json.columns:
            df_json = df_json.rename(columns={"id": "id_bling"})

        # Combinar com colunas originais
        df = pd.concat(
            [
                df_raw[["id", "bling_id", "data_ingestao"]],
                df_json,
            ],
            axis=1,
        )

        print(f"✅ JSON expandido! {len(df.columns)} colunas disponíveis")
        return df

    # =====================================================
    # 4. APLICAR TRANSFORMAÇÕES
    # =====================================================

    def aplicar_transformacoes(self, df):
        """
        Aplica TODAS as limpezas e transformações
        """
        print("\n3️⃣ APLICANDO TRANSFORMAÇÕES...")

        # === REMOVER COLUNAS DESNECESSÁRIAS ===
        print("   • Removendo colunas desnecessárias...")
        colunas_remover = [
            "id_bling",
            "numero",
            "contato.nome",
            "dataSaida",
            "dataPrevista",
            "parcelas",
            "observacoes",
            "outrasDespesas",
            "numeroPedidoCompra",
            "observacoesInternas",
            "taxas.valorBase",
            "taxas.custoFrete",
            "taxas.taxaComissao",
            "contato.tipoPessoa",
            "contato.numeroDocumento",
            "desconto.valor",
            "desconto.unidade",
            "situacao.valor",
            "vendedor.id",
            "categoria.id",
            "notaFiscal.id",
            "transporte.contato.id",
            "transporte.contato.nome",
            "transporte.volumes",
            "transporte.etiqueta.uf",
            "transporte.etiqueta.cep",
            "transporte.etiqueta.nome",
            "transporte.etiqueta.bairro",
            "transporte.etiqueta.numero",
            "transporte.etiqueta.endereco",
            "transporte.etiqueta.nomePais",
            "transporte.etiqueta.municipio",
            "transporte.etiqueta.complemento",
            "transporte.pesoBruto",
            "transporte.prazoEntrega",
            "transporte.fretePorConta",
            "transporte.quantidadeVolumes",
            "tributacao.totalIPI",
            "tributacao.totalICMS",
            "intermediador.cnpj",
            "intermediador.nomeUsuario",
        ]
        df = df.drop(columns=[col for col in colunas_remover if col in df.columns])

        # === RENOMEAR COLUNAS ===
        print("   • Renomeando colunas...")
        df = df.rename(
            columns={
                "id": "pedido_id",
                "bling_id": "bling_pedido_id",
                "total": "valor_total",
                "data": "data_pedido",
                "numeroLoja": "numero_pedido",
                "loja.id": "canal_id",
                "contato.id": "bling_cliente_id",
                "transporte.frete": "valor_frete",
            }
        )

        # === CONVERTER DATA DO PEDIDO ===
        print("   • Convertendo data_pedido...")
        df["data_pedido"] = df["data_pedido"].replace(["0000-00-00", "", " "], pd.NaT)
        df["data_pedido"] = pd.to_datetime(df["data_pedido"], errors="coerce")

        datas_invalidas = df["data_pedido"].isna().sum()
        if datas_invalidas > 0:
            print(f"   ⚠️  {datas_invalidas} datas inválidas (serão ignoradas)")

        # === EXTRAIR MÉTRICAS DE ITENS ===
        print("   • Extraindo métricas de itens...")

        def extrair_metricas_itens(itens_json):
            if itens_json is None or not isinstance(itens_json, list):
                return {"qtd_itens": 0, "qtd_produtos": 0}

            qtd_itens = len(itens_json)
            qtd_produtos = sum(item.get("quantidade", 0) for item in itens_json)

            return {"qtd_itens": qtd_itens, "qtd_produtos": qtd_produtos}

        metricas = df["itens"].apply(extrair_metricas_itens)
        df["quantidade_itens_total"] = metricas.apply(lambda x: x["qtd_itens"])
        df["quantidade_produtos_total"] = metricas.apply(lambda x: x["qtd_produtos"])

        # Remover coluna itens
        df = df.drop(columns=["itens"])

        # === LIMPAR STRINGS VAZIAS ===
        print("   • Limpando strings vazias...")
        for coluna in df.select_dtypes(include=["object"]).columns:
            df[coluna] = df[coluna].replace(r"^\s*$", np.nan, regex=True)
            df[coluna] = df[coluna].replace(["", " "], np.nan)

        # === MAPEAR SITUAÇÕES ===
        print("   • Mapeando situações (ID → nome)...")
        try:
            mapa_situacoes = obter_mapeamento_situacoes()
            if mapa_situacoes:
                df["situacao.id"] = df["situacao.id"].map(mapa_situacoes)
                print(f"   ✅ Situações mapeadas")
            else:
                print("   ⚠️  Nenhuma situação encontrada. Execute: python main_situacoes.py")
        except Exception as e:
            print(f"   ⚠️  Erro ao mapear situações: {e}")

        df = df.rename(columns={"situacao.id": "situacao"})

        # === BUSCAR CLIENTE_ID ===
        print("   • Buscando cliente_id na dim_contatos...")
        df = self._mapear_cliente_id(df)

        # === ADICIONAR METADADOS ===
        print("   • Adicionando metadados...")
        df["data_processamento"] = datetime.now()

        print("✅ Todas as transformações aplicadas!")
        return df

    # =====================================================
    # 5. MAPEAR CLIENTE_ID
    # =====================================================

    def _mapear_cliente_id(self, df):
        """
        Busca o cliente_id na dim_contatos usando o bling_cliente_id
        """
        session = Session()

        try:
            query = text(
                """
                SELECT bling_cliente_id, cliente_id
                FROM processed.dim_contatos
            """
            )

            resultado = session.execute(query)
            mapa_clientes = {row.bling_cliente_id: row.cliente_id for row in resultado}

            if mapa_clientes:
                df["cliente_id"] = df["bling_cliente_id"].map(mapa_clientes)

                clientes_mapeados = df["cliente_id"].notna().sum()
                clientes_nao_mapeados = df["cliente_id"].isna().sum()

                print(f"   ✅ {clientes_mapeados} clientes mapeados")

                if clientes_nao_mapeados > 0:
                    print(f"   ⚠️  {clientes_nao_mapeados} clientes não encontrados na dim_contatos")
            else:
                print("   ⚠️  Nenhum cliente encontrado na dim_contatos")
                df["cliente_id"] = None

            df = df.drop(columns=["bling_cliente_id"])

        except Exception as e:
            print(f"   ⚠️  Erro ao mapear clientes: {e}")
            df["cliente_id"] = None
        finally:
            session.close()

        return df

    # =====================================================
    # 6. PREPARAR PARA EXPORTAÇÃO
    # =====================================================

    def preparar_para_exportacao(self, df):
        """
        Ordena colunas e prepara DataFrame final
        """
        print("\n4️⃣ PREPARANDO DADOS PARA EXPORTAÇÃO...")

        colunas_finais = [
            "pedido_id",
            "bling_pedido_id",
            "numero_pedido",
            "data_pedido",
            "cliente_id",
            "canal_id",
            "valor_total",
            "valor_frete",
            "quantidade_itens_total",
            "quantidade_produtos_total",
            "situacao",
            "data_ingestao",
            "data_processamento",
        ]

        df = df[[col for col in colunas_finais if col in df.columns]]

        print(f"✅ Dados preparados! {len(df)} registros x {len(df.columns)} colunas")
        return df

    # =====================================================
    # 7. VALIDAR DADOS
    # =====================================================

    def validar_dados(self, df):
        """
        Executa validações de qualidade
        """
        print("\n5️⃣ EXECUTANDO VALIDAÇÕES...")

        total = len(df)

        # Remover registros sem data
        df = df[df["data_pedido"].notna()]
        removidos_sem_data = total - len(df)

        if removidos_sem_data > 0:
            print(f"   ⚠️  {removidos_sem_data} registros sem data removidos")

        # Validações
        com_numero = df["numero_pedido"].notna().sum()
        com_cliente = df["cliente_id"].notna().sum()
        com_situacao = df["situacao"].notna().sum()

        print(f"\n   📊 ESTATÍSTICAS DE QUALIDADE:")
        print(f"      • Total após filtros: {len(df)}")
        print(f"      • Com número pedido: {com_numero} ({com_numero/len(df)*100:.1f}%)")
        print(f"      • Com cliente: {com_cliente} ({com_cliente/len(df)*100:.1f}%)")
        print(f"      • Com situação: {com_situacao} ({com_situacao/len(df)*100:.1f}%)")

        # Verificar duplicatas
        duplicatas = df.duplicated(subset=["bling_pedido_id"]).sum()
        if duplicatas > 0:
            print(f"\n   ⚠️  {duplicatas} registros duplicados encontrados!")
            df = df.drop_duplicates(subset=["bling_pedido_id"], keep="first")
        else:
            print(f"\n   ✅ Nenhuma duplicata encontrada")

        return df

    # =====================================================
    # 8. EXPORTAR COM COMPARAÇÃO INTELIGENTE
    # =====================================================

    def exportar_para_processed(self, df):
        """
        Exporta dados comparando antes de salvar (IGUAL EXTRATORES)
        - Busca registros existentes
        - Compara campos relevantes
        - INSERT apenas novos
        - UPDATE apenas diferentes
        - SKIP idênticos
        """
        print("\n6️⃣ EXPORTANDO PARA PROCESSED.FATO_PEDIDOS...")
        
        if len(df) == 0:
            print("⚠️  Nenhum registro para exportar")
            return 0
        
        session = Session()
        
        try:
            # === BUSCAR REGISTROS EXISTENTES ===
            print("🔍 Buscando registros existentes para comparação...")
            inicio_busca = datetime.now()
            
            query = text("""
                SELECT 
                    pedido_id,
                    bling_pedido_id,
                    valor_total,
                    situacao,
                    quantidade_itens_total,
                    quantidade_produtos_total
                FROM processed.fato_pedidos
            """)
            
            df_existentes = pd.read_sql(query, self.engine)
            fim_busca = datetime.now()
            
            print(f"📋 {len(df_existentes)} registros existentes carregados em {fim_busca - inicio_busca}")
            
            # === CLASSIFICAR: NOVOS, DIFERENTES, IDÊNTICOS ===
            print("🔍 Comparando registros...")
            inicio_comparacao = datetime.now()
            
            # Criar dicionário de existentes para lookup rápido
            existentes_dict = df_existentes.set_index('bling_pedido_id').to_dict('index')
            
            registros_novos = []
            registros_atualizar = []
            registros_identicos = 0
            
            for idx, row in df.iterrows():
                bling_id = row['bling_pedido_id']
                
                if bling_id not in existentes_dict:
                    # NOVO → INSERT
                    registros_novos.append(row)
                else:
                    # EXISTE → Comparar campos relevantes
                    existente = existentes_dict[bling_id]
                    
                    # Comparar valores (arredondar floats)
                    valor_mudou = round(float(row['valor_total']), 2) != round(float(existente['valor_total']), 2)
                    situacao_mudou = str(row['situacao']) != str(existente['situacao'])
                    qtd_mudou = (
                        int(row['quantidade_itens_total']) != int(existente['quantidade_itens_total']) or
                        int(row['quantidade_produtos_total']) != int(existente['quantidade_produtos_total'])
                    )
                    
                    if valor_mudou or situacao_mudou or qtd_mudou:
                        # DIFERENTE → UPDATE
                        row['pedido_id'] = existente['pedido_id']  # Manter ID existente
                        registros_atualizar.append(row)
                    else:
                        # IDÊNTICO → SKIP
                        registros_identicos += 1
            
            fim_comparacao = datetime.now()
            print(f"✅ Comparação concluída em {fim_comparacao - inicio_comparacao}")
            
            # === RELATÓRIO ===
            print(f"\n📊 CLASSIFICAÇÃO DOS REGISTROS:")
            print(f"   • 🆕 Novos (inserir): {len(registros_novos)}")
            print(f"   • 🔄 Diferentes (atualizar): {len(registros_atualizar)}")
            print(f"   • ⏭️ Idênticos (ignorar): {registros_identicos}")
            
            # === INSERIR NOVOS ===
            if registros_novos:
                print(f"\n💾 Inserindo {len(registros_novos)} registros novos...")
                df_novos = pd.DataFrame(registros_novos)
                df_novos.to_sql(
                    name='fato_pedidos',
                    con=self.engine,
                    schema='processed',
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=500
                )
                print(f"✅ Inserções concluídas")
            
            # === ATUALIZAR DIFERENTES ===
            if registros_atualizar:
                print(f"\n🔄 Atualizando {len(registros_atualizar)} registros diferentes...")
                
                for i, row in enumerate(registros_atualizar):
                    stmt = text("""
                        UPDATE processed.fato_pedidos
                        SET 
                            bling_pedido_id = :bling_pedido_id,
                            numero_pedido = :numero_pedido,
                            data_pedido = :data_pedido,
                            cliente_id = :cliente_id,
                            canal_id = :canal_id,
                            valor_total = :valor_total,
                            valor_frete = :valor_frete,
                            quantidade_itens_total = :quantidade_itens_total,
                            quantidade_produtos_total = :quantidade_produtos_total,
                            situacao = :situacao,
                            data_processamento = :data_processamento
                        WHERE pedido_id = :pedido_id
                    """)
                    
                    session.execute(stmt, {
                        'pedido_id': int(row['pedido_id']),
                        'bling_pedido_id': int(row['bling_pedido_id']),
                        'numero_pedido': str(row['numero_pedido']) if pd.notna(row['numero_pedido']) else None,
                        'data_pedido': row['data_pedido'].date() if pd.notna(row['data_pedido']) else None,
                        'cliente_id': int(row['cliente_id']) if pd.notna(row['cliente_id']) else None,
                        'canal_id': int(row['canal_id']) if pd.notna(row['canal_id']) else None,
                        'valor_total': float(row['valor_total']),
                        'valor_frete': float(row['valor_frete']) if pd.notna(row['valor_frete']) else 0,
                        'quantidade_itens_total': int(row['quantidade_itens_total']),
                        'quantidade_produtos_total': int(row['quantidade_produtos_total']),
                        'situacao': str(row['situacao']) if pd.notna(row['situacao']) else None,
                        'data_processamento': row['data_processamento']
                    })
                    
                    if (i + 1) % 100 == 0:
                        session.commit()
                        print(f"   Atualizados {i + 1}/{len(registros_atualizar)} registros...")
                
                session.commit()
                print(f"✅ Atualizações concluídas")
            
            if not registros_novos and not registros_atualizar:
                print(f"\n✨ Nenhum registro novo ou alterado! DW já está atualizado.")
            
            # === VERIFICAR TOTAL ===
            query = text("SELECT COUNT(*) FROM processed.fato_pedidos")
            total = session.execute(query).scalar()
            
            print(f"\n🎉 EXPORTAÇÃO CONCLUÍDA!")
            print(f"   • Total na tabela: {total}")
            print(f"   • Economia: {registros_identicos} atualizações desnecessárias evitadas!")
            
            return len(df)
            
        except Exception as e:
            session.rollback()
            print(f"❌ ERRO ao exportar: {e}")
            raise
        finally:
            session.close()

    # =====================================================
    # 9. ATUALIZAR STATUS RAW
    # =====================================================

    def atualizar_status_raw(self, df):
        """
        Atualiza status dos registros processados
        """
        print("\n7️⃣ ATUALIZANDO STATUS NA TABELA RAW...")

        session = Session()

        try:
            ids_processados = df["pedido_id"].tolist()

            query = text(
                """
                UPDATE raw.vendas_raw
                SET status_processamento = 'processado'
                WHERE id = ANY(:ids)
            """
            )

            resultado = session.execute(query, {"ids": ids_processados})
            session.commit()

            print(f"✅ {resultado.rowcount} registros marcados como 'processado'")

        except Exception as e:
            session.rollback()
            print(f"⚠️  Erro ao atualizar status: {e}")
        finally:
            session.close()

    # =====================================================
    # 10. EXECUTAR TRANSFORMAÇÃO COMPLETA
    # =====================================================

    def executar_transformacao_completa(self):
        """
        Executa o pipeline completo de transformação
        """
        try:
            # 1. Extrair dados raw
            df_raw = self.extrair_dados_raw()

            if len(df_raw) == 0:
                print("\n✅ Nenhum registro pendente para processar!")
                return

            # 2. Expandir JSON
            df = self.expandir_json(df_raw)

            # 3. Aplicar transformações
            df = self.aplicar_transformacoes(df)

            # 4. Preparar para exportação
            df = self.preparar_para_exportacao(df)

            # 5. Validar
            df = self.validar_dados(df)

            # 6. Exportar (COM COMPARAÇÃO INTELIGENTE)
            total_exportado = self.exportar_para_processed(df)

            # 7. Atualizar status
            self.atualizar_status_raw(df)

            # Relatório final
            print(f"\n{'='*70}")
            print(f"🎉 TRANSFORMAÇÃO CONCLUÍDA!")
            print(f"{'='*70}")
            print(f"\n   📊 RESUMO:")
            print(f"      • Registros processados: {len(df)}")
            print(f"      • Registros exportados: {total_exportado}")

        except Exception as e:
            print(f"\n❌ ERRO na transformação: {e}")
            raise