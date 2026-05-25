# =====================================================
# TRANSFORMADOR DE VENDAS - MULTI-CNPJ
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

# =====================================================
# 1. CLASSE TRANSFORMADORA
# =====================================================

class VendasTransformer:
    """
    Transformador específico para vendas
    Aplica todas as limpezas e padronizações necessárias
    """

    def __init__(self, empresa_id):
        self.empresa_id = empresa_id
        self.engine = engine

    # =====================================================
    # 2. EXTRAIR DADOS RAW
    # =====================================================

    def extrair_dados_raw(self):
        """
        Extrai dados da tabela raw.vendas_raw
        """
        print(f"\n1️⃣ EXTRAINDO DADOS DE RAW.VENDAS_RAW (empresa_id={self.empresa_id})...")

        query = text("""
            SELECT 
                id,
                bling_id,
                dados_json,
                data_ingestao
            FROM raw.vendas_raw
            WHERE status_processamento = 'pendente'
              AND empresa_id = :empresa_id
            ORDER BY bling_id
        """)

        df_raw = pd.read_sql(query, self.engine, params={"empresa_id": self.empresa_id})
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
                "numero": "numero_pedido_bling",
                "bling_id": "bling_pedido_id",
                "total": "valor_total",
                "data": "data_pedido",
                "numeroLoja": "numero_pedido_lv",
                "loja.id": "bling_canal_id",  # ✅ ID do Bling (FK para dim_canais)
                "contato.id": "bling_cliente_id",
                "transporte.frete": "valor_frete",
                "situacao.id": "situacao", 
            }
        )

        # === ADICIONAR EMPRESA_ID ===
        print(f"   • Adicionando empresa_id={self.empresa_id}...")
        df["empresa_id"] = self.empresa_id

        # === CONVERTER DATA DO PEDIDO ===
        print("   • Convertendo data_pedido...")
        df["data_pedido"] = df["data_pedido"].replace(["0000-00-00", "", " "], pd.NaT)
        df["data_pedido"] = pd.to_datetime(df["data_pedido"], errors="coerce")

        datas_invalidas = df["data_pedido"].isna().sum()
        if datas_invalidas > 0:
            print(f"   ⚠️  {datas_invalidas} datas inválidas (serão ignoradas)")

        # ✅ Tratar bling_canal_id = 0 → NULL
        pedidos_sem_canal = (df["bling_canal_id"] == 0).sum()
        if pedidos_sem_canal > 0:
            print(f"   🔧 {pedidos_sem_canal} pedidos com canal=0 convertidos para NULL")
        df.loc[df["bling_canal_id"] == 0, "bling_canal_id"] = None

        # 🆕 NOVO: Mapear NULL → OUTROS
        pedidos_null = df["bling_canal_id"].isna().sum()
        if pedidos_null > 0:
            print(f"   🔧 {pedidos_null} pedidos SEM canal → mapeando para 'OUTROS'")
            canal_outros_id = self._garantir_canal_outros()
            
            if canal_outros_id:
                df.loc[df["bling_canal_id"].isna(), "bling_canal_id"] = canal_outros_id
                print(f"   ✅ Pedidos mapeados para canal OUTROS (ID: {canal_outros_id})")
            else:
                print(f"   ⚠️ Não foi possível mapear para OUTROS - registros ficarão NULL")

        # === EXTRAIR MÉTRICAS DE ITENS ===
        print("   • Extraindo métricas de itens...")

        def extrair_metricas_itens(itens_json):
            if itens_json is None or not isinstance(itens_json, list):
                return {"qtd_itens": 0, "qtd_produtos": 0}

            qtd_itens = len(itens_json)  # # ← quantidade_itens_total (Capacete + pneu = 2 itens)
            qtd_produtos = sum(item.get("quantidade", 0) for item in itens_json) # # ← quantidade_produtos_total (2 Capacete + 2 pneus = 4 produtos)

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

        print("   • Convertendo situacao para Integer...")
        # Tratar situacao = 0 → NULL
        pedidos_sem_situacao = (df["situacao"] == 0).sum()
        if pedidos_sem_situacao > 0:
            print(f"   🔧 {pedidos_sem_situacao} pedidos com situacao=0 convertidos para NULL")
        df.loc[df["situacao"] == 0, "situacao"] = None
        
        # Garantir que situacao é Integer
        df["situacao"] = pd.to_numeric(df["situacao"], errors="coerce").astype("Int64")

        # === BUSCAR CLIENTE_ID ===
        print("   • Buscando cliente_id na dim_contatos...")
        df = self._mapear_cliente_id(df)

        # === ADICIONAR METADADOS ===
        print("   • Adicionando metadados...")
        df["data_processamento"] = datetime.now()

        # === GARANTIR SITUAÇÕES NA DIM (FK obrigatória) ===
        df = self._garantir_situacoes_faltantes(df)

        print("✅ Todas as transformações aplicadas!")
        return df

    # =====================================================
    # 5. GARANTIR SITUAÇÕES FALTANTES NA DIM
    # =====================================================

    def _garantir_situacoes_faltantes(self, df):
        """
        Garante que todos os situacao_id dos pedidos existem em dim_situacao.
        Evita violação da FK fk_pedidos_situacao.
        """
        situacoes_pedidos = (
            df["situacao"].dropna().astype(int).unique().tolist()
            if "situacao" in df.columns
            else []
        )
        if not situacoes_pedidos:
            return df

        session = Session()
        try:
            query_existentes = text("""
                SELECT bling_situacao_id
                FROM processed.dim_situacao
                WHERE empresa_id = :empresa_id
                  AND bling_situacao_id = ANY(:ids)
            """)
            resultado = session.execute(
                query_existentes,
                {"empresa_id": self.empresa_id, "ids": situacoes_pedidos},
            )
            existentes = {row.bling_situacao_id for row in resultado}
            faltantes = [s for s in situacoes_pedidos if s not in existentes]

            if not faltantes:
                return df

            print(f"   🔧 {len(faltantes)} situações ausentes em dim_situacao — inserindo...")

            query_nomes_raw = text("""
                SELECT bling_situacao_id, nome
                FROM raw.situacoes_raw
                WHERE empresa_id = :empresa_id
                  AND bling_situacao_id = ANY(:ids)
            """)
            nomes_raw = {
                row.bling_situacao_id: row.nome
                for row in session.execute(
                    query_nomes_raw,
                    {"empresa_id": self.empresa_id, "ids": faltantes},
                )
            }

            query_nomes_vendas = text("""
                SELECT DISTINCT ON ((dados_json->'situacao'->>'id')::integer)
                    (dados_json->'situacao'->>'id')::integer AS bling_situacao_id,
                    COALESCE(
                        NULLIF(TRIM(dados_json->'situacao'->>'valor'), ''),
                        NULLIF(TRIM(dados_json->'situacao'->>'nome'), '')
                    ) AS nome
                FROM raw.vendas_raw
                WHERE empresa_id = :empresa_id
                  AND (dados_json->'situacao'->>'id')::integer = ANY(:ids)
                ORDER BY (dados_json->'situacao'->>'id')::integer, data_ingestao DESC
            """)
            nomes_vendas = {
                row.bling_situacao_id: row.nome
                for row in session.execute(
                    query_nomes_vendas,
                    {"empresa_id": self.empresa_id, "ids": faltantes},
                )
            }

            agora = datetime.now()
            inseridos = 0
            for situacao_id in faltantes:
                nome = (
                    nomes_raw.get(situacao_id)
                    or nomes_vendas.get(situacao_id)
                    or f"Situação {situacao_id}"
                )
                nome = str(nome).strip() if nome else f"Situação {situacao_id}"

                stmt = text("""
                    INSERT INTO processed.dim_situacao
                    (bling_situacao_id, empresa_id, situacao, data_ingestao, data_processamento)
                    VALUES (:bling_situacao_id, :empresa_id, :situacao, :data_ingestao, :data_processamento)
                    ON CONFLICT (bling_situacao_id, empresa_id) DO NOTHING
                """)
                session.execute(
                    stmt,
                    {
                        "bling_situacao_id": int(situacao_id),
                        "empresa_id": self.empresa_id,
                        "situacao": nome[:100],
                        "data_ingestao": agora,
                        "data_processamento": agora,
                    },
                )
                inseridos += 1

            session.commit()
            print(f"   ✅ {inseridos} situações garantidas em dim_situacao")

        except Exception as e:
            session.rollback()
            print(f"   ⚠️ Erro ao garantir situações na dim: {e}")
            raise
        finally:
            session.close()

        return df

    # =====================================================
    # 6. GARANTIR CANAL "OUTROS"
    # =====================================================

    def _garantir_canal_outros(self):
        """
        Garante que existe um registro 'OUTROS' na dim_canais
        CADA EMPRESA tem seu próprio canal OUTROS com ID único
        
        Empresa 1 = -1000001
        Empresa 2 = -1000002
        Empresa 3 = -1000003
        """
        session = Session()
        
        try:
            # Verificar se já existe
            query = text("""
                SELECT bling_canal_id 
                FROM processed.dim_canais 
                WHERE nome_canal = 'OUTROS' 
                  AND empresa_id = :empresa_id
            """)
            
            resultado = session.execute(query, {"empresa_id": self.empresa_id}).fetchone()
            
            if resultado:
                return resultado[0]
            else:
                # 🆕 ID ÚNICO baseado na empresa
                canal_outros_id = -(1000000 + self.empresa_id)
                
                query_insert = text("""
                    INSERT INTO processed.dim_canais 
                    (bling_canal_id, empresa_id, nome_canal, data_ingestao, data_processamento)
                    VALUES 
                    (:bling_canal_id, :empresa_id, 'OUTROS', NOW(), NOW())
                    ON CONFLICT (bling_canal_id, empresa_id) DO NOTHING
                    RETURNING bling_canal_id
                """)
                
                resultado = session.execute(query_insert, {
                    "bling_canal_id": canal_outros_id,
                    "empresa_id": self.empresa_id
                })
                session.commit()
                
                return canal_outros_id
            
        except Exception as e:
            session.rollback()
            print(f"   ⚠️ Erro ao garantir canal OUTROS: {e}")
            return None
        finally:
            session.close()

    # =====================================================
    # 6. MAPEAR CLIENTE_ID
    # =====================================================

    def _mapear_cliente_id(self, df):
        """
        Busca o cliente_id na dim_contatos usando o bling_cliente_id
        """
        session = Session()

        try:
            query = text("""
                SELECT bling_cliente_id, cliente_id
                FROM processed.dim_contatos
                WHERE empresa_id = :empresa_id
            """)

            resultado = session.execute(query, {"empresa_id": self.empresa_id})
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
    # 7. PREPARAR PARA EXPORTAÇÃO
    # =====================================================

    def preparar_para_exportacao(self, df):
        """
        Ordena colunas e prepara DataFrame final
        """
        print("\n4️⃣ PREPARANDO DADOS PARA EXPORTAÇÃO...")

        colunas_finais = [
            "pedido_id",
            "bling_pedido_id",
            "empresa_id",
            "numero_pedido_lv",
            "numero_pedido_bling",
            "data_pedido",
            "cliente_id",
            "bling_canal_id", 
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
    # 8. VALIDAR DADOS
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
        com_numero = df["numero_pedido_bling"].notna().sum()
        com_cliente = df["cliente_id"].notna().sum()
        com_canal = df["bling_canal_id"].notna().sum()
        com_situacao = df["situacao"].notna().sum()

        print(f"\n   📊 ESTATÍSTICAS DE QUALIDADE:")
        print(f"      • Total após filtros: {len(df)}")
        print(f"      • Com número pedido: {com_numero} ({com_numero/len(df)*100:.1f}%)")
        print(f"      • Com cliente: {com_cliente} ({com_cliente/len(df)*100:.1f}%)")
        print(f"      • Com canal: {com_canal} ({com_canal/len(df)*100:.1f}%)")
        print(f"      • Com situação: {com_situacao} ({com_situacao/len(df)*100:.1f}%)")

        # Verificar duplicatas
        duplicatas = df.duplicated(subset=["bling_pedido_id", "empresa_id"]).sum()
        if duplicatas > 0:
            print(f"\n   ⚠️  {duplicatas} registros duplicados encontrados!")
            df = df.drop_duplicates(subset=["bling_pedido_id", "empresa_id"], keep="first")
        else:
            print(f"\n   ✅ Nenhuma duplicata encontrada")

        return df

    # =====================================================
    # 9. EXPORTAR COM COMPARAÇÃO INTELIGENTE
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
        print(f"\n6️⃣ EXPORTANDO PARA PROCESSED.FATO_PEDIDOS (empresa_id={self.empresa_id})...")
        
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
                    empresa_id,
                    valor_total,
                    situacao,
                    quantidade_itens_total,
                    quantidade_produtos_total
                FROM processed.fato_pedidos
                WHERE empresa_id = :empresa_id
            """)
            
            df_existentes = pd.read_sql(query, self.engine, params={"empresa_id": self.empresa_id})
            fim_busca = datetime.now()
            
            print(f"📋 {len(df_existentes)} registros existentes carregados em {fim_busca - inicio_busca}")
            
            # === CLASSIFICAR: NOVOS, DIFERENTES, IDÊNTICOS ===
            print("🔍 Comparando registros...")
            inicio_comparacao = datetime.now()
            
            # Criar dicionário de existentes para lookup rápido (chave composta)
            existentes_dict = {}
            for _, row in df_existentes.iterrows():
                chave = (row['bling_pedido_id'], row['empresa_id'])
                existentes_dict[chave] = row.to_dict()
            
            registros_novos = []
            registros_atualizar = []
            registros_identicos = 0
            
            for idx, row in df.iterrows():
                chave = (row['bling_pedido_id'], row['empresa_id'])
                
                if chave not in existentes_dict:
                    # NOVO → INSERT
                    registros_novos.append(row)
                else:
                    # EXISTE → Comparar campos relevantes
                    existente = existentes_dict[chave]
                    
                    valor_mudou = round(float(row['valor_total']), 2) != round(float(existente['valor_total']), 2)
                    situacao_mudou = (int(row['situacao']) if pd.notna(row['situacao']) else None) != existente['situacao']
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
                            empresa_id = :empresa_id,
                            numero_pedido_lv = :numero_pedido_lv,
                            numero_pedido_bling = :numero_pedido_bling,
                            data_pedido = :data_pedido,
                            cliente_id = :cliente_id,
                            bling_canal_id = :bling_canal_id,
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
                        'empresa_id': int(row['empresa_id']),
                        'numero_pedido_lv': str(row['numero_pedido_lv']) if pd.notna(row['numero_pedido_lv']) else None,
                        'numero_pedido_bling': str(row['numero_pedido_bling']) if pd.notna(row['numero_pedido_bling']) else None,
                        'data_pedido': row['data_pedido'].date() if pd.notna(row['data_pedido']) else None,
                        'cliente_id': int(row['cliente_id']) if pd.notna(row['cliente_id']) else None,
                        'bling_canal_id': int(row['bling_canal_id']) if pd.notna(row['bling_canal_id']) else None,
                        'valor_total': float(row['valor_total']),
                        'valor_frete': float(row['valor_frete']) if pd.notna(row['valor_frete']) else 0,
                        'quantidade_itens_total': int(row['quantidade_itens_total']),
                        'quantidade_produtos_total': int(row['quantidade_produtos_total']),
                        'situacao': int(row['situacao']) if pd.notna(row['situacao']) else None,  
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
            query = text("SELECT COUNT(*) FROM processed.fato_pedidos WHERE empresa_id = :empresa_id")
            total = session.execute(query, {"empresa_id": self.empresa_id}).scalar()
            
            print(f"\n🎉 EXPORTAÇÃO CONCLUÍDA!")
            print(f"   • Total na tabela (empresa {self.empresa_id}): {total}")
            print(f"   • Economia: {registros_identicos} atualizações desnecessárias evitadas!")
            
            return len(df)
            
        except Exception as e:
            session.rollback()
            print(f"❌ ERRO ao exportar: {e}")
            raise
        finally:
            session.close()

    # =====================================================
    # 10. ATUALIZAR STATUS RAW
    # =====================================================

    def atualizar_status_raw(self, df):
        """
        Atualiza status dos registros processados
        """
        print("\n7️⃣ ATUALIZANDO STATUS NA TABELA RAW...")

        session = Session()

        try:
            ids_processados = df["pedido_id"].tolist()

            query = text("""
                UPDATE raw.vendas_raw
                SET status_processamento = 'processado'
                WHERE id = ANY(:ids)
                  AND empresa_id = :empresa_id
            """)

            resultado = session.execute(query, {
                "ids": ids_processados,
                "empresa_id": self.empresa_id
            })
            session.commit()

            print(f"✅ {resultado.rowcount} registros marcados como 'processado'")

        except Exception as e:
            session.rollback()
            print(f"⚠️  Erro ao atualizar status: {e}")
        finally:
            session.close()

    # =====================================================
    # 11. EXECUTAR TRANSFORMAÇÃO COMPLETA
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