# =====================================================
# TRANSFORMADOR DE PRODUTOS - MULTI-CNPJ
# =====================================================
# CORREÇÃO: Implementa comparação inteligente (igual sales_dw.py)
# - Compara antes de salvar
# - INSERT apenas novos
# - UPDATE apenas diferentes
# - SKIP idênticos

import pandas as pd
import numpy as np
import re
from datetime import datetime
from sqlalchemy import text
from config.database import Session, engine

# =====================================================
# 1. CLASSE TRANSFORMADORA
# =====================================================


class ProdutosTransformer:
    """
    Transformador de produtos com COMPARAÇÃO INTELIGENTE
    """

    def __init__(self, empresa_id):
        """
        Args:
            empresa_id: ID da empresa na tabela dim_empresas
        """
        self.engine = engine
        self.empresa_id = empresa_id

    # =====================================================
    # 2. EXTRAIR DADOS RAW
    # =====================================================

    def extrair_dados_raw(self):
        """Extrai dados da tabela raw.produtos_raw"""
        print(f"\n1️⃣ EXTRAINDO DADOS DE RAW.PRODUTOS_RAW (Empresa ID: {self.empresa_id})...")

        query = text("""
            SELECT 
                id,
                bling_id,
                dados_json,
                data_ingestao
            FROM raw.produtos_raw
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
        """Expande o JSON em colunas"""
        print("\n2️⃣ EXPANDINDO JSON...")

        df_json = pd.json_normalize(df_raw["dados_json"])

        if "id" in df_json.columns:
            df_json = df_json.rename(columns={"id": "id_bling_json"})

        df = pd.concat(
            [df_raw[["id", "bling_id", "data_ingestao"]], df_json],
            axis=1,
        )

        print(f"✅ {len(df.columns)} colunas")
        return df

    # =====================================================
    # 4. IDENTIFICAR BICICLETAS
    # =====================================================

    def eh_bicicleta(self, nome):
        """
        Identifica se é bicicleta
        Aceita: bicicleta, bike, bke
        Exclui: caixa, embalagem, adesivo
        """
        if pd.isna(nome):
            return False

        nome_lower = str(nome).lower()

        # Tem "bicicleta"
        if re.search(r"\bbicicleta\b", nome_lower):
            return True

        # Tem "bike" ou "bke" MAS não tem exclusões
        if re.search(r"\b(bike|bke)\b", nome_lower) and not re.search(
            r"\bcaixa\b|\bembalagem\b|\badesivo\b", nome_lower
        ):
            return True

        return False

    # =====================================================
    # 5. FUNÇÕES DE EXTRAÇÃO
    # =====================================================

    def extrair_aro(self, nome):
        nome_str = str(nome)
        match = re.search(r"\baro[:\s]*(\d{1,2})\b", nome_str, re.IGNORECASE)
        if match:
            return match.group(1)
        aros = ["12", "14", "16", "18", "20", "24", "26", "27", "28", "29", "700"]
        for aro in aros:
            if re.search(r"\b" + aro + r"\b", nome_str):
                return aro
        return None

    def extrair_cores_completo(self, nome):
        cores = []
        match = re.search(r"cor:\s*([^;]+)", str(nome), re.IGNORECASE)
        if match:
            cores_texto = match.group(1).strip()
            cores_lista = re.split(r"[+/]", cores_texto)
            cores = [cor.strip() for cor in cores_lista if cor.strip()]
        if not cores:
            match_com = re.search(r"-\s*(\w+)\s+com\s+(\w+)", str(nome), re.IGNORECASE)
            if match_com:
                cores = [match_com.group(1).strip(), match_com.group(2).strip()]
        if not cores:
            cores_conhecidas = [
                "PRETO",
                "BRANCO",
                "VERMELHO",
                "AZUL",
                "VERDE",
                "AMARELO",
                "ROSA",
                "ROXO",
                "LARANJA",
                "CINZA",
                "PRATA",
                "DOURADO",
                "BEGE",
                "MARROM",
                "VINHO",
                "TURQUESA",
                "CORAL",
                "NUDE",
                "PINK",
                "LILÁS",
                "LILAS",
                "GRAFITE",
                "CHUMBO",
                "CHAMPAGNE",
                "PRETO FOSCO",
                "AZUL MARINHO",
                "AZUL CLARO",
                "VERDE MILITAR",
                "VERDE LIMÃO",
                "VERDE NEON",
                "VERDE PÉROLA",
                "VERDE PEROLA",
                "VERMELHO FERRARI",
                "AMARELO NEON",
                "ROSA PINK",
                "AMARELO DEGRADE",
            ]
            nome_upper = str(nome).upper()
            cores_ordenadas = sorted(cores_conhecidas, key=len, reverse=True)
            for cor in cores_ordenadas:
                if re.search(r"\b" + re.escape(cor) + r"\b", nome_upper):
                    cores.append(cor.title())
                    nome_upper = nome_upper.replace(cor, "", 1)
        return cores

    def extrair_tamanho(self, nome):
        match = re.search(r"tamanho[:\s]*(\d{1,2})", str(nome), re.IGNORECASE)
        if match:
            return match.group(1)
        match_final = re.search(r"[a-z]\s+(\d{2})$", str(nome), re.IGNORECASE)
        if match_final:
            num = match_final.group(1)
            if num in [
                "13",
                "15",
                "17",
                "19",
                "21",
                "48",
                "50",
                "52",
                "54",
                "56",
                "58",
            ]:
                return num
        return None

    def extrair_marchas(self, nome):
        nome_str = str(nome).lower()
        if re.search(r"sem\s+marchas?", nome_str):
            return "0"
        match = re.search(r"(\d{1,2})\s*(?:vel\b|v\b|velocidades?|marchas?)", nome_str)
        if match:
            return match.group(1)
        return None

    def extrair_marca(self, nome):
        marcas_conhecidas = [
            "KSW",
            "CALOI",
            "OGGI",
            "TSW",
            "SENSE",
            "ABSOLUTE",
            "COLLI",
            "HOUSTON",
            "TRACK",
            "SOUTH",
            "AUDAX",
            "SCOTT",
            "GIANT",
            "TREK",
            "SPECIALIZED",
            "CANNONDALE",
            "MOSSO",
            "VIKING",
            "FIRST",
            "GIOS",
            "GT",
            "SCHWINN",
            "LOTUS",
            "SOUL",
            "GROOVE",
            "KODE",
            "OPTIMUS",
            "VENZO",
            "ALFAMEQ",
            "ATHOR",
            "GONEW",
            "GTSM1",
            "SHIMANO",
            "NATHOR",
            "BANDEIRANTE",
            "MONARK",
            "POTI",
            "VERDEN",
            "OXER",
            "DROPP",
            "REDSTONE",
            "ELLEVEN",
            "HIGH ONE",
            "MOVE",
            "KALF",
            "LAHSEN",
            "RAVA",
            "BMC",
            "MERIDA",
            "CUBE",
            "ORBEA",
            "SAMY",
            "SOUSA",
            "GTI",
            "GTA NX11",
            "GTA NX",
            "GTA",
            "WENDY",
            "KOG",
            "PRO X",
            "VIKINGX",
            "HUPI",
            "KSX",
        ]

        correcoes_marcas = {
            "ABOSOLUTE": "ABSOLUTE",
            "ABSOLUT": "ABSOLUTE",
            "ABSOLUTY": "ABSOLUTE",
        }

        nome_upper = str(nome).upper()
        for erro, correto in correcoes_marcas.items():
            if re.search(r"\b" + erro + r"\b", nome_upper):
                return correto

        pattern = r"\b(" + "|".join(marcas_conhecidas) + r")\b"
        match = re.search(pattern, nome_upper)
        if match:
            return match.group(1)
        return None

    def detectar_freio(self, nome):
        nome_lower = str(nome).lower()
        if re.search(r"hidr[aá]ulico|hidraulico", nome_lower):
            return "Disco Hidráulico"
        if re.search(r"disco\s+mec[aâ]nico|freio\s+a?\s*disco(?!\s+hidr)", nome_lower):
            return "Disco Mecânico"
        if re.search(r"v-brake|v\s*brake|vbrake", nome_lower):
            return "V-Brake"
        return None

    def classificar_genero(self, nome):
        nome_lower = str(nome).lower()
        if re.search(
            r"\bfeminin[oa]\b|\bfem\b|\bmeninas?\b|\bmulher\b|\bdama\b", nome_lower
        ):
            return "Feminino"
        if re.search(r"\bmasculin[oa]\b|\bmasc\b|\bmeninos?\b|\bhomem\b", nome_lower):
            return "Masculino"
        if re.search(r"\bunissex\b|\bunisex\b", nome_lower):
            return "Unissex"
        return None

    def classificar_publico(self, nome):
        nome_lower = str(nome).lower()
        if re.search(r"\binfantil\b|\bcrian[cç]a\b|\bkids\b", nome_lower):
            return "Infantil"
        elif re.search(r"\bjuvenil\b|\badolescente\b", nome_lower):
            return "Juvenil"
        elif re.search(r"\badulto\b", nome_lower):
            return "Adulto"
        return None

    def classificar_categoria(self, nome):
        nome_lower = str(nome).lower()
        if re.search(r"\bel[eé]trica\b|\beletrica\b|\be-bike\b", nome_lower):
            return "Elétrica"
        if re.search(r"\bmtb\b|\bmountain\b", nome_lower):
            return "MTB"
        if re.search(r"\bspeed\b|\broad\b", nome_lower):
            return "Speed"
        if re.search(r"\burbana\b|\bpasseio\b", nome_lower):
            return "Urbana"
        if re.search(r"\bbmx\b", nome_lower):
            return "BMX"
        return None

    # =====================================================
    # 6. APLICAR TRANSFORMAÇÕES
    # =====================================================

    def aplicar_transformacoes(self, df):
        """Aplica todas as transformações"""
        print("\n3️⃣ APLICANDO TRANSFORMAÇÕES...")

        # Remover colunas desnecessárias
        colunas_drop = [
            "tipo",
            "formato",
            "descricaoCurta",
            "imagemURL",
            "idProdutoPai",
            "id_bling_json",
            "estoque.saldoVirtualTotal",
        ]
        df = df.drop(columns=[c for c in colunas_drop if c in df.columns])

        # Renomear colunas
        df = df.rename(
            columns={
                "id": "produto_id",
                "bling_id": "bling_produto_id",
                "nome": "descricao_produto",
                "codigo": "sku",
                "precoCusto": "preco_custo",
                "preco": "preco_venda",
            }
        )

        # Criar colunas de atributos
        df["aro"] = None
        df["cor_principal"] = None
        df["cor_secundaria"] = None
        df["cor_terciaria"] = None
        df["tamanho"] = None
        df["marchas"] = None
        df["marca"] = None
        df["freio"] = None
        df["genero"] = None
        df["publico"] = None
        df["categoria"] = None

        # Identificar e processar bicicletas
        if "descricao_produto" in df.columns:
            df_bikes = df[df["descricao_produto"].apply(self.eh_bicicleta)].copy()

            print(f"   • {len(df_bikes)} bicicletas identificadas")

            if len(df_bikes) > 0:
                df_bikes.loc[:, "aro"] = df_bikes["descricao_produto"].apply(
                    self.extrair_aro
                )

                cores_lista = df_bikes["descricao_produto"].apply(
                    self.extrair_cores_completo
                )
                df_bikes.loc[:, "cor_principal"] = cores_lista.apply(
                    lambda x: x[0] if len(x) > 0 else None
                )
                df_bikes.loc[:, "cor_secundaria"] = cores_lista.apply(
                    lambda x: x[1] if len(x) > 1 else None
                )
                df_bikes.loc[:, "cor_terciaria"] = cores_lista.apply(
                    lambda x: x[2] if len(x) > 2 else None
                )

                df_bikes.loc[:, "tamanho"] = df_bikes["descricao_produto"].apply(
                    self.extrair_tamanho
                )
                df_bikes.loc[:, "marchas"] = df_bikes["descricao_produto"].apply(
                    self.extrair_marchas
                )
                df_bikes.loc[:, "marca"] = df_bikes["descricao_produto"].apply(
                    self.extrair_marca
                )
                df_bikes.loc[:, "freio"] = df_bikes["descricao_produto"].apply(
                    self.detectar_freio
                )
                df_bikes.loc[:, "genero"] = df_bikes["descricao_produto"].apply(
                    self.classificar_genero
                )
                df_bikes.loc[:, "publico"] = df_bikes["descricao_produto"].apply(
                    self.classificar_publico
                )
                df_bikes.loc[:, "categoria"] = df_bikes["descricao_produto"].apply(
                    self.classificar_categoria
                )

                df.update(df_bikes)

        # Arredondar preços
        for col in ["preco_venda", "preco_custo"]:
            if col in df.columns:
                df[col] = df[col].round(2)

        # Adicionar empresa_id e metadados
        df["empresa_id"] = self.empresa_id  
        df["data_processamento"] = datetime.now()

        print("✅ Transformações aplicadas")
        return df

    # =====================================================
    # 7. PREPARAR PARA EXPORTAÇÃO
    # =====================================================

    def preparar_para_exportacao(self, df):
        """Seleciona colunas finais"""
        print("\n4️⃣ PREPARANDO PARA EXPORTAÇÃO...")

        colunas_finais = [
            "produto_id",
            "bling_produto_id",
            "empresa_id", 
            "sku",
            "descricao_produto",
            "preco_venda",
            "preco_custo",
            "aro",
            "marca",
            "cor_principal",
            "cor_secundaria",
            "cor_terciaria",
            "tamanho",
            "marchas",
            "freio",
            "genero",
            "publico",
            "categoria",
            "situacao",
            "data_ingestao",
            "data_processamento",
        ]

        df = df[[col for col in colunas_finais if col in df.columns]]

        print(f"✅ {len(df)} registros x {len(df.columns)} colunas")
        return df

    # =====================================================
    # 8. VALIDAR DADOS
    # =====================================================

    def validar_dados(self, df):
        """Valida qualidade dos dados"""
        print("\n5️⃣ VALIDANDO DADOS...")

        total = len(df)
        com_sku = df["sku"].notna().sum()
        com_preco = df["preco_venda"].notna().sum()

        print(f"\n   📊 ESTATÍSTICAS:")
        print(f"      • Total: {total}")
        print(f"      • Com SKU: {com_sku} ({com_sku/total*100:.1f}%)")
        print(f"      • Com preço: {com_preco} ({com_preco/total*100:.1f}%)")

        # Duplicatas
        duplicatas = df.duplicated(subset=["bling_produto_id", "empresa_id"]).sum()  
        if duplicatas > 0:
            print(f"\n   ⚠️  {duplicatas} duplicatas encontradas - removendo...")
            df = df.drop_duplicates(subset=["bling_produto_id", "empresa_id"], keep="first")  
        else:
            print(f"\n   ✅ Sem duplicatas")

        return df

    # =====================================================
    # 9. EXPORTAR COM COMPARAÇÃO INTELIGENTE 
    # =====================================================

    def exportar_para_processed(self, df):
        """
        ✅ CORREÇÃO: Implementa comparação inteligente (igual sales_dw.py)

        ESTRATÉGIA:
        1. Buscar produtos existentes
        2. Comparar campos relevantes
        3. INSERT apenas novos
        4. UPDATE apenas diferentes
        5. SKIP idênticos
        """
        print("\n6️⃣ EXPORTANDO PARA PROCESSED.DIM_PRODUTOS...")

        if len(df) == 0:
            print("⚠️  Nenhum registro para exportar")
            return 0

        session = Session()

        try:
            # === BUSCAR REGISTROS EXISTENTES ===
            print("🔍 Buscando registros existentes para comparação...")
            inicio_busca = datetime.now()

            query = text(
                """
                SELECT 
                    produto_id,
                    bling_produto_id,
                    preco_venda,
                    preco_custo,
                    aro,
                    marca,
                    cor_principal,
                    situacao
                FROM processed.dim_produtos
                WHERE empresa_id = :empresa_id
            """
            )

            df_existentes = pd.read_sql(query, self.engine, params={"empresa_id": self.empresa_id})
            fim_busca = datetime.now()

            print(
                f"📋 {len(df_existentes)} registros existentes carregados em {fim_busca - inicio_busca}"
            )

            # === CLASSIFICAR: NOVOS, DIFERENTES, IDÊNTICOS ===
            print("🔍 Comparando registros...")
            inicio_comparacao = datetime.now()

            existentes_dict = df_existentes.set_index("bling_produto_id").to_dict(
                "index"
            )

            registros_novos = []
            registros_atualizar = []
            registros_identicos = 0

            for idx, row in df.iterrows():
                bling_id = row["bling_produto_id"]

                if bling_id not in existentes_dict:
                    # NOVO → INSERT
                    registros_novos.append(row)
                else:
                    # EXISTE → Comparar campos relevantes
                    existente = existentes_dict[bling_id]

                    # Comparar valores com segurança (None-safe)
                    def comparar_preco(novo, existente):
                        if pd.isna(novo) and pd.isna(existente):
                            return False
                        if pd.isna(novo) or pd.isna(existente):
                            return True
                        return round(float(novo), 2) != round(float(existente), 2)

                    def comparar_string(novo, existente):
                        novo_str = str(novo) if pd.notna(novo) else ""
                        exist_str = str(existente) if pd.notna(existente) else ""
                        return novo_str != exist_str

                    preco_venda_mudou = comparar_preco(
                        row.get("preco_venda"), existente.get("preco_venda")
                    )
                    preco_custo_mudou = comparar_preco(
                        row.get("preco_custo"), existente.get("preco_custo")
                    )
                    aro_mudou = comparar_string(row.get("aro"), existente.get("aro"))
                    marca_mudou = comparar_string(
                        row.get("marca"), existente.get("marca")
                    )
                    cor_mudou = comparar_string(
                        row.get("cor_principal"), existente.get("cor_principal")
                    )
                    situacao_mudou = comparar_string(
                        row.get("situacao"), existente.get("situacao")
                    )

                    if (
                        preco_venda_mudou
                        or preco_custo_mudou
                        or aro_mudou
                        or marca_mudou
                        or cor_mudou
                        or situacao_mudou
                    ):
                        # DIFERENTE → UPDATE
                        row["produto_id"] = existente["produto_id"]
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

                if "produto_id" in df_novos.columns:
                    df_novos = df_novos.drop(columns=["produto_id"])

                df_novos.to_sql(
                    name="dim_produtos",
                    con=self.engine,
                    schema="processed",
                    if_exists="append",
                    index=False,
                    chunksize=500,
                )
                print(f"✅ Inserções concluídas")

            # === ATUALIZAR DIFERENTES ===
            if registros_atualizar:
                print(
                    f"\n🔄 Atualizando {len(registros_atualizar)} registros diferentes..."
                )

                for i, row in enumerate(registros_atualizar):
                    stmt = text(
                        """
                        UPDATE processed.dim_produtos
                        SET 
                            bling_produto_id = :bling_produto_id,
                            empresa_id = :empresa_id,
                            sku = :sku,
                            descricao_produto = :descricao_produto,
                            preco_venda = :preco_venda,
                            preco_custo = :preco_custo,
                            aro = :aro,
                            marca = :marca,
                            cor_principal = :cor_principal,
                            cor_secundaria = :cor_secundaria,
                            cor_terciaria = :cor_terciaria,
                            tamanho = :tamanho,
                            marchas = :marchas,
                            freio = :freio,
                            genero = :genero,
                            publico = :publico,
                            categoria = :categoria,
                            situacao = :situacao,
                            data_processamento = :data_processamento
                        WHERE produto_id = :produto_id
                    """
                    )

                    session.execute(
                        stmt,
                        {
                            "produto_id": int(row["produto_id"]),
                            "bling_produto_id": int(row["bling_produto_id"]),
                            "empresa_id": int(self.empresa_id), 
                            "sku": str(row["sku"]) if pd.notna(row["sku"]) else None,
                            "descricao_produto": str(row["descricao_produto"]),
                            "preco_venda": (
                                float(row["preco_venda"])
                                if pd.notna(row["preco_venda"])
                                else None
                            ),
                            "preco_custo": (
                                float(row["preco_custo"])
                                if pd.notna(row["preco_custo"])
                                else None
                            ),
                            "aro": str(row["aro"]) if pd.notna(row["aro"]) else None,
                            "marca": (
                                str(row["marca"]) if pd.notna(row["marca"]) else None
                            ),
                            "cor_principal": (
                                str(row["cor_principal"])
                                if pd.notna(row["cor_principal"])
                                else None
                            ),
                            "cor_secundaria": (
                                str(row["cor_secundaria"])
                                if pd.notna(row["cor_secundaria"])
                                else None
                            ),
                            "cor_terciaria": (
                                str(row["cor_terciaria"])
                                if pd.notna(row["cor_terciaria"])
                                else None
                            ),
                            "tamanho": (
                                str(row["tamanho"])
                                if pd.notna(row["tamanho"])
                                else None
                            ),
                            "marchas": (
                                str(row["marchas"])
                                if pd.notna(row["marchas"])
                                else None
                            ),
                            "freio": (
                                str(row["freio"]) if pd.notna(row["freio"]) else None
                            ),
                            "genero": (
                                str(row["genero"]) if pd.notna(row["genero"]) else None
                            ),
                            "publico": (
                                str(row["publico"])
                                if pd.notna(row["publico"])
                                else None
                            ),
                            "categoria": (
                                str(row["categoria"])
                                if pd.notna(row["categoria"])
                                else None
                            ),
                            "situacao": (
                                str(row["situacao"])
                                if pd.notna(row["situacao"])
                                else None
                            ),
                            "data_processamento": row["data_processamento"],
                        },
                    )

                    if (i + 1) % 100 == 0:
                        session.commit()
                        print(
                            f"   Atualizados {i + 1}/{len(registros_atualizar)} registros..."
                        )

                session.commit()
                print(f"✅ Atualizações concluídas")

            if not registros_novos and not registros_atualizar:
                print(f"\n✨ Nenhum registro novo ou alterado! DW já está atualizado.")

            # === VERIFICAR TOTAL ===
            query = text("""
                SELECT COUNT(*) 
                FROM processed.dim_produtos 
                WHERE empresa_id = :empresa_id
            """)
            total = session.execute(query, {"empresa_id": self.empresa_id}).scalar()

            print(f"\n🎉 EXPORTAÇÃO CONCLUÍDA!")
            print(f"   • Total na tabela (empresa {self.empresa_id}): {total}")
            print(
                f"   • Economia: {registros_identicos} atualizações desnecessárias evitadas!"
            )

            return len(df)

        except Exception as e:
            session.rollback()
            print(f"❌ ERRO ao exportar: {e}")
            raise
        finally:
            session.close()

    # =====================================================
    # 10. ATUALIZAR STATUS
    # =====================================================

    def atualizar_status_raw(self, df):
        """Atualiza status em raw.produtos_raw"""
        print("\n7️⃣ ATUALIZANDO STATUS...")

        session = Session()

        try:
            ids = df["produto_id"].tolist()

            query = text(
                """
                UPDATE raw.produtos_raw
                SET status_processamento = 'processado'
                WHERE id = ANY(:ids)
                  AND empresa_id = :empresa_id
            """
            )

            resultado = session.execute(query, {
                "ids": ids,
                "empresa_id": self.empresa_id
            })
            session.commit()

            print(f"✅ {resultado.rowcount} registros atualizados")

        except Exception as e:
            session.rollback()
            print(f"⚠️  Erro: {e}")
        finally:
            session.close()

    # =====================================================
    # 11. EXECUTAR TRANSFORMAÇÃO COMPLETA
    # =====================================================

    def executar_transformacao_completa(self):
        """Pipeline completo"""
        try:
            df_raw = self.extrair_dados_raw()

            if len(df_raw) == 0:
                print("\n✅ Nenhum registro pendente")
                return

            df = self.expandir_json(df_raw)
            df = self.aplicar_transformacoes(df)
            df = self.preparar_para_exportacao(df)
            df = self.validar_dados(df)
            self.exportar_para_processed(df)
            self.atualizar_status_raw(df)

            print(f"\n{'='*70}")
            print(f"🎉 TRANSFORMAÇÃO CONCLUÍDA!")
            print(f"{'='*70}")

        except Exception as e:
            print(f"\n❌ ERRO: {e}")
            raise