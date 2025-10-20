# =====================================================
# TRANSFORMADOR DE PRODUTOS
# =====================================================
# Baseado no explore_produtos_raw.py 100% testado

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
    Transformador completo de produtos com regras validadas:
    - Identificação rigorosa de bicicletas
    - Gênero com 100% cobertura em bikes
    - Marca KOG em infantis sem marca
    - Público com 100% cobertura em bikes
    """

    def __init__(self):
        self.engine = engine

    # =====================================================
    # 2. EXTRAIR DADOS RAW
    # =====================================================

    def extrair_dados_raw(self):
        """Extrai dados da tabela raw.produtos_raw"""
        print("\n1️⃣ EXTRAINDO DADOS DE RAW.PRODUTOS_RAW...")

        query = """
            SELECT 
                id,
                bling_id,
                dados_json,
                data_ingestao
            FROM raw.produtos_raw
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
    # 4. FUNÇÕES DE EXTRAÇÃO DE ATRIBUTOS
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

    def classificar_genero_melhorado(self, nome):
        """
        Classifica gênero com marcas femininas (Sunny, MWZA, HERA)
        """
        if pd.isna(nome):
            return None

        nome_lower = str(nome).lower()

        # Feminino: palavras OU marcas femininas
        if re.search(
            r"\bfeminin[oa]\b|\bfem\b|\bmeninas?\b|\bmulher\b|\bdama\b", nome_lower
        ):
            return "Feminino"

        # Marcas femininas
        if re.search(r"\bsunny\b|\bmwza\b|\bhera\b", nome_lower):
            return "Feminino"

        # Masculino
        if re.search(r"\bmasculin[oa]\b|\bmasc\b|\bmeninos?\b|\bhomem\b", nome_lower):
            return "Masculino"

        # Unissex
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
    # 5. IDENTIFICAR BICICLETAS (REGRAS RIGOROSAS)
    # =====================================================

    def eh_bicicleta(self, row):
        """
        Regras para identificar bicicletas:
        0. SE primeira palavra for "bicicleta/bike/bke/bicicelta" → É BICICLETA
        1. Tem "bicicleta", "bike" ou "bke" no nome
        2. Tem pelo menos 3 atributos preenchidos
        3. NÃO tem palavras de exclusão
        4. Primeira palavra NÃO é Quadro/Sapatilha/Raio/etc
        """
        nome = row.get("descricao_produto", "")

        if pd.isna(nome):
            return False

        nome_str = str(nome)
        nome_lower = nome_str.lower()

        # Regra 0: Primeira palavra é bicicleta/bike/bke?
        primeira_palavra = (
            nome_str.strip().split()[0].lower() if nome_str.strip() else ""
        )
        if re.search(r"^(bicicleta|bike|bke|bicicelta)$", primeira_palavra):
            return True

        # Regra 1: Tem palavras-chave?
        tem_palavra_chave = bool(
            re.search(r"\bbicicleta\b|\bbike\b|\bbke\b", nome_lower)
        )

        if not tem_palavra_chave:
            return False

        # Regra 4: Primeira palavra NÃO pode ser estas
        primeira_palavra_upper = (
            nome_str.strip().split()[0].upper() if nome_str.strip() else ""
        )
        if primeira_palavra_upper in (
            "QUADRO",
            "SAPATILHA",
            "RAIO",
            "PNEU",
            "PE",
            "PAR",
            "PEDIVELA",
            "KIT",
            "CATRACA",
            "CAMBIO",
            "CASSETE",
        ):
            return False

        # Regra 3: Tem palavras de exclusão?
        palavras_exclusao = [
            "caixa",
            "embalagem",
            "garfo",
            "capacete",
            "luva",
            "pedal",
            "selim",
            "guidao",
            "corrente",
        ]

        for palavra in palavras_exclusao:
            if re.search(r"\b" + palavra + r"\b", nome_lower):
                return False

        # Regra 2: Tem pelo menos 3 atributos?
        atributos = [
            row.get("aro"),
            row.get("cor_principal"),
            row.get("tamanho"),
            row.get("marchas"),
            row.get("marca"),
            row.get("freio"),
            row.get("genero"),
            row.get("publico"),
            row.get("categoria"),
        ]

        atributos_preenchidos = sum(1 for attr in atributos if pd.notna(attr))

        return atributos_preenchidos >= 3

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

        # Aplicar extrações EM TODOS OS PRODUTOS
        if "descricao_produto" in df.columns:
            print("   • Extraindo atributos de todos os produtos...")

            df["aro"] = df["descricao_produto"].apply(self.extrair_aro)

            cores_lista = df["descricao_produto"].apply(self.extrair_cores_completo)
            df["cor_principal"] = cores_lista.apply(
                lambda x: x[0] if len(x) > 0 else None
            )
            df["cor_secundaria"] = cores_lista.apply(
                lambda x: x[1] if len(x) > 1 else None
            )
            df["cor_terciaria"] = cores_lista.apply(
                lambda x: x[2] if len(x) > 2 else None
            )

            df["tamanho"] = df["descricao_produto"].apply(self.extrair_tamanho)
            df["marchas"] = df["descricao_produto"].apply(self.extrair_marchas)
            df["marca"] = df["descricao_produto"].apply(self.extrair_marca)
            df["freio"] = df["descricao_produto"].apply(self.detectar_freio)
            df["genero"] = df["descricao_produto"].apply(
                self.classificar_genero_melhorado
            )
            df["publico"] = df["descricao_produto"].apply(self.classificar_publico)
            df["categoria"] = df["descricao_produto"].apply(self.classificar_categoria)

            print("   ✅ Atributos extraídos")

        # Identificar bicicletas
        print("   • Identificando bicicletas...")
        df["tipo_produto"] = df.apply(
            lambda row: "Bicicleta" if self.eh_bicicleta(row) else None, axis=1
        )
        total_bikes = df["tipo_produto"].notna().sum()
        print(f"   ✅ {total_bikes} bicicletas identificadas")

        # PREENCHER GÊNERO 100% EM BICICLETAS
        print("   • Preenchendo gênero (100% cobertura)...")
        df.loc[
            (df["tipo_produto"] == "Bicicleta") & (df["genero"].isna()), "genero"
        ] = "Unissex"

        # PREENCHER MARCA KOG EM INFANTIS SEM MARCA
        print("   • Preenchendo marca KOG em bikes infantis...")
        df.loc[
            (df["tipo_produto"] == "Bicicleta")
            & (df["publico"] == "Infantil")
            & (df["marca"].isna()),
            "marca",
        ] = "KOG"

        # PREENCHER PÚBLICO 100% EM BICICLETAS
        print("   • Preenchendo público (100% cobertura)...")
        df.loc[
            (df["tipo_produto"] == "Bicicleta") & (df["publico"].isna()), "publico"
        ] = "Adulto"

        # Arredondar preços
        for col in ["preco_venda", "preco_custo"]:
            if col in df.columns:
                df[col] = df[col].round(2)

        # Adicionar metadados
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
            "tipo_produto",  # ← ADICIONAR ESTA LINHA
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

        # Bicicletas
        bikes = df[df["tipo_produto"] == "Bicicleta"]
        if len(bikes) > 0:
            cobertura_genero = bikes["genero"].notna().sum() / len(bikes) * 100
            cobertura_publico = bikes["publico"].notna().sum() / len(bikes) * 100
            print(f"\n   🚴 BICICLETAS:")
            print(f"      • Total: {len(bikes)}")
            print(f"      • Cobertura gênero: {cobertura_genero:.1f}%")
            print(f"      • Cobertura público: {cobertura_publico:.1f}%")

        # Duplicatas
        duplicatas = df.duplicated(subset=["bling_produto_id"]).sum()
        if duplicatas > 0:
            print(f"\n   ⚠️  {duplicatas} duplicatas - removendo...")
            df = df.drop_duplicates(subset=["bling_produto_id"], keep="first")
        else:
            print(f"\n   ✅ Sem duplicatas")

        return df

    # =====================================================
    # 9. EXPORTAR PARA PROCESSED
    # =====================================================

    def exportar_para_processed(self, df):
        """Exporta para processed.dim_produtos EM LOTES"""
        print("\n6️⃣ EXPORTANDO PARA PROCESSED.DIM_PRODUTOS...")

        if len(df) == 0:
            print("⚠️  Nenhum registro para exportar")
            return 0

        try:
            # DIVIDIR EM LOTES DE 1000 REGISTROS
            batch_size = 1000
            total_batches = (len(df) // batch_size) + 1
            
            print(f"   Exportando {len(df)} registros em {total_batches} lotes...")
            
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                
                batch.to_sql(
                    name="dim_produtos",
                    con=self.engine,
                    schema="processed",
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=100,
                )
                
                print(f"   ✅ Lote {batch_num}/{total_batches} exportado ({len(batch)} registros)")

            print(f"✅ {len(df)} registros exportados com sucesso!")

            # Verificar total
            query = text("SELECT COUNT(*) FROM processed.dim_produtos")
            with engine.connect() as conn:
                total = conn.execute(query).scalar()
                print(f"✅ Total na tabela: {total}")

            return len(df)

        except Exception as e:
            print(f"❌ ERRO ao exportar: {e}")
            raise

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
            """
            )

            resultado = session.execute(query, {"ids": ids})
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
