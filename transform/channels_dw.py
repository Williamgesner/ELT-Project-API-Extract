# =====================================================
# TRANSFORMADOR DE CANAIS DE VENDA
# =====================================================
# Responsável por: Limpar e transformar dados de canais_raw
# para dim_canais no schema processed

import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from config.database import Session, engine

# =====================================================
# 1. CLASSE TRANSFORMADORA
# =====================================================

class CanaisTransformer:
    """
    Transformador específico para canais de venda
    Aplica limpezas e padronizações necessárias
    """

    def __init__(self):
        self.engine = engine

    # =====================================================
    # 2. EXTRAIR DADOS RAW
    # =====================================================

    def extrair_dados_raw(self):
        """
        Extrai dados da tabela raw.canais_raw
        """
        print("\n1️⃣ EXTRAINDO DADOS DE RAW.CANAIS_RAW...")

        query = """
            SELECT 
                id,
                bling_canal_id,
                descricao,
                dados_json,
                data_ingestao
            FROM raw.canais_raw
            ORDER BY bling_canal_id
        """

        df_raw = pd.read_sql(query, self.engine)
        print(f"✅ {len(df_raw)} registros extraídos")

        return df_raw

    # =====================================================
    # 3. EXPANDIR JSON E APLICAR TRANSFORMAÇÕES
    # =====================================================

    def aplicar_transformacoes(self, df_raw):
        """
        Expande JSON e aplica transformações
        """
        print("\n2️⃣ EXPANDINDO JSON E APLICANDO TRANSFORMAÇÕES...")

        # Normalizar o JSON
        df_json = pd.json_normalize(df_raw["dados_json"])

        # Remover colunas duplicadas do JSON antes de concatenar
        colunas_duplicadas = ["id", "descricao"]
        for col in colunas_duplicadas:
            if col in df_json.columns:
                df_json = df_json.drop(columns=[col])

        # Combinar com colunas originais
        df = pd.concat(
            [
                df_raw[["id", "bling_canal_id", "descricao", "data_ingestao"]],
                df_json,
            ],
            axis=1,
        )

        print(f"✅ JSON expandido! {len(df.columns)} colunas disponíveis")

        # === LIMPAR STRINGS VAZIAS ===
        print("   • Limpando strings vazias...")
        for coluna in df.select_dtypes(include=["object"]).columns:
            df[coluna] = df[coluna].replace(r"^\s*$", np.nan, regex=True)
            df[coluna] = df[coluna].replace(["", " "], np.nan)

        # === RENOMEAR COLUNAS ===
        print("   • Renomeando colunas...")
        df = df.rename(
            columns={
                "bling_canal_id": "canal_id",
                "descricao": "nome_canal"
            }
        )

        # Se houver coluna 'tipo', renomear também
        if "tipo" in df.columns:
            df = df.rename(columns={"tipo": "tipo_canal"})

        # === ADICIONAR METADADOS ===
        print("   • Adicionando metadados...")
        df["data_processamento"] = datetime.now()

        print("✅ Transformações aplicadas!")
        return df

    # =====================================================
    # 4. PREPARAR PARA EXPORTAÇÃO
    # =====================================================

    def preparar_para_exportacao(self, df):
        """
        Seleciona apenas as colunas necessárias para dim_canais
        """
        print("\n3️⃣ PREPARANDO DADOS PARA EXPORTAÇÃO...")

        colunas_finais = [
            "canal_id",
            "nome_canal",
            "data_ingestao",
            "data_processamento",
        ]

        # Selecionar apenas as 4 colunas
        df = df[colunas_finais]

        print(f"✅ Dados preparados! {len(df)} registros x {len(df.columns)} colunas")
        return df

    # =====================================================
    # 5. VALIDAR DADOS
    # =====================================================

    def validar_dados(self, df):
        """
        Executa validações de qualidade
        """
        print("\n4️⃣ EXECUTANDO VALIDAÇÕES...")

        total = len(df)
        com_nome = df["nome_canal"].notna().sum()

        print(f"\n   📊 ESTATÍSTICAS DE QUALIDADE:")
        print(f"      • Total: {total}")
        print(f"      • Com nome: {com_nome} ({com_nome/total*100:.1f}%)")

        # Verificar duplicatas
        duplicatas = df.duplicated(subset=["canal_id"]).sum()
        if duplicatas > 0:
            print(f"\n   ⚠️  {duplicatas} registros duplicados encontrados!")
            df = df.drop_duplicates(subset=["canal_id"], keep="first")
        else:
            print(f"\n   ✅ Nenhuma duplicata encontrada")

        return df

    # =====================================================
    # 6. EXPORTAR PARA PROCESSED
    # =====================================================

    def exportar_para_processed(self, df):
        """
        Exporta dados para processed.dim_canais
        """
        print("\n5️⃣ EXPORTANDO PARA PROCESSED.DIM_CANAIS...")

        if len(df) == 0:
            print("⚠️  Nenhum registro para exportar")
            return 0

        try:
            df.to_sql(
                name="dim_canais",
                con=self.engine,
                schema="processed",
                if_exists="append",  # Sempre append (não recriar)
                index=False,
                method="multi",
                chunksize=100,
            )

            print(f"✅ {len(df)} registros exportados com sucesso!")

            # Verificar total na tabela
            query = text("SELECT COUNT(*) FROM processed.dim_canais")
            with engine.connect() as conn:
                total = conn.execute(query).scalar()
                print(f"✅ Verificação: {total} registros na tabela")

            return len(df)

        except Exception as e:
            if "duplicate key" in str(e).lower():
                print(f"⚠️  Alguns canais já existiam no banco (ignorados)")
                print(f"💡 Use TRUNCATE TABLE processed.dim_canais; para recriar")
                return 0
            else:
                print(f"❌ ERRO ao exportar: {e}")
                raise

    # =====================================================
    # 7. EXECUTAR TRANSFORMAÇÃO COMPLETA
    # =====================================================

    def executar_transformacao_completa(self):
        """
        Executa o pipeline completo de transformação
        """
        try:
            # 1. Extrair dados raw
            df_raw = self.extrair_dados_raw()

            if len(df_raw) == 0:
                print("\n⚠️  Nenhum canal encontrado em raw.canais_raw")
                print("💡 Execute primeiro: python main_channels.py")
                return

            # 2. Aplicar transformações
            df = self.aplicar_transformacoes(df_raw)

            # 3. Preparar para exportação
            df = self.preparar_para_exportacao(df)

            # 4. Validar
            df = self.validar_dados(df)

            # 5. Exportar
            total_exportado = self.exportar_para_processed(df)

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