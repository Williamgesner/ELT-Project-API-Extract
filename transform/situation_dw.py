# =====================================================
# TRANSFORMADOR DE SITUAÇÕES - MULTI-CNPJ
# =====================================================
# Responsável por: Limpar e transformar dados de situacao_raw
# Implementa comparação inteligente (igual os demais)

import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from config.database import Session, engine

# =====================================================
# 1. CLASSE TRANSFORMADORA
# =====================================================


class SituacoesTransformer:
    """
    Transformador de situações com COMPARAÇÃO INTELIGENTE
    """

    def __init__(self, empresa_id):
        self.empresa_id = empresa_id
        self.engine = engine

    # =====================================================
    # 2. EXTRAIR DADOS RAW
    # =====================================================

    def extrair_dados_raw(self):
        """Extrai dados da tabela raw.situacoes_raw"""
        print(f"\n1️⃣ EXTRAINDO DADOS DE RAW.SITUACOES_RAW (empresa_id={self.empresa_id})...")

        query = text("""
            SELECT 
                id,
                bling_situacao_id,
                nome,
                empresa_id,
                data_ingestao
            FROM raw.situacoes_raw
            WHERE empresa_id = :empresa_id
            ORDER BY bling_situacao_id
        """)

        df_raw = pd.read_sql(query, self.engine, params={"empresa_id": self.empresa_id})
        print(f"✅ {len(df_raw)} registros extraídos")

        return df_raw

    # =====================================================
    # 3. APLICAR LIMPEZAS E TRANSFORMAÇÕES
    # =====================================================

    def aplicar_limpezas(self, df):
        """Aplica limpezas e transformações"""
        print("\n2️⃣ APLICANDO LIMPEZAS E TRANSFORMAÇÕES...")

        # === RENOMEAR COLUNAS ===
        print("   • Renomeando colunas...")
        df = df.rename(columns={
            "nome": "situacao"
        })

        # === LIMPAR STRINGS VAZIAS ===
        print("   • Convertendo strings vazias para NaN...")
        for coluna in df.select_dtypes(include=["object"]).columns:
            df[coluna] = df[coluna].replace(r"^\s*$", np.nan, regex=True).infer_objects(copy=False)
            df[coluna] = df[coluna].replace("", np.nan)
            df[coluna] = df[coluna].replace(" ", np.nan)

        # === LIMPAR E PADRONIZAR SITUAÇÃO ===
        print("   • Limpando e padronizando situação...")
        df["situacao"] = df["situacao"].apply(self._limpar_situacao)

        # === ADICIONAR METADADOS ===
        print("   • Adicionando metadados de processamento...")
        df["data_processamento"] = datetime.now()

        print("✅ Todas as limpezas aplicadas com sucesso!")
        return df

    # =====================================================
    # 4. FUNÇÃO AUXILIAR DE LIMPEZA
    # =====================================================

    def _limpar_situacao(self, situacao):
        """Limpa e padroniza nome da situação"""
        if pd.isna(situacao):
            return np.nan

        situacao = str(situacao).strip()
        situacao = " ".join(situacao.split())

        return situacao if situacao else np.nan

    # =====================================================
    # 5. PREPARAR PARA EXPORTAÇÃO
    # =====================================================

    def preparar_para_exportacao(self, df):
        """Ordena colunas e prepara DataFrame final"""
        print("\n3️⃣ PREPARANDO DADOS PARA EXPORTAÇÃO...")

        colunas_finais = [
            "id",
            "bling_situacao_id",
            "empresa_id",
            "situacao",
            "data_ingestao",
            "data_processamento",
        ]

        df = df[[col for col in colunas_finais if col in df.columns]]

        print(f"✅ Dados preparados! {len(df)} registros x {len(df.columns)} colunas")
        return df

    # =====================================================
    # 6. VALIDAR DADOS
    # =====================================================

    def validar_dados(self, df):
        """Executa validações de qualidade"""
        print("\n4️⃣ EXECUTANDO VALIDAÇÕES DE QUALIDADE...")

        total = len(df)
        com_situacao = df["situacao"].notna().sum()

        print(f"\n   📊 ESTATÍSTICAS DE QUALIDADE:")
        print(f"      • Total: {total}")
        print(f"      • Com situação: {com_situacao} ({com_situacao/total*100:.1f}%)")

        # Verificar duplicatas
        duplicatas = df.duplicated(subset=["bling_situacao_id", "empresa_id"]).sum()
        if duplicatas > 0:
            print(f"\n   ⚠️  {duplicatas} registros duplicados encontrados!")
            print("      Removendo duplicatas...")
            df = df.drop_duplicates(subset=["bling_situacao_id", "empresa_id"], keep="first")
        else:
            print(f"\n   ✅ Nenhuma duplicata encontrada")

        return df

    # =====================================================
    # 7. EXPORTAR COM COMPARAÇÃO INTELIGENTE ✅
    # =====================================================

    def exportar_para_processed(self, df):
        """
        ✅ Implementa comparação inteligente (igual contacts_dw.py)

        ESTRATÉGIA:
        1. Buscar situações existentes
        2. Comparar campos relevantes
        3. INSERT apenas novos
        4. UPDATE apenas diferentes
        5. SKIP idênticos
        """
        print(f"\n5️⃣ EXPORTANDO PARA PROCESSED.DIM_SITUACAO (empresa_id={self.empresa_id})...")

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
                    id,
                    bling_situacao_id,
                    empresa_id,
                    situacao
                FROM processed.dim_situacao
                WHERE empresa_id = :empresa_id
            """)

            df_existentes = pd.read_sql(query, self.engine, params={"empresa_id": self.empresa_id})
            fim_busca = datetime.now()

            print(
                f"📋 {len(df_existentes)} registros existentes carregados em {fim_busca - inicio_busca}"
            )

            # === CLASSIFICAR: NOVOS, DIFERENTES, IDÊNTICOS ===
            print("🔍 Comparando registros...")
            inicio_comparacao = datetime.now()

            # Criar dicionário com chave composta
            existentes_dict = {}
            for _, row in df_existentes.iterrows():
                chave = (row['bling_situacao_id'], row['empresa_id'])
                existentes_dict[chave] = row.to_dict()

            registros_novos = []
            registros_atualizar = []
            registros_identicos = 0

            for idx, row in df.iterrows():
                chave = (row['bling_situacao_id'], row['empresa_id'])

                if chave not in existentes_dict:
                    # NOVO → INSERT
                    registros_novos.append(row)
                else:
                    # EXISTE → Comparar campos relevantes
                    existente = existentes_dict[chave]

                    # Função auxiliar para comparar com segurança (None-safe)
                    def comparar_campo(novo, existente):
                        novo_str = str(novo).strip() if pd.notna(novo) else ""
                        exist_str = (
                            str(existente).strip() if pd.notna(existente) else ""
                        )
                        return novo_str != exist_str

                    situacao_mudou = comparar_campo(row.get("situacao"), existente.get("situacao"))

                    if situacao_mudou:
                        # DIFERENTE → UPDATE
                        row["id"] = existente["id"]
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

                # Remover id se existir (será gerado automaticamente)
                if "id" in df_novos.columns:
                    df_novos = df_novos.drop(columns=["id"])

                df_novos.to_sql(
                    name="dim_situacao",
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
                    stmt = text("""
                        UPDATE processed.dim_situacao
                        SET 
                            bling_situacao_id = :bling_situacao_id,
                            empresa_id = :empresa_id,
                            situacao = :situacao,
                            data_processamento = :data_processamento
                        WHERE id = :id
                    """)

                    session.execute(
                        stmt,
                        {
                            "id": int(row["id"]),
                            "bling_situacao_id": int(row["bling_situacao_id"]),
                            "empresa_id": int(row["empresa_id"]),
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
            query = text("SELECT COUNT(*) FROM processed.dim_situacao WHERE empresa_id = :empresa_id")
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
    # 8. EXECUTAR TRANSFORMAÇÃO COMPLETA
    # =====================================================

    def executar_transformacao_completa(self):
        """Executa o pipeline completo de transformação"""
        try:
            # 1. Extrair dados raw
            df_raw = self.extrair_dados_raw()

            if len(df_raw) == 0:
                print("\n✅ Nenhum registro para processar!")
                print("   Todas as situações já foram transformadas.")
                return

            # 2. Aplicar limpezas
            df = self.aplicar_limpezas(df_raw)

            # 3. Preparar para exportação
            df = self.preparar_para_exportacao(df)

            # 4. Validar
            df = self.validar_dados(df)

            # 5. Exportar (COM COMPARAÇÃO INTELIGENTE ✅)
            total_exportado = self.exportar_para_processed(df)

            # Relatório final
            print(f"\n{'='*70}")
            print(f"🎉 TRANSFORMAÇÃO CONCLUÍDA!")
            print(f"{'='*70}")
            print(f"\n   📊 RESUMO:")
            print(f"      • Registros processados: {len(df)}")
            print(f"      • Registros exportados: {total_exportado}")
            print(f"      • Colunas: {len(df.columns)}")

        except Exception as e:
            print(f"\n❌ ERRO na transformação: {e}")
            raise