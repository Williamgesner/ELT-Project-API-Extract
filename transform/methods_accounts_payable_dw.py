# =====================================================
# TRANSFORMADOR DE FORMAS DE PAGAMENTO - MULTI-CNPJ
# =====================================================
# Responsável por: Limpar e transformar dados de formas_pagamentos_raw para dim_formas_pagamento no schema processed
# ESTRATÉGIA: Comparar antes de salvar (igual outros transformers)

import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from config.database import Session, engine
from models.dim_fato.dim_formas_pagamento import DimFormasPagamento

# =====================================================
# 1. CLASSE TRANSFORMADORA
# =====================================================

class FormasPagamentoTransformer:
    """
    Transformador específico para formas de pagamento
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
        Extrai dados da tabela raw.formas_pagamentos_raw
        """
        print("\n1️⃣ EXTRAINDO DADOS DE RAW.FORMAS_PAGAMENTOS_RAW...")

        query = text("""
            SELECT 
                id,
                bling_id,
                empresa_id,
                dados_json,
                data_ingestao
            FROM raw.formas_pagamentos_raw
            WHERE empresa_id = :empresa_id
            ORDER BY bling_id
        """)

        df_raw = pd.read_sql(query, self.engine, params={"empresa_id": self.empresa_id})
        print(f"✅ {len(df_raw)} registros extraídos (empresa_id = {self.empresa_id})")

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
                df_raw[["id", "bling_id", "empresa_id", "data_ingestao"]],
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
        Aplica TODAS as transformações necessárias
        """
        print("\n3️⃣ APLICANDO TRANSFORMAÇÕES...")

        # === REMOVENDO COLUNAS DESNECESSÁRIAS ===
        print("   • Removendo colunas desnecessárias...")
        colunas_remover = [
            "id_bling",
            "id",
            "fixa",
            "juros",
            "multa",
            "padrao",
            "situacao",
            "finalidade",
            "tipoPagamento"
        ]
        df = df.drop(columns=[col for col in colunas_remover if col in df.columns])

        # === RENOMEAR COLUNAS ===
        print("   • Renomeando colunas...")
        df = df.rename(
            columns={
                "bling_id": "forma_pagamento_id",
                "descricao": "forma_pagamento",
            }
        )

        # === CONVERTENDO E PADRONIZANDO STRINGS VAZIAS ===
        print("   • Convertendo strings vazias para NaN...")
        for coluna in df.select_dtypes(include=["object"]).columns:
            df[coluna] = df[coluna].replace(r"^\s*$", np.nan, regex=True)
            df[coluna] = df[coluna].replace("", np.nan)
            df[coluna] = df[coluna].replace(" ", np.nan)

        # === LIMPAR E PADRONIZAR FORMA DE PAGAMENTO ===
        print("   • Padronizando forma de pagamento...")
        df["forma_pagamento"] = df["forma_pagamento"].str.strip()
        df["forma_pagamento"] = df["forma_pagamento"].str.title()

        # === ADICIONAR METADADOS ===
        print("   • Adicionando metadados de processamento...")
        df["data_processamento"] = datetime.now()

        print("✅ Todas as transformações aplicadas com sucesso!")
        return df

    # =====================================================
    # 5. PREPARAR PARA EXPORTAÇÃO
    # =====================================================

    def preparar_para_exportacao(self, df):
        """
        Seleciona apenas as colunas que vão para processed.dim_formas_pagamento
        """
        print("\n4️⃣ PREPARANDO DADOS PARA EXPORTAÇÃO...")

        # Colunas finais conforme definido no teste
        colunas_finais = [
            "forma_pagamento_id",
            "empresa_id",
            "forma_pagamento",
            "data_ingestao",
            "data_processamento",
        ]

        # Verificar se todas as colunas existem
        colunas_disponiveis = [col for col in colunas_finais if col in df.columns]
        colunas_faltando = [col for col in colunas_finais if col not in df.columns]

        if colunas_faltando:
            print(f"⚠️  Colunas não encontradas: {colunas_faltando}")

        df_final = df[colunas_disponiveis].copy()

        print(f"✅ {len(df_final)} registros prontos para exportação")
        print(f"   Colunas selecionadas: {list(df_final.columns)}")

        return df_final

    # =====================================================
    # 6. VALIDAR DADOS
    # =====================================================

    def validar_dados(self, df):
        """
        Valida os dados antes de exportar
        """
        print("\n5️⃣ VALIDANDO DADOS...")

        # Verificar chaves de negócio duplicadas
        duplicados = df[df.duplicated(subset=["forma_pagamento_id", "empresa_id"])][["forma_pagamento_id", "empresa_id"]]
        if len(duplicados) > 0:
            print(f"⚠️  ATENÇÃO: {len(duplicados)} forma_pagamento_ids duplicados para mesma empresa!")

        # Verificar valores obrigatórios
        nulos_id = df["forma_pagamento_id"].isna().sum()
        if nulos_id > 0:
            print(f"⚠️  {nulos_id} registros sem forma_pagamento_id")
            df = df[df["forma_pagamento_id"].notna()]

        nulos_descricao = df["forma_pagamento"].isna().sum()
        if nulos_descricao > 0:
            print(f"⚠️  {nulos_descricao} registros sem forma_pagamento")

        print(f"✅ Validação concluída! {len(df)} registros válidos")

        return df

    # =====================================================
    # 7. EXPORTAR PARA PROCESSED
    # =====================================================

    def exportar_para_processed(self, df):
        """
        Exporta dados para processed.dim_formas_pagamento
        COM COMPARAÇÃO INTELIGENTE (igual outros transformers)
        """
        print("\n6️⃣ EXPORTANDO PARA PROCESSED.DIM_FORMAS_PAGAMENTO...")

        session = Session()

        try:
            # Converter para dicionários
            registros = df.to_dict("records")

            # ESTRATÉGIA: UPSERT com comparação inteligente
            registros_inseridos = 0
            registros_atualizados = 0
            registros_identicos = 0

            for registro in registros:
                # Buscar registro existente
                resultado = session.execute(
                    text("""
                        SELECT forma_pagamento_id, empresa_id, forma_pagamento, 
                               data_ingestao, data_processamento
                        FROM processed.dim_formas_pagamento
                        WHERE forma_pagamento_id = :id
                        AND empresa_id = :empresa_id
                    """),
                    {"id": registro["forma_pagamento_id"], "empresa_id": self.empresa_id},
                ).fetchone()

                if resultado is None:
                    # INSERIR novo registro
                    stmt = insert(DimFormasPagamento).values(**registro)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['forma_pagamento_id', 'empresa_id'],
                        set_={
                            'forma_pagamento': stmt.excluded.forma_pagamento,
                            'data_ingestao': stmt.excluded.data_ingestao,
                            'data_processamento': stmt.excluded.data_processamento
                        }
                    )
                    session.execute(stmt)
                    registros_inseridos += 1
                else:
                    # Comparar se mudou algo (exceto data_processamento)
                    campos_comparar = [
                        "forma_pagamento",
                        "data_ingestao",
                    ]

                    mudou = False
                    for campo in campos_comparar:
                        valor_novo = registro.get(campo)
                        valor_antigo = getattr(resultado, campo, None)

                        # Comparar considerando None e NaN como iguais
                        if pd.isna(valor_novo) and pd.isna(valor_antigo):
                            continue
                        if valor_novo != valor_antigo:
                            mudou = True
                            break

                    if mudou:
                        # ATUALIZAR registro
                        session.execute(
                            text("""
                                UPDATE processed.dim_formas_pagamento
                                SET forma_pagamento = :forma_pagamento,
                                    data_ingestao = :data_ingestao,
                                    data_processamento = :data_processamento
                                WHERE forma_pagamento_id = :forma_pagamento_id
                                AND empresa_id = :empresa_id
                            """),
                            registro,
                        )
                        registros_atualizados += 1
                    else:
                        registros_identicos += 1

            session.commit()

            # Relatório
            total = registros_inseridos + registros_atualizados + registros_identicos
            print(f"\n✅ EXPORTAÇÃO CONCLUÍDA:")
            print(f"   • Inseridos: {registros_inseridos}")
            print(f"   • Atualizados: {registros_atualizados}")
            print(f"   • Idênticos (ignorados): {registros_identicos}")
            print(f"   • Total processado: {total}")
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
        """Pipeline completo"""
        try:
            df_raw = self.extrair_dados_raw()

            if len(df_raw) == 0:
                print(f"\n✅ Nenhum registro encontrado para empresa_id = {self.empresa_id}")
                return

            df = self.expandir_json(df_raw)
            df = self.aplicar_transformacoes(df)
            df = self.preparar_para_exportacao(df)
            df = self.validar_dados(df)
            self.exportar_para_processed(df)

            print(f"\n{'='*70}")
            print(f"🎉 TRANSFORMAÇÃO CONCLUÍDA!")
            print(f"{'='*70}")

        except Exception as e:
            print(f"\n❌ ERRO: {e}")
            raise