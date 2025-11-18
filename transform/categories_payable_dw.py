# =====================================================
# TRANSFORMADOR DE CATEGORIAS DE CONTAS A PAGAR - MULTI-CNPJ
# =====================================================
# Responsável por: Limpar e transformar dados de categorias_contas_pagar_raw para dim_categorias_contas_pagar no schema processed
# ESTRATÉGIA: Comparar antes de salvar (igual outros transformers)

import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from config.database import Session, engine
from models.dim_fato.dim_categorias_contas_pagar import DimCategoriasContasPagar

# =====================================================
# MAPEAMENTO DE TIPOS - CATEGORIAS
# =====================================================

def obter_mapeamento_tipo_categoria():
    """
    Retorna o dicionário de mapeamento de tipo de categoria
    
    Mapeamento conforme regra de negócio:
    0 → Todas
    1 → Despesa
    2 → Receita
    3 → Receita e despesa
    """
    return {
        0: "Todas",
        1: "Despesa",
        2: "Receita",
        3: "Receita e Despesa",
    }

# =====================================================
# 1. CLASSE TRANSFORMADORA
# =====================================================

class CategoriasContasPagarTransformer:
    """
    Transformador específico para categorias de contas a pagar
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
        Extrai dados da tabela raw.categorias_contas_pagar_raw
        """
        print("\n1️⃣ EXTRAINDO DADOS DE RAW.CATEGORIAS_CONTAS_PAGAR_RAW...")

        query = text("""
            SELECT 
                id,
                bling_id,
                empresa_id,
                dados_json,
                data_ingestao
            FROM raw.categorias_contas_pagar_raw
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
        colunas_remover = ["id_bling", "idCategoriaPai"]
        df = df.drop(columns=[col for col in colunas_remover if col in df.columns])

        # === RENOMEAR COLUNAS ===
        print("   • Renomeando colunas...")
        df = df.rename(
            columns={
                "id": "categoria_id",
                "bling_id": "bling_categoria_id",
                "tipo": "tipo_categoria",
                "descricao": "categoria",
            }
        )

        # === PADRONIZAÇÃO DO CAMPO "TIPO" (ID → STRING) ===
        print("   • Mapeando tipo de categoria...")
        try:
            mapa_tipos = obter_mapeamento_tipo_categoria()
            if "tipo_categoria" in df.columns:
                # Converter para int antes de mapear
                df["tipo_categoria"] = pd.to_numeric(df["tipo_categoria"], errors="coerce")
                df["tipo_categoria"] = df["tipo_categoria"].map(mapa_tipos)
                print("      ✅ Tipos mapeados com sucesso")
        except Exception as e:
            print(f"      ⚠️  Erro ao mapear tipos: {e}")

        # === CONVERTENDO E PADRONIZANDO STRINGS VAZIAS ===
        print("   • Convertendo strings vazias para NaN...")
        for coluna in df.select_dtypes(include=["object"]).columns:
            df[coluna] = df[coluna].replace(r"^\s*$", np.nan, regex=True).infer_objects(copy=False)
            df[coluna] = df[coluna].replace("", np.nan)
            df[coluna] = df[coluna].replace(" ", np.nan)

        # === LIMPAR E PADRONIZAR CATEGORIA ===
        print("   • Padronizando categoria...")
        if "categoria" in df.columns:
            df["categoria"] = df["categoria"].str.strip()
            df["categoria"] = df["categoria"].str.title()

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
        Seleciona apenas as colunas que vão para processed.dim_categorias_contas_pagar
        """
        print("\n4️⃣ PREPARANDO DADOS PARA EXPORTAÇÃO...")

        # Colunas finais conforme definido no teste
        colunas_finais = [
            "categoria_id",
            "bling_categoria_id",
            "empresa_id",
            "tipo_categoria",
            "categoria",
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
        duplicados = df[df.duplicated(subset=["bling_categoria_id", "empresa_id"])][["bling_categoria_id", "empresa_id"]]
        if len(duplicados) > 0:
            print(f"⚠️  ATENÇÃO: {len(duplicados)} bling_categoria_ids duplicados para mesma empresa!")

        # Verificar valores obrigatórios
        nulos_id = df["categoria_id"].isna().sum()
        if nulos_id > 0:
            print(f"⚠️  {nulos_id} registros sem categoria_id")
            df = df[df["categoria_id"].notna()]

        nulos_categoria = df["categoria"].isna().sum()
        if nulos_categoria > 0:
            print(f"⚠️  {nulos_categoria} registros sem categoria")

        print(f"✅ Validação concluída! {len(df)} registros válidos")

        return df

    # =====================================================
    # 7. EXPORTAR PARA PROCESSED
    # =====================================================

    def exportar_para_processed(self, df):
        """
        Exporta dados para processed.dim_categorias_contas_pagar
        COM COMPARAÇÃO INTELIGENTE (igual outros transformers)
        """
        print("\n6️⃣ EXPORTANDO PARA PROCESSED.DIM_CATEGORIAS_CONTAS_PAGAR...")

        session = Session()

        try:
            # Converter para dicionários
            registros = df.to_dict("records")
            total_registros = len(registros)

            print(f"   📦 Total de registros a processar: {total_registros}")

            # === PRÉ-ANÁLISE: VERIFICAR O QUE VAI SER FEITO ===
            print(f"\n   🔍 Analisando registros existentes...")

            registros_novos = 0
            registros_modificados = 0
            registros_iguais = 0

            for registro in registros:
                # Buscar registro existente
                resultado = session.execute(
                    text("""
                        SELECT categoria_id, bling_categoria_id, empresa_id, tipo_categoria, categoria, data_ingestao, data_processamento
                        FROM processed.dim_categorias_contas_pagar
                        WHERE categoria_id = :id
                        AND empresa_id = :empresa_id
                    """),
                    {"id": registro["categoria_id"], "empresa_id": self.empresa_id},
                ).fetchone()

                if resultado is None:
                    registros_novos += 1
                else:
                    # Comparar se mudou algo
                    campos_comparar = [
                        "bling_categoria_id",
                        "tipo_categoria",
                        "categoria",
                        "data_ingestao",
                    ]

                    mudou = False
                    for campo in campos_comparar:
                        valor_novo = registro.get(campo)
                        valor_antigo = getattr(resultado, campo, None)

                        if pd.isna(valor_novo) and pd.isna(valor_antigo):
                            continue
                        if valor_novo != valor_antigo:
                            mudou = True
                            break

                    if mudou:
                        registros_modificados += 1
                    else:
                        registros_iguais += 1

            # === MOSTRAR ESTATÍSTICAS ANTES DA EXPORTAÇÃO ===
            print(f"\n{'='*70}")
            print(f"📊 ESTATÍSTICAS:")
            print(f"   • Registros INSERIDOS:     {registros_novos:>6} (novos)")
            print(f"   • Registros ATUALIZADOS:   {registros_modificados:>6} (modificados)")
            print(f"   • Registros IDÊNTICOS:     {registros_iguais:>6} (sem alteração)")
            print(f"   {'─'*50}")
            print(f"   • TOTAL PROCESSADO:        {total_registros:>6}")

            if registros_iguais > 0:
                economia_pct = (registros_iguais / total_registros) * 100
                print(f"\n💡 ECONOMIA:")
                print(f"   • {registros_iguais} registros idênticos serão pulados!")
                print(f"   • {economia_pct:.1f}% não precisam ser atualizados")

            print(f"{'='*70}\n")

            # === PROCESSAR REGISTROS ===
            registros_inseridos = 0
            registros_atualizados = 0
            registros_identicos = 0

            print(f"   🔄 Processando registros...")

            for registro in registros:
                # Buscar registro existente
                resultado = session.execute(
                    text("""
                        SELECT categoria_id, bling_categoria_id, empresa_id, tipo_categoria, categoria, data_ingestao, data_processamento
                        FROM processed.dim_categorias_contas_pagar
                        WHERE categoria_id = :id
                        AND empresa_id = :empresa_id
                    """),
                    {"id": registro["categoria_id"], "empresa_id": self.empresa_id},
                ).fetchone()

                if resultado is None:
                    # INSERIR novo registro
                    stmt = insert(DimCategoriasContasPagar).values(**registro)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['bling_categoria_id', 'empresa_id'],
                        set_={
                            'tipo_categoria': stmt.excluded.tipo_categoria,
                            'categoria': stmt.excluded.categoria,
                            'data_ingestao': stmt.excluded.data_ingestao,
                            'data_processamento': stmt.excluded.data_processamento
                        }
                    )
                    session.execute(stmt)
                    registros_inseridos += 1
                else:
                    # Comparar se mudou algo (exceto data_processamento)
                    campos_comparar = [
                        "bling_categoria_id",
                        "tipo_categoria",
                        "categoria",
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
                                UPDATE processed.dim_categorias_contas_pagar
                                SET bling_categoria_id = :bling_categoria_id,
                                    tipo_categoria = :tipo_categoria,
                                    categoria = :categoria,
                                    data_ingestao = :data_ingestao,
                                    data_processamento = :data_processamento
                                WHERE categoria_id = :categoria_id
                                AND empresa_id = :empresa_id
                            """),
                            registro,
                        )
                        registros_atualizados += 1
                    else:
                        registros_identicos += 1

            session.commit()

            # === CONFIRMAÇÃO FINAL ===
            print(f"   ✅ Processamento concluído!")
            print(f"\n{'='*70}")
            print(f"✅ EXPORTAÇÃO CONCLUÍDA COM SUCESSO!")
            print(f"{'='*70}\n")

            return len(df)

        except Exception as e:
            session.rollback()
            print(f"\n{'='*70}")
            print(f"❌ ERRO AO EXPORTAR!")
            print(f"{'='*70}")
            print(f"Erro: {e}")
            print(f"{'='*70}\n")
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