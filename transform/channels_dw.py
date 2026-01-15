# =====================================================
# TRANSFORMADOR DE CANAIS DE VENDA - MULTI-CNPJ
# =====================================================
# Responsável por: Limpar e transformar dados de canais_raw
# para dim_canais no schema processed

import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from config.database import Session, engine
from models.dim_fato.dim_canais import DimCanais

# =====================================================
# 1. CLASSE TRANSFORMADORA
# =====================================================

class CanaisTransformer:
    """
    Transformador específico para canais de venda
    Aplica limpezas e padronizações necessárias
    """

    def __init__(self, empresa_id):
        self.empresa_id = empresa_id
        self.engine = engine

    # =====================================================
    # 2. EXTRAIR DADOS RAW
    # =====================================================

    def extrair_dados_raw(self):
        """
        Extrai dados da tabela raw.canais_raw
        """
        print("\n1️⃣ EXTRAINDO DADOS DE RAW.CANAIS_RAW...")

        query = text("""
            SELECT 
                id,
                bling_canal_id,
                empresa_id,
                descricao,
                dados_json,
                data_ingestao
            FROM raw.canais_raw
            WHERE empresa_id = :empresa_id
            ORDER BY bling_canal_id
        """)

        df_raw = pd.read_sql(query, self.engine, params={"empresa_id": self.empresa_id})
        print(f"✅ {len(df_raw)} registros extraídos (empresa_id = {self.empresa_id})")

        return df_raw

    # =====================================================
    # 2.1 REGRAS DE CANAIS VÁLIDOS POR EMPRESA
    # =====================================================

    def _obter_canais_validos_por_empresa(self):
        """
        Define quais canais são válidos (reais) para cada empresa.
        Canais fora dessa lista são marcados como REPLICADO.
        
        MANUTENÇÃO: Adicione ou remova canais aqui conforme necessário
        """
        regras_canais = {
            1: [  # EMPRESA 1 - INGA
                "MAGALU INGA",
                "INGABIKE",
                "SITE",
                "MERCADO LIVRE INGA",
                "AMAZON INGÁ BIKE",
                "TIKTOK INGA",
                "SHOPEE INGA",
                "FBA AMAZON",
                "OUTROS"
            ],
            2: [  # EMPRESA 2 - G2
                "SHOPEE G2",
                "MAGAZINE LUIZA G2",
                "OUTROS"
            ],

            3: [  # EMPRESA 3 - G3
                "AMAZON G3",
                "SHOPEE G3",
                "OUTROS"
            ],

            4: [  # EMPRESA 4 - G4
                "AMAZON - SAMUEL",
                "AMAZON FBA ONSITE G4",
                "AMAZON G4",
                "MERCADO LIVRE - SAMUEL",
                "MERCADO LIVRE G4",
                "MERCADO LIVRE SFARIAS",
                "SHOPEE - SAMUEL",
                "SHOPEE - SAMUEL2",
                "SHOPEE G4",
                "SHOPEE S. FARIAS",
                "VIA VAREJO",
                "SHOPEE_MURILO",
                "OUTROS"
            ],

            5: [  # EMPRESA 5 - G5
                "MAGALU G5",
                "SHOPEE G5",
                "NETSHOES G5",
                "MERCADO LIVRE G5",
                "SHOPEE - JOAO VITOR",
                "KOG BIKE",
                "OUTROS"
            ],

            6: [  # EMPRESA 6 - DIAS
                "MAGALU KOG",
                "VIA VAREJO KOG",
                "NETSHOES KOG",
                "OUTROS"
            ],
        }
        
        return regras_canais.get(self.empresa_id, None)

    # =====================================================
    # 2.2 CLASSIFICAR CANAL COMO REAL OU REPLICADO
    # =====================================================

    def _classificar_canal(self, nome_canal):
        """
        Classifica um canal como REAL ou REPLICADO
        baseado nas regras da empresa
        """
        canais_validos = self._obter_canais_validos_por_empresa()
        
        # Se não há regra específica para esta empresa,
        # considera todos os canais como REAL
        if canais_validos is None:
            return "REAL"
        
        # Verifica se o canal está na lista de válidos
        if nome_canal in canais_validos:
            return "REAL"
        else:
            return "REPLICADO"

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
                df_raw[["id", "bling_canal_id", "empresa_id", "descricao", "data_ingestao"]],
                df_json,
            ],
            axis=1,
        )

        print(f"✅ JSON expandido! {len(df.columns)} colunas disponíveis")

        # === LIMPAR STRINGS VAZIAS ===
        print("   • Limpando strings vazias...")
        for coluna in df.select_dtypes(include=["object"]).columns:
            df[coluna] = df[coluna].replace(r"^\s*$", np.nan, regex=True).infer_objects(copy=False)
            df[coluna] = df[coluna].replace(["", " "], np.nan)

        # === RENOMEAR COLUNAS ===
        print("   • Renomeando colunas...")
        df = df.rename(
            columns={
                "descricao": "nome_canal"
            }
        )

        # === PADRONIZANDO NOMES ===
        df["nome_canal"] = df["nome_canal"].str.upper()

        # === CLASSIFICAR CANAIS ===
        print("   • Classificando canais como REAL ou REPLICADO...")
        df["canal_valido"] = df["nome_canal"].apply(self._classificar_canal)
        
        canais_reais = (df["canal_valido"] == "REAL").sum()
        canais_replicados = (df["canal_valido"] == "REPLICADO").sum()
        print(f"      ✅ {canais_reais} canais REAIS")
        print(f"      ⚠️  {canais_replicados} canais REPLICADOS")

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

        # ✅ Colunas que correspondem ao modelo DimCanais
        colunas_finais = [
            "bling_canal_id", 
            "empresa_id",
            "nome_canal",
            "canal_valido",  # ADICIONADO
            "data_ingestao",
            "data_processamento",
        ]

        # Selecionar apenas as colunas necessárias
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
        duplicatas = df.duplicated(subset=["bling_canal_id", "empresa_id"]).sum()
        if duplicatas > 0:
            print(f"\n   ⚠️  {duplicatas} registros duplicados encontrados!")
            df = df.drop_duplicates(subset=["bling_canal_id", "empresa_id"], keep="first")
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
                resultado = session.execute(
                    text("""
                        SELECT bling_canal_id, empresa_id, nome_canal, canal_valido, data_ingestao, data_processamento
                        FROM processed.dim_canais
                        WHERE bling_canal_id = :bling_canal_id
                        AND empresa_id = :empresa_id
                    """),
                    {
                        "bling_canal_id": registro["bling_canal_id"], 
                        "empresa_id": self.empresa_id
                    },
                ).fetchone()

                if resultado is None:
                    registros_novos += 1
                else:
                    # Comparar se mudou algo
                    campos_comparar = ["nome_canal", "canal_valido", "data_ingestao"]  

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
            print(f"   🔄 Processando registros...")

            for registro in registros:
                # ✅ INSERIR com on_conflict_do_update usando bling_canal_id
                stmt = insert(DimCanais).values(**registro)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['bling_canal_id', 'empresa_id'], 
                    set_={
                        'nome_canal': stmt.excluded.nome_canal,
                        'canal_valido': stmt.excluded.canal_valido, 
                        'data_ingestao': stmt.excluded.data_ingestao,
                        'data_processamento': stmt.excluded.data_processamento
                    }
                )
                session.execute(stmt)

            session.commit()

            # === CONFIRMAÇÃO FINAL ===
            print(f"   ✅ Processamento concluído!")
            print(f"\n{'='*70}")
            print(f"✅ EXPORTAÇÃO CONCLUÍDA COM SUCESSO!")
            print(f"{'='*70}\n")

            # Verificar total na tabela para esta empresa
            query = text("SELECT COUNT(*) FROM processed.dim_canais WHERE empresa_id = :empresa_id")
            total = session.execute(query, {"empresa_id": self.empresa_id}).scalar()
            print(f"✅ Verificação: {total} registros na tabela para empresa_id = {self.empresa_id}")

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
    # 7. GARANTIR CANAL "OUTROS"
    # =====================================================

    def _garantir_canal_outros(self):
        """
        Garante que existe um canal 'OUTROS' para esta empresa
        Usado para pedidos sem canal definido (vendas manuais, terceiros, etc)
        
        Empresa 1 = -1000001
        Empresa 2 = -1000002
        Empresa 3 = -1000003
        """
        print("\n6️⃣ GARANTINDO CANAL 'OUTROS'...")
        
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
                print(f"   ✅ Canal 'OUTROS' já existe (ID: {resultado[0]})")
            else:
                # Criar novo registro com ID único por empresa
                canal_outros_id = -(1000000 + self.empresa_id) # Criei negativo propositalmente, para garantir que cada empresa tenha seu próprio OUTROS sem conflitar com IDs reais do Bling
                
                query_insert = text("""
                    INSERT INTO processed.dim_canais 
                    (bling_canal_id, empresa_id, nome_canal, canal_valido, data_ingestao, data_processamento)
                    VALUES 
                    (:bling_canal_id, :empresa_id, 'OUTROS', 'REAL', NOW(), NOW())
                    ON CONFLICT (bling_canal_id, empresa_id) DO NOTHING
                """)
                
                session.execute(query_insert, {
                    "bling_canal_id": canal_outros_id,
                    "empresa_id": self.empresa_id
                })
                session.commit()
                
                print(f"   ✅ Canal 'OUTROS' criado com sucesso (ID: {canal_outros_id})")
            
        except Exception as e:
            session.rollback()
            print(f"   ⚠️ Erro ao garantir canal OUTROS: {e}")
        finally:
            session.close()

    # =====================================================
    # 8. EXECUTAR TRANSFORMAÇÃO COMPLETA
    # =====================================================

    def executar_transformacao_completa(self):
        """
        Executa o pipeline completo de transformação
        """
        try:
            # 1. Extrair dados raw
            df_raw = self.extrair_dados_raw()

            if len(df_raw) == 0:
                print(f"\n⚠️  Nenhum canal encontrado em raw.canais_raw para empresa_id = {self.empresa_id}")
                # Mesmo sem canais do Bling, garante que existe canal OUTROS
                self._garantir_canal_outros()
                return

            # 2. Aplicar transformações
            df = self.aplicar_transformacoes(df_raw)

            # 3. Preparar para exportação
            df = self.preparar_para_exportacao(df)

            # 4. Validar
            df = self.validar_dados(df)

            # 5. Exportar
            total_exportado = self.exportar_para_processed(df)

            # 6. Garantir canal OUTROS
            self._garantir_canal_outros()

            # Relatório final
            print(f"\n{'='*70}")
            print(f"🎉 TRANSFORMAÇÃO CONCLUÍDA!")
            print(f"{'='*70}")
            print(f"\n   📊 RESUMO:")
            print(f"      • Registros processados: {len(df)}")
            print(f"      • Registros exportados: {total_exportado}")
            print(f"      • Canal 'OUTROS' garantido ✅")

        except Exception as e:
            print(f"\n❌ ERRO na transformação: {e}")
            raise