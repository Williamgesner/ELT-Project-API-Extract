# =====================================================
# TRANSFORMADOR DE CONTAS A PAGAR
# =====================================================
# Responsável por: Limpar e transformar dados de contas_pagar_raw para fato_contas_pagar no schema processed
# ESTRATÉGIA: Comparar antes de salvar (igual outros transformers)

import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from config.database import Session, engine
from models.dim_fato.fato_contas_pagar import FatoContasPagar

# =====================================================
# MAPEAMENTO DE SITUAÇÕES - CONTAS A PAGAR
# =====================================================

def obter_mapeamento_situacoes_contas_pagar():
    """
    Retorna o dicionário de mapeamento de situações para contas a pagar
    
    Mapeamento conforme regra de negócio:
    1 → Em aberto
    2 → Pago
    3 → Parcialmente recebido
    4 → Devolvido
    5 → Cancelado
    """
    return {
        1: "Em aberto",
        2: "Pago",
        3: "Parcialmente recebido",
        4: "Devolvido",
        5: "Cancelado"
    }

# =====================================================
# 1. CLASSE TRANSFORMADORA
# =====================================================

class ContasPagarTransformer:
    """
    Transformador específico para contas a pagar
    Aplica todas as limpezas e padronizações necessárias
    """

    def __init__(self):
        self.engine = engine

    # =====================================================
    # 2. EXTRAIR DADOS RAW
    # =====================================================

    def extrair_dados_raw(self):
        """
        Extrai dados da tabela raw.contas_pagar_raw
        """
        print("\n1️⃣ EXTRAINDO DADOS DE RAW.CONTAS_PAGAR_RAW...")

        query = """
            SELECT 
                id,
                bling_id,
                dados_json,
                data_ingestao
            FROM raw.contas_pagar_raw
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
        Aplica TODAS as transformações necessárias
        """
        print("\n3️⃣ APLICANDO TRANSFORMAÇÕES...")

        # === REMOVENDO COLUNAS DESNECESSÁRIAS ===
        print("   • Removendo colunas desnecessárias...")
        colunas_remover = ["id_bling"]
        df = df.drop(columns=[col for col in colunas_remover if col in df.columns])

        # === RENOMEAR COLUNAS ===
        print("   • Renomeando colunas...")
        df = df.rename(
            columns={
                "id": "contas_pagar_id",
                "bling_id": "bling_contas_pagar_id",
                "contato.id": "bling_cliente_id",
                "formaPagamento.id": "forma_pagamento_id",
                "vencimento": "data_vencimento",
            }
        )

        # === CONVERTENDO DATA DE VENCIMENTO PARA DATE ===
        print("   • Convertendo data de vencimento...")
        df["data_vencimento"] = pd.to_datetime(df["data_vencimento"], errors="coerce")

        # === CONVERTENDO E PADRONIZANDO STRINGS VAZIAS ===
        print("   • Convertendo strings vazias para NaN...")
        for coluna in df.select_dtypes(include=["object"]).columns:
            df[coluna] = df[coluna].replace(r"^\s*$", np.nan, regex=True)
            df[coluna] = df[coluna].replace("", np.nan)
            df[coluna] = df[coluna].replace(" ", np.nan)

        # === PADRONIZAÇÃO DO CAMPO "SITUAÇÃO" (ID → STRING) ===
        print("   • Mapeando situação...")
        try:
            mapa_situacoes = obter_mapeamento_situacoes_contas_pagar()
            if "situacao.id" in df.columns:
                # Guardar o ID original antes de mapear
                df["situacao_id_original"] = pd.to_numeric(df["situacao.id"], errors="coerce")
                df["situacao"] = df["situacao_id_original"].map(mapa_situacoes)
                print("      ✅ Situações mapeadas com sucesso")
            elif "situacao" in df.columns:
                # Guardar o ID original antes de mapear
                df["situacao_id_original"] = pd.to_numeric(df["situacao"], errors="coerce")
                df["situacao"] = df["situacao_id_original"].map(mapa_situacoes)
                print("      ✅ Situações mapeadas com sucesso")
        except Exception as e:
            print(f"      ⚠️  Erro ao mapear situações: {e}")

        # === APLICAR LÓGICA CONDICIONAL PARA "EM ABERTO" (SITUAÇÃO = 1) ===
        print("   • Aplicando regra de negócio para contas 'Em aberto'...")
        try:
            if "situacao_id_original" in df.columns and "data_vencimento" in df.columns:
                hoje = pd.Timestamp.now().normalize()  # Data de hoje sem hora
                
                # Criar máscara para situacao = 1 (Em aberto)
                mascara_em_aberto = df["situacao_id_original"] == 1
                
                # Contar quantas são "Em aberto"
                total_em_aberto = mascara_em_aberto.sum()
                
                if total_em_aberto > 0:
                    # Aplicar regras condicionais
                    # 1. Atrasada: vencimento < hoje
                    mascara_atrasada = mascara_em_aberto & (df["data_vencimento"] < hoje)
                    df.loc[mascara_atrasada, "situacao"] = "Atrasada"
                    qtd_atrasadas = mascara_atrasada.sum()
                    
                    # 2. Vencendo hoje: vencimento = hoje
                    mascara_vencendo_hoje = mascara_em_aberto & (df["data_vencimento"] == hoje)
                    df.loc[mascara_vencendo_hoje, "situacao"] = "Vencendo hoje"
                    qtd_vencendo_hoje = mascara_vencendo_hoje.sum()
                    
                    # 3. Em aberto: vencimento > hoje (mantém "Em aberto")
                    mascara_futuro = mascara_em_aberto & (df["data_vencimento"] > hoje)
                    qtd_futuro = mascara_futuro.sum()
                    
                    print(f"      ✅ Regra aplicada para {total_em_aberto} contas 'Em aberto':")
                    print(f"         • {qtd_atrasadas} → Atrasada (vencimento < hoje)")
                    print(f"         • {qtd_vencendo_hoje} → Vencendo hoje (vencimento = hoje)")
                    print(f"         • {qtd_futuro} → Em aberto (vencimento > hoje)")
                else:
                    print(f"      ℹ️  Nenhuma conta com situação 'Em aberto' encontrada")
                
                # Remover coluna auxiliar
                df = df.drop(columns=["situacao_id_original"])
        except Exception as e:
            print(f"      ⚠️  Erro ao aplicar regra condicional: {e}")
            # Se der erro, remove a coluna auxiliar se existir
            if "situacao_id_original" in df.columns:
                df = df.drop(columns=["situacao_id_original"])

        # === ADICIONAR METADADOS ===
        print("   • Adicionando metadados de processamento...")
        df["data_processamento"] = datetime.now()

        # === TRATAR FORMA DE PAGAMENTO INVÁLIDA ===
        print("   • Tratando forma_pagamento_id inválidos...")
        # Converter 0 para NULL (pois 0 não existe na dim_formas_pagamento)
        if "forma_pagamento_id" in df.columns:
            df.loc[df["forma_pagamento_id"] == 0, "forma_pagamento_id"] = np.nan
            df.loc[df["forma_pagamento_id"].isna(), "forma_pagamento_id"] = np.nan
            nulos_forma = df["forma_pagamento_id"].isna().sum()
            if nulos_forma > 0:
                print(f"      ⚠️  {nulos_forma} contas sem forma de pagamento definida (será NULL)")

        print("✅ Todas as transformações aplicadas com sucesso!")
        return df

    # =====================================================
    # 5. PREPARAR PARA EXPORTAÇÃO
    # =====================================================

    def preparar_para_exportacao(self, df):
        """
        Seleciona apenas as colunas que vão para processed.fato_contas_pagar
        """
        print("\n4️⃣ PREPARANDO DADOS PARA EXPORTAÇÃO...")

        # Colunas finais conforme definido no teste
        colunas_finais = [
            "contas_pagar_id",
            "bling_contas_pagar_id",
            "valor",
            "situacao",
            "data_vencimento",
            "bling_cliente_id",
            "forma_pagamento_id",
            "data_ingestao",
            "data_processamento",
        ]

        # Verificar se todas as colunas existem
        colunas_disponiveis = [col for col in colunas_finais if col in df.columns]
        colunas_faltando = [col for col in colunas_finais if col not in df.columns]

        if colunas_faltando:
            print(f"⚠️  Colunas não encontradas: {colunas_faltando}")

        df_final = df[colunas_disponiveis].copy()

        # === CONVERTER NaN PARA None (CRÍTICO PARA POSTGRESQL) ===
        print("   • Convertendo NaN para None (compatibilidade PostgreSQL)...")
        # Método mais robusto: replace + fillna
        if "forma_pagamento_id" in df_final.columns:
            # Substituir NaN por None
            df_final["forma_pagamento_id"] = df_final["forma_pagamento_id"].replace({np.nan: None})
            # Converter para object para aceitar None
            df_final["forma_pagamento_id"] = df_final["forma_pagamento_id"].astype(object)
            
        if "bling_cliente_id" in df_final.columns:
            df_final["bling_cliente_id"] = df_final["bling_cliente_id"].replace({np.nan: None})
            df_final["bling_cliente_id"] = df_final["bling_cliente_id"].astype(object)

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
        duplicados = df[df["bling_contas_pagar_id"].duplicated()]["bling_contas_pagar_id"]
        if len(duplicados) > 0:
            print(f"⚠️  ATENÇÃO: {len(duplicados)} bling_contas_pagar_ids duplicados!")

        # Verificar valores obrigatórios
        nulos_bling = df["bling_contas_pagar_id"].isna().sum()
        if nulos_bling > 0:
            print(f"⚠️  {nulos_bling} registros sem bling_contas_pagar_id")
            df = df[df["bling_contas_pagar_id"].notna()]

        nulos_valor = df["valor"].isna().sum()
        if nulos_valor > 0:
            print(f"⚠️  {nulos_valor} registros sem valor")

        nulos_data = df["data_vencimento"].isna().sum()
        if nulos_data > 0:
            print(f"⚠️  {nulos_data} registros sem data de vencimento")

        print(f"✅ Validação concluída! {len(df)} registros válidos")

        return df

    # =====================================================
    # 7. EXPORTAR PARA PROCESSED
    # =====================================================

    def exportar_para_processed(self, df):
        """
        Exporta dados para processed.fato_contas_pagar
        COM COMPARAÇÃO INTELIGENTE (igual outros transformers)
        """
        print("\n6️⃣ EXPORTANDO PARA PROCESSED.FATO_CONTAS_PAGAR...")

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
                        SELECT contas_pagar_id, bling_contas_pagar_id, valor, situacao, 
                               data_vencimento, bling_cliente_id, forma_pagamento_id,
                               data_ingestao, data_processamento
                        FROM processed.fato_contas_pagar
                        WHERE contas_pagar_id = :id
                    """),
                    {"id": registro["contas_pagar_id"]},
                ).fetchone()

                if resultado is None:
                    registros_novos += 1
                else:
                    # Comparar se mudou algo
                    campos_comparar = [
                        "bling_contas_pagar_id",
                        "valor",
                        "situacao",
                        "data_vencimento",
                        "bling_cliente_id",
                        "forma_pagamento_id",
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

            # Barra de progresso simples
            print(f"   🔄 Processando registros...")
            intervalo_print = max(1, total_registros // 10)  # Printar a cada 10%
            batch_size = 1000  # Commit a cada 1000 registros

            for idx, registro in enumerate(registros, 1):
                # Print de progresso
                if idx % intervalo_print == 0 or idx == total_registros:
                    percentual = (idx / total_registros) * 100
                    print(f"      • Processados: {idx}/{total_registros} ({percentual:.0f}%)")

                try:
                    # Buscar registro existente
                    resultado = session.execute(
                        text("""
                            SELECT contas_pagar_id, bling_contas_pagar_id, valor, situacao, 
                                   data_vencimento, bling_cliente_id, forma_pagamento_id,
                                   data_ingestao, data_processamento
                            FROM processed.fato_contas_pagar
                            WHERE contas_pagar_id = :id
                        """),
                        {"id": registro["contas_pagar_id"]},
                    ).fetchone()

                    if resultado is None:
                        # INSERIR novo registro
                        stmt = insert(FatoContasPagar).values(**registro)
                        session.execute(stmt)
                        registros_inseridos += 1
                    else:
                        # Comparar se mudou algo (exceto data_processamento)
                        campos_comparar = [
                            "bling_contas_pagar_id",
                            "valor",
                            "situacao",
                            "data_vencimento",
                            "bling_cliente_id",
                            "forma_pagamento_id",
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
                                    UPDATE processed.fato_contas_pagar
                                    SET bling_contas_pagar_id = :bling_contas_pagar_id,
                                        valor = :valor,
                                        situacao = :situacao,
                                        data_vencimento = :data_vencimento,
                                        bling_cliente_id = :bling_cliente_id,
                                        forma_pagamento_id = :forma_pagamento_id,
                                        data_ingestao = :data_ingestao,
                                        data_processamento = :data_processamento
                                    WHERE contas_pagar_id = :contas_pagar_id
                                """),
                                registro,
                            )
                            registros_atualizados += 1
                        else:
                            registros_identicos += 1

                    # COMMIT EM LOTES (a cada 1000 registros)
                    if idx % batch_size == 0:
                        session.commit()
                        print(f"      💾 Batch commit realizado ({idx} registros processados)")

                except Exception as e:
                    # Se der erro em UM registro, pula e continua
                    print(f"\n      ⚠️  Erro no registro {idx} (ID: {registro.get('contas_pagar_id')}): {str(e)[:100]}")
                    print(f"      ⏭️  Pulando e continuando...")
                    session.rollback()  # Rollback apenas deste registro
                    continue

            # Commit final (registros que sobraram)
            print(f"\n   💾 Salvando alterações finais no banco de dados...")
            session.commit()
            print(f"   ✅ Commit final realizado com sucesso!")

            # === CONFIRMAÇÃO FINAL ===
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
    # 8. ATUALIZAR STATUS
    # =====================================================

    def atualizar_status_raw(self, df):
        """Atualiza status em raw.contas_pagar_raw"""
        print("\n7️⃣ ATUALIZANDO STATUS...")

        session = Session()

        try:
            ids = df["contas_pagar_id"].tolist()

            query = text(
                """
                UPDATE raw.contas_pagar_raw
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
    # 9. EXECUTAR TRANSFORMAÇÃO COMPLETA
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
