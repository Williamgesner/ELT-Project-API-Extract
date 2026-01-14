# =====================================================
# TRANSFORMADOR DE CONTAS A PAGAR - VERSÃO CORRIGIDA
# =====================================================

import pandas as pd
import numpy as np
from datetime import datetime, date
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from config.database import Session, engine
from models.dim_fato.fato_contas_pagar import FatoContasPagar

def obter_mapeamento_situacoes_contas_pagar():
    """Mapeamento de situações"""
    return {
        1: "Em aberto",
        2: "Pago",
        3: "Parcialmente recebido",
        4: "Devolvido",
        5: "Cancelado"
    }

class ContasPagarTransformer:
    
    def __init__(self, empresa_id):
        self.empresa_id = empresa_id
        self.engine = engine

    # =====================================================
    # Sincronizar_delecoes
    # =====================================================
    
    def sincronizar_delecoes(self):
        """
        Remove da processed registros que foram DELETADOS no Bling.
        
        LÓGICA CORRIGIDA:
        1. Busca todos os IDs na RAW (que vieram da última extração)
        2. Busca todos os IDs na PROCESSED
        3. Remove da PROCESSED os IDs que NÃO estão na RAW
        
        IMPORTANTE: Deve rodar APÓS a extração completa (para garantir que
        a RAW tem todos os registros atuais do Bling)
        """
        print("\n🗑️  SINCRONIZANDO DELEÇÕES...")
        
        session = Session()
        try:
            # 1. Buscar todos os IDs na PROCESSED
            query_processed = text("""
                SELECT bling_contas_pagar_id
                FROM processed.fato_contas_pagar
                WHERE empresa_id = :empresa_id
            """)
            ids_processed = session.execute(
                query_processed, 
                {"empresa_id": self.empresa_id}
            ).fetchall()
            ids_processed = {int(row[0]) for row in ids_processed}
            
            # 2. Buscar todos os IDs na RAW
            query_raw = text("""
                SELECT bling_id
                FROM raw.contas_pagar_raw
                WHERE empresa_id = :empresa_id
            """)
            ids_raw = session.execute(
                query_raw, 
                {"empresa_id": self.empresa_id}
            ).fetchall()
            ids_raw = {int(row[0]) for row in ids_raw}

            # 🛡️ PROTEÇÃO CRÍTICA
            if len(ids_raw) == 0 and len(ids_processed) > 0:
                print(f"   🚨 RAW VAZIA - API PODE TER FALHADO!")
                print(f"   🛡️ ABORTANDO sincronização (proteção ativa)")
                print(f"   ✅ {len(ids_processed)} registros preservados")
                return
            
            # 3. Identificar IDs que estão na PROCESSED mas NÃO na RAW
            ids_deletados = ids_processed - ids_raw
            
            if len(ids_deletados) == 0:
                print(f"   ✅ Nenhum registro deletado (banco sincronizado)")
                print(f"   📊 RAW: {len(ids_raw)} registros | PROCESSED: {len(ids_processed)} registros")
                return
            
            print(f"   ⚠️  {len(ids_deletados)} registro(s) encontrado(s) na PROCESSED mas NÃO na RAW")
            print(f"   🗑️  Estes registros foram DELETADOS no Bling e serão removidos da PROCESSED")
            
            # 4. Deletar da PROCESSED
            query_delete = text("""
                DELETE FROM processed.fato_contas_pagar
                WHERE empresa_id = :empresa_id
                AND bling_contas_pagar_id = ANY(:ids)
            """)
            
            resultado = session.execute(query_delete, {
                "empresa_id": self.empresa_id,
                "ids": list(ids_deletados)
            })
            session.commit()
            
            print(f"   ✅ {resultado.rowcount} registro(s) removido(s) da PROCESSED")
            
            # 5. Mostrar quais foram removidos (auditoria)
            if len(ids_deletados) <= 10:
                print(f"   📋 IDs removidos: {sorted(list(ids_deletados))}")
            else:
                print(f"   📋 Primeiros 10 IDs removidos: {sorted(list(ids_deletados))[:10]}")
            
            # 6. OPCIONAL mas RECOMENDADO: Remover da RAW também
            print(f"\n   🧹 Limpando registros órfãos da RAW...")
            query_delete_raw = text("""
                DELETE FROM raw.contas_pagar_raw
                WHERE empresa_id = :empresa_id
                AND bling_id = ANY(:ids)
            """)
            resultado_raw = session.execute(query_delete_raw, {
                "empresa_id": self.empresa_id,
                "ids": list(ids_deletados)
            })
            session.commit()
            print(f"   ✅ {resultado_raw.rowcount} registro(s) removido(s) da RAW")
                
        except Exception as e:
            session.rollback()
            print(f"   ⚠️  Erro ao sincronizar deleções: {e}")
            raise
        finally:
            session.close()

    # =====================================================
    # Peparar_registros_para_processamento
    # Agora só marca baseado em regras de negócio
    # =====================================================
    
    def preparar_registros_para_processamento(self):
        """
        Marca como 'pendente' registros que precisam ser reavaliados.
        
        LÓGICA:
        - Contas "Em aberto" (situacao=1) onde vencimento mudou de status
        - Força reprocessamento baseado em passagem do tempo
        
        NÃO mexe com deleções (isso é feito em sincronizar_delecoes)
        """
        print("\n🔄 PREPARANDO REGISTROS PARA REAVALIAÇÃO...")
        
        session = Session()
        try:
            # Marcar como pendente contas que mudaram de situação por causa da data
            query = text("""
                UPDATE raw.contas_pagar_raw
                SET status_processamento = 'pendente'
                WHERE empresa_id = :empresa_id
                AND status_processamento = 'processado'
                AND (dados_json->>'situacao')::int = 1
                AND (
                    (dados_json->>'vencimento')::date < CURRENT_DATE
                    OR
                    (dados_json->>'vencimento')::date = CURRENT_DATE
                )
            """)
            
            resultado = session.execute(query, {"empresa_id": self.empresa_id})
            session.commit()
            
            if resultado.rowcount > 0:
                print(f"   ⚡ {resultado.rowcount} contas marcadas para reavaliação (mudança de situação)")
            else:
                print(f"   ✅ Nenhuma conta precisa de reavaliação")
                
        except Exception as e:
            session.rollback()
            print(f"   ⚠️  Erro ao preparar registros: {e}")
            raise
        finally:
            session.close()

    def extrair_dados_raw(self):
        """Extrai dados da tabela raw.contas_pagar_raw"""
        print("\n1️⃣ EXTRAINDO DADOS DE RAW.CONTAS_PAGAR_RAW...")

        query = text("""
            SELECT 
                id,
                bling_id,
                empresa_id,
                dados_json,
                data_ingestao
            FROM raw.contas_pagar_raw
            WHERE empresa_id = :empresa_id
            AND status_processamento = 'pendente'
            ORDER BY bling_id
        """)

        df_raw = pd.read_sql(query, self.engine, params={"empresa_id": self.empresa_id})
        print(f"✅ {len(df_raw)} registros pendentes extraídos (empresa_id = {self.empresa_id})")   

        return df_raw

    def expandir_json(self, df_raw):
        """Expande o JSON em colunas"""
        print("\n2️⃣ EXPANDINDO JSON EM COLUNAS...")

        df_json = pd.json_normalize(df_raw["dados_json"])

        if "id" in df_json.columns:
            df_json = df_json.rename(columns={"id": "id_bling"})

        df = pd.concat(
            [
                df_raw[["id", "bling_id", "empresa_id", "data_ingestao"]],
                df_json,
            ],
            axis=1,
        )

        print(f"✅ JSON expandido! {len(df.columns)} colunas disponíveis")
        return df

    def aplicar_transformacoes(self, df):
        """Aplica TODAS as transformações necessárias"""
        print("\n3️⃣ APLICANDO TRANSFORMAÇÕES...")

        # Removendo colunas desnecessárias
        print("   • Removendo colunas desnecessárias...")
        colunas_remover = ["id_bling"]
        df = df.drop(columns=[col for col in colunas_remover if col in df.columns])

        # Renomear colunas
        print("   • Renomeando colunas...")
        df = df.rename(
            columns={
                "id": "contas_pagar_id",
                "bling_id": "bling_contas_pagar_id",
                "contato.id": "bling_cliente_id",
                "formaPagamento.id": "forma_pagamento_id",
                "categoria.id": "bling_categoria_id",
                "vencimento": "data_vencimento",
            }
        )

        # Convertendo data de vencimento
        print("   • Convertendo data de vencimento...")
        df["data_vencimento"] = pd.to_datetime(df["data_vencimento"], errors="coerce").dt.date

        # Convertendo strings vazias
        print("   • Convertendo strings vazias para NaN...")
        for coluna in df.select_dtypes(include=["object"]).columns:
            # Corrigir FutureWarning: adicionar infer_objects(copy=False) explicitamente
            # conforme recomendação do pandas
            df[coluna] = df[coluna].replace(r"^\s*$", np.nan, regex=True).infer_objects(copy=False)
            df[coluna] = df[coluna].replace("", np.nan).infer_objects(copy=False)
            df[coluna] = df[coluna].replace(" ", np.nan).infer_objects(copy=False)
            # Converter explicitamente para object mantendo NaN
            df[coluna] = df[coluna].astype(object)

        # Mapeando situação
        print("   • Mapeando situação...")
        try:
            mapa_situacoes = obter_mapeamento_situacoes_contas_pagar()
            if "situacao.id" in df.columns:
                df["situacao_id_original"] = pd.to_numeric(df["situacao.id"], errors="coerce")
                df["situacao"] = df["situacao_id_original"].map(mapa_situacoes)
                print("      ✅ Situações mapeadas com sucesso")
            elif "situacao" in df.columns:
                df["situacao_id_original"] = pd.to_numeric(df["situacao"], errors="coerce")
                df["situacao"] = df["situacao_id_original"].map(mapa_situacoes)
                print("      ✅ Situações mapeadas com sucesso")
        except Exception as e:
            print(f"      ⚠️  Erro ao mapear situações: {e}")

        # Aplicar regra de negócio para "Em aberto"
        print("   • Aplicando regra de negócio para contas 'Em aberto'...")
        try:
            if "situacao_id_original" in df.columns and "data_vencimento" in df.columns:
                hoje = pd.Timestamp.now().normalize().date()
                
                mascara_em_aberto = df["situacao_id_original"] == 1
                total_em_aberto = mascara_em_aberto.sum()
                
                if total_em_aberto > 0:
                    # Atrasada: vencimento < hoje
                    mascara_atrasada = mascara_em_aberto & (df["data_vencimento"] < hoje)
                    df.loc[mascara_atrasada, "situacao"] = "Atrasada"
                    qtd_atrasadas = mascara_atrasada.sum()
                    
                    # Vencendo hoje
                    mascara_vencendo_hoje = mascara_em_aberto & (df["data_vencimento"] == hoje)
                    df.loc[mascara_vencendo_hoje, "situacao"] = "Vencendo hoje"
                    qtd_vencendo_hoje = mascara_vencendo_hoje.sum()
                    
                    # Em aberto (futuro)
                    mascara_futuro = mascara_em_aberto & (df["data_vencimento"] > hoje)
                    qtd_futuro = mascara_futuro.sum()
                    
                    print(f"      ✅ Regra aplicada para {total_em_aberto} contas 'Em aberto':")
                    print(f"         • {qtd_atrasadas} → Atrasada")
                    print(f"         • {qtd_vencendo_hoje} → Vencendo hoje")
                    print(f"         • {qtd_futuro} → Em aberto")
                else:
                    print(f"      ℹ️  Nenhuma conta com situação 'Em aberto' encontrada")
                
                df = df.drop(columns=["situacao_id_original"])
        except Exception as e:
            print(f"      ⚠️  Erro ao aplicar regra condicional: {e}")
            if "situacao_id_original" in df.columns:
                df = df.drop(columns=["situacao_id_original"])

        # ========================================================================
        # 🔧 ÚNICA ALTERAÇÃO: Validar BIGINT em TODOS os IDs (linhas 326-360)
        # ========================================================================
        print("   • Validando e convertendo IDs BIGINT...")
        
        BIGINT_MAX = 9223372036854775807
        BIGINT_MIN = -9223372036854775808
        
        campos_bigint = {
            "bling_contas_pagar_id": "ID Bling Contas Pagar",
            "bling_cliente_id": "ID Cliente",
            "forma_pagamento_id": "ID Forma Pagamento",
            "bling_categoria_id": "ID Categoria"
        }
        
        for campo, descricao in campos_bigint.items():
            if campo in df.columns:
                df[campo] = pd.to_numeric(df[campo], errors="coerce")
                
                mask_invalidos = (df[campo] > BIGINT_MAX) | (df[campo] < BIGINT_MIN)
                qtd_invalidos = mask_invalidos.sum()
                
                if qtd_invalidos > 0:
                    print(f"      ⚠️  {descricao}: {qtd_invalidos} valores fora do range BIGINT! Convertendo para NULL...")
                    df.loc[mask_invalidos, campo] = None
                
                if campo == "forma_pagamento_id":
                    df[campo] = df[campo].replace(0, np.nan)
                
                df[campo] = df[campo].astype('Int64')
                
                nulos = df[campo].isna().sum()
                if nulos > 0 and qtd_invalidos == 0:
                    print(f"      ℹ️  {descricao}: {nulos} valores NULL")
        # ========================================================================
        # FIM DA ÚNICA ALTERAÇÃO
        # ========================================================================

        # Adicionar metadados
        print("   • Adicionando metadados de processamento...")
        df["data_processamento"] = datetime.now()

        print("✅ Todas as transformações aplicadas com sucesso!")
        return df

    def preparar_para_exportacao(self, df):
        """Seleciona apenas as colunas finais"""
        print("\n4️⃣ PREPARANDO DADOS PARA EXPORTAÇÃO...")

        colunas_finais = [
            "contas_pagar_id",
            "bling_contas_pagar_id",
            "empresa_id",
            "valor",
            "situacao",
            "data_vencimento",
            "bling_cliente_id",
            "forma_pagamento_id",
            "bling_categoria_id",
            "data_ingestao",
            "data_processamento",
        ]

        colunas_disponiveis = [col for col in colunas_finais if col in df.columns]
        colunas_faltando = [col for col in colunas_finais if col not in df.columns]

        if colunas_faltando:
            print(f"⚠️  Colunas não encontradas: {colunas_faltando}")

        df_final = df[colunas_disponiveis].copy()

        # ===================================================================================
        # CORREÇÃO DO FUTUREWARNING - ADICIONAR .infer_objects() (Atualização do Pandas)
        # ===================================================================================
        print("   • Convertendo NaN/NA para None...")
        
        # Converter IDs BigInteger para int64 para evitar erro "bigint out of range"
        print("   • Convertendo IDs para tipos numéricos seguros...")
        if "contas_pagar_id" in df_final.columns:
            # Converter para int64 (não usar downcast para evitar perda de precisão)
            df_final["contas_pagar_id"] = pd.to_numeric(df_final["contas_pagar_id"], errors="coerce")
            # Usar Int64 (nullable integer) para manter compatibilidade com NaN
            df_final["contas_pagar_id"] = df_final["contas_pagar_id"].astype("Int64")
        
        if "bling_contas_pagar_id" in df_final.columns:
            df_final["bling_contas_pagar_id"] = pd.to_numeric(df_final["bling_contas_pagar_id"], errors="coerce")
            df_final["bling_contas_pagar_id"] = df_final["bling_contas_pagar_id"].astype("Int64")
        
        if "forma_pagamento_id" in df_final.columns:
            df_final["forma_pagamento_id"] = df_final["forma_pagamento_id"].replace({np.nan: None, pd.NA: None})
            df_final["forma_pagamento_id"] = df_final["forma_pagamento_id"].astype(object)
            
        if "bling_cliente_id" in df_final.columns:
            df_final["bling_cliente_id"] = df_final["bling_cliente_id"].replace({np.nan: None, pd.NA: None})
            df_final["bling_cliente_id"] = df_final["bling_cliente_id"].astype(object)
        
        if "bling_categoria_id" in df_final.columns:
            df_final["bling_categoria_id"] = df_final["bling_categoria_id"].replace({pd.NA: None, np.nan: None})
            df_final["bling_categoria_id"] = df_final["bling_categoria_id"].astype(object)

        print(f"✅ {len(df_final)} registros prontos para exportação")
        return df_final

    def validar_dados(self, df):
        """Valida os dados antes de exportar"""
        print("\n5️⃣ VALIDANDO DADOS...")

        # Verificar duplicados
        duplicados = df[df.duplicated(subset=["bling_contas_pagar_id", "empresa_id"])][["bling_contas_pagar_id", "empresa_id"]]
        if len(duplicados) > 0:
            print(f"⚠️  {len(duplicados)} bling_contas_pagar_ids duplicados!")

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

    def validar_foreign_keys(self, df):
        """Valida se as FKs existem nas tabelas de dimensão"""
        print("\n🔍 VALIDANDO FOREIGN KEYS...")
        
        session = Session()
        total_original = len(df)
        
        try:
            # Validar bling_categoria_id
            if "bling_categoria_id" in df.columns:
                print("   • Validando bling_categoria_id...")
                
                categorias_validas = session.execute(
                    text("""
                        SELECT bling_categoria_id 
                        FROM processed.dim_categorias_contas_pagar
                        WHERE empresa_id = :empresa_id
                    """),
                    {"empresa_id": self.empresa_id}
                ).fetchall()
                categorias_validas = {cat[0] for cat in categorias_validas}
                
                df_com_categoria = df[df["bling_categoria_id"].notna()]
                
                if len(df_com_categoria) > 0:
                    categorias_invalidas = df_com_categoria[
                        ~df_com_categoria["bling_categoria_id"].isin(categorias_validas)
                    ]
                    
                    if len(categorias_invalidas) > 0:
                        print(f"      ⚠️  {len(categorias_invalidas)} registros com bling_categoria_id INVÁLIDA!")
                        print(f"      🔧 Convertendo para NULL...")
                        df.loc[~df["bling_categoria_id"].isin(categorias_validas), "bling_categoria_id"] = None
                    else:
                        print(f"      ✅ Todas as {len(df_com_categoria)} categorias são válidas")
                else:
                    print(f"      ℹ️  Nenhum registro com categoria definida")
            
            # Validar forma_pagamento_id
            if "forma_pagamento_id" in df.columns:
                print("   • Validando forma_pagamento_id...")
                
                formas_validas = session.execute(
                    text("""
                        SELECT forma_pagamento_id 
                        FROM processed.dim_formas_pagamento
                        WHERE empresa_id = :empresa_id
                    """),
                    {"empresa_id": self.empresa_id}
                ).fetchall()
                formas_validas = {forma[0] for forma in formas_validas}
                
                df_com_forma = df[df["forma_pagamento_id"].notna()]
                
                if len(df_com_forma) > 0:
                    formas_invalidas = df_com_forma[
                        ~df_com_forma["forma_pagamento_id"].isin(formas_validas)
                    ]
                    
                    if len(formas_invalidas) > 0:
                        print(f"      ⚠️  {len(formas_invalidas)} registros com forma_pagamento_id INVÁLIDA!")
                        print(f"      🔧 Convertendo para NULL...")
                        df.loc[~df["forma_pagamento_id"].isin(formas_validas), "forma_pagamento_id"] = None
                    else:
                        print(f"      ✅ Todas as {len(df_com_forma)} formas de pagamento são válidas")
                else:
                    print(f"      ℹ️  Nenhum registro com forma de pagamento definida")
            
            # Validar data_vencimento
            if "data_vencimento" in df.columns:
                print("   • Validando data_vencimento...")
                
                datas_validas = session.execute(
                    text("SELECT data_completa FROM processed.dim_tempo")
                ).fetchall()
                datas_validas = {data[0] for data in datas_validas}
                
                if len(df) > 0 and not isinstance(df["data_vencimento"].iloc[0], date):
                    print("      ⚠️  Convertendo data_vencimento para tipo date...")
                    df["data_vencimento"] = pd.to_datetime(df["data_vencimento"]).dt.date
                
                datas_invalidas = df[~df["data_vencimento"].isin(datas_validas)]
                
                if len(datas_invalidas) > 0:
                    print(f"      ⚠️  {len(datas_invalidas)} registros com data_vencimento NÃO CADASTRADA!")
                    print(f"      ❌ REMOVENDO estes registros")
                    df = df[df["data_vencimento"].isin(datas_validas)]
                else:
                    print(f"      ✅ Todas as datas existem na dim_tempo")
            
            print(f"\n   📊 Registros após validação: {len(df)} de {total_original}")
            if len(df) < total_original:
                removidos = total_original - len(df)
                print(f"   🗑️  {removidos} registros removidos por FK inválida")
            
            print("✅ Validação de Foreign Keys concluída!\n")
            
            return df
        
        except Exception as e:
            print(f"❌ Erro ao validar Foreign Keys: {e}")
            raise
        finally:
            session.close()

    def exportar_para_processed(self, df):
        """Exporta dados para processed.fato_contas_pagar"""
        print("\n6️⃣ EXPORTANDO PARA PROCESSED.FATO_CONTAS_PAGAR...")

        session = Session()

        try:
            registros = df.to_dict("records")
            total_registros = len(registros)
            
            print(f"   📦 Total de registros a processar: {total_registros}")

            # Buscar registros existentes
            print(f"   🔍 Buscando registros existentes no banco...")
            
            ids_processar = [r["contas_pagar_id"] for r in registros]
            
            query_existentes = text("""
                SELECT contas_pagar_id, bling_contas_pagar_id, empresa_id, valor, situacao, 
                       data_vencimento, bling_cliente_id, forma_pagamento_id, bling_categoria_id,
                       data_ingestao, data_processamento
                FROM processed.fato_contas_pagar
                WHERE contas_pagar_id = ANY(:ids)
                AND empresa_id = :empresa_id
            """)
            
            resultados = session.execute(query_existentes, {
                "ids": ids_processar,
                "empresa_id": self.empresa_id
            }).fetchall()
            
            registros_existentes = {r.contas_pagar_id: r for r in resultados}
            
            # Pré-análise
            print(f"\n   🔍 Analisando registros existentes...")
            
            registros_novos = 0
            registros_modificados = 0
            registros_iguais = 0
            
            for registro in registros:
                contas_pagar_id = registro["contas_pagar_id"]
                
                if contas_pagar_id not in registros_existentes:
                    registros_novos += 1
                else:
                    resultado = registros_existentes[contas_pagar_id]
                    
                    campos_comparar = [
                        "bling_contas_pagar_id",
                        "valor",
                        "situacao",
                        "data_vencimento",
                        "bling_cliente_id",
                        "forma_pagamento_id",
                        "bling_categoria_id",
                    ]

                    mudou = False
                    for campo in campos_comparar:
                        valor_novo = registro.get(campo)
                        valor_antigo = getattr(resultado, campo, None)
                        
                        # Tratamento especial para datas
                        if campo == "data_vencimento":
                            if valor_novo and isinstance(valor_novo, datetime):
                                valor_novo = valor_novo.date()
                            if valor_antigo and isinstance(valor_antigo, datetime):
                                valor_antigo = valor_antigo.date()
                            # Comparar datas como strings para evitar problemas de tipo
                            if valor_novo and valor_antigo:
                                if str(valor_novo) == str(valor_antigo):
                                    continue
                            elif valor_novo is None and valor_antigo is None:
                                continue
                            else:
                                mudou = True
                                break
                        
                        # Tratamento especial para valores decimais
                        elif campo == "valor":
                            if valor_novo is not None and valor_antigo is not None:
                                try:
                                    diff = abs(float(valor_novo) - float(valor_antigo))
                                    if diff < 0.01:  # Tolerância para diferenças de arredondamento
                                        continue
                                except (ValueError, TypeError):
                                    pass
                            elif valor_novo is None and valor_antigo is None:
                                continue
                            else:
                                mudou = True
                                break
                        
                        # Tratamento especial para IDs (BigInteger)
                        elif campo in ["bling_contas_pagar_id", "bling_cliente_id", "forma_pagamento_id", "bling_categoria_id"]:
                            # Converter para int para comparação segura
                            try:
                                novo_int = int(valor_novo) if valor_novo is not None else None
                                antigo_int = int(valor_antigo) if valor_antigo is not None else None
                                if novo_int == antigo_int:
                                    continue
                                else:
                                    mudou = True
                                    break
                            except (ValueError, TypeError):
                                # Se não conseguir converter, comparar como está
                                if valor_novo == valor_antigo:
                                    continue
                                elif valor_novo is None and valor_antigo is None:
                                    continue
                                else:
                                    mudou = True
                                    break
                        
                        # Tratamento para strings (situacao)
                        elif campo == "situacao":
                            novo_str = str(valor_novo).strip() if valor_novo is not None else ""
                            antigo_str = str(valor_antigo).strip() if valor_antigo is not None else ""
                            if novo_str == antigo_str:
                                continue
                            else:
                                mudou = True
                                break
                        
                        # Comparação genérica
                        else:
                            if valor_novo is None and valor_antigo is None:
                                continue
                            if (valor_novo is None) != (valor_antigo is None):
                                mudou = True
                                break
                            # Comparar convertendo para string para evitar problemas de tipo
                            if str(valor_novo) != str(valor_antigo):
                                mudou = True
                                break

                    if mudou:
                        registros_modificados += 1
                    else:
                        registros_iguais += 1

            # Mostrar estatísticas
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

            # Processar registros
            print(f"   🔄 Processando registros...")
            intervalo_print = max(1, total_registros // 10)
            batch_size = 1000

            for idx, registro in enumerate(registros, 1):
                if idx % intervalo_print == 0 or idx == total_registros:
                    percentual = (idx / total_registros) * 100
                    print(f"      • Processados: {idx}/{total_registros} ({percentual:.0f}%)")

                try:
                    # Converter IDs para int64 antes de processar (evita bigint out of range)
                    # Tratar valores None, NaN, pd.NA
                    def safe_int_convert(value):
                        """Converte valor para int de forma segura"""
                        if value is None or pd.isna(value):
                            return None
                        try:
                            return int(float(value))  # Converter via float primeiro para lidar com strings numéricas
                        except (ValueError, TypeError, OverflowError):
                            return None
                    
                    contas_pagar_id = safe_int_convert(registro.get("contas_pagar_id"))
                    if contas_pagar_id is None:
                        print(f"      ⚠️  Registro {idx}: contas_pagar_id inválido, pulando...")
                        continue
                    registro["contas_pagar_id"] = contas_pagar_id
                    
                    # Converter outros IDs BigInteger
                    registro["bling_contas_pagar_id"] = safe_int_convert(registro.get("bling_contas_pagar_id"))
                    registro["bling_cliente_id"] = safe_int_convert(registro.get("bling_cliente_id"))
                    registro["forma_pagamento_id"] = safe_int_convert(registro.get("forma_pagamento_id"))
                    registro["bling_categoria_id"] = safe_int_convert(registro.get("bling_categoria_id"))
                    
                    if contas_pagar_id not in registros_existentes:
                        stmt = insert(FatoContasPagar).values(**registro)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['bling_contas_pagar_id', 'empresa_id'],
                            set_={
                                'valor': stmt.excluded.valor,
                                'situacao': stmt.excluded.situacao,
                                'data_vencimento': stmt.excluded.data_vencimento,
                                'bling_cliente_id': stmt.excluded.bling_cliente_id,
                                'forma_pagamento_id': stmt.excluded.forma_pagamento_id,
                                'bling_categoria_id': stmt.excluded.bling_categoria_id,
                                'data_ingestao': stmt.excluded.data_ingestao,
                                'data_processamento': stmt.excluded.data_processamento
                            }
                        )
                        session.execute(stmt)
                    else:
                        resultado = registros_existentes[contas_pagar_id]
                        
                        campos_comparar = [
                            "bling_contas_pagar_id",
                            "valor",
                            "situacao",
                            "data_vencimento",
                            "bling_cliente_id",
                            "forma_pagamento_id",
                            "bling_categoria_id",
                        ]

                        mudou = False
                        for campo in campos_comparar:
                            valor_novo = registro.get(campo)
                            valor_antigo = getattr(resultado, campo, None)
                            
                            # Tratamento especial para datas
                            if campo == "data_vencimento":
                                if valor_novo and isinstance(valor_novo, datetime):
                                    valor_novo = valor_novo.date()
                                if valor_antigo and isinstance(valor_antigo, datetime):
                                    valor_antigo = valor_antigo.date()
                                # Comparar datas como strings para evitar problemas de tipo
                                if valor_novo and valor_antigo:
                                    if str(valor_novo) == str(valor_antigo):
                                        continue
                                elif valor_novo is None and valor_antigo is None:
                                    continue
                                else:
                                    mudou = True
                                    break
                            
                            # Tratamento especial para valores decimais
                            elif campo == "valor":
                                if valor_novo is not None and valor_antigo is not None:
                                    try:
                                        diff = abs(float(valor_novo) - float(valor_antigo))
                                        if diff < 0.01:  # Tolerância para diferenças de arredondamento
                                            continue
                                    except (ValueError, TypeError):
                                        pass
                                elif valor_novo is None and valor_antigo is None:
                                    continue
                                else:
                                    mudou = True
                                    break
                            
                            # Tratamento especial para IDs (BigInteger)
                            elif campo in ["bling_contas_pagar_id", "bling_cliente_id", "forma_pagamento_id", "bling_categoria_id"]:
                                # Converter para int para comparação segura
                                try:
                                    novo_int = int(valor_novo) if valor_novo is not None else None
                                    antigo_int = int(valor_antigo) if valor_antigo is not None else None
                                    if novo_int == antigo_int:
                                        continue
                                    else:
                                        mudou = True
                                        break
                                except (ValueError, TypeError):
                                    # Se não conseguir converter, comparar como está
                                    if valor_novo == valor_antigo:
                                        continue
                                    elif valor_novo is None and valor_antigo is None:
                                        continue
                                    else:
                                        mudou = True
                                        break
                            
                            # Tratamento para strings (situacao)
                            elif campo == "situacao":
                                novo_str = str(valor_novo).strip() if valor_novo is not None else ""
                                antigo_str = str(valor_antigo).strip() if valor_antigo is not None else ""
                                if novo_str == antigo_str:
                                    continue
                                else:
                                    mudou = True
                                    break
                            
                            # Comparação genérica
                            else:
                                if valor_novo is None and valor_antigo is None:
                                    continue
                                if (valor_novo is None) != (valor_antigo is None):
                                    mudou = True
                                    break
                                # Comparar convertendo para string para evitar problemas de tipo
                                if str(valor_novo) != str(valor_antigo):
                                    mudou = True
                                    break

                        if mudou:
                            session.execute(
                                text("""
                                    UPDATE processed.fato_contas_pagar
                                    SET bling_contas_pagar_id = :bling_contas_pagar_id,
                                        valor = :valor,
                                        situacao = :situacao,
                                        data_vencimento = :data_vencimento,
                                        bling_cliente_id = :bling_cliente_id,
                                        forma_pagamento_id = :forma_pagamento_id,
                                        bling_categoria_id = :bling_categoria_id,
                                        data_ingestao = :data_ingestao,
                                        data_processamento = :data_processamento
                                    WHERE contas_pagar_id = :contas_pagar_id
                                    AND empresa_id = :empresa_id
                                """),
                                registro,
                            )

                    if idx % batch_size == 0:
                        session.commit()
                        print(f"      💾 Batch commit realizado ({idx} registros processados)")

                except Exception as e:
                    print(f"\n      ⚠️  Erro no registro {idx}: {str(e)[:100]}")
                    print(f"      ⏭️  Pulando e continuando...")
                    session.rollback()
                    continue

            print(f"\n   💾 Salvando alterações finais no banco de dados...")
            session.commit()
            print(f"   ✅ Commit final realizado com sucesso!")

            print(f"\n{'='*70}")
            print(f"✅ EXPORTAÇÃO CONCLUÍDA COM SUCESSO!")
            print(f"{'='*70}\n")

            return len(df)

        except Exception as e:
            session.rollback()
            print(f"\n❌ ERRO AO EXPORTAR: {e}")
            raise
        finally:
            session.close()
    
    def atualizar_status_raw(self, df):
        """Atualiza status em raw.contas_pagar_raw"""
        print("\n7️⃣ ATUALIZANDO STATUS...")

        session = Session()

        try:
            ids = df["contas_pagar_id"].tolist()

            query = text("""
                UPDATE raw.contas_pagar_raw
                SET status_processamento = 'processado'
                WHERE id = ANY(:ids)
                AND empresa_id = :empresa_id
            """)

            resultado = session.execute(query, {"ids": ids, "empresa_id": self.empresa_id})
            session.commit()

            print(f"✅ {resultado.rowcount} registros atualizados")

        except Exception as e:
            session.rollback()
            print(f"⚠️  Erro: {e}")
            raise
        finally:
            session.close()

    # =====================================================
    # Executar_transformacao_completa()
    # ORDEM CORRETA DE EXECUÇÃO
    # =====================================================
    
    def executar_transformacao_completa(self):
        """
        Pipeline completo COM ORDEM CORRETA
        
        ORDEM CORRIGIDA:
        1. Sincronizar deleções (limpar o que foi deletado no Bling)
        2. Preparar registros (marcar pendentes por regra de negócio)
        3. Extrair e transformar os pendentes
        4. Exportar para processed
        5. Atualizar status
        """
        try:
            print(f"\n{'='*70}")
            print(f"🚀 INICIANDO TRANSFORMAÇÃO COMPLETA")
            print(f"   Empresa ID: {self.empresa_id}")
            print(f"{'='*70}")
            
            # ETAPA 1: Sincronizar deleções (PRIMEIRO!)
            # Remove da processed o que não existe mais no Bling
            self.sincronizar_delecoes()
            
            # ETAPA 2: Preparar registros para reavaliação
            # Marca como pendente baseado em regras de negócio
            self.preparar_registros_para_processamento()
            
            # ETAPA 3: Extrair dados pendentes
            df_raw = self.extrair_dados_raw()

            if len(df_raw) == 0:
                print(f"\n✅ Nenhum registro pendente para empresa_id = {self.empresa_id}")
                print(f"\n{'='*70}")
                print(f"🎉 TRANSFORMAÇÃO CONCLUÍDA (NADA A FAZER)")
                print(f"{'='*70}")
                return

            # ETAPA 4-7: Pipeline de transformação
            df = self.expandir_json(df_raw)
            df = self.aplicar_transformacoes(df)
            df = self.preparar_para_exportacao(df)
            df = self.validar_dados(df)
            df = self.validar_foreign_keys(df)
            
            # Verificar se ainda há registros
            if len(df) == 0:
                print("\n⚠️  Nenhum registro válido após validação!")
                print(f"\n{'='*70}")
                print(f"⚠️  TRANSFORMAÇÃO CONCLUÍDA (SEM DADOS VÁLIDOS)")
                print(f"{'='*70}")
                return
            
            # ETAPA 8: Exportar
            self.exportar_para_processed(df)
            
            # ETAPA 9: Atualizar status
            self.atualizar_status_raw(df)

            print(f"\n{'='*70}")
            print(f"🎉 TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO!")
            print(f"   • Empresa ID: {self.empresa_id}")
            print(f"   • Registros processados: {len(df)}")
            print(f"{'='*70}")

        except Exception as e:
            print(f"\n{'='*70}")
            print(f"❌ ERRO NA TRANSFORMAÇÃO!")
            print(f"{'='*70}")
            print(f"Erro: {e}")
            print(f"{'='*70}")
            raise