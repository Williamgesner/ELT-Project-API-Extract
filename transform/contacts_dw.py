# =====================================================
# TRANSFORMADOR DE CONTATOS - MULTI-CNPJ
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


class ContatosTransformer:
    """
    Transformador de contatos com COMPARAÇÃO INTELIGENTE
    """

    def __init__(self, empresa_id):
        self.empresa_id = empresa_id
        self.engine = engine

    # =====================================================
    # 2. EXTRAIR DADOS RAW
    # =====================================================

    def extrair_dados_raw(self):
        """Extrai dados da tabela raw.contatos_raw"""
        print(f"\n1️⃣ EXTRAINDO DADOS DE RAW.CONTATOS_RAW (empresa_id={self.empresa_id})...")

        query = text("""
            SELECT 
                id,
                bling_id,
                dados_json,
                data_ingestao
            FROM raw.contatos_raw
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
        print("\n2️⃣ EXPANDINDO JSON EM COLUNAS...")

        # Normalizar o JSON principal
        df_json = pd.json_normalize(df_raw["dados_json"])

        # Renomear 'id' do JSON para 'id_bling'
        if "id" in df_json.columns:
            df_json = df_json.rename(columns={"id": "id_bling"})

        # Combinar com as colunas originais
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
    # 4. APLICAR LIMPEZAS E TRANSFORMAÇÕES
    # =====================================================

    def aplicar_limpezas(self, df):
        """Aplica TODAS as limpezas e transformações"""
        print("\n3️⃣ APLICANDO LIMPEZAS E TRANSFORMAÇÕES...")

        # === REMOVER COLUNAS DESNECESSÁRIAS ===
        print("   • Removendo colunas desnecessárias...")
        colunas_remover = [
            "ie",
            "rg",
            "email",
            "codigo",
            "celular",
            "id_bling",
            "fantasia",
            "situacao",
            "pais.nome",
            "indicadorIe",
            "vendedor.id",
            "orgaoEmissor",
            "tiposContato",
            "pessoasContato",
            "emailNotaFiscal",
            "inscricaoMunicipal",
            "dadosAdicionais.sexo",
            "endereco.cobranca.uf",
            "endereco.cobranca.cep",
            "endereco.geral.bairro",
            "endereco.geral.numero",
            "endereco.geral.endereco",
            "financeiro.categoria.id",
            "financeiro.limiteCredito",
            "endereco.cobranca.bairro",
            "endereco.cobranca.numero",
            "endereco.cobranca.endereco",
            "endereco.geral.complemento",
            "endereco.cobranca.municipio",
            "financeiro.condicaoPagamento",
            "dadosAdicionais.naturalidade",
            "endereco.cobranca.complemento",
            "dadosAdicionais.dataNascimento",
            "endereco_estruturado.tem_endereco",
            "endereco_estruturado.data_processamento",
            "endereco_estruturado.endereco_detalhado.cep",
            "endereco_estruturado.endereco_detalhado.pais",
            "endereco_estruturado.endereco_detalhado.cidade",
            "endereco_estruturado.endereco_detalhado.estado",
            "endereco_estruturado.endereco_detalhado.numero",
            "endereco_estruturado.endereco_detalhado.bairro",
            "endereco_estruturado.endereco_completo_formatado",
            "endereco_estruturado.endereco_detalhado.logradouro",
            "endereco_estruturado.endereco_detalhado.complemento",
        ]
        df = df.drop(columns=[col for col in colunas_remover if col in df.columns])

        # === RENOMEAR COLUNAS ===
        print("   • Renomeando colunas...")
        df = df.rename(
            columns={
                "id": "cliente_id",
                "tipo": "tipo_pessoa",
                "numeroDocumento": "cpf_cnpj",
                "endereco.geral.municipio": "cidade",
                "endereco.geral.uf": "estado",
                "endereco.geral.cep": "cep",
                "bling_id": "bling_cliente_id",
            }
        )

        # === ADICIONAR EMPRESA_ID ===
        print(f"   • Adicionando empresa_id={self.empresa_id}...")
        df["empresa_id"] = self.empresa_id

        # === LIMPAR STRINGS VAZIAS ===
        print("   • Convertendo strings vazias para NaN...")
        for coluna in df.select_dtypes(include=["object"]).columns:
            df[coluna] = df[coluna].replace(r"^\s*$", np.nan, regex=True)
            df[coluna] = df[coluna].replace("", np.nan)
            df[coluna] = df[coluna].replace(" ", np.nan)

        # === LIMPAR E PADRONIZAR NOMES ===
        print("   • Limpando e padronizando nomes...")
        df["nome"] = df["nome"].apply(self._limpar_nome)
        df["cpf_cnpj"] = df["cpf_cnpj"].apply(self._padronizar_cpf_cnpj)

        # === DETERMINAR TIPO DE PESSOA ===
        print("   • Determinando tipo de pessoa...")
        df["tipo_pessoa"] = df.apply(self._determinar_tipo_pessoa, axis=1)

        # === PADRONIZAR CEP ===
        print("   • Padronizando CEP...")
        df["cep"] = df["cep"].apply(self._padronizar_cep)

        # === PADRONIZAR TELEFONE ===
        print("   • Padronizando telefone...")
        df["telefone"] = df["telefone"].apply(self._padronizar_telefone)

        # === PADRONIZAR ESTADOS === 
        print("   • Padronizando estado...")

        # Lista de estados válidos do Brasil
        estados_validos = [
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 
            'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 
            'RS', 'RO', 'RR', 'SC', 'SP', 'TO'
        ]

        # Converter para maiúsculo primeiro
        df["estado"] = df["estado"].apply(lambda x: str(x).upper() if pd.notna(x) else np.nan)

        # Identificar estados inválidos antes de limpar
        estados_invalidos = df[df["estado"].notna() & ~df["estado"].isin(estados_validos)]["estado"].unique()
        qtd_invalidos = len(df[df["estado"].notna() & ~df["estado"].isin(estados_validos)])

        # Se houver estados inválidos, mostrar e limpar
        if len(estados_invalidos) > 0:
            print(f"      ⚠️  {qtd_invalidos} registros foram encontrados como não estado: {', '.join(estados_invalidos)}")
            # Transformar estados inválidos em NaN
            df.loc[~df["estado"].isin(estados_validos), "estado"] = np.nan

        # === ADICIONAR METADADOS ===
        print("   • Adicionando metadados de processamento...")
        df["data_processamento"] = datetime.now()

        print("✅ Todas as limpezas aplicadas com sucesso!")
        return df

    # =====================================================
    # 5. FUNÇÕES AUXILIARES DE LIMPEZA
    # =====================================================

    def _limpar_nome(self, nome):
        """Limpa e padroniza nomes"""
        if pd.isna(nome):
            return np.nan

        nome = str(nome).strip()
        # nome = re.sub(r"[^a-zA-Z0-9\s\-]", " ", nome) # Remove caracteres especiais
        nome = " ".join(nome.split())
        nome = nome.title()

        sufixos = ["Epp", "Ltda", "Eireli", "Ltd", "Limitada"]
        for sufixo in sufixos:
            sufixo_upper = sufixo.upper()
            pattern = rf"\s*-?\s*({re.escape(sufixo)})(?:\s+.*)?$"
            nome = re.sub(pattern, f" - {sufixo_upper}", nome, flags=re.IGNORECASE)

        nome = nome.strip()
        return nome if nome else np.nan

    def _padronizar_cpf_cnpj(self, documento):
        """Padroniza CPF/CNPJ com zeros à esquerda"""
        if pd.isna(documento):
            return np.nan

        doc = re.sub(r"\D", "", str(documento).strip())

        if not doc:
            return np.nan

        if len(doc) <= 11:
            return doc.zfill(11)
        elif len(doc) <= 14:
            return doc.zfill(14)
        else:
            return np.nan

    def _determinar_tipo_pessoa(self, row):
        """Determina tipo de pessoa baseado no CPF/CNPJ"""
        if pd.notna(row["tipo_pessoa"]) and row["tipo_pessoa"] in ["F", "J"]:
            return row["tipo_pessoa"]

        if pd.isna(row["cpf_cnpj"]):
            return np.nan

        tamanho = len(str(row["cpf_cnpj"]))

        if tamanho == 11:
            return "F"
        elif tamanho == 14:
            return "J"
        else:
            return np.nan

    def _padronizar_cep(self, cep):
        """Padroniza CEP no formato xx.xxx-xx"""
        if pd.isna(cep):
            return np.nan

        cep_numeros = re.sub(r"\D", "", str(cep).strip())

        if len(cep_numeros) != 8:
            return np.nan

        return f"{cep_numeros[:2]}.{cep_numeros[2:5]}-{cep_numeros[5:]}"

    def _padronizar_telefone(self, telefone):
        """Formata telefone: (XX) XXXXX-XXXX ou (XX) XXXX-XXXX"""
        if pd.isna(telefone):
            return np.nan

        tel_num = re.sub(r"\D", "", str(telefone).strip())

        if not tel_num:
            return np.nan

        tamanho = len(tel_num)

        if tamanho == 11:
            return f"({tel_num[:2]}) {tel_num[2:7]}-{tel_num[7:]}"
        elif tamanho == 10:
            return f"({tel_num[:2]}) {tel_num[2:6]}-{tel_num[6:]}"
        else:
            return np.nan

    # =====================================================
    # 6. PREPARAR PARA EXPORTAÇÃO
    # =====================================================

    def preparar_para_exportacao(self, df):
        """Ordena colunas e prepara DataFrame final"""
        print("\n4️⃣ PREPARANDO DADOS PARA EXPORTAÇÃO...")

        colunas_finais = [
            "cliente_id",
            "bling_cliente_id",
            "empresa_id",
            "nome",
            "cpf_cnpj",
            "tipo_pessoa",
            "telefone",
            "cidade",
            "estado",
            "cep",
            "data_ingestao",
            "data_processamento",
        ]

        df = df[[col for col in colunas_finais if col in df.columns]]

        print(f"✅ Dados preparados! {len(df)} registros x {len(df.columns)} colunas")
        return df

    # =====================================================
    # 7. VALIDAR DADOS
    # =====================================================

    def validar_dados(self, df):
        """Executa validações de qualidade"""
        print("\n5️⃣ EXECUTANDO VALIDAÇÕES DE QUALIDADE...")

        total = len(df)
        com_nome = df["nome"].notna().sum()
        com_cpf = df["cpf_cnpj"].notna().sum()
        com_cidade = df["cidade"].notna().sum()
        com_telefone = df["telefone"].notna().sum()

        print(f"\n   📊 ESTATÍSTICAS DE QUALIDADE:")
        print(f"      • Total: {total}")
        print(f"      • Com nome: {com_nome} ({com_nome/total*100:.1f}%)")
        print(f"      • Com CPF/CNPJ: {com_cpf} ({com_cpf/total*100:.1f}%)")
        print(f"      • Com cidade: {com_cidade} ({com_cidade/total*100:.1f}%)")
        print(f"      • Com telefone: {com_telefone} ({com_telefone/total*100:.1f}%)")

        # Verificar duplicatas
        duplicatas = df.duplicated(subset=["bling_cliente_id", "empresa_id"]).sum()
        if duplicatas > 0:
            print(f"\n   ⚠️  {duplicatas} registros duplicados encontrados!")
            print("      Removendo duplicatas...")
            df = df.drop_duplicates(subset=["bling_cliente_id", "empresa_id"], keep="first")
        else:
            print(f"\n   ✅ Nenhuma duplicata encontrada")

        return df

    # =====================================================
    # 8. EXPORTAR COM COMPARAÇÃO INTELIGENTE ✅ CORRIGIDO
    # =====================================================

    def exportar_para_processed(self, df):
        """
        ✅ CORREÇÃO: Implementa comparação inteligente (igual sales_dw.py)

        ESTRATÉGIA:
        1. Buscar contatos existentes
        2. Comparar campos relevantes
        3. INSERT apenas novos
        4. UPDATE apenas diferentes
        5. SKIP idênticos
        """
        print(f"\n6️⃣ EXPORTANDO PARA PROCESSED.DIM_CONTATOS (empresa_id={self.empresa_id})...")

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
                    cliente_id,
                    bling_cliente_id,
                    empresa_id,
                    nome,
                    cpf_cnpj,
                    telefone,
                    cidade,
                    cep
                FROM processed.dim_contatos
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
                chave = (row['bling_cliente_id'], row['empresa_id'])
                existentes_dict[chave] = row.to_dict()

            registros_novos = []
            registros_atualizar = []
            registros_identicos = 0

            for idx, row in df.iterrows():
                chave = (row['bling_cliente_id'], row['empresa_id'])

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

                    nome_mudou = comparar_campo(row.get("nome"), existente.get("nome"))
                    cpf_mudou = comparar_campo(
                        row.get("cpf_cnpj"), existente.get("cpf_cnpj")
                    )
                    telefone_mudou = comparar_campo(
                        row.get("telefone"), existente.get("telefone")
                    )
                    cidade_mudou = comparar_campo(
                        row.get("cidade"), existente.get("cidade")
                    )
                    cep_mudou = comparar_campo(row.get("cep"), existente.get("cep"))

                    if (
                        nome_mudou
                        or cpf_mudou
                        or telefone_mudou
                        or cidade_mudou
                        or cep_mudou
                    ):
                        # DIFERENTE → UPDATE
                        row["cliente_id"] = existente["cliente_id"]
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

                # Remover cliente_id se existir (será gerado automaticamente)
                if "cliente_id" in df_novos.columns:
                    df_novos = df_novos.drop(columns=["cliente_id"])

                df_novos.to_sql(
                    name="dim_contatos",
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
                        UPDATE processed.dim_contatos
                        SET 
                            bling_cliente_id = :bling_cliente_id,
                            empresa_id = :empresa_id,
                            nome = :nome,
                            cpf_cnpj = :cpf_cnpj,
                            tipo_pessoa = :tipo_pessoa,
                            telefone = :telefone,
                            cidade = :cidade,
                            estado = :estado,
                            cep = :cep,
                            data_processamento = :data_processamento
                        WHERE cliente_id = :cliente_id
                    """)

                    session.execute(
                        stmt,
                        {
                            "cliente_id": int(row["cliente_id"]),
                            "bling_cliente_id": int(row["bling_cliente_id"]),
                            "empresa_id": int(row["empresa_id"]),
                            "nome": str(row["nome"]) if pd.notna(row["nome"]) else None,
                            "cpf_cnpj": (
                                str(row["cpf_cnpj"])
                                if pd.notna(row["cpf_cnpj"])
                                else None
                            ),
                            "tipo_pessoa": (
                                str(row["tipo_pessoa"])
                                if pd.notna(row["tipo_pessoa"])
                                else None
                            ),
                            "telefone": (
                                str(row["telefone"])
                                if pd.notna(row["telefone"])
                                else None
                            ),
                            "cidade": (
                                str(row["cidade"]) if pd.notna(row["cidade"]) else None
                            ),
                            "estado": (
                                str(row["estado"]) if pd.notna(row["estado"]) else None
                            ),
                            "cep": str(row["cep"]) if pd.notna(row["cep"]) else None,
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
            query = text("SELECT COUNT(*) FROM processed.dim_contatos WHERE empresa_id = :empresa_id")
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
    # 9. ATUALIZAR STATUS RAW
    # =====================================================

    def atualizar_status_raw(self, df):
        """Atualiza status dos registros processados em raw.contatos_raw"""
        print("\n7️⃣ ATUALIZANDO STATUS NA TABELA RAW...")

        session = Session()

        try:
            ids_processados = df["cliente_id"].tolist()

            query = text("""
                UPDATE raw.contatos_raw
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
    # 10. EXECUTAR TRANSFORMAÇÃO COMPLETA
    # =====================================================

    def executar_transformacao_completa(self):
        """Executa o pipeline completo de transformação"""
        try:
            # 1. Extrair dados raw
            df_raw = self.extrair_dados_raw()

            if len(df_raw) == 0:
                print("\n✅ Nenhum registro pendente para processar!")
                print("   Todos os contatos já foram transformados.")
                return

            # 2. Expandir JSON
            df = self.expandir_json(df_raw)

            # 3. Aplicar limpezas
            df = self.aplicar_limpezas(df)

            # 4. Preparar para exportação
            df = self.preparar_para_exportacao(df)

            # 5. Validar
            df = self.validar_dados(df)

            # 6. Exportar (COM COMPARAÇÃO INTELIGENTE ✅)
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
            print(f"      • Colunas: {len(df.columns)}")

        except Exception as e:
            print(f"\n❌ ERRO na transformação: {e}")
            raise