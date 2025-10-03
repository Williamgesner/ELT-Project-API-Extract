# =====================================================
# TRANSFORMADOR DE CONTATOS
# =====================================================
# Responsável por: Limpar e transformar dados de contatos_raw
# para dim_contatos no schema processed

import pandas as pd
import numpy as np
import re
from datetime import datetime
from sqlalchemy import text
from config.database import Session, engine

# =====================================================
# 1. CONECTANDO AO BANCO E IMPORTAR DADOS
# =====================================================


class ContatosTransformer:
    """
    Transformador específico para contatos
    Aplica todas as limpezas e padronizações necessárias
    """

    def __init__(self):
        self.engine = engine

    # Buscar dados da tabela contatos_raw
    def extrair_dados_raw(self):
        """
        Extrai dados da tabela raw.contatos_raw
        """
        print("\n1️⃣ EXTRAINDO DADOS DE RAW.CONTATOS_RAW...")

        query = """
            SELECT 
                id,
                bling_id,
                dados_json,
                data_ingestao
            FROM raw.contatos_raw
            WHERE status_processamento = 'pendente'
            ORDER BY bling_id
        """

        df_raw = pd.read_sql(query, self.engine)
        print(f"✅ {len(df_raw)} registros extraídos (status = 'pendente')")

        return df_raw

# =====================================================
# 2. EXPANDIR JSON EM COLUNAS
# =====================================================

    def expandir_json(self, df_raw):
        """
        Expande o JSON em colunas
        """
        print("\n2️⃣ EXPANDINDO JSON EM COLUNAS...")

        # Normalizar o JSON principal
        df_json = pd.json_normalize(df_raw["dados_json"])

        # Renomear 'id' do JSON para 'id_bling' para não dar divergência
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
# 3. LIMPEZA DE DADOS
# =====================================================

    def aplicar_limpezas(self, df):
        """
        Aplica TODAS as limpezas e transformações
        SEGUINDO OS ARQUIVOS DO ANALYSIS/ QUE FIZ PARA TESTES E VALIDAÇÕES
        """
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
            }
        )

        # === CONVERTENDO E PADRONIZANDO STRINGS VAZIAS, ESPAÇOS, NONE PARA NaN ===
        print("   • Convertendo strings vazias para NaN...")
        for coluna in df.select_dtypes(include=["object"]).columns:
            df[coluna] = df[coluna].replace(r"^\s*$", np.nan, regex=True)
            df[coluna] = df[coluna].replace("", np.nan)
            df[coluna] = df[coluna].replace(" ", np.nan)

        # === LIMPAR E PADRONIZAR NOMES ===
        print("   • Limpando e padronizando nomes...")
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

        # === ADICIONAR METADADOS ===
        print("   • Adicionando metadados de processamento...")
        df["data_processamento"] = datetime.now()

        print("✅ Todas as limpezas aplicadas com sucesso!")
        return df

    def _limpar_nome(self, nome):
        """Limpa e padroniza nomes"""
        if pd.isna(nome):
            return np.nan
        
        nome = str(nome).strip()
        nome = re.sub(r'[^a-zA-Z0-9\s\-]', ' ', nome) # Remover caracteres especiais (manter letras, números, espaços e hífen)
        nome = ' '.join(nome.split()) # Remover espaços múltiplos
        nome = nome.title() # Capitalizar (Title Case)
        
        # Lista de sufixos empresariais
        sufixos = ['Me', 'Epp', 'Ltda', 'Eireli', 'Sa', 'S A', 'Cia', 'Ltd', 'Limitada', "Ss"]
        # Padronizar cada sufixo
        for sufixo in sufixos:
            sufixo_upper = sufixo.upper()
            pattern = rf'\s*-?\s*({re.escape(sufixo)})(?:\s+.*)?$'
            nome = re.sub(pattern, f' - {sufixo_upper}', nome, flags=re.IGNORECASE)

        # Remover espaços extras no final
        nome = nome.strip()
        return nome if nome else np.nan

    def _padronizar_cpf_cnpj(self, documento):
        """Padroniza CPF/CNPJ com zeros à esquerda"""
        if pd.isna(documento):
            return np.nan

        doc = re.sub(r"\D", "", str(documento).strip())  # Remover caracteres não numéricos

        if not doc:
            return np.nan

        # Preencher com zeros
        if len(doc) <= 11:
            return doc.zfill(11)
        elif len(doc) <= 14:
            return doc.zfill(14)
        else:
            return np.nan

    def _determinar_tipo_pessoa(self, row):
        """
        Determina tipo de pessoa baseado no CPF/CNPJ:
        - 11 dígitos (CPF) → "F" (Pessoa Física)
        - 14 dígitos (CNPJ) → "J" (Pessoa Jurídica)
        - Mantém o valor existente se já estiver preenchido
        """

         # Se tipo_pessoa já está preenchido, manter
        if pd.notna(row["tipo_pessoa"]) and row["tipo_pessoa"] in ["F", "J"]:
            return row["tipo_pessoa"]

        # Se não tem CPF/CNPJ, deixar vazio
        if pd.isna(row["cpf_cnpj"]):
            return np.nan

        # Determinar baseado no tamanho do documento
        tamanho = len(str(row["cpf_cnpj"]))

        if tamanho == 11:
            return "F" # CPF = Pessoa Física
        elif tamanho == 14:
            return "J" # CNPJ = Pessoa Jurídica
        else:
            return np.nan # Documento inválido

    def _padronizar_cep(self, cep):
        """Padroniza CEP no formato xx.xxx-xx"""
        if pd.isna(cep):
            return np.nan

        # Remover todos os caracteres não numéricos
        cep_numeros = re.sub(r"\D", "", str(cep).strip())

        # Se não tiver exatamente 8 dígitos, retornar vazio
        if len(cep_numeros) != 8:
            return np.nan

        return f"{cep_numeros[:2]}.{cep_numeros[2:5]}-{cep_numeros[5:]}"

    def _padronizar_telefone(self, telefone):
        """
        Formata telefone como:
        - Celular: (XX) XXXXX-XXXX (11 dígitos)
        - Fixo: (XX) XXXX-XXXX (10 dígitos)
        """
        if pd.isna(telefone):
            return np.nan

        # Remover caracteres não numéricos
        tel_num = re.sub(r"\D", "", str(telefone).strip())

        if not tel_num:
            return np.nan

        tamanho = len(tel_num)

        if tamanho == 11:
            # Celular
            return f"({tel_num[:2]}) {tel_num[2:7]}-{tel_num[7:]}"
        elif tamanho == 10:
            # Fixo
            return f"({tel_num[:2]}) {tel_num[2:6]}-{tel_num[6:]}"
        else:
            # Inválido
            return np.nan

# =====================================================
# 4. PREPARANDO DADOS PARA EXPORTAÇÃO
# =====================================================

    def preparar_para_exportacao(self, df):
        """
        Ordena colunas e prepara DataFrame final
        """
        print("\n4️⃣ PREPARANDO DADOS PARA EXPORTAÇÃO...")

        colunas_finais = [
            # IDs e Metadados
            "cliente_id",
            "bling_id",

            # Dados do Cliente
            "nome",
            "cpf_cnpj",
            "tipo_pessoa",
            "telefone",

            # Endereço
            "cidade",
            "estado",
            "cep",

            # Metadados do Sistem
            "data_ingestao",
            "data_processamento"
        ]
        # Só pega as colunas que realmente existem no DataFrame. Se alguma não existir, simplesmente ignora.
        df = df[[col for col in colunas_finais if col in df.columns]] 

        print(f"✅ Dados preparados! {len(df)} registros x {len(df.columns)} colunas")
        return df

# =====================================================
# 5. EXECUTANDO VALIDAÇÃO DE DADOS
# =====================================================

    def validar_dados(self, df):
        """
        Executa validações de qualidade
        """
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
        duplicatas = df.duplicated(subset=["bling_id"]).sum()
        if duplicatas > 0:
            print(f"\n   ⚠️  {duplicatas} registros duplicados encontrados!")
            print("      Removendo duplicatas...")
            df = df.drop_duplicates(subset=["bling_id"], keep="first")
        else:
            print(f"\n   ✅ Nenhuma duplicata encontrada")

        return df

# =====================================================
# 5. EXPORTANDO OS DADOS PARA O BANCO DE DADOS 
# =====================================================

    def exportar_para_processed(self, df):
        """
        Exporta dados para processed.dim_contatos
        """
        print("\n6️⃣ EXPORTANDO PARA PROCESSED.DIM_CONTATOS...")

        try:
            # IMPORTANTE: Use 'replace' na PRIMEIRA execução
            # Depois mude para 'append' para adicionar novos registros
            df.to_sql(
                name="dim_contatos",
                con=self.engine,
                schema="processed",
                if_exists="append",  # ⚠️ MUDAR PARA 'append' após primeira execução
                index=False,
                method="multi",
                chunksize=1000,
            )

            print(f"✅ {len(df)} registros exportados com sucesso!")

            # Verificar
            query = text("SELECT COUNT(*) FROM processed.dim_contatos")
            with engine.connect() as conn:
                total = conn.execute(query).scalar()
                print(f"✅ Verificação: {total} registros na tabela")

            return len(df)

        except Exception as e:
            print(f"❌ ERRO ao exportar: {e}")
            raise

# =====================================================
# 6. ATUALIZANDO STATUS DO RAW
# =====================================================

    def atualizar_status_raw(self, df):
        """
        Atualiza status dos registros processados em raw.contatos_raw
        """
        print("\n7️⃣ ATUALIZANDO STATUS NA TABELA RAW...")

        session = Session()

        try:
            ids_processados = df["cliente_id"].tolist()

            query = text(
                """
                UPDATE raw.contatos_raw
                SET status_processamento = 'processado'
                WHERE id = ANY(:ids)
            """
            )

            resultado = session.execute(query, {"ids": ids_processados})
            session.commit()

            print(f"✅ {resultado.rowcount} registros marcados como 'processado'")

        except Exception as e:
            session.rollback()
            print(f"⚠️  Erro ao atualizar status: {e}")
        finally:
            session.close()

# =====================================================
# 7. EXECUTANDO TODOS OS SCRIPTS 
# =====================================================

    def executar_transformacao_completa(self):
        """
        Executa o pipeline completo de transformação
        """
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

            # 6. Exportar
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
