# =====================================================
# TRANSFORMADOR DE NFE - MULTI-CNPJ
# =====================================================
# ✅ CORREÇÃO: Usa chave composta (bling_nfe_id, empresa_id)
# Comparação robusta que evita falsos positivos
# Inserção em lotes para evitar estouro de parâmetros PostgreSQL

import pandas as pd
import numpy as np
from datetime import datetime, date
from decimal import Decimal
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from config.database import Session, engine

# ✅ Configuração global do Pandas
pd.set_option('future.no_silent_downcasting', True)

# =====================================================
# 0. FUNÇÃO DE COMPARAÇÃO ROBUSTA
# =====================================================

def valores_sao_iguais(val1, val2):
    """
    Compara dois valores de forma robusta, tratando casos especiais
    
    Casos tratados:
    - None vs NaN vs pd.NA
    - Timestamps com/sem timezone
    - Floats com pequenas diferenças
    - Strings vazias vs None
    - Dates em formatos diferentes
    
    Returns:
        bool: True se valores são considerados iguais
    """
    # Caso 1: Ambos são nulos (None, NaN, pd.NA, vazio)
    val1_is_null = val1 is None or pd.isna(val1) or (isinstance(val1, str) and val1.strip() == '')
    val2_is_null = val2 is None or pd.isna(val2) or (isinstance(val2, str) and val2.strip() == '')
    
    if val1_is_null and val2_is_null:
        return True
    
    if val1_is_null or val2_is_null:
        return False  # Um é nulo e outro não
    
    # Caso 2: Comparação de timestamps/datas
    if isinstance(val1, (datetime, pd.Timestamp, date)) or isinstance(val2, (datetime, pd.Timestamp, date)):
        try:
            # Normalizar ambos para datetime
            if isinstance(val1, date) and not isinstance(val1, datetime):
                val1 = datetime.combine(val1, datetime.min.time())
            if isinstance(val2, date) and not isinstance(val2, datetime):
                val2 = datetime.combine(val2, datetime.min.time())
            
            # Converter para pandas Timestamp (normaliza timezone)
            ts1 = pd.Timestamp(val1)
            ts2 = pd.Timestamp(val2)
            
            # 🔧 CORREÇÃO: Usar floor() em vez de round()
            # floor() trunca os microsegundos em vez de arredondar
            # Comparar até segundos (ignorar microsegundos que podem variar)
            return ts1.floor('s') == ts2.floor('s')
        except:
            return False
    
    # Caso 3: Comparação de números (float/int/Decimal)
    if isinstance(val1, (int, float, Decimal, np.integer, np.floating)) or \
       isinstance(val2, (int, float, Decimal, np.integer, np.floating)):
        try:
            # Converter para float
            num1 = float(val1)
            num2 = float(val2)
            
            # Comparar com tolerância para erros de ponto flutuante
            # Tolerância: 0.01 (1 centavo para valores monetários)
            return abs(num1 - num2) < 0.01
        except:
            return False
    
    # Caso 4: Comparação de strings
    if isinstance(val1, str) and isinstance(val2, str):
        # Normalizar espaços
        return val1.strip() == val2.strip()
    
    # Caso 5: Comparação padrão
    try:
        return val1 == val2
    except:
        # Se der erro na comparação, considera diferentes
        return False

# =====================================================
# 1. FUNÇÃO AUXILIAR DE MAPEAMENTO
# =====================================================

def obter_mapeamento_situacao_nfe():
    """
    Retorna o dicionário de mapeamento de situacao da nfe
    
    Mapeamento conforme regra de negócio:
    1 → Pendente
    2 → Cancelada
    3 → Aguardando recibo
    4 → Rejeitada
    5 → Autorizada
    6 → Emitida DANFE
    7 → Registrada
    8 → Aguardando protocolo
    9 → Denegada
    10 → Consulta situação
    11 → Bloqueada
    """
    return {
        1: "Pendente",
        2: "Cancelada",
        3: "Aguardando recibo",
        4: "Rejeitada",
        5: "Autorizada",
        6: "Emitida DANFE",
        7: "Registrada",
        8: "Aguardando protocolo",
        9: "Denegada",
        10: "Consulta situação",
        11: "Bloqueada",
    }

def obter_mapeamento_tipo_nfe():
    """
    Retorna o dicionário de mapeamento do tipo da nfe
    
    Mapeamento conforme regra de negócio:
    0 → Entrada
    1 → Saida
    """
    return {
        0: "Entrada",
        1: "Saida",
    }

# =====================================================
# 2. CLASSE TRANSFORMADORA
# =====================================================

class NFeTransformer:
    """
    Transformador específico para NFe
    Aplica todas as limpezas e padronizações necessárias
    Inserção em lotes
    """

    def __init__(self, empresa_id):
        self.empresa_id = empresa_id
        self.engine = engine

    # =====================================================
    # 3. EXTRAIR DADOS RAW
    # =====================================================

    def extrair_dados_raw(self):
        """
        Extrai dados da tabela raw.nfe_raw
        """
        print("\n1️⃣ EXTRAINDO DADOS DE RAW.NFE_RAW...")

        query_nfe = text("""
            SELECT 
                id,
                bling_id,
                empresa_id,
                dados_json,
                data_ingestao
            FROM raw.nfe_raw
            WHERE empresa_id = :empresa_id
            ORDER BY bling_id
        """)

        df_nfe_raw = pd.read_sql(query_nfe, self.engine, params={"empresa_id": self.empresa_id})
        print(f"✅ {len(df_nfe_raw)} NFe extraídas (empresa_id = {self.empresa_id})")

        # Buscar dados de vendas para relacionamento
        print("   • Importando dados de vendas para relacionamento...")
        query_vendas = text("""
            SELECT 
                bling_id,
                dados_json->>'numero' as numero_pedido,
                dados_json->'notaFiscal'->>'id' as nf_id
            FROM raw.vendas_raw
            WHERE empresa_id = :empresa_id
            AND dados_json->'notaFiscal'->>'id' IS NOT NULL
            AND dados_json->'notaFiscal'->>'id' != '0'
        """)

        df_vendas = pd.read_sql(query_vendas, self.engine, params={"empresa_id": self.empresa_id})
        print(f"   ✅ {len(df_vendas)} vendas com NFe vinculada importadas")

        return df_nfe_raw, df_vendas

    # =====================================================
    # 4. EXPANDIR JSON
    # =====================================================

    def expandir_json(self, df_nfe_raw):
        """
        Expande o JSON em colunas
        """
        print("\n2️⃣ EXPANDINDO JSON EM COLUNAS...")

        # Normalizar o JSON principal
        df_json = pd.json_normalize(df_nfe_raw["dados_json"])

        # Renomear 'id' do JSON para 'id_bling' para não dar divergência
        if "id" in df_json.columns:
            df_json = df_json.rename(columns={"id": "id_bling"})

        # Combinar com as colunas originais
        df = pd.concat(
            [df_nfe_raw[["id", "bling_id", "empresa_id", "data_ingestao"]], df_json],
            axis=1,
        )

        print(f"✅ JSON expandido! {len(df.columns)} colunas disponíveis")
        return df

    # =====================================================
    # 5. APLICAR TRANSFORMAÇÕES
    # =====================================================

    def aplicar_transformacoes(self, df, df_vendas):
        """
        Aplica TODAS as transformações necessárias
        """
        print("\n3️⃣ APLICANDO TRANSFORMAÇÕES...")

        # === REMOVENDO COLUNAS DESNECESSÁRIAS ===
        print("   • Removendo colunas desnecessárias...")
        colunas_remover = [
            "id_bling",
            "chaveAcesso",
            "contato.ie",
            "contato.rg",
            "contato.nome",
            "contato.email",
            "contato.endereco.uf",
            "contato.endereco.cep",
            "contato.endereco.bairro",
            "contato.endereco.numero",
            "contato.endereco.endereco",
            "contato.endereco.municipio",
            "contato.endereco.complemento",
            "contato.telefone",
            "contato.numeroDocumento",
            "serie",
            "xml",
            "linkDanfe",
            "linkPDF",
            "optanteSimplesNacional"
        ]
        
        # Remover apenas as colunas que existem
        colunas_existentes = [col for col in colunas_remover if col in df.columns]
        df = df.drop(columns=colunas_existentes)

        # === RENOMEANDO COLUNAS ===
        print("   • Renomeando colunas...")
        df = df.rename(
            columns={
                "id": "nfe_id",
                "bling_id": "bling_nfe_id",
                "numero": "numero_nfe",
                "dataEmissao": "data_emissao",
                "dataOperacao": "data_entrada",
                "valorNota": "valor_nf",
                "valorFrete": "valor_frete",
                "loja.id": "bling_canal_id",
                "contato.id": "bling_cliente_id",
                "naturezaOperacao.id": "bling_natureza_operacao_id"
            }
        )

        # === CONVERTENDO FORMATOS ===
        print("   • Convertendo formatos de data e valores...")
        
        # Datas
        df["data_emissao"] = pd.to_datetime(df["data_emissao"], errors='coerce').dt.date
        df["data_entrada"] = pd.to_datetime(df["data_entrada"], errors='coerce').dt.date
        
        # Valores numéricos
        df["valor_nf"] = pd.to_numeric(df["valor_nf"], errors='coerce')
        df["valor_frete"] = pd.to_numeric(df["valor_frete"], errors='coerce')

        # === MAPEANDO SITUAÇÃO ===
        print("   • Mapeando situação da NFe...")
        try:
            mapa_situacao = obter_mapeamento_situacao_nfe()
            if "situacao" in df.columns:
                df["situacao"] = pd.to_numeric(df["situacao"], errors="coerce")
                df["situacao"] = df["situacao"].map(mapa_situacao)
                print("      ✅ Situação mapeada com sucesso")
        except Exception as e:
            print(f"      ⚠️  Erro ao mapear situação: {e}")

        # === MAPEANDO TIPO ===
        print("   • Mapeando tipo da NFe...")
        try:
            mapa_tipos = obter_mapeamento_tipo_nfe()
            if "tipo" in df.columns:
                df["tipo"] = pd.to_numeric(df["tipo"], errors="coerce")
                df["tipo"] = df["tipo"].map(mapa_tipos)
                print("      ✅ Tipos mapeados com sucesso")
        except Exception as e:
            print(f"      ⚠️  Erro ao mapear tipos: {e}")

        # =====================================================
        # 🔧 CORREÇÃO: RELACIONAMENTO HÍBRIDO COM PEDIDOS
        # =====================================================
        print("   • Relacionando NFe com Pedidos (método híbrido)...")
        
        # ✅ MÉTODO 1: Buscar por numero_pedido_lv (ecommerce)
        query_pedidos_lv = text("""
            SELECT 
                numero_pedido_lv,
                numero_pedido_bling as numero_pedido
            FROM processed.fato_pedidos
            WHERE empresa_id = :empresa_id
            AND numero_pedido_lv IS NOT NULL
        """)
        
        df_pedidos_lv = pd.read_sql(query_pedidos_lv, self.engine, params={"empresa_id": self.empresa_id})
        print(f"      • Método 1 (ecommerce): {len(df_pedidos_lv)} pedidos com numero_pedido_lv")
        
        # Preparar dados método 1
        df_pedidos_lv['numero_pedido_lv'] = df_pedidos_lv['numero_pedido_lv'].astype(str).str.strip()
        df_pedidos_lv['numero_pedido'] = pd.to_numeric(df_pedidos_lv['numero_pedido'], errors='coerce')
        
        # ✅ MÉTODO 2: Usar df_vendas que veio do raw (notaFiscal.id)
        df_vendas['nf_id'] = pd.to_numeric(df_vendas['nf_id'], errors='coerce')
        df_vendas['numero_pedido'] = pd.to_numeric(df_vendas['numero_pedido'], errors='coerce')
        print(f"      • Método 2 (notaFiscal): {len(df_vendas)} vendas com notaFiscal.id")
        
        # ✅ JOIN MÉTODO 1: numeroPedidoLoja → numero_pedido_lv
        df = df.merge(
            df_pedidos_lv[['numero_pedido_lv', 'numero_pedido']],
            left_on='numeroPedidoLoja',
            right_on='numero_pedido_lv',
            how='left',
            suffixes=('', '_metodo1')
        )
        
        # ✅ JOIN MÉTODO 2: bling_nfe_id → notaFiscal.id
        df = df.merge(
            df_vendas[['nf_id', 'numero_pedido']],
            left_on='bling_nfe_id',
            right_on='nf_id',
            how='left',
            suffixes=('', '_metodo2')
        )
        
        # ✅ COALESCE: Usar método 1, se não tiver, usar método 2
        df['numero_pedido_final'] = df['numero_pedido'].fillna(df['numero_pedido_metodo2'])
        
        # Renomear para coluna final
        df = df.drop(columns=['numero_pedido', 'numero_pedido_metodo2'], errors='ignore')
        df = df.rename(columns={'numero_pedido_final': 'numero_pedido'})
        
        # Remover colunas auxiliares
        df = df.drop(columns=['numero_pedido_lv', 'nf_id', 'numeroPedidoLoja'], errors='ignore')
        
        # Verificar resultados do relacionamento
        nfe_com_pedido = df['numero_pedido'].notna().sum()
        nfe_sem_pedido = df['numero_pedido'].isna().sum()
        
        print(f"      ✅ Relacionamento concluído (híbrido)!")
        print(f"      • NFe com pedido: {nfe_com_pedido}")
        print(f"      • NFe sem pedido: {nfe_sem_pedido}")
        # =====================================================
        # FIM DA CORREÇÃO
        # =====================================================

        # === CONVERTENDO STRINGS VAZIAS PARA NaN ===
        print("   • Convertendo strings vazias para NaN...")
        for coluna in df.select_dtypes(include=["object"]).columns:
            df[coluna] = df[coluna].replace(r"^\s*$", np.nan, regex=True)
            df[coluna] = df[coluna].replace("", np.nan)
            df[coluna] = df[coluna].replace(" ", np.nan)

        # === ADICIONAR METADADOS ===
        print("   • Adicionando metadados de processamento...")
        df["data_processamento"] = datetime.now()

        print("✅ Todas as transformações aplicadas com sucesso!")
        return df

    # =====================================================
    # 6. SELECIONAR COLUNAS FINAIS E CONVERTER TIPOS
    # =====================================================

    def selecionar_colunas_finais(self, df):
        """
        Seleciona apenas as colunas que existem no modelo da tabela
        E CONVERTE TIPOS CORRETAMENTE PARA EVITAR OVERFLOW
        """
        print("\n4️⃣ SELECIONANDO COLUNAS FINAIS E CONVERTENDO TIPOS...")

        colunas_finais = [
            "nfe_id",
            "bling_nfe_id",
            "empresa_id",
            "tipo",
            "numero_nfe",
            "situacao",
            "data_emissao",
            "data_entrada",
            "valor_nf",
            "valor_frete",
            "numero_pedido",
            "bling_canal_id",
            "bling_cliente_id",
            "bling_natureza_operacao_id",
            "data_ingestao",
            "data_processamento",
        ]

        # Selecionar apenas colunas que existem
        colunas_existentes = [col for col in colunas_finais if col in df.columns]
        df_final = df[colunas_existentes].copy()

        # 🔧 CORREÇÃO CRÍTICA: Converter NaN para None em colunas BIGINT
        print("   • Convertendo NaN para None (compatibilidade PostgreSQL BIGINT)...")
        
        colunas_bigint = ['bling_canal_id', 'bling_cliente_id', 'bling_natureza_operacao_id', 'numero_pedido']
        
        for coluna in colunas_bigint:
            if coluna in df_final.columns:
                df_final[coluna] = df_final[coluna].replace({np.nan: None, pd.NA: None})
                df_final[coluna] = df_final[coluna].astype(object)  # Permite None no PostgreSQL

        # Converter NaN para None em colunas NUMERIC/DECIMAL (PBI estava dando erro)
        print("   • Convertendo NaN para None (compatibilidade PostgreSQL NUMERIC)...")
        
        colunas_numeric = ['valor_nf', 'valor_frete']
        
        for coluna in colunas_numeric:
            if coluna in df_final.columns:
                # Substituir NaN, inf, -inf por None
                df_final[coluna] = df_final[coluna].replace({
                    np.nan: None, 
                    pd.NA: None,
                    np.inf: None,
                    -np.inf: None
                })
                # Converter para float quando não é None, mantendo None para NULL no banco
                df_final[coluna] = df_final[coluna].apply(
                    lambda x: float(x) if x is not None and pd.notna(x) else None
                )

        # Normalizar colunas DATE (data_emissao, data_entrada)
        print("   • Normalizando colunas de data (DATE)...")
        colunas_date = ['data_emissao', 'data_entrada']
        for coluna in colunas_date:
            if coluna in df_final.columns:
                # Garantir conversão segura para date; NaT vira None
                df_final[coluna] = pd.to_datetime(df_final[coluna], errors='coerce').dt.date
                df_final[coluna] = df_final[coluna].where(pd.notna(df_final[coluna]), None)

        # Converter NaT para None em colunas DATETIME
        print("   • Convertendo NaT para None (compatibilidade PostgreSQL TIMESTAMP)...")
        
        colunas_datetime = ['data_ingestao', 'data_processamento']
        
        for coluna in colunas_datetime:
            if coluna in df_final.columns:
                # Normalizar valores problemáticos: pandas.NaT, string "NaT", vazio
                df_final[coluna] = (
                    df_final[coluna]
                    .replace({pd.NaT: None})
                    .replace(["NaT", "nat", "", " "], None)
                )
                # Tentar converter para datetime e manter None quando inválido
                df_final[coluna] = pd.to_datetime(df_final[coluna], errors='coerce')
                df_final[coluna] = df_final[coluna].where(df_final[coluna].notna(), None)
                # Se for Timestamp do pandas, converter para datetime Python puro
                df_final[coluna] = df_final[coluna].apply(
                    lambda x: x.to_pydatetime() if (x is not None and hasattr(x, 'to_pydatetime')) else x
                )

        print(f"✅ {len(colunas_existentes)} colunas selecionadas e tipos convertidos")
        return df_final

    # =====================================================
    # 7. COMPARAR E SALVAR (EM LOTES) - VERSÃO MULTI-CNPJ ✅ CORRIGIDO
    # =====================================================

    def comparar_e_salvar(self, df_novo):
        """
        ✅ CORREÇÃO: Usa chave composta (bling_nfe_id, empresa_id)
        Compara dados novos com existentes usando comparação robusta
        Insere em lotes pequenos para evitar estouro
        """
        print("\n5️⃣ COMPARANDO COM DADOS EXISTENTES (COMPARAÇÃO ROBUSTA)...")

        session = Session()

        try:
            # Buscar dados existentes PARA ESTA EMPRESA
            query_existente = text("""
                SELECT * FROM processed.fato_nfe
                WHERE empresa_id = :empresa_id
            """)
            
            try:
                df_existente = pd.read_sql(query_existente, self.engine, params={"empresa_id": self.empresa_id})
                print(f"   • Registros existentes (empresa_id={self.empresa_id}): {len(df_existente)}")
            except:
                # Tabela não existe ou está vazia
                df_existente = pd.DataFrame()
                print(f"   • Tabela vazia ou não existe")

            if df_existente.empty:
                # Primeira carga - inserir tudo EM LOTES
                print("   • Primeira carga: inserindo todos os registros...")
                registros_novos = df_novo.to_dict('records')
                
                if registros_novos:
                    # ✅ CORREÇÃO: Remover nfe_id para deixar banco gerar
                    registros_sem_id = [{k: v for k, v in reg.items() if k != 'nfe_id'} for reg in registros_novos]
                    
                    # INSERIR EM LOTES DE 100 REGISTROS
                    batch_size = 100
                    total_batches = (len(registros_sem_id) + batch_size - 1) // batch_size
                    
                    print(f"   • Total de lotes: {total_batches}")
                    
                    for i in range(0, len(registros_sem_id), batch_size):
                        batch = registros_sem_id[i:i + batch_size]
                        batch_num = (i // batch_size) + 1
                        
                        print(f"      • Lote {batch_num}/{total_batches}: {len(batch)} registros...", end=" ")
                        
                        stmt = insert(self.get_modelo_tabela()).values(batch)
                        session.execute(stmt)
                        session.commit()  # Commit por lote
                        
                        print("✅")
                    
                    print(f"   ✅ {len(registros_novos)} registros inseridos")
                
                return {
                    'inseridos': len(registros_novos),
                    'atualizados': 0,
                    'sem_alteracao': 0
                }

            # ✅ CORREÇÃO: Criar dicionário com CHAVE COMPOSTA
            print("   • Criando índice de registros existentes...")
            
            # Colunas para comparação (EXCLUINDO metadados que sempre mudam!)
            colunas_comparacao = [col for col in df_novo.columns 
                                 if col not in ['data_processamento','data_ingestao', 'nfe_id']]
            
            # ✅ CHAVE COMPOSTA (bling_nfe_id, empresa_id)
            registros_existentes = {}
            for _, row in df_existente.iterrows():
                chave = (row['bling_nfe_id'], row['empresa_id'])  # ✅ CHAVE COMPOSTA!
                registros_existentes[chave] = row.to_dict()
            
            print(f"   • Índice criado: {len(registros_existentes)} registros")

            # Separar em: novos, alterados e sem alteração
            novos = []
            alterados = []
            sem_alteracao = []
            
            print("   • Comparando registros (usando função robusta)...")
            
            for _, row_novo in df_novo.iterrows():
                chave = (row_novo['bling_nfe_id'], row_novo['empresa_id'])  # ✅ CHAVE COMPOSTA!
                
                if chave not in registros_existentes:
                    # Registro completamente novo
                    novos.append(row_novo.to_dict())
                else:
                    # Registro existe - verificar se mudou
                    row_existe = registros_existentes[chave]
                    
                    houve_alteracao = False
                    
                    for col in colunas_comparacao:
                        if col in ['bling_nfe_id', 'empresa_id']:
                            continue
                        
                        val_novo = row_novo.get(col)
                        val_existe = row_existe.get(col)
                        
                        # 🔧 USAR FUNÇÃO DE COMPARAÇÃO ROBUSTA!
                        if not valores_sao_iguais(val_novo, val_existe):
                            houve_alteracao = True
                            break
                    
                    if houve_alteracao:
                        alterados.append(row_novo.to_dict())
                    else:
                        sem_alteracao.append(chave)

            print(f"\n   📊 ANÁLISE:")
            print(f"      • 🆕 Novos: {len(novos)}")
            print(f"      • 🔄 Alterados: {len(alterados)}")
            print(f"      • ✓ Sem alteração: {len(sem_alteracao)}")

            # Inserir novos EM LOTES
            inseridos = 0
            if novos:
                print(f"\n   ➕ Inserindo {len(novos)} novos registros...")
                
                batch_size = 100
                total_batches = (len(novos) + batch_size - 1) // batch_size
                
                for i in range(0, len(novos), batch_size):
                    batch = novos[i:i + batch_size]
                    batch_num = (i // batch_size) + 1
                    
                    print(f"      • Lote {batch_num}/{total_batches}: {len(batch)} registros...", end=" ")
                    
                    # ✅ CORREÇÃO: Remover nfe_id para deixar banco gerar (evita conflito PK)
                    batch_sem_id = [{k: v for k, v in reg.items() if k != 'nfe_id'} for reg in batch]
                    
                    stmt = insert(self.get_modelo_tabela()).values(batch_sem_id)
                    session.execute(stmt)
                    session.commit()  # Commit por lote
                    
                    print("✅")
                    inseridos += len(batch)

            # Atualizar alterados EM LOTES
            atualizados = 0
            if alterados:
                print(f"\n   🔄 Atualizando {len(alterados)} registros alterados...")
                
                batch_size = 100
                total_batches = (len(alterados) + batch_size - 1) // batch_size
                
                for i in range(0, len(alterados), batch_size):
                    batch = alterados[i:i + batch_size]
                    batch_num = (i // batch_size) + 1
                    
                    print(f"      • Lote {batch_num}/{total_batches}: {len(batch)} registros...", end=" ")
                    
                    for registro in batch:
                        # ✅ CORREÇÃO: Pegar nfe_id correto do registro existente
                        chave = (registro['bling_nfe_id'], registro['empresa_id'])
                        registro['nfe_id'] = registros_existentes[chave]['nfe_id']
                        
                        stmt = insert(self.get_modelo_tabela()).values(registro)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['bling_nfe_id', 'empresa_id'],
                            set_={k: v for k, v in registro.items() 
                                 if k not in ['nfe_id', 'bling_nfe_id', 'empresa_id']}
                        )
                        session.execute(stmt)
                        atualizados += 1
                    
                    session.commit()  # Commit por lote
                    print("✅")

            print(f"\n   ✅ Salvamento concluído!")

            return {
                'inseridos': inseridos,
                'atualizados': atualizados,
                'sem_alteracao': len(sem_alteracao)
            }

        except Exception as e:
            session.rollback()
            print(f"\n   ❌ Erro ao salvar: {e}")
            raise
        finally:
            session.close()

    def get_modelo_tabela(self):
        """
        Retorna o modelo SQLAlchemy da tabela
        """
        from models.dim_fato.fato_nfe import FatoNFe
        return FatoNFe.__table__

    # =====================================================
    # 8. ORQUESTRAÇÃO COMPLETA
    # =====================================================

    def executar_transformacao_completa(self):
        """
        Executa TODAS as etapas da transformação
        """
        try:
            print("\n" + "=" * 70)
            print("🔄 TRANSFORMAÇÃO: NFE_RAW → FATO_NFE")
            print("=" * 70)

            # 1. Extrair
            df_nfe_raw, df_vendas = self.extrair_dados_raw()

            if len(df_nfe_raw) == 0:
                print(f"\n⚠️ Nenhuma NFe encontrada para empresa_id = {self.empresa_id}")
                return

            # 2. Expandir JSON
            df_expandido = self.expandir_json(df_nfe_raw)

            # 3. Transformar
            df_transformado = self.aplicar_transformacoes(df_expandido, df_vendas)

            # 4. Selecionar colunas finais E CONVERTER TIPOS
            df_final = self.selecionar_colunas_finais(df_transformado)

            # 5. Estatísticas
            print("\n📊 ESTATÍSTICAS:")
            print(f"   • Total de NFe: {len(df_final)}")
            
            if 'tipo' in df_final.columns:
                print(f"\n   • Por tipo:")
                for tipo in df_final['tipo'].dropna().unique():
                    qtd = (df_final['tipo'] == tipo).sum()
                    print(f"      - {tipo}: {qtd}")
            
            if 'situacao' in df_final.columns:
                print(f"\n   • Por situação:")
                situacoes = df_final['situacao'].value_counts()
                for situacao, qtd in situacoes.head(5).items():
                    print(f"      - {situacao}: {qtd}")

            # 6. Comparar e salvar (EM LOTES COM COMPARAÇÃO ROBUSTA!)
            stats = self.comparar_e_salvar(df_final)

            # 7. Relatório final
            print("\n" + "=" * 70)
            print("📊 RESULTADO FINAL")
            print("=" * 70)
            print(f"   ➕ Inseridos: {stats['inseridos']}")
            print(f"   🔄 Atualizados: {stats['atualizados']}")
            print(f"   ✓ Sem alteração: {stats['sem_alteracao']}")
            print(f"   📈 Total processado: {sum(stats.values())}")
            
            # Calcular eficiência
            if stats['sem_alteracao'] > 0:
                total = sum(stats.values())
                eficiencia = (stats['sem_alteracao'] / total) * 100
                print(f"   ⚡ Eficiência: {eficiencia:.1f}% sem alteração (evitou reprocessamento)")
            
            print("=" * 70)

        except Exception as e:
            print(f"\n❌ Erro durante transformação: {e}")
            import traceback
            traceback.print_exc()
            raise