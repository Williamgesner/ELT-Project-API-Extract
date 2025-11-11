# Responsável por: Limpar e transformar dados de nfe_raw para fato_nfe no schema processed
# Inserção em lotes para evitar estouro de parâmetros PostgreSQL

import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from config.database import Session, engine

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

    def __init__(self):
        self.engine = engine

    # =====================================================
    # 3. EXTRAIR DADOS RAW
    # =====================================================

    def extrair_dados_raw(self):
        """
        Extrai dados da tabela raw.nfe_raw
        """
        print("\n1️⃣ EXTRAINDO DADOS DE RAW.NFE_RAW...")

        query_nfe = """
            SELECT 
                id,
                bling_id,
                dados_json,
                data_ingestao
            FROM raw.nfe_raw
            ORDER BY bling_id
        """

        df_nfe_raw = pd.read_sql(query_nfe, self.engine)
        print(f"✅ {len(df_nfe_raw)} NFe extraídas")

        # Buscar dados de vendas para relacionamento
        print("   • Importando dados de vendas para relacionamento...")
        query_vendas = """
            SELECT 
                bling_id,
                dados_json->>'numero' as numero_pedido,
                dados_json->'notaFiscal'->>'id' as nf_id
            FROM raw.vendas_raw
            WHERE dados_json->'notaFiscal'->>'id' IS NOT NULL
                AND dados_json->'notaFiscal'->>'id' != '0'
        """

        df_vendas = pd.read_sql(query_vendas, self.engine)
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
            [df_nfe_raw[["id", "bling_id", "data_ingestao"]], df_json],
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

        # === RELACIONAMENTO COM VENDAS (NUMERO_PEDIDO) ===
        print("   • Relacionando NFe com Vendas para pegar numero_pedido...")
        
        # Preparar dados de vendas
        df_vendas['nf_id'] = pd.to_numeric(df_vendas['nf_id'], errors='coerce')
        df_vendas['numero_pedido'] = pd.to_numeric(df_vendas['numero_pedido'], errors='coerce')

        # Fazer LEFT JOIN para trazer numero_pedido
        df = df.merge(
            df_vendas[['nf_id', 'numero_pedido']],
            left_on='bling_nfe_id',
            right_on='nf_id',
            how='left'
        )

        # Remover coluna auxiliar
        if 'nf_id' in df.columns:
            df = df.drop(columns=['nf_id'])

        # Verificar resultados do relacionamento
        nfe_com_pedido = df['numero_pedido'].notna().sum()
        nfe_sem_pedido = df['numero_pedido'].isna().sum()

        print(f"      ✅ Relacionamento concluído!")
        print(f"      • NFe com pedido: {nfe_com_pedido}")
        print(f"      • NFe sem pedido: {nfe_sem_pedido}")

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
    # 7. COMPARAR E SALVAR (EM LOTES)
    # =====================================================

    def comparar_e_salvar(self, df_novo):
        """
        Compara dados novos com existentes e salva apenas diferenças
        Insere em lotes pequenos para evitar estouro
        """
        print("\n5️⃣ COMPARANDO COM DADOS EXISTENTES...")

        session = Session()

        try:
            # Buscar dados existentes
            query_existente = "SELECT * FROM processed.fato_nfe"
            
            try:
                df_existente = pd.read_sql(query_existente, self.engine)
                print(f"   • Registros existentes: {len(df_existente)}")
            except:
                # Tabela não existe ou está vazia
                df_existente = pd.DataFrame()
                print(f"   • Tabela vazia ou não existe")

            if df_existente.empty:
                # Primeira carga - inserir tudo EM LOTES
                print("   • Primeira carga: inserindo todos os registros...")
                registros_novos = df_novo.to_dict('records')
                
                if registros_novos:
                    # INSERIR EM LOTES DE 100 REGISTROS
                    batch_size = 100
                    total_batches = (len(registros_novos) + batch_size - 1) // batch_size
                    
                    print(f"   • Total de lotes: {total_batches}")
                    
                    for i in range(0, len(registros_novos), batch_size):
                        batch = registros_novos[i:i + batch_size]
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

            # Colunas para comparação (excluindo metadados)
            colunas_comparacao = [col for col in df_novo.columns 
                                 if col not in ['data_processamento', 'data_ingestao']]

            # Merge para identificar novos e alterados
            df_comparacao = df_novo.merge(
                df_existente[colunas_comparacao],
                on='bling_nfe_id',
                how='left',
                suffixes=('_novo', '_existe'),
                indicator=True
            )

            # Registros completamente novos
            novos = df_comparacao[df_comparacao['_merge'] == 'left_only']
            
            # Registros que precisam ser verificados
            existentes = df_comparacao[df_comparacao['_merge'] == 'both']

            # Detectar alterações
            alterados = []
            for idx, row in existentes.iterrows():
                houve_alteracao = False
                for col in colunas_comparacao:
                    if col == 'bling_nfe_id':
                        continue
                    
                    col_novo = f"{col}_novo"
                    col_existe = f"{col}_existe"
                    
                    if col_novo in row and col_existe in row:
                        val_novo = row[col_novo]
                        val_existe = row[col_existe]
                        
                        # Comparação considerando NaN
                        if pd.isna(val_novo) and pd.isna(val_existe):
                            continue
                        if val_novo != val_existe:
                            houve_alteracao = True
                            break
                
                if houve_alteracao:
                    alterados.append(row['bling_nfe_id'])

            print(f"\n   📊 ANÁLISE:")
            print(f"      • Novos: {len(novos)}")
            print(f"      • Alterados: {len(alterados)}")
            print(f"      • Sem alteração: {len(existentes) - len(alterados)}")

            # Inserir novos EM LOTES
            inseridos = 0
            if not novos.empty:
                print(f"\n   ➕ Inserindo {len(novos)} novos registros...")
                
                # Pegar apenas as colunas originais (sem sufixos)
                colunas_originais = [col for col in df_novo.columns]
                novos_limpo = df_novo[df_novo['bling_nfe_id'].isin(novos['bling_nfe_id'])]
                
                registros_novos = novos_limpo.to_dict('records')
                
                if registros_novos:
                    # INSERIR EM LOTES DE 100 REGISTROS
                    batch_size = 100
                    total_batches = (len(registros_novos) + batch_size - 1) // batch_size
                    
                    for i in range(0, len(registros_novos), batch_size):
                        batch = registros_novos[i:i + batch_size]
                        batch_num = (i // batch_size) + 1
                        
                        print(f"      • Lote {batch_num}/{total_batches}: {len(batch)} registros...", end=" ")
                        
                        stmt = insert(self.get_modelo_tabela()).values(batch)
                        session.execute(stmt)
                        session.commit()  # Commit por lote
                        
                        print("✅")
                        inseridos += len(batch)

            # Atualizar alterados EM LOTES
            atualizados = 0
            if alterados:
                print(f"\n   🔄 Atualizando {len(alterados)} registros alterados...")
                
                # ATUALIZAR EM LOTES DE 100 REGISTROS
                batch_size = 100
                total_batches = (len(alterados) + batch_size - 1) // batch_size
                
                for i in range(0, len(alterados), batch_size):
                    batch_ids = alterados[i:i + batch_size]
                    batch_num = (i // batch_size) + 1
                    
                    print(f"      • Lote {batch_num}/{total_batches}: {len(batch_ids)} registros...", end=" ")
                    
                    for bling_id in batch_ids:
                        registro = df_novo[df_novo['bling_nfe_id'] == bling_id].iloc[0].to_dict()
                        
                        stmt = insert(self.get_modelo_tabela()).values(registro)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['bling_nfe_id'],
                            set_={k: v for k, v in registro.items() 
                                 if k not in ['nfe_id', 'bling_nfe_id']}
                        )
                        session.execute(stmt)
                        atualizados += 1
                    
                    session.commit()  # Commit por lote
                    print("✅")

            print(f"\n   ✅ Salvamento concluído!")

            return {
                'inseridos': inseridos,
                'atualizados': atualizados,
                'sem_alteracao': len(existentes) - len(alterados)
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

            # 6. Comparar e salvar (EM LOTES!)
            stats = self.comparar_e_salvar(df_final)

            # 7. Relatório final
            print("\n" + "=" * 70)
            print("📊 RESULTADO FINAL")
            print("=" * 70)
            print(f"   ➕ Inseridos: {stats['inseridos']}")
            print(f"   🔄 Atualizados: {stats['atualizados']}")
            print(f"   ✓ Sem alteração: {stats['sem_alteracao']}")
            print(f"   📈 Total processado: {sum(stats.values())}")
            print("=" * 70)

        except Exception as e:
            print(f"\n❌ Erro durante transformação: {e}")
            import traceback
            traceback.print_exc()
            raise