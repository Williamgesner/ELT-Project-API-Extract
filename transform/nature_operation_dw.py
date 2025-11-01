# Responsável por: transformar dados de natureza_operacao_raw para dim_natureza_operacao

import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from config.settings import database_url
from config.database import Session
from models.dim_fato.dim_natureza_operacao import DimNaturezaOperacao

# =====================================================
# 1. CLASSE PARA TRANSFORMAÇÃO
# =====================================================

class NaturezaOperacaoTransformer:
    """
    Transforma dados de natureza_operacao_raw para dim_natureza_operacao
    """
    
    def __init__(self):
        self.engine = create_engine(database_url)
        self.session = Session()
    
    def extrair_dados_raw(self):
        """
        Extrai dados da tabela raw.natureza_operacao_raw
        """
        print("\n1️⃣ EXTRAINDO DADOS DE natureza_operacao_raw...")
        
        query = """
            SELECT 
                id,
                bling_id,
                dados_json,
                data_ingestao
            FROM raw.natureza_operacao_raw
            ORDER BY bling_id
        """
        
        df_raw = pd.read_sql(query, self.engine)
        print(f"✅ {len(df_raw)} registros extraídos")
        
        return df_raw
    
    def transformar_dados(self, df_raw):
        """
        Transforma os dados: expande JSON e limpa
        """
        print("\n2️⃣ TRANSFORMANDO DADOS...")
        
        # Expandir JSON
        df_json = pd.json_normalize(df_raw["dados_json"])
        
        # Renomear 'id' para evitar conflito
        if "id" in df_json.columns:
            df_json = df_json.rename(columns={"id": "id_bling"})
        
        # Combinar com colunas originais
        df = pd.concat(
            [df_raw[["id", "bling_id", "data_ingestao"]], df_json],
            axis=1,
        )
        
        # Remover colunas desnecessárias
        colunas_remover = ["id_bling", "padrao", "situacao"]
        colunas_existentes = [col for col in colunas_remover if col in df.columns]
        df = df.drop(columns=colunas_existentes)
        
        # Renomear colunas
        df = df.rename(
            columns={
                "id": "natureza_operacao_id",
                "bling_id": "bling_natureza_operacao_id",
                "descricao": "natureza_operacao"
            }
        )
        
        # Selecionar colunas finais
        colunas_finais = [
            "natureza_operacao_id",
            "bling_natureza_operacao_id",
            "natureza_operacao",
            "data_ingestao"
        ]
        
        df = df[colunas_finais]
        
        print(f"✅ {len(df)} registros transformados")
        print(f"📋 Colunas finais: {list(df.columns)}")
        
        return df
    
    def carregar_dados_dw(self, df):
        """
        Carrega dados na tabela dim_natureza_operacao
        """
        print("\n3️⃣ CARREGANDO DADOS NO DW...")
        
        registros_inseridos = 0
        registros_atualizados = 0
        registros_erro = 0
        
        for _, row in df.iterrows():
            try:
                stmt = insert(DimNaturezaOperacao).values(
                    bling_natureza_operacao_id=int(row['bling_natureza_operacao_id']),
                    natureza_operacao=row['natureza_operacao'],
                    data_ingestao=row['data_ingestao']
                )
                
                # UPSERT: Se já existe, atualiza
                stmt = stmt.on_conflict_do_update(
                    index_elements=['bling_natureza_operacao_id'],
                    set_={
                        'natureza_operacao': stmt.excluded.natureza_operacao,
                        'data_ingestao': stmt.excluded.data_ingestao
                    }
                )
                
                self.session.execute(stmt)
                registros_inseridos += 1
                
            except Exception as e:
                print(f"❌ Erro ao processar natureza {row['bling_natureza_operacao_id']}: {e}")
                registros_erro += 1
                continue
        
        # Commit final
        try:
            self.session.commit()
            print(f"✅ Dados carregados com sucesso!")
        except Exception as e:
            print(f"❌ Erro no commit: {e}")
            self.session.rollback()
        
        return {
            'inseridos': registros_inseridos,
            'atualizados': registros_atualizados,
            'erros': registros_erro
        }
    
    def validar_dados_dw(self):
        """
        Valida os dados carregados no DW
        """
        print("\n4️⃣ VALIDANDO DADOS NO DW...")
        
        query_validacao = text("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT bling_natureza_operacao_id) as ids_unicos
            FROM processed.dim_natureza_operacao
        """)
        
        resultado = self.session.execute(query_validacao).fetchone()
        
        print(f"📊 Total de registros: {resultado.total}")
        print(f"📊 IDs únicos: {resultado.ids_unicos}")
        
        # Mostrar alguns exemplos
        query_exemplos = text("""
            SELECT 
                natureza_operacao_id,
                bling_natureza_operacao_id,
                natureza_operacao
            FROM processed.dim_natureza_operacao
            ORDER BY natureza_operacao_id
            LIMIT 5
        """)
        
        exemplos = self.session.execute(query_exemplos).fetchall()
        
        print(f"\n📋 Primeiros 5 registros:")
        for ex in exemplos:
            print(f"   • ID {ex.natureza_operacao_id} (Bling: {ex.bling_natureza_operacao_id}): {ex.natureza_operacao}")
    
    def executar_transformacao_completa(self):
        """
        Executa o processo ETL completo
        """
        print("\n🔄 TRANSFORMAÇÃO: NATUREZA DE OPERAÇÃO → DW")
        print("=" * 70)
        
        inicio = datetime.now()
        
        try:
            # 1. Extrair
            df_raw = self.extrair_dados_raw()
            
            if df_raw.empty:
                print("⚠️  Nenhum dado encontrado em natureza_operacao_raw")
                return
            
            # 2. Transformar
            df_transformado = self.transformar_dados(df_raw)
            
            # 3. Carregar
            stats = self.carregar_dados_dw(df_transformado)
            
            # 4. Validar
            self.validar_dados_dw()
            
            fim = datetime.now()
            tempo_total = fim - inicio
            
            # Relatório final
            print(f"\n{'='*70}")
            print(f"✅ TRANSFORMAÇÃO CONCLUÍDA!")
            print(f"{'='*70}")
            print(f"\n⏱️  Tempo total: {tempo_total}")
            print(f"\n📊 ESTATÍSTICAS:")
            print(f"   • Registros processados: {stats['inseridos']}")
            print(f"   • Erros: {stats['erros']}")
            
            print(f"\n💡 PRÓXIMOS PASSOS:")
            print(f"   1. Usar esta dimensão nas transformações de NFe")
            print(f"   2. Fazer JOIN: nfe_raw.natureza_operacao_id = dim_natureza_operacao.bling_natureza_operacao_id")
            
        except Exception as e:
            print(f"\n❌ ERRO na transformação: {e}")
            self.session.rollback()
            raise
        finally:
            self.session.close()