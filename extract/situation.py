# Responsável por: extrair situações (status de pedidos) da API Bling

import requests
import time
from datetime import datetime
from config.settings import headers
from config.database import Session
from models.situation_raw import SituacoesRaw
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

# =====================================================
# 1. EXTRATOR DE SITUAÇÕES
# =====================================================

class SituacoesExtractor:
    """
    Extrator de situações baseado nos IDs encontrados nos pedidos
    
    Estratégia:
    1. Busca IDs únicos de situação na tabela vendas_raw
    2. Para cada ID, busca detalhes na API: /situacoes/{id}
    3. Salva no banco
    """
    
    def __init__(self):
        self.base_url = "https://api.bling.com.br/Api/v3/situacoes"
        self.headers = headers
    
    def obter_situacoes_dos_pedidos(self):
        """
        Busca IDs únicos de situação na tabela vendas_raw
        """
        print("\n🔍 BUSCANDO IDs DE SITUAÇÃO NOS PEDIDOS...")
        print("=" * 70)
        
        session = Session()
        
        try:
            # Query para buscar situacao.id únicos do JSON
            query = text("""
                SELECT DISTINCT (dados_json->'situacao'->>'id')::integer as situacao_id
                FROM raw.vendas_raw
                WHERE dados_json->'situacao'->>'id' IS NOT NULL
                ORDER BY situacao_id
            """)
            
            resultado = session.execute(query)
            situacoes_ids = [row.situacao_id for row in resultado]
            
            print(f"✅ {len(situacoes_ids)} situações únicas encontradas nos pedidos")
            print(f"IDs: {situacoes_ids}")
            
            return situacoes_ids
            
        except Exception as e:
            print(f"❌ Erro ao buscar situações: {e}")
            return []
        finally:
            session.close()
    
    def buscar_detalhes_situacao(self, situacao_id, tentativas=3):
        """
        Busca detalhes de uma situação específica na API
        
        Endpoint: GET /situacoes/{idSituacao}
        """
        url = f"{self.base_url}/{situacao_id}"
        
        for tentativa in range(tentativas):
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                
                if response.status_code == 200:
                    dados = response.json()
                    return dados.get('data', {})
                elif response.status_code == 404:
                    print(f"   ⚠️  Situação {situacao_id} não encontrada (404)")
                    return None
                else:
                    print(f"   ❌ Erro HTTP {response.status_code} na situação {situacao_id}")
                    
                    if tentativa < tentativas - 1:
                        time.sleep(0.5 * (tentativa + 1))
                        continue
                    else:
                        return None
                        
            except Exception as e:
                print(f"   ❌ Erro ao buscar situação {situacao_id}: {e}")
                
                if tentativa < tentativas - 1:
                    time.sleep(0.5 * (tentativa + 1))
                else:
                    return None
        
        return None
    
    def salvar_situacao(self, situacao_data):
        """
        Salva uma situação no banco usando UPSERT
        """
        if not situacao_data:
            return False
        
        session = Session()
        
        try:
            situacao_id = situacao_data.get('id')
            nome = situacao_data.get('nome')
            cor = situacao_data.get('cor', '')
            
            # UPSERT: Insert ou Update se já existir
            stmt = insert(SituacoesRaw).values(
                bling_situacao_id=situacao_id,
                nome=nome,
                cor=cor,
                dados_json=situacao_data,
                data_ingestao=datetime.now()
            )
            
            stmt = stmt.on_conflict_do_update(
                index_elements=['bling_situacao_id'],
                set_={
                    'nome': stmt.excluded.nome,
                    'cor': stmt.excluded.cor,
                    'dados_json': stmt.excluded.dados_json,
                    'data_ingestao': stmt.excluded.data_ingestao
                }
            )
            
            session.execute(stmt)
            session.commit()
            
            return True
            
        except Exception as e:
            session.rollback()
            print(f"   ✗ Erro ao salvar situação {situacao_data.get('id')}: {e}")
            return False
        finally:
            session.close()
    
    def executar_extracao_completa(self):
        """
        Executa o processo completo de extração de situações
        """
        print("\n📊 EXTRAÇÃO: SITUAÇÕES DE VENDAS")
        print("=" * 70)
        print("Estratégia: Buscar situações baseadas nos pedidos existentes")
        print("=" * 70)
        
        inicio = datetime.now()
        
        try:
            # 1. Buscar IDs de situação dos pedidos
            situacoes_ids = self.obter_situacoes_dos_pedidos()
            
            if not situacoes_ids:
                print("\n❌ Nenhum ID de situação encontrado nos pedidos.")
                print("💡 Execute primeiro a extração de vendas (main_sales.py)")
                return
            
            # 2. Buscar detalhes de cada situação
            print(f"\n💾 BUSCANDO DETALHES DE {len(situacoes_ids)} SITUAÇÕES...")
            print("-" * 70)
            
            stats = {'sucesso': 0, 'erro': 0, 'nao_encontrado': 0}
            
            for i, situacao_id in enumerate(situacoes_ids, 1):
                print(f"\n[{i}/{len(situacoes_ids)}] Situação ID: {situacao_id}")
                
                # Buscar detalhes
                detalhes = self.buscar_detalhes_situacao(situacao_id)
                
                if detalhes:
                    # Salvar no banco
                    if self.salvar_situacao(detalhes):
                        nome = detalhes.get('nome', 'Sem nome')
                        print(f"   ✓ {situacao_id}: {nome}")
                        stats['sucesso'] += 1
                    else:
                        stats['erro'] += 1
                else:
                    stats['nao_encontrado'] += 1
                
                # Delay entre requisições
                time.sleep(0.4)
            
            fim = datetime.now()
            tempo_total = fim - inicio
            
            # Relatório final
            print(f"\n{'='*70}")
            print(f"🎉 EXTRAÇÃO CONCLUÍDA!")
            print(f"{'='*70}")
            print(f"\n⏱️  Tempo total: {tempo_total}")
            print(f"\n📊 ESTATÍSTICAS:")
            print(f"   ✅ Sucesso: {stats['sucesso']}")
            print(f"   ❌ Erros: {stats['erro']}")
            print(f"   ⚠️  Não encontrados: {stats['nao_encontrado']}")
            
            if stats['sucesso'] > 0:
                print(f"\n💡 PRÓXIMOS PASSOS:")
                print(f"   1. Verificar dados: SELECT * FROM raw.situacoes_raw;")
                print(f"   2. Usar na transformação de vendas (vendas_dw.py)")
            
        except Exception as e:
            print(f"\n❌ ERRO na extração: {e}")
            raise


# =====================================================
# 2. FUNÇÃO AUXILIAR - OBTER MAPEAMENTO
# =====================================================

def obter_mapeamento_situacoes():
    """
    Retorna dicionário {id: nome} para lookup rápido
    
    Uso na transformação:
        mapa = obter_mapeamento_situacoes()
        df['situacao'] = df['situacao_id'].map(mapa)
    """
    session = Session()
    
    try:
        situacoes = session.query(
            SituacoesRaw.bling_situacao_id,
            SituacoesRaw.nome
        ).all()
        
        mapa = {sit.bling_situacao_id: sit.nome for sit in situacoes}
        
        print(f"📋 {len(mapa)} situações carregadas para mapeamento")
        return mapa
        
    except Exception as e:
        print(f"❌ Erro ao carregar mapeamento: {e}")
        return {}
    finally:
        session.close()