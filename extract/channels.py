# Responsável por: extrair canais de venda da API Bling

import requests
import time
from datetime import datetime
from config.database import Session
from config.settings import endpoints
from models.channels_raw import CanaisRaw
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

# =====================================================
# 1. EXTRATOR DE CANAIS DE VENDA
# =====================================================

class CanaisExtractor:
    """
    Extrator de canais de venda baseado nos IDs encontrados nos pedidos
    
    Estratégia:
    1. Busca IDs únicos de canal na tabela vendas_raw
    2. Para cada ID, busca detalhes na API: /canais-venda/{id}
    3. Salva no banco
    """
    
    def __init__(self, api_key, empresa_id):
        """
        Args:
            api_key: Token de autenticação da API Bling
            empresa_id: ID da empresa na tabela dim_empresas
        """
        self.base_url = endpoints["canais"]
        self.empresa_id = empresa_id
        
        # Definir headers com a API key específica
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def obter_canais_dos_pedidos(self):
        """
        Busca IDs únicos de canal na tabela vendas_raw
        """
        print(f"\n🔍 BUSCANDO IDs DE CANAIS NOS PEDIDOS (Empresa ID: {self.empresa_id})...")
        print("=" * 70)
        
        session = Session()
        
        try:
            # Query para buscar loja.id únicos do JSON
            query = text("""
                SELECT DISTINCT (dados_json->'loja'->>'id')::integer as canal_id
                FROM raw.vendas_raw
                WHERE dados_json->'loja'->>'id' IS NOT NULL
                  AND empresa_id = :empresa_id
                ORDER BY canal_id
            """)
            
            resultado = session.execute(query, {"empresa_id": self.empresa_id})
            canais_ids = [row.canal_id for row in resultado]
            
            print(f"✅ {len(canais_ids)} canais únicos encontrados nos pedidos")
            print(f"IDs: {canais_ids}")
            
            return canais_ids
            
        except Exception as e:
            print(f"❌ Erro ao buscar canais: {e}")
            return []
        finally:
            session.close()
    
    def buscar_detalhes_canal(self, canal_id, tentativas=3):
        """
        Busca detalhes de um canal específico na API
        
        Endpoint: GET /canais-venda/{idCanalVenda}
        """
        url = f"{self.base_url}/{canal_id}"
        
        for tentativa in range(tentativas):
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                
                if response.status_code == 200:
                    dados = response.json()
                    return dados.get('data', {})
                elif response.status_code == 404:
                    print(f"   ⚠️  Canal {canal_id} não encontrado (404)")
                    return None
                else:
                    print(f"   ❌ Erro HTTP {response.status_code} no canal {canal_id}")
                    
                    if tentativa < tentativas - 1:
                        time.sleep(0.5 * (tentativa + 1))
                        continue
                    else:
                        return None
                        
            except Exception as e:
                print(f"   ❌ Erro ao buscar canal {canal_id}: {e}")
                
                if tentativa < tentativas - 1:
                    time.sleep(0.5 * (tentativa + 1))
                else:
                    return None
        
        return None
    
    def salvar_canal(self, canal_data):
        """
        Salva um canal no banco usando UPSERT
        """
        if not canal_data:
            return False
        
        session = Session()
        
        try:
            canal_id = canal_data.get('id')
            descricao = canal_data.get('descricao', 'Sem descrição')
            
            # UPSERT: Insert ou Update se já existir
            stmt = insert(CanaisRaw).values(
                bling_canal_id=canal_id,
                empresa_id=self.empresa_id,  
                descricao=descricao,
                dados_json=canal_data,
                data_ingestao=datetime.now()
            )
            
            stmt = stmt.on_conflict_do_update(
                index_elements=['bling_canal_id', 'empresa_id'], 
                set_={
                    'descricao': stmt.excluded.descricao,
                    'dados_json': stmt.excluded.dados_json,
                    'data_ingestao': stmt.excluded.data_ingestao
                }
            )
            
            session.execute(stmt)
            session.commit()
            
            return True
            
        except Exception as e:
            session.rollback()
            print(f"   ✗ Erro ao salvar canal {canal_data.get('id')}: {e}")
            return False
        finally:
            session.close()
    
    def executar_extracao_completa(self):
        """
        Executa o processo completo de extração de canais
        """
        print(f"\n📊 EXTRAÇÃO: CANAIS DE VENDA (Empresa ID: {self.empresa_id})")
        print("=" * 70)
        print("Estratégia: Buscar canais baseados nos pedidos existentes")
        print("=" * 70)
        
        inicio = datetime.now()
        
        try:
            # 1. Buscar IDs de canal dos pedidos
            canais_ids = self.obter_canais_dos_pedidos()
            
            if not canais_ids:
                print("\n❌ Nenhum ID de canal encontrado nos pedidos.")
                print("💡 Execute primeiro a extração de vendas (main_sales.py)")
                return
            
            # 2. Buscar detalhes de cada canal
            print(f"\n💾 BUSCANDO DETALHES DE {len(canais_ids)} CANAIS...")
            print("-" * 70)
            
            stats = {'sucesso': 0, 'erro': 0, 'nao_encontrado': 0}
            
            for i, canal_id in enumerate(canais_ids, 1):
                print(f"\n[{i}/{len(canais_ids)}] Canal ID: {canal_id}")
                
                # Buscar detalhes
                detalhes = self.buscar_detalhes_canal(canal_id)
                
                if detalhes:
                    # Salvar no banco
                    if self.salvar_canal(detalhes):
                        descricao = detalhes.get('descricao', 'Sem descrição')
                        print(f"   ✓ {canal_id}: {descricao}")
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
                print(f"   1. Verificar dados: SELECT * FROM raw.canais_raw WHERE empresa_id = {self.empresa_id};")
                print(f"   2. Usar na transformação de vendas (vendas_dw.py)")
            
        except Exception as e:
            print(f"\n❌ ERRO na extração: {e}")
            raise


# =====================================================
# 2. FUNÇÃO AUXILIAR - OBTER MAPEAMENTO
# =====================================================

def obter_mapeamento_canais(empresa_id):
    """
    Retorna dicionário {id: descricao} para lookup rápido
    
    Args:
        empresa_id: ID da empresa para filtrar canais
    
    Uso na transformação:
        mapa = obter_mapeamento_canais(empresa_id=1)
        df['canal_nome'] = df['canal_id'].map(mapa)
    """
    session = Session()
    
    try:
        canais = session.query(
            CanaisRaw.bling_canal_id,
            CanaisRaw.descricao
        ).filter(
            CanaisRaw.empresa_id == empresa_id
        ).all()
        
        mapa = {canal.bling_canal_id: canal.descricao for canal in canais}
        
        print(f"📋 {len(mapa)} canais carregados para mapeamento (empresa_id={empresa_id})")
        return mapa
        
    except Exception as e:
        print(f"❌ Erro ao carregar mapeamento: {e}")
        return {}
    finally:
        session.close()