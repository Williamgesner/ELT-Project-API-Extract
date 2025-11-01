# Responsável por: extrair naturezas de operação da API Bling

import requests
import time
from datetime import datetime
from config.settings import headers
from config.database import Session
from config.settings import endpoints
from models.nature_operation_raw import NaturezaOperacaoRaw
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

# =====================================================
# 1. EXTRATOR DE NATUREZAS DE OPERAÇÃO
# =====================================================

class NaturezaOperacaoExtractor:
    """
    Extrator de naturezas de operação
    
    Estratégia:
    1. Busca TODAS as naturezas via GET /naturezas-operacoes
    2. Salva cada uma no banco
    """
    
    def __init__(self):
        self.base_url = endpoints["natureza_operacao"]
        self.headers = headers
    
    def buscar_todas_naturezas(self):
        """
        Busca todas as naturezas de operação da API
        
        Endpoint: GET /naturezas-operacoes
        """
        url = self.base_url
        
        try:
            print(f"\n🔍 Buscando naturezas de operação da API...")
            print(f"   URL: {url}")
            
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                dados = response.json()
                naturezas = dados.get('data', [])
                print(f"   ✅ {len(naturezas)} naturezas encontradas")
                return naturezas
            else:
                print(f"   ❌ Erro HTTP {response.status_code}")
                print(f"   Resposta: {response.text[:200]}")
                return []
                
        except Exception as e:
            print(f"   ❌ Erro ao buscar naturezas: {e}")
            return []
    
    def salvar_natureza(self, natureza):
        """
        Salva natureza de operação no banco
        """
        session = Session()
        
        try:
            natureza_id = natureza.get('id')
            
            stmt = insert(NaturezaOperacaoRaw).values(
                bling_id=natureza_id,
                dados_json=natureza,
                data_ingestao=datetime.now()
            )
            
            # Se já existir, atualiza
            stmt = stmt.on_conflict_do_update(
                index_elements=['bling_id'],
                set_={
                    'dados_json': stmt.excluded.dados_json,
                    'data_ingestao': stmt.excluded.data_ingestao
                }
            )
            
            session.execute(stmt)
            session.commit()
            return True
            
        except Exception as e:
            print(f"   ❌ Erro ao salvar natureza {natureza_id}: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
    def executar_extracao_completa(self):
        """
        Executa o processo completo de extração de naturezas de operação
        """
        print("\n🌿 EXTRAÇÃO: NATUREZAS DE OPERAÇÃO")
        print("=" * 70)
        print("Este processo busca TODAS as naturezas de operação do Bling")
        print("=" * 70)
        
        inicio = datetime.now()
        
        try:
            # 1. Buscar todas as naturezas
            naturezas = self.buscar_todas_naturezas()
            
            if not naturezas:
                print("\n❌ Nenhuma natureza encontrada na API.")
                return
            
            # 2. Salvar cada natureza
            print(f"\n💾 SALVANDO {len(naturezas)} NATUREZAS NO BANCO...")
            print("-" * 70)
            
            stats = {'sucesso': 0, 'erro': 0}
            
            for i, natureza in enumerate(naturezas, 1):
                natureza_id = natureza.get('id')
                descricao = natureza.get('descricao', 'Sem descrição')
                
                print(f"\n[{i}/{len(naturezas)}] ID: {natureza_id}")
                print(f"   Descrição: {descricao}")
                
                # Salvar no banco
                if self.salvar_natureza(natureza):
                    print(f"   ✅ Salva com sucesso")
                    stats['sucesso'] += 1
                else:
                    stats['erro'] += 1
            
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
            
            if stats['sucesso'] > 0:
                print(f"\n💡 PRÓXIMOS PASSOS:")
                print(f"   1. Verificar dados: SELECT * FROM raw.natureza_operacao_raw;")
                print(f"   2. Usar na transformação de NFe")
            
        except Exception as e:
            print(f"\n❌ ERRO na extração: {e}")
            raise


# =====================================================
# 2. FUNÇÕES AUXILIARES
# =====================================================

def verificar_naturezas_cadastradas():
    """
    Verifica quantas naturezas estão cadastradas no banco
    """
    session = Session()
    
    try:
        query = text("""
            SELECT COUNT(*) as total
            FROM raw.natureza_operacao_raw
        """)
        
        resultado = session.execute(query).fetchone()
        
        print(f"\n📊 Naturezas de operação cadastradas: {resultado.total}")
        
        if resultado.total > 0:
            # Mostrar algumas
            query_lista = text("""
                SELECT bling_id, dados_json->>'descricao' as descricao
                FROM raw.natureza_operacao_raw
                ORDER BY bling_id
                LIMIT 5
            """)
            
            lista = session.execute(query_lista).fetchall()
            
            print(f"\n📋 Primeiras 5:")
            for nat in lista:
                print(f"   • ID {nat.bling_id}: {nat.descricao}")
        
        return resultado.total
            
    except Exception as e:
        print(f"❌ Erro: {e}")
        return 0
    finally:
        session.close()