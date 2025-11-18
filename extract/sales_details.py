"""
EXTRATOR DE DETALHES COMPLETOS DE VENDAS
=========================================
Responsável por: buscar detalhes completos de cada venda (incluindo itens)
e atualizar o JSON na tabela vendas_raw

Fluxo:
1. Ler IDs das vendas já extraídas
2. Para cada ID, buscar detalhes completos na API
3. Atualizar o JSON com os dados completos (incluindo itens)
"""

import requests
import time
from datetime import datetime
from config.settings import endpoints
from config.database import Session
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from models.sales_raw import VendasRaw

class VendasDetalhesExtractor:
    """
    Extrator específico para buscar detalhes completos de vendas
    """
    
    def __init__(self, api_key, empresa_id):
        """
        Args:
            api_key: Token de autenticação da API Bling
            empresa_id: ID da empresa na tabela dim_empresas
        """
        self.base_url = endpoints['vendas']
        self.session = Session()
        self.empresa_id = empresa_id
        
        # Definir headers com a API key específica
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def buscar_detalhes_venda(self, venda_id, tentativas=3):
        """
        Busca detalhes completos de uma venda específica
        
        Args:
            venda_id: ID da venda no Bling
            tentativas: Número de tentativas em caso de erro
            
        Returns:
            dict: Dados completos da venda ou None se falhar
        """
        url = f"{self.base_url}/{venda_id}"
        
        for tentativa in range(tentativas):
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                
                if response.status_code == 200:
                    dados = response.json()
                    return dados.get('data', {})
                elif response.status_code == 404:
                    print(f"   ⚠️  Venda {venda_id} não encontrada (404)")
                    return None
                else:
                    print(f"   ❌ Erro HTTP {response.status_code} na venda {venda_id}")
                    
                    if tentativa < tentativas - 1:
                        time.sleep(0.5 * (tentativa + 1))
                        continue
                    else:
                        return None
                        
            except Exception as e:
                print(f"   ❌ Erro ao buscar venda {venda_id}: {e}")
                
                if tentativa < tentativas - 1:
                    time.sleep(0.5 * (tentativa + 1))
                else:
                    return None
        
        return None
    
    def atualizar_venda_com_detalhes(self, venda_id, dados_completos):
        """
        Atualiza o registro da venda com os dados completos
        
        Args:
            venda_id: ID da venda no Bling
            dados_completos: Dados completos da API (incluindo itens)
        """
        try:
            stmt = insert(VendasRaw).values(
                bling_id=venda_id,
                empresa_id=self.empresa_id, 
                dados_json=dados_completos,
                data_ingestao=datetime.now(),
                status_processamento='pendente'
            )
            
            stmt = stmt.on_conflict_do_update(
                index_elements=['bling_id', 'empresa_id'],  
                set_={
                    'dados_json': stmt.excluded.dados_json,
                    'data_ingestao': stmt.excluded.data_ingestao
                }
            )
            
            self.session.execute(stmt)
            return True
            
        except Exception as e:
            print(f"   ❌ Erro ao salvar venda {venda_id}: {e}")
            return False
    
    def executar_extracao_detalhes(self, delay_entre_requests=0.4, batch_size=100):
        """
        Executa a extração de detalhes para todas as vendas
        
        Args:
            delay_entre_requests: Tempo entre requisições (respeitar rate limit)
            batch_size: Quantas vendas processar antes de fazer commit
        """
        print(f"\n🔍 EXTRATOR DE DETALHES COMPLETOS DE VENDAS (Empresa ID: {self.empresa_id})")
        print("=" * 70)
        print("Este processo busca os detalhes de CADA venda individualmente")
        print("para obter os itens dos pedidos.")
        print("=" * 70)
        
        inicio_total = datetime.now()
        
        try:
            # 1. Buscar IDs de todas as vendas
            print("\n1️⃣ BUSCANDO IDS DAS VENDAS...")
            query = text("""
                SELECT bling_id, dados_json
                FROM raw.vendas_raw
                WHERE empresa_id = :empresa_id
                ORDER BY bling_id
            """)
            
            resultado = self.session.execute(query, {"empresa_id": self.empresa_id})
            vendas = resultado.fetchall()
            
            if not vendas:
                print("❌ Nenhuma venda encontrada no banco")
                return
            
            total_vendas = len(vendas)
            print(f"✅ {total_vendas} vendas encontradas")
            
            # 2. Identificar quais precisam de atualização
            print("\n2️⃣ VERIFICANDO QUAIS VENDAS JÁ TÊM ITENS...")
            vendas_sem_itens = []
            vendas_com_itens = 0
            
            for venda in vendas:
                if 'itens' not in venda.dados_json or not venda.dados_json.get('itens'):
                    vendas_sem_itens.append(venda.bling_id)
                else:
                    vendas_com_itens += 1
            
            print(f"✅ {vendas_com_itens} vendas já têm itens")
            print(f"🔄 {len(vendas_sem_itens)} vendas precisam ser atualizadas")
            
            if not vendas_sem_itens:
                print("\n🎉 Todas as vendas já têm detalhes completos!")
                return
            
            # 3. Buscar detalhes das vendas sem itens
            print(f"\n3️⃣ BUSCANDO DETALHES DE {len(vendas_sem_itens)} VENDAS...")
            print(f"⏱️  Tempo estimado: ~{(len(vendas_sem_itens) * delay_entre_requests / 60):.1f} minutos")
            print("=" * 70)
            
            stats = {
                'processadas': 0,
                'atualizadas': 0,
                'com_itens': 0,
                'sem_itens': 0,
                'erros': 0
            }
            
            inicio_processamento = datetime.now()
            
            for i, venda_id in enumerate(vendas_sem_itens, 1):
                # Progresso
                if i % 50 == 0 or i == 1:
                    tempo_decorrido = datetime.now() - inicio_processamento
                    velocidade = i / tempo_decorrido.total_seconds() if tempo_decorrido.total_seconds() > 0 else 0
                    tempo_restante = (len(vendas_sem_itens) - i) / velocidade if velocidade > 0 else 0
                    
                    print(f"\n📊 Progresso: {i}/{len(vendas_sem_itens)} ({(i/len(vendas_sem_itens)*100):.1f}%)")
                    print(f"   ⏱️  Tempo decorrido: {tempo_decorrido}")
                    print(f"   ⏳ Tempo restante estimado: {tempo_restante/60:.1f} minutos")
                    print(f"   🚀 Velocidade: {velocidade:.2f} vendas/segundo")
                
                # Buscar detalhes
                detalhes = self.buscar_detalhes_venda(venda_id)
                
                if detalhes:
                    # Verificar se tem itens
                    itens = detalhes.get('itens', [])
                    
                    if itens:
                        stats['com_itens'] += 1
                        if i % 50 == 0:
                            print(f"   ✅ Venda {venda_id}: {len(itens)} itens encontrados")
                    else:
                        stats['sem_itens'] += 1
                        if i % 50 == 0:
                            print(f"   ⚠️  Venda {venda_id}: sem itens")
                    
                    # Atualizar no banco
                    if self.atualizar_venda_com_detalhes(venda_id, detalhes):
                        stats['atualizadas'] += 1
                    else:
                        stats['erros'] += 1
                else:
                    stats['erros'] += 1
                
                stats['processadas'] += 1
                
                # Commit em lotes
                if i % batch_size == 0:
                    self.session.commit()
                    print(f"   💾 Commit realizado ({i} vendas processadas)")
                
                # Delay entre requisições
                time.sleep(delay_entre_requests)
            
            # Commit final
            self.session.commit()
            print("\n   💾 Commit final realizado")
            
            # Relatório final
            fim_total = datetime.now()
            tempo_total = fim_total - inicio_total
            
            print(f"\n{'='*70}")
            print("🎉 EXTRAÇÃO DE DETALHES CONCLUÍDA!")
            print(f"{'='*70}")
            
            print(f"\n⏱️  TEMPOS:")
            print(f"   • Tempo total: {tempo_total}")
            print(f"   • Tempo de processamento: {datetime.now() - inicio_processamento}")
            
            print(f"\n📊 ESTATÍSTICAS:")
            print(f"   • Vendas processadas: {stats['processadas']}")
            print(f"   • Vendas atualizadas: {stats['atualizadas']}")
            print(f"   • Vendas com itens: {stats['com_itens']}")
            print(f"   • Vendas sem itens: {stats['sem_itens']}")
            print(f"   • Erros: {stats['erros']}")
            
            print(f"\n📈 RESUMO:")
            print(f"   • Total de vendas no banco: {total_vendas}")
            print(f"   • Vendas com detalhes completos: {vendas_com_itens + stats['atualizadas']}")
            
            if stats['com_itens'] > 0:
                media_itens = stats['com_itens'] / stats['processadas'] * 100
                print(f"   • Taxa de vendas com itens: {media_itens:.1f}%")
            
            print(f"\n🚀 Performance: {stats['processadas']/tempo_total.total_seconds():.2f} vendas/segundo")
            
            # Validação final
            print(f"\n4️⃣ VALIDAÇÃO FINAL...")
            query_validacao = text("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN dados_json ? 'itens' THEN 1 ELSE 0 END) as com_campo_itens,
                    SUM(CASE WHEN jsonb_array_length(dados_json->'itens') > 0 THEN 1 ELSE 0 END) as com_itens_preenchidos
                FROM raw.vendas_raw
                WHERE empresa_id = :empresa_id
            """)
            
            validacao = self.session.execute(query_validacao, {"empresa_id": self.empresa_id}).fetchone()
            
            print(f"   • Total de vendas: {validacao.total}")
            print(f"   • Com campo 'itens': {validacao.com_campo_itens}")
            print(f"   • Com itens preenchidos: {validacao.com_itens_preenchidos}")
            
            if validacao.com_itens_preenchidos > 0:
                taxa = (validacao.com_itens_preenchidos / validacao.total) * 100
                print(f"   • Taxa de cobertura: {taxa:.1f}%")
            
            print(f"\n✅ Processo concluído com sucesso!")
            
        except KeyboardInterrupt:
            print("\n\n⚠️  PROCESSO INTERROMPIDO PELO USUÁRIO")
            print("💾 Fazendo commit dos dados processados até agora...")
            self.session.commit()
            print("✅ Dados salvos. Você pode continuar de onde parou executando novamente.")
            
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO: {e}")
            self.session.rollback()
            raise
            
        finally:
            self.session.close()


if __name__ == "__main__":
    try:
        # ATENÇÃO: Passar empresa_id aqui!
        empresa_id = 1  # TODO: Ajustar conforme necessário
        
        extrator = VendasDetalhesExtractor(empresa_id)
        extrator.executar_extracao_detalhes(
            delay_entre_requests=0.4,  # Respeitar rate limit (2.5 req/s)
            batch_size=100             # Commit a cada 100 vendas
        )
        
    except KeyboardInterrupt:
        print("\n⚠️  Execução interrompida")
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        raise