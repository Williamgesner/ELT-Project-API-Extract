"""
EXTRATOR DE DETALHES DE NFE
==============================================
Responsável por: enriquecer NFe com valorNota

OBJETIVO: Adicionar campo 'valorNota' no JSON de TODAS as NFe

Fluxo:
1. Ler IDs de todas as NFe em nfe_raw
2. Verificar quais JÁ têm 'valorNota' (pular essas)
3. Para cada NFe sem valorNota, buscar detalhes na API
4. Atualizar o JSON existente (não apaga nada!)
"""

import requests
import time
import json
from datetime import datetime
from config.settings import endpoints, headers
from config.database import Session
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from models.nfe_raw import NFeRaw

class NFeDetalhesExtractor:
    """
    Extrator COMPLETO para enriquecer NFe com valorNota
    """
    
    def __init__(self):
        self.base_url = endpoints['nfe']
        self.headers = headers
        self.session = Session()
    
    def buscar_detalhes_nfe(self, nfe_id, tentativas=3):
        """
        Busca detalhes completos de uma NFe específica
        
        Args:
            nfe_id: ID da NFe no Bling
            tentativas: Número de tentativas em caso de erro
            
        Returns:
            dict: Dados completos da NFe ou None se falhar
        """
        url = f"{self.base_url}/{nfe_id}"
        
        for tentativa in range(tentativas):
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                
                if response.status_code == 200:
                    dados = response.json()
                    return dados.get('data', {})
                elif response.status_code == 404:
                    print(f"   ⚠️  NFe {nfe_id} não encontrada (404)")
                    return None
                elif response.status_code == 429:
                    print(f"   ⚠️  Rate limit atingido. Aguardando 60 segundos...")
                    time.sleep(60)
                    continue
                else:
                    print(f"   ❌ Erro HTTP {response.status_code} na NFe {nfe_id}")
                    
                    if tentativa < tentativas - 1:
                        time.sleep(1 * (tentativa + 1))
                        continue
                    else:
                        return None
                        
            except Exception as e:
                print(f"   ❌ Erro ao buscar NFe {nfe_id}: {e}")
                
                if tentativa < tentativas - 1:
                    time.sleep(1 * (tentativa + 1))
                else:
                    return None
        
        return None
    
    def atualizar_nfe_com_valorNota(self, nfe_id, dados_completos):
        """
        Atualiza o registro da NFe com os dados completos
        
        Args:
            nfe_id: ID da NFe no Bling
            dados_completos: Dados completos da API (com valorNota)
        """
        try:
            stmt = insert(NFeRaw).values(
                bling_id=nfe_id,
                dados_json=dados_completos,
                data_ingestao=datetime.now()
            )
            
            stmt = stmt.on_conflict_do_update(
                index_elements=['bling_id'],
                set_={
                    'dados_json': stmt.excluded.dados_json,
                    'data_ingestao': stmt.excluded.data_ingestao
                }
            )
            
            self.session.execute(stmt)
            return True
            
        except Exception as e:
            print(f"   ❌ Erro ao salvar NFe {nfe_id}: {e}")
            return False
    
    def executar_enriquecimento_completo(self, delay_entre_requests=0.35, batch_size=100):
        """
        Executa o enriquecimento de TODAS as NFe
        
        Args:
            delay_entre_requests: Tempo entre requisições (respeitar rate limit)
            batch_size: Quantas NFe processar antes de fazer commit
        """
        print("\n💎 ENRIQUECIMENTO COMPLETO DE NFe")
        print("=" * 70)
        print("Este processo adiciona 'valorNota' em TODAS as NFe")
        print("=" * 70)
        
        # Confirmação antes de iniciar
        print("\n⚠️  ATENÇÃO:")
        print("   • Este processo vai buscar TODAS as NFe do banco")
        print("   • Vai levar várias horas para completar")
        print("   • Você pode interromper (Ctrl+C) e continuar depois")
        print("   • O progresso é salvo automaticamente")
        
        inicio_total = datetime.now()
        
        try:
            # 1. Buscar IDs de TODAS as NFe
            print(f"\n1️⃣ BUSCANDO TODAS AS NFe DO BANCO...")
            query = text("""
                SELECT bling_id, dados_json
                FROM raw.nfe_raw
                ORDER BY bling_id
            """)
            
            resultado = self.session.execute(query)
            nfes = resultado.fetchall()
            
            if not nfes:
                print("❌ Nenhuma NFe encontrada no banco")
                return
            
            total_nfes = len(nfes)
            print(f"✅ {total_nfes} NFe encontradas no banco")
            
            # 2. Identificar quais precisam de enriquecimento
            print("\n2️⃣ VERIFICANDO QUAIS NFe JÁ TÊM valorNota...")
            nfes_sem_valor = []
            nfes_com_valor = 0
            
            for nfe in nfes:
                # Verificar se tem valorNota
                tem_valor = 'valorNota' in nfe.dados_json
                
                if not tem_valor:
                    nfes_sem_valor.append(nfe.bling_id)
                else:
                    nfes_com_valor += 1
            
            print(f"✅ {nfes_com_valor} NFe já têm valorNota")
            print(f"🔄 {len(nfes_sem_valor)} NFe precisam ser enriquecidas")
            
            if not nfes_sem_valor:
                print("\n🎉 Todas as NFe já têm valorNota!")
                return
            
            # Estimativa de tempo
            tempo_estimado_min = (len(nfes_sem_valor) * delay_entre_requests) / 60
            print(f"\n⏱️  TEMPO ESTIMADO: ~{tempo_estimado_min:.0f} minutos ({tempo_estimado_min/60:.1f} horas)")
            
            # 3. Enriquecer as NFe sem valorNota
            print(f"\n3️⃣ ENRIQUECENDO {len(nfes_sem_valor)} NFe...")
            print("=" * 70)
            
            stats = {
                'processadas': 0,
                'atualizadas': 0,
                'com_valor_nota': 0,
                'erros': 0,
                'rate_limit': 0
            }
            
            inicio_processamento = datetime.now()
            ultima_atualizacao = inicio_processamento
            
            for i, nfe_id in enumerate(nfes_sem_valor, 1):
                # Progresso a cada 50 registros
                if i % 50 == 0 or i == 1:
                    tempo_decorrido = datetime.now() - inicio_processamento
                    velocidade = i / tempo_decorrido.total_seconds() if tempo_decorrido.total_seconds() > 0 else 0
                    tempo_restante = (len(nfes_sem_valor) - i) / velocidade if velocidade > 0 else 0
                    
                    print(f"\n📊 Progresso: {i}/{len(nfes_sem_valor)} ({(i/len(nfes_sem_valor)*100):.1f}%)")
                    print(f"   ⏱️  Tempo decorrido: {tempo_decorrido}")
                    print(f"   ⏳ Tempo restante: ~{tempo_restante/60:.0f} minutos")
                    print(f"   🚀 Velocidade: {velocidade:.2f} NFe/segundo")
                    print(f"   ✅ Sucesso: {stats['atualizadas']}")
                    print(f"   ❌ Erros: {stats['erros']}")
                
                # Buscar detalhes
                detalhes = self.buscar_detalhes_nfe(nfe_id)
                
                if detalhes:
                    # Verificar se tem valorNota
                    tem_valor_nota = 'valorNota' in detalhes
                    
                    if tem_valor_nota:
                        stats['com_valor_nota'] += 1
                    
                    # Atualizar no banco (mesmo sem valorNota, atualiza o JSON)
                    if self.atualizar_nfe_com_valorNota(nfe_id, detalhes):
                        stats['atualizadas'] += 1
                    else:
                        stats['erros'] += 1
                else:
                    stats['erros'] += 1
                
                stats['processadas'] += 1
                
                # Commit em lotes
                if i % batch_size == 0:
                    self.session.commit()
                    print(f"\n   💾 Commit realizado ({i} NFe processadas)")
                    
                    # Atualizar tempo da última atualização
                    ultima_atualizacao = datetime.now()
                
                # Delay entre requisições
                time.sleep(delay_entre_requests)
            
            # Commit final
            self.session.commit()
            print("\n   💾 Commit final realizado")
            
            # Relatório final
            fim_total = datetime.now()
            tempo_total = fim_total - inicio_total
            
            print(f"\n{'='*70}")
            print("🎉 ENRIQUECIMENTO COMPLETO CONCLUÍDO!")
            print(f"{'='*70}")
            
            print(f"\n⏱️  TEMPOS:")
            print(f"   • Tempo total: {tempo_total}")
            print(f"   • Tempo de processamento: {datetime.now() - inicio_processamento}")
            
            print(f"\n📊 ESTATÍSTICAS:")
            print(f"   • NFe processadas: {stats['processadas']}")
            print(f"   • NFe atualizadas: {stats['atualizadas']}")
            print(f"   • NFe com valorNota: {stats['com_valor_nota']}")
            print(f"   • Erros: {stats['erros']}")
            
            print(f"\n📈 ANÁLISE:")
            if stats['processadas'] > 0:
                taxa_valor = (stats['com_valor_nota'] / stats['processadas']) * 100
                taxa_sucesso = (stats['atualizadas'] / stats['processadas']) * 100
                
                print(f"   • Taxa com valorNota: {taxa_valor:.1f}%")
                print(f"   • Taxa de sucesso: {taxa_sucesso:.1f}%")
            
            print(f"\n🚀 Performance: {stats['processadas']/tempo_total.total_seconds():.2f} NFe/segundo")
            
            # Validação final
            print(f"\n4️⃣ VALIDAÇÃO FINAL...")
            query_validacao = text("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN dados_json ? 'valorNota' THEN 1 ELSE 0 END) as com_valor_nota
                FROM raw.nfe_raw
            """)
            
            validacao = self.session.execute(query_validacao).fetchone()
            
            print(f"   • Total de NFe no banco: {validacao.total}")
            print(f"   • NFe com valorNota: {validacao.com_valor_nota}")
            
            if validacao.com_valor_nota > 0:
                taxa = (validacao.com_valor_nota / validacao.total) * 100
                print(f"   • Taxa de cobertura: {taxa:.1f}%")
            
            print(f"\n{'='*70}")
            print("💡 PRÓXIMOS PASSOS:")
            print(f"{'='*70}")
            print("1. ✅ Enriquecimento concluído")
            print("2. ✅ Todas as NFe têm valorNota")
            print("3. 🚀 Execute: explore_nfe_raw_final.py")
            print("4. 📊 Isso vai gerar a tabela final com numero_pedido")
            print(f"{'='*70}")
            
            print(f"\n✅ Enriquecimento concluído com sucesso!")
            
        except KeyboardInterrupt:
            print("\n\n⚠️  PROCESSO INTERROMPIDO PELO USUÁRIO")
            print("💾 Fazendo commit dos dados processados até agora...")
            self.session.commit()
            print("✅ Dados salvos. Você pode continuar executando novamente.")
            print(f"📊 Processadas: {stats['processadas']}/{len(nfes_sem_valor)}")
            
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO: {e}")
            self.session.rollback()
            raise
            
        finally:
            self.session.close()


if __name__ == "__main__":
    try:
        print("\n" + "=" * 70)
        print("💎 ENRIQUECIMENTO COMPLETO DE NFe")
        print("=" * 70)
        print("\nEste script vai adicionar 'valorNota' em TODAS as NFe")
        print("Tempo estimado: 4-5 horas para ~16.000 registros")
        print("\n" + "=" * 70)
        
        extrator = NFeDetalhesExtractor()
        extrator.executar_enriquecimento_completo(
            delay_entre_requests=0.35,  # Respeitar rate limit (2.5 req/s)
            batch_size=100              # Commit a cada 100 NFe
        )
        
    except KeyboardInterrupt:
        print("\n⚠️  Execução interrompida")
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        raise