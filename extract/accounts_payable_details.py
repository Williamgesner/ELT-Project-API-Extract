"""
EXTRATOR DE DETALHES COMPLETOS DE CONTAS A PAGAR
================================================================================
Responsável por: buscar detalhes completos de cada conta a pagar (incluindo categoria)
e atualizar o JSON na tabela contas_pagar_raw

GARANTIA DE CONTINUIDADE:
- Verifica dinamicamente quais contas ainda precisam ser processadas
- Processa em lotes pequenos com commits frequentes
- Continua automaticamente de onde parou após erros de API

Fluxo:
1. Loop contínuo que verifica contas pendentes
2. Para cada lote de contas pendentes:
   - Buscar detalhes completos na API: GET /contas/pagar/{id}
   - Extrair categoria.id do JSON
   - Atualizar o JSON com os dados completos
   - Fazer commit no banco
3. Repetir até não haver mais contas pendentes

IMPORTANTE: Este processo é necessário porque o endpoint de listagem (/contas/pagar)
não retorna a categoria, apenas o endpoint individual (/contas/pagar/{id})
"""

import requests
import time
from datetime import datetime
from config.settings import endpoints
from config.database import Session
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from models.accounts_payable_raw import ContasPagarRaw

class ContasPagarDetalhesExtractor:
    """
    Extrator específico para buscar detalhes completos de contas a pagar
    COM GARANTIA DE RETOMADA AUTOMÁTICA
    """
    
    def __init__(self, api_key, empresa_id):
        """
        Args:
            api_key: Token de autenticação da API Bling
            empresa_id: ID da empresa na tabela dim_empresas
        """
        self.base_url = endpoints['contas_pagar']
        self.session = Session()
        self.empresa_id = empresa_id
        
        # Sobrescrever headers com a API key específica
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def buscar_detalhes_conta(self, conta_id, tentativas=3):
        """
        Busca detalhes completos de uma conta a pagar específica
        
        Args:
            conta_id: ID da conta no Bling
            tentativas: Número de tentativas em caso de erro
            
        Returns:
            dict: Dados completos da conta
            str: "NOT_FOUND" se 404
            None: se erro
        """
        url = f"{self.base_url}/{conta_id}"
        
        for tentativa in range(tentativas):
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                
                if response.status_code == 200:
                    dados = response.json()
                    return dados.get('data', {})
                elif response.status_code == 404:
                    # Retornar flag especial para 404
                    return "NOT_FOUND"
                elif response.status_code == 401:
                    print(f"   🔑 ERRO DE AUTENTICAÇÃO - API Key expirada!")
                    print(f"   💡 Atualize a chave e execute novamente - o processo continuará de onde parou")
                    raise Exception("API Key expirada - atualize e reinicie")
                else:
                    print(f"   ❌ Erro HTTP {response.status_code} na conta {conta_id}")
                    
                    if tentativa < tentativas - 1:
                        time.sleep(0.5 * (tentativa + 1))
                        continue
                    else:
                        return None
                        
            except requests.exceptions.RequestException as e:
                print(f"   ❌ Erro de conexão ao buscar conta {conta_id}: {e}")
                
                if tentativa < tentativas - 1:
                    time.sleep(0.5 * (tentativa + 1))
                else:
                    return None
            except Exception as e:
                # Se for erro de autenticação, propagar
                if "API Key expirada" in str(e):
                    raise
                print(f"   ❌ Erro inesperado ao buscar conta {conta_id}: {e}")
                return None
        
        return None
    
    def atualizar_conta_com_detalhes(self, conta_id, dados_completos):
        """
        Atualiza o registro da conta com os dados completos
        
        Args:
            conta_id: ID da conta no Bling
            dados_completos: Dados completos da API (incluindo categoria)
        """
        try:
            stmt = insert(ContasPagarRaw).values(
                bling_id=conta_id,
                empresa_id=self.empresa_id,
                dados_json=dados_completos,
                data_ingestao=datetime.now(),
                status_processamento='pendente'  # Marca como pendente para reprocessar
            )
            
            stmt = stmt.on_conflict_do_update(
                index_elements=['bling_id', 'empresa_id'], 
                set_={
                    'dados_json': stmt.excluded.dados_json,
                    'data_ingestao': stmt.excluded.data_ingestao,
                    'status_processamento': 'pendente'  # Reprocessar após enriquecimento
                }
            )
            
            self.session.execute(stmt)
            return True
            
        except Exception as e:
            print(f"   ❌ Erro ao salvar conta {conta_id}: {e}")
            return False
    
    def marcar_conta_sem_categoria(self, conta_id, motivo="sem_categoria"):
        """
        Marca conta como processada quando não consegue obter categoria
        Motivos: 404 (não existe mais), erro de API, etc.
        
        Args:
            conta_id: ID da conta no Bling
            motivo: Motivo pelo qual não tem categoria
        """
        try:
            query = text("""
                UPDATE raw.contas_pagar_raw
                SET dados_json = jsonb_set(
                        COALESCE(dados_json, '{}'::jsonb),
                        '{_metadata_sem_categoria}', 
                        to_jsonb(:motivo::text),
                        true
                    ),
                    status_processamento = 'processado'
                WHERE bling_id = :conta_id
                  AND empresa_id = :empresa_id
            """)
            self.session.execute(query, {
                "conta_id": conta_id,
                "empresa_id": self.empresa_id,
                "motivo": motivo
            })
            return True
        except Exception as e:
            print(f"   ❌ Erro ao marcar conta {conta_id}: {e}")
            return False
    
    def executar_extracao_detalhes(self, delay_entre_requests=0.35, batch_size=100):
        """
        Executa a extração de detalhes para todas as contas a pagar
        
        GARANTIA DE CONTINUIDADE: Verifica dinamicamente quais contas ainda precisam
        ser processadas, permitindo retomar de onde parou após erros de API.
        
        Args:
            delay_entre_requests: Tempo entre requisições (respeitar rate limit)
            batch_size: Quantas contas processar antes de fazer commit
        """
        print(f"\n🔍 EXTRATOR DE DETALHES COMPLETOS DE CONTAS A PAGAR (Empresa ID: {self.empresa_id})")
        print("=" * 70)
        print("Este processo busca os detalhes de CADA conta individualmente")
        print("para obter a categoria (categoria.id).")
        print("✅ RETOMADA AUTOMÁTICA: Continua de onde parou em caso de erro")
        print("=" * 70)
        
        inicio_total = datetime.now()
        stats = {
            'processadas': 0,
            'atualizadas': 0,
            'com_categoria': 0,
            'sem_categoria': 0,
            'erros': 0,
            'ja_tinham_categoria': 0,
            'contas_404': 0  # Contas que não existem mais no Bling
        }
        
        total_contas = 0
        
        try:
            # Loop principal - continua até não haver mais contas para processar
            inicio_processamento = datetime.now()
            iteracao = 0
            
            while True:
                iteracao += 1
                
                # 1. BUSCAR DINAMICAMENTE contas que ainda precisam ser processadas
                print(f"\n{'='*70}")
                print(f"🔄 ITERAÇÃO {iteracao} - Verificando contas pendentes...")
                
                query = text("""
                    SELECT bling_id
                    FROM raw.contas_pagar_raw
                    WHERE empresa_id = :empresa_id
                      AND status_processamento != 'processado'
                      AND (dados_json IS NULL 
                           OR NOT (dados_json ? 'categoria')
                           OR dados_json->'categoria' IS NULL
                           OR dados_json->'categoria' = 'null'::jsonb)
                    ORDER BY bling_id
                    LIMIT :batch_size
                """)
                
                resultado = self.session.execute(query, {
                    "empresa_id": self.empresa_id,
                    "batch_size": batch_size
                })
                contas_pendentes = [row.bling_id for row in resultado.fetchall()]
                
                if not contas_pendentes:
                    print("✅ Nenhuma conta pendente encontrada!")
                    break
                
                print(f"📋 {len(contas_pendentes)} contas pendentes neste lote")
                
                # 2. Contar total de contas (para estatísticas) - só na primeira iteração
                if iteracao == 1:
                    query_total = text("""
                        SELECT 
                            COUNT(*) as total,
                            SUM(CASE WHEN dados_json ? 'categoria' 
                                     AND dados_json->'categoria' IS NOT NULL 
                                     AND dados_json->'categoria' != 'null'::jsonb 
                                THEN 1 ELSE 0 END) as com_categoria
                        FROM raw.contas_pagar_raw
                        WHERE empresa_id = :empresa_id
                    """)
                    totais = self.session.execute(query_total, {"empresa_id": self.empresa_id}).fetchone()
                    total_contas = totais.total
                    stats['ja_tinham_categoria'] = totais.com_categoria
                    
                    print(f"\n📊 SITUAÇÃO INICIAL:")
                    print(f"   • Total de contas: {total_contas}")
                    print(f"   • Já com categoria: {stats['ja_tinham_categoria']}")
                    print(f"   • Pendentes: {total_contas - stats['ja_tinham_categoria']}")
                    print(f"   ⏱️  Tempo estimado: ~{((total_contas - stats['ja_tinham_categoria']) * delay_entre_requests / 60):.1f} minutos")
                
                # 3. Processar este lote
                print(f"\n{'='*70}")
                print(f"🔄 PROCESSANDO LOTE {iteracao}...")
                
                lote_inicio = datetime.now()
                
                for i, conta_id in enumerate(contas_pendentes, 1):
                    # Progresso dentro do lote
                    if i % 10 == 0:
                        progresso_lote = (i / len(contas_pendentes)) * 100
                        tempo_lote = datetime.now() - lote_inicio
                        velocidade = i / tempo_lote.total_seconds() if tempo_lote.total_seconds() > 0 else 0
                        
                        # Estatísticas globais
                        if total_contas > 0:
                            contas_processadas_total = stats['ja_tinham_categoria'] + stats['processadas']
                            progresso_global = (contas_processadas_total / total_contas) * 100
                            
                            print(f"\n   📊 Lote {iteracao}: {i}/{len(contas_pendentes)} ({progresso_lote:.1f}%)")
                            print(f"   📈 Global: {contas_processadas_total}/{total_contas} ({progresso_global:.1f}%)")
                            print(f"   🚀 Velocidade: {velocidade:.2f} contas/seg")
                    
                    # Buscar detalhes
                    detalhes = self.buscar_detalhes_conta(conta_id)
                    
                    if detalhes == "NOT_FOUND":
                        # Conta não existe mais no Bling (404)
                        # Marcar como processada para não tentar de novo
                        if i % 10 == 0:
                            print(f"   🗑️  Conta {conta_id}: não existe mais no Bling (404)")
                        
                        if self.marcar_conta_sem_categoria(conta_id, motivo="404_nao_encontrada"):
                            self.session.commit()  # Commit imediato
                            self.session.expire_all()  # Limpar cache da sessão
                            stats['contas_404'] += 1
                        else:
                            stats['erros'] += 1
                            
                    elif detalhes:
                        # Sucesso - conta existe e retornou dados
                        categoria = detalhes.get('categoria', {})
                        
                        if categoria and categoria.get('id'):
                            stats['com_categoria'] += 1
                            if i % 10 == 0:
                                print(f"   ✅ Conta {conta_id}: categoria {categoria.get('id')}")
                        else:
                            stats['sem_categoria'] += 1
                            if i % 10 == 0:
                                print(f"   ⚠️  Conta {conta_id}: sem categoria no Bling")
                        
                        # Atualizar no banco com os dados completos
                        if self.atualizar_conta_com_detalhes(conta_id, detalhes):
                            stats['atualizadas'] += 1
                        else:
                            stats['erros'] += 1
                    else:
                        # Erro de API (timeout, 500, etc)
                        # Marcar como processada para não travar
                        if self.marcar_conta_sem_categoria(conta_id, motivo="erro_api"):
                            stats['erros'] += 1
                        else:
                            stats['erros'] += 1
                    
                    stats['processadas'] += 1
                    
                    # Delay entre requisições
                    time.sleep(delay_entre_requests)
                
                # 4. Commit após processar o lote
                self.session.commit()
                tempo_lote = datetime.now() - lote_inicio
                print(f"\n   💾 Lote {iteracao} finalizado e salvo no banco!")
                print(f"   ⏱️  Tempo do lote: {tempo_lote}")
                print(f"   ✅ {len(contas_pendentes)} contas processadas neste lote")
            
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
            print(f"   • Contas processadas nesta execução: {stats['processadas']}")
            print(f"   • Contas atualizadas: {stats['atualizadas']}")
            print(f"   • Contas com categoria: {stats['com_categoria']}")
            print(f"   • Contas sem categoria: {stats['sem_categoria']}")
            print(f"   • Contas não encontradas (404): {stats['contas_404']}")
            print(f"   • Erros: {stats['erros']}")
            
            if stats['contas_404'] > 0:
                print(f"\n💡 NOTA sobre contas 404:")
                print(f"   • {stats['contas_404']} contas retornaram 404 (não existem mais no Bling)")
                print(f"   • Os dados originais dessas contas foram MANTIDOS no banco")
                print(f"   • Apenas não foi possível enriquecer com categoria")
            
            if total_contas > 0:
                print(f"\n📈 RESUMO:")
                print(f"   • Total de contas no banco: {total_contas}")
                print(f"   • Já tinham categoria: {stats['ja_tinham_categoria']}")
                print(f"   • Contas com categoria agora: {stats['ja_tinham_categoria'] + stats['atualizadas']}")
                
                if stats['com_categoria'] > 0:
                    taxa_categoria = stats['com_categoria'] / stats['processadas'] * 100
                    print(f"   • Taxa de contas com categoria (nesta execução): {taxa_categoria:.1f}%")
            
            if stats['processadas'] > 0:
                print(f"\n🚀 Performance: {stats['processadas']/tempo_total.total_seconds():.2f} contas/segundo")
            
            # Validação final
            print(f"\n4️⃣ VALIDAÇÃO FINAL...")
            query_validacao = text("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN dados_json ? 'categoria' 
                             AND dados_json->'categoria' IS NOT NULL 
                             AND dados_json->'categoria' != 'null'::jsonb 
                        THEN 1 ELSE 0 END) as com_categoria,
                    SUM(CASE WHEN status_processamento = 'processado'
                             AND (NOT (dados_json ? 'categoria')
                                  OR dados_json->'categoria' IS NULL
                                  OR dados_json->'categoria' = 'null'::jsonb)
                        THEN 1 ELSE 0 END) as processadas_sem_categoria,
                    SUM(CASE WHEN status_processamento != 'processado'
                        THEN 1 ELSE 0 END) as pendentes
                FROM raw.contas_pagar_raw
                WHERE empresa_id = :empresa_id
            """)
            
            validacao = self.session.execute(query_validacao, {"empresa_id": self.empresa_id}).fetchone()
            
            print(f"✅ Validação:")
            print(f"   • Total de contas: {validacao.total}")
            print(f"   • Com categoria: {validacao.com_categoria} ({validacao.com_categoria/validacao.total*100:.1f}%)")
            print(f"   • Processadas sem categoria: {validacao.processadas_sem_categoria}")
            print(f"   • Ainda pendentes: {validacao.pendentes}")
            
            # Calcular quantas contas ainda precisam de categoria
            contas_sem_categoria = validacao.total - validacao.com_categoria

            if contas_sem_categoria == 0:
                print(f"\n🎉 SUCESSO TOTAL:")
                print(f"   • TODAS as {validacao.total} contas têm categoria!")
                print(f"\n💡 PRÓXIMOS PASSOS:")
                print(f"   1. Processar categorias:")
                print(f"      python main_transform_categories_payable.py")
                print(f"   2. Reprocessar contas a pagar (agora com categoria!):")
                print(f"      python main_transform_accounts_payable.py")
            else:
                print(f"\n⚠️  ATENÇÃO:")
                print(f"   • {contas_sem_categoria} contas ainda SEM categoria")
                print(f"   • {validacao.com_categoria} contas COM categoria ({validacao.com_categoria/validacao.total*100:.1f}%)")
                print(f"\n💡 Execute novamente para tentar buscar as categorias faltantes")

            # Informação adicional sobre status de processamento
            if validacao.pendentes > 0:
                print(f"\n📋 STATUS:")
                print(f"   • {validacao.pendentes} contas com status='pendente' (aguardando transformação)")
                print(f"   • Isto é NORMAL - execute a transformação para processar")
            
        except Exception as e:
            print(f"\n❌ Erro durante extração: {e}")
            print(f"\n💾 Fazendo commit das contas processadas até agora...")
            self.session.commit()
            
            print(f"\n💡 PARA CONTINUAR:")
            print(f"   1. Se o erro foi de API Key expirada, atualize a chave em config.settings")
            print(f"   2. Execute novamente: python main_update_accounts_payable_detail.py")
            print(f"   3. O processo continuará automaticamente de onde parou!")
            
            raise
        finally:
            self.session.close()