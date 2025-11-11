# Responsável por: extrair NFe (entrada E saída)

from datetime import datetime
import requests
import time
from core.base_extractor import BaseExtractor
from models.nfe_raw import NFeRaw
from config.settings import endpoints, headers

# =====================================================
# 1. EXTRATOR DE NFe (COM SUPORTE A ENTRADA E SAÍDA)
# =====================================================

class NFeExtractor(BaseExtractor):
    
    """
    Extrator de NFe com suporte a tipos
    - Extrai lista completa da API /nfe
    - ENTRADA (tipo 0) e SAÍDA (tipo 1)
    - Relacionamento com pedidos via vendas_raw.notaFiscal.id
    """
    
    def __init__(self):
        """
        Inicializa o extrator de NFe
        """
        super().__init__(endpoints['nfe'], NFeRaw)
    
    def extract_dados_bling_paginado_com_tipo(self, tipo, limite_por_pagina=100, 
                                               delay_entre_requests=0.35, max_paginas=1000, 
                                               max_tentativas=3):
        """
        Extrai NFe de um tipo específico da API Bling
        
        Args:
            tipo (int): 0 para Entrada, 1 para Saída
            limite_por_pagina (int): Registros por página (máx 100)
            delay_entre_requests (float): Delay entre requisições
            max_paginas (int): Limite de páginas
            max_tentativas (int): Tentativas por página
        
        Returns:
            list: Lista de NFe extraídas
        """
        todos_registros = []
        pagina_atual = 1
        total_paginas = None
        registros_unicos = set()
        
        tipo_nome = "ENTRADA" if tipo == 0 else "SAÍDA"
        print(f"Extraindo NFe de {tipo_nome} (tipo={tipo})...")

        while pagina_atual <= max_paginas:
            params = {
                "limite": limite_por_pagina,
                "pagina": pagina_atual,
                "tipo": tipo  # ← FILTRO CRÍTICO
            }

            print(f"   Processando página {pagina_atual}{'/' + str(total_paginas) if total_paginas else ''}...")
            
            sucesso = False
            for tentativa in range(max_tentativas):
                try:
                    response = requests.get(
                        self.base_url,
                        headers=headers,
                        params=params,
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        registros = data.get('data', [])
                        
                        # Primeira página: captura total de páginas
                        if pagina_atual == 1:
                            # A API Bling pode retornar totalPages de diferentes formas
                            if 'totalPages' in data:
                                total_paginas = data['totalPages']
                            elif registros and isinstance(registros, list) and len(registros) > 0:
                                # Alguns endpoints retornam no primeiro item da lista
                                if isinstance(registros[0], dict) and 'totalPages' in registros[0]:
                                    total_paginas = registros[0]['totalPages']
                            
                            if total_paginas:
                                print(f"   Total de páginas: {total_paginas}")
                        
                        # Adiciona registros únicos
                        novos = 0
                        for registro in registros:
                            registro_id = registro.get('id')
                            if registro_id and registro_id not in registros_unicos:
                                registros_unicos.add(registro_id)
                                todos_registros.append(registro)
                                novos += 1
                        
                        print(f"   ✅ Página {pagina_atual}: {len(registros)} registros ({novos} novos)")
                        sucesso = True
                        break
                    
                    elif response.status_code == 401:
                        print(f"   ❌ Erro 401: Token expirado ou inválido")
                        print(f"   💡 Atualize a API_KEY no .env e execute novamente")
                        raise Exception("API Key expirada - atualize e reinicie")
                    
                    elif response.status_code == 429:
                        print(f"   ⚠️ Rate limit atingido - aguardando...")
                        time.sleep(2)
                        continue
                    
                    else:
                        print(f"   ❌ Erro HTTP {response.status_code}")
                        if tentativa < max_tentativas - 1:
                            time.sleep(0.5 * (tentativa + 1))
                            continue
                        
                except requests.exceptions.Timeout:
                    print(f"   ⏱️ Timeout na requisição")
                    if tentativa < max_tentativas - 1:
                        time.sleep(1)
                        continue
                    
                except requests.exceptions.RequestException as e:
                    print(f"   ❌ Erro de conexão: {e}")
                    if tentativa < max_tentativas - 1:
                        time.sleep(0.5 * (tentativa + 1))
                    else:
                        print(f"   ⚠️ FALHA TOTAL na página {pagina_atual} após {max_tentativas} tentativas")
                        return todos_registros
                
                except Exception as e:
                    # Se for erro de autenticação, propagar
                    if "API Key expirada" in str(e):
                        raise
                    print(f"   ❌ Erro inesperado: {e}")
                    return todos_registros
            
            if not sucesso:
                print(f"   ⚠️ Parando extração de {tipo_nome} - falha na página {pagina_atual}")
                break
            
            # Se não há mais registros ou última página
            if not registros or (total_paginas and pagina_atual >= total_paginas):
                break
            
            pagina_atual += 1
            time.sleep(delay_entre_requests)
        
        print(f"   🎉 {tipo_nome}: {len(todos_registros)} notas extraídas")
        return todos_registros
    
    def executar_extracao_completa(self):
        """
        Executa o processo completo de extração de NFe (TODAS - entrada e saída)
        """
        try:
            print("\n📄 EXTRAÇÃO: NOTAS FISCAIS ELETRÔNICAS (NFe)")
            print("=" * 60)
            inicio_extracao = datetime.now()

            # ===== EXTRAIR NOTAS DE SAÍDA (tipo=1) =====
            print("\n📤 EXTRAINDO NOTAS DE SAÍDA...")
            print("-" * 60)
            nfe_saida = self.extract_dados_bling_paginado_com_tipo(
                tipo=1,
                limite_por_pagina=100,
                delay_entre_requests=0.35,
                max_paginas=1000,
                max_tentativas=3
            )

            # ===== EXTRAIR NOTAS DE ENTRADA (tipo=0) =====
            print("\n📥 EXTRAINDO NOTAS DE ENTRADA...")
            print("-" * 60)
            nfe_entrada = self.extract_dados_bling_paginado_com_tipo(
                tipo=0,
                limite_por_pagina=100,
                delay_entre_requests=0.35,
                max_paginas=1000,
                max_tentativas=3
            )

            # ===== COMBINAR TODAS AS NFe =====
            todas_nfe = nfe_saida + nfe_entrada
            
            fim_extracao = datetime.now()
            tempo_extracao = fim_extracao - inicio_extracao

            if not todas_nfe:
                print("\n❌ Nenhuma NFe foi extraída. Verificar API ou configurações.")
                return
            
            print("\n" + "=" * 60)
            print("📊 EXTRAÇÃO CONCLUÍDA")
            print("=" * 60)
            print(f"⏱️ Tempo de extração: {tempo_extracao}")
            print(f"📤 NFe SAÍDA: {len(nfe_saida)}")
            print(f"📥 NFe ENTRADA: {len(nfe_entrada)}")
            print(f"📈 TOTAL: {len(todas_nfe)}")
            print(f"🚀 Velocidade: {len(todas_nfe)/tempo_extracao.total_seconds():.1f} notas/segundo")

            # Preparar dados
            print("\n📝 Preparando dados para salvamento...")
            dados_para_salvar = []
            
            for nfe in todas_nfe:
                dados_formatados = {
                    'bling_id': nfe['id'],
                    'dados_json': nfe  # JSON completo da lista
                }
                dados_para_salvar.append(dados_formatados)

            # Salvamento inteligente
            print(f"\n💾 Iniciando salvamento inteligente...")
            inicio_salvamento = datetime.now()
            
            stats = self.salvar_dados_postgres_bulk(dados_para_salvar)
            
            fim_salvamento = datetime.now()
            tempo_salvamento = fim_salvamento - inicio_salvamento
            tempo_total = fim_salvamento - inicio_extracao

            # Relatório final de performance
            print(f"\n🏁 EXECUÇÃO COMPLETA!")
            print(f"⏱️ Tempo total: {tempo_total}")
            print(f"⏱️ Tempo de salvamento: {tempo_salvamento}")
            print(f"🚀 Performance geral: {len(todas_nfe)/tempo_total.total_seconds():.1f} notas/segundo")
            
            # Eficiência do algoritmo
            if stats['total'] > 0:
                eficiencia = (stats['ignorados'] / stats['total']) * 100
                print(f"⚡ Eficiência: {eficiencia:.1f}% dos registros eram idênticos (evitou escritas desnecessárias)")

            # Estatísticas de NFe
            self._exibir_estatisticas_nfe(todas_nfe)

            print("\n🎉 Script de NFe executado com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️ Execução interrompida pelo usuário")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
            print("Script interrompido para análise do erro")
            print("Todos os dados extraídos até este ponto foram preservados")
            raise
    
    def _exibir_estatisticas_nfe(self, notas):
        """
        Exibe estatísticas das NFe extraídas
        """
        print(f"\n📊 ESTATÍSTICAS DAS NFe:")
        print("-" * 60)
        
        # Estatísticas por tipo
        tipos = {}
        for nfe in notas:
            tipo = nfe.get('tipo')
            if tipo == 0:
                tipo_nome = "Entrada"
            elif tipo == 1:
                tipo_nome = "Saída"
            else:
                tipo_nome = f"Tipo {tipo}"
            tipos[tipo_nome] = tipos.get(tipo_nome, 0) + 1
        
        print(f"   • Total de NFe: {len(notas)}")
        print(f"\n   • Distribuição por tipo:")
        for tipo, qtd in sorted(tipos.items()):
            percentual = (qtd / len(notas)) * 100
            print(f"      - {tipo}: {qtd} ({percentual:.1f}%)")
        
        # Estatísticas por situação
        situacoes = {}
        for nfe in notas:
            situacao = nfe.get('situacao', 'Desconhecida')
            situacoes[situacao] = situacoes.get(situacao, 0) + 1
        
        if situacoes:
            print(f"\n   • Distribuição por situação:")
            for situacao, qtd in sorted(situacoes.items()):
                print(f"      - Situação {situacao}: {qtd}")
        
        # Informação importante sobre relacionamento
        print(f"\n💡 RELACIONAMENTO:")
        print(f"   • NFe relaciona com Pedidos via vendas_raw.notaFiscal.id")
        print(f"   • Use: vendas_raw.dados_json->'notaFiscal'->>'id' = nfe_raw.bling_id")