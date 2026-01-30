# Responsável por: orquestrar a extração de produtos especificamente - VERSÃO OTIMIZADA

from datetime import datetime, timedelta
from core.base_extractor import BaseExtractor
from models.product_raw import ProdutoRaw
from config.settings import endpoints
from config.extraction_mode import ExtractionMode

class ProdutosExtractor(BaseExtractor):
    
    """
    Extrator específico para produtos da API Bling - VERSÃO OTIMIZADA
    Suporta modo FULL e INCREMENTAL
    """
    
    def __init__(self, api_key, empresa_id):
        """
        Inicializa o extrator de produtos
        
        Args:
            api_key: Token de autenticação da API Bling
            empresa_id: ID da empresa na tabela dim_empresas
        """
        super().__init__(endpoints['produtos'], ProdutoRaw)
        self.empresa_id = empresa_id
        
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def executar_extracao_completa(self, extraction_mode=ExtractionMode.INCREMENTAL):
        """
        Executa o processo completo de extração de produtos
        
        Args:
            extraction_mode: ExtractionMode.FULL ou ExtractionMode.INCREMENTAL
        """
        try:
            inicio_extracao = datetime.now()
            
            # Definir janela de extração baseada no modo
            if extraction_mode == ExtractionMode.INCREMENTAL:
                print(f"🔄 MODO INCREMENTAL: Extraindo produtos dos últimos 7 dias (Empresa ID: {self.empresa_id})...")
                data_inclusao_inicial = datetime.now() - timedelta(days=7)
                limpar_orfaos = False
            else:
                print(f"📊 MODO FULL: Extraindo todos os produtos desde 2024 (Empresa ID: {self.empresa_id})...")
                data_inclusao_inicial = datetime(2024, 1, 1, 0, 0, 0)
                limpar_orfaos = True
            
            data_inclusao_final = datetime.now()
            janela_dias = 360

            todos_produtos = []
            ids_vistos = set()

            inicio_janela = data_inclusao_inicial
            while inicio_janela <= data_inclusao_final:
                fim_janela = min(
                    inicio_janela + timedelta(days=janela_dias) - timedelta(seconds=1),
                    data_inclusao_final,
                )

                filtros_adicionais = {
                    "criterio": 5,
                    "dataInclusaoInicial": inicio_janela.strftime("%Y-%m-%d %H:%M:%S"),
                    "dataInclusaoFinal": fim_janela.strftime("%Y-%m-%d %H:%M:%S"),
                }

                print(
                    f"\n📅 Janela de inclusão (produtos): "
                    f"{filtros_adicionais['dataInclusaoInicial']} → {filtros_adicionais['dataInclusaoFinal']}"
                )

                produtos_janela = self.extract_dados_bling_paginado(
                    limite_por_pagina=100,
                    delay_entre_requests=0.35,
                    max_paginas=1000,
                    max_tentativas=3,
                    filtros_adicionais=filtros_adicionais,
                )

                for produto in produtos_janela:
                    produto_id = produto.get("id")
                    if produto_id is None or produto_id in ids_vistos:
                        continue
                    ids_vistos.add(produto_id)
                    todos_produtos.append(produto)

                inicio_janela = fim_janela + timedelta(seconds=1)

            fim_extracao = datetime.now()
            tempo_extracao = fim_extracao - inicio_extracao

            if not todos_produtos:
                print("❌ Nenhum produto foi extraído. Verificar API ou configurações.")
                exit()
            
            print(f"\n📊 EXTRAÇÃO CONCLUÍDA:")
            print(f"⏱️ Tempo de extração: {tempo_extracao}")
            print(f"📈 Produtos extraídos: {len(todos_produtos)}")
            print(f"🚀 Velocidade: {len(todos_produtos)/tempo_extracao.total_seconds():.1f} produtos/segundo")

            print("\n📝 Preparando dados para salvamento...")
            dados_para_salvar = []
            
            for produto in todos_produtos:
                dados_formatados = {
                    'bling_id': produto['id'],
                    'empresa_id': self.empresa_id, 
                    'dados_json': produto
                }
                dados_para_salvar.append(dados_formatados)

            print(f"\n💾 Iniciando salvamento inteligente...")
            inicio_salvamento = datetime.now()
            
            stats = self.salvar_dados_postgres_bulk(dados_para_salvar, limpar_orfaos=limpar_orfaos)
            
            fim_salvamento = datetime.now()
            tempo_salvamento = fim_salvamento - inicio_salvamento
            tempo_total = fim_salvamento - inicio_extracao

            print(f"\n🏁 EXECUÇÃO COMPLETA!")
            print(f"⏱️ Tempo total: {tempo_total}")
            print(f"⏱️ Tempo de salvamento: {tempo_salvamento}")
            print(f"🚀 Performance geral: {len(todos_produtos)/tempo_total.total_seconds():.1f} produtos/segundo")
            
            if stats['total'] > 0:
                eficiencia = (stats['ignorados'] / stats['total']) * 100
                print(f"⚡ Eficiência: {eficiencia:.1f}% dos registros eram idênticos (evitou escritas desnecessárias)")

            print("\n🎉 Script de produtos executado com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️ Execução interrompida pelo usuário")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
            print("Script interrompido para análise do erro")
            print("Todos os dados extraídos até este ponto foram preservados")
            raise
