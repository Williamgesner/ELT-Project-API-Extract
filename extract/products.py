# Responsável por: extrair produtos da API Bling
# VERSÃO OTIMIZADA: Suporta modo FULL e INCREMENTAL

from datetime import datetime, timedelta
from core.base_extractor import BaseExtractor
from models.product_raw import ProdutoRaw
from config.settings import endpoints
from config.extraction_mode import ExtractionMode

# =====================================================
# 1. CRIANDO A CLASSE PARA EXTRAÇÃO DE PRODUTOS
# =====================================================

class ProdutosExtractor(BaseExtractor):
    
    """
    Extrator específico para produtos da API Bling
    Herda toda a lógica comum da BaseExtractor e adiciona só o que é específico
    
    MODOS DE EXTRAÇÃO:
    - FULL: Extrai desde 2024-01-01 usando dataInclusao + Limpeza de órfãos ATIVA
    - INCREMENTAL: Extrai últimos 7 dias usando dataAlteracao + Limpeza DESABILITADA
    
    IMPORTANTE: Produtos possui filtro de dataAlteração na API Bling!
    Isso permite capturar mudanças em produtos antigos (ex: produto de 2023 
    com preço alterado hoje será capturado no incremental).
    """
    
    def __init__(self, api_key, empresa_id):
        """
        Inicializa o extrator de produtos
        Passa para a classe pai (BaseExtractor) a URL e modelo específicos
        
        Args:
            api_key: Token de autenticação da API Bling
            empresa_id: ID da empresa na tabela dim_empresas
        """
        super().__init__(endpoints['produtos'], ProdutoRaw)
        self.empresa_id = empresa_id
        
        # Sobrescrever headers do base_extractor com a API key específica
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def executar_extracao_completa(self, extraction_mode=ExtractionMode.FULL):
        """
        Executa o processo completo de extração de produtos
        
        Args:
            extraction_mode: ExtractionMode.FULL ou ExtractionMode.INCREMENTAL
                - FULL: dataInclusao desde 2024-01-01 + Remove órfãos
                - INCREMENTAL: dataAlteracao últimos 7 dias (sem remoção)
        """
        try:
            inicio_extracao = datetime.now()
            
            # =====================================================
            # DEFINIR JANELA DE EXTRAÇÃO BASEADA NO MODO
            # =====================================================
            if extraction_mode == ExtractionMode.INCREMENTAL:
                # MODO INCREMENTAL: Produtos ALTERADOS nos últimos 7 dias
                # Usa dataAlteracao para capturar mudanças em produtos de qualquer época
                print(f"\n⚡ MODO INCREMENTAL: Produtos")
                print(f"📅 Período: Alterados nos últimos 7 dias")
                print(f"🔍 Filtro API: dataAlteracao")
                print(f"🛡️ Limpeza de órfãos: DESABILITADA")
                
                data_inicial = datetime.now() - timedelta(days=7)
                usar_filtro_alteracao = True
                limpar_orfaos = False
                
            else:
                # MODO FULL: Todos os produtos desde 2024 (sincronização completa)
                # Usa dataInclusao para extração completa do período
                print(f"\n📊 MODO FULL: Produtos")
                print(f"📅 Período: Desde 2024-01-01")
                print(f"🔍 Filtro API: dataInclusao")
                print(f"🧹 Limpeza de órfãos: ATIVA")
                
                data_inicial = datetime(2024, 1, 1, 0, 0, 0)
                usar_filtro_alteracao = False
                limpar_orfaos = True
            
            # ⚠️ CRÍTICO: Informar período de extração ao base_extractor
            # Usado para limpeza de órfãos APENAS dentro do escopo de data
            # No FULL, limpa órfãos desde 2024-01-01
            # No INCREMENTAL, não limpa (None)
            self.data_inicial_extracao = data_inicial if limpar_orfaos else None
            
            # ✅ CORREÇÃO CRÍTICA: Produtos NÃO possui campo de data confiável no JSONB
            # Campo dataInclusao não existe no JSON retornado pela API
            # Solução: Desabilitar filtro de data (busca TODOS os registros da empresa)
            self.campo_data_filtro = None

            print(f"\n🏭 EXTRAÇÃO: PRODUTOS (Empresa ID: {self.empresa_id})")
            print("=" * 60)

            # =====================================================
            # EXTRAÇÃO COM JANELAS DE DATAS (SEGURANÇA API)
            # =====================================================
            # API Bling aceita dois filtros de data:
            # - dataInclusaoInicial/Final: quando o produto foi criado
            # - dataAlteracaoInicial/Final: quando o produto foi modificado
            # Formato: "YYYY-MM-DD HH:MM:SS"
            #
            # Janelas de 360 dias evitam erro de intervalo (máx 365 dias)
            
            print("Extraindo produtos da API...")
            
            data_final = datetime.now()
            janela_dias = 360  # Margem de segurança (< 365)

            todos_produtos = []
            ids_vistos = set()

            inicio_janela = data_inicial
            
            while inicio_janela <= data_final:
                # Calcula fim da janela (máximo 360 dias ou até data_final)
                fim_janela = min(
                    inicio_janela + timedelta(days=janela_dias) - timedelta(seconds=1),
                    data_final,
                )

                # =====================================================
                # FILTROS DIFERENTES POR MODO
                # =====================================================
                if usar_filtro_alteracao:
                    # INCREMENTAL: Produtos ALTERADOS (pega mudanças em produtos antigos)
                    filtros_adicionais = {
                        "criterio": 5,  # "Todos" (mais seguro com filtros de data)
                        "dataAlteracaoInicial": inicio_janela.strftime("%Y-%m-%d %H:%M:%S"),
                        "dataAlteracaoFinal": fim_janela.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    print(
                        f"\n📅 Janela ALTERAÇÃO: "
                        f"{filtros_adicionais['dataAlteracaoInicial']} → "
                        f"{filtros_adicionais['dataAlteracaoFinal']}"
                    )
                else:
                    # FULL: Produtos INCLUÍDOS (extração completa do período)
                    filtros_adicionais = {
                        "criterio": 5,  # "Todos" (mais seguro com filtros de data)
                        "dataInclusaoInicial": inicio_janela.strftime("%Y-%m-%d %H:%M:%S"),
                        "dataInclusaoFinal": fim_janela.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    print(
                        f"\n📅 Janela INCLUSÃO: "
                        f"{filtros_adicionais['dataInclusaoInicial']} → "
                        f"{filtros_adicionais['dataInclusaoFinal']}"
                    )

                # Extração paginada desta janela
                produtos_janela = self.extract_dados_bling_paginado(
                    limite_por_pagina=100,       # Máximo permitido pela API
                    delay_entre_requests=0.35,   # Delay mínimo, com margem de segurança
                    max_paginas=1000,            # Limite de segurança
                    max_tentativas=3,            # 3 tentativas antes de parar tudo
                    filtros_adicionais=filtros_adicionais,
                )

                # Deduplica registros (evita duplicatas entre janelas)
                for produto in produtos_janela:
                    produto_id = produto.get("id")
                    if produto_id is None or produto_id in ids_vistos:
                        continue
                    ids_vistos.add(produto_id)
                    todos_produtos.append(produto)

                # Próxima janela (evita sobreposição - +1 segundo)
                inicio_janela = fim_janela + timedelta(seconds=1)

            fim_extracao = datetime.now()
            tempo_extracao = fim_extracao - inicio_extracao

            # =====================================================
            # VALIDAÇÃO DOS DADOS EXTRAÍDOS
            # =====================================================
            if not todos_produtos:
                if extraction_mode == ExtractionMode.INCREMENTAL:
                    print("✨ Nenhum produto alterado nos últimos 7 dias.")
                    print("   Isso é normal no modo incremental.")
                else:
                    print("⚠️ Nenhum produto foi extraído.")
                    print("   Possíveis causas:")
                    print("   • Não há produtos no período especificado")
                    print("   • Problema de conectividade com a API")
                    print("   • Filtros muito restritivos")
                return
            
            print(f"\n📊 EXTRAÇÃO CONCLUÍDA:")
            print(f"⏱️  Tempo de extração: {tempo_extracao}")
            print(f"📈 Produtos extraídos: {len(todos_produtos)}")
            print(f"🚀 Velocidade: {len(todos_produtos)/tempo_extracao.total_seconds():.1f} produtos/segundo")

            # =====================================================
            # PREPARAR DADOS PARA SALVAMENTO
            # =====================================================
            print("\n📝 Preparando dados para salvamento...")
            dados_para_salvar = []
            
            for produto in todos_produtos:
                dados_formatados = {
                    'bling_id': produto['id'],
                    'empresa_id': self.empresa_id, 
                    'dados_json': produto  # JSON completo e puro (sem alterações)
                }
                dados_para_salvar.append(dados_formatados)

            # =====================================================
            # SALVAMENTO INTELIGENTE COM COMPARAÇÃO
            # =====================================================
            # A função salvar_dados_postgres_bulk já possui:
            # - Comparação inteligente (ignora ordem de listas, normaliza números)
            # - UPDATE apenas data_ingestao para registros idênticos
            # - UPDATE completo para registros diferentes
            # - INSERT para registros novos
            # - Limpeza de órfãos (se limpar_orfaos=True)
            
            print(f"\n💾 Iniciando salvamento inteligente...")
            print(f"🔄 Modo: {'FULL (com limpeza de órfãos)' if limpar_orfaos else 'INCREMENTAL (sem limpeza)'}")
            
            inicio_salvamento = datetime.now()
            
            # ⚠️ CRÍTICO: Passar flag limpar_orfaos corretamente
            stats = self.salvar_dados_postgres_bulk(
                dados_para_salvar, 
                limpar_orfaos=limpar_orfaos
            )
            
            fim_salvamento = datetime.now()
            tempo_salvamento = fim_salvamento - inicio_salvamento
            tempo_total = fim_salvamento - inicio_extracao

            # =====================================================
            # RELATÓRIO FINAL DE PERFORMANCE
            # =====================================================
            print(f"\n🏁 EXECUÇÃO COMPLETA!")
            print(f"⏱️  Tempo total: {tempo_total}")
            print(f"⏱️  Tempo de extração: {tempo_extracao}")
            print(f"⏱️  Tempo de salvamento: {tempo_salvamento}")
            print(f"🚀 Performance geral: {len(todos_produtos)/tempo_total.total_seconds():.1f} produtos/segundo")
            
            # Eficiência do algoritmo de comparação
            if stats['total'] > 0:
                eficiencia = (stats['ignorados'] / stats['total']) * 100
                print(f"⚡ Eficiência: {eficiencia:.1f}% dos registros eram idênticos")
                print(f"   (Evitou {stats['ignorados']} escritas desnecessárias)")

            print(f"\n✅ Produtos extraídos e salvos com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️  Execução interrompida pelo usuário")
            print("💾 Dados processados até este ponto foram preservados")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
            print("Script interrompido para análise do erro")
            print("Todos os dados extraídos até este ponto foram preservados")
            raise