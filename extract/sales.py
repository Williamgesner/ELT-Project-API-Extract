# Responsável por: extrair vendas da API Bling
# VERSÃO OTIMIZADA: Suporta modo FULL e INCREMENTAL
# Normalização anti-falsos-positivos para payloads de lista

from datetime import datetime, timedelta
from core.base_extractor import BaseExtractor
from models.sales_raw import VendasRaw
from config.settings import endpoints
from config.extraction_mode import ExtractionMode

# =====================================================
# NORMALIZAÇÃO (ANTI "FALSO POSITIVO" NA LISTA DE VENDAS)
# =====================================================

def _normalizar_placeholders_venda(dados_json: dict) -> dict:
    """
    Normaliza inconsistências comuns do payload "lista" do Bling para
    reduzir falsos positivos na comparação (ex.: "0000-00-00" vs null).
    
    ✅ VERSÃO CORRIGIDA: Agora é RECURSIVA e normaliza listas vazias!
    
    Importante: isso NÃO mexe em `itens` (que já é removido no fluxo principal).
    """
    if not isinstance(dados_json, dict):
        return dados_json

    # Datas "inválidas" que às vezes vêm como placeholder
    for campo_data in ("dataPrevista",):
        if campo_data in dados_json and dados_json[campo_data] in ("0000-00-00", "", "0000-00-00T00:00:00", None):
            dados_json[campo_data] = None

    # Strings vazias → None (novos campos adicionados)
    for campo_string in ("observacoesInternas", "numeroPedidoCompra", "numeroLoja"):
        if campo_string in dados_json and dados_json[campo_string] in ("", " ", None):
            dados_json[campo_string] = None

    # Objetos que às vezes vêm como {"id": 0} ou null (equivalentes a "sem valor")
    for campo_objeto in ("vendedor", "categoria", "notaFiscal"):
        obj = dados_json.get(campo_objeto)
        if isinstance(obj, dict):
            obj_id = obj.get("id")
            # considerar id 0 / "0" como "ausente"
            if obj_id in (0, "0", None) and len(obj.keys()) == 1:
                dados_json[campo_objeto] = None

    # Transporte (objeto complexo)
    transporte = dados_json.get("transporte")
    if isinstance(transporte, dict):
        # Volumes vazios → None
        if transporte.get("volumes") == []:
            transporte["volumes"] = None
        
        # Campos numéricos = 0 → None
        for campo_num in ("pesoBruto", "prazoEntrega", "fretePorConta", "quantidadeVolumes"):
            if transporte.get(campo_num) in (0, None):
                transporte[campo_num] = None
        
        # Contato vazio
        contato = transporte.get("contato")
        if isinstance(contato, dict) and contato.get("id") in (0, None):
            transporte["contato"] = None
        
        # Etiqueta vazia
        etiqueta = transporte.get("etiqueta")
        if isinstance(etiqueta, dict):
            campos_vazios = all(
                etiqueta.get(k) in ("", None) 
                for k in ("uf", "cep", "nome", "bairro", "numero", "endereco", "municipio")
            )
            if campos_vazios:
                transporte["etiqueta"] = None

    # Intermediador
    intermediador = dados_json.get("intermediador")
    if isinstance(intermediador, dict):
        if intermediador.get("cnpj") in ("", None) and intermediador.get("nomeUsuario") in ("", None):
            dados_json["intermediador"] = None

    # ✅ CORREÇÃO CRÍTICA: Normalização RECURSIVA de TODOS os campos
    # (evita falsos positivos de listas vazias e objetos aninhados)
    for campo in list(dados_json.keys()):  # list() para evitar modificar dict durante iteração
        valor = dados_json[campo]
        
        # Listas vazias → None
        if isinstance(valor, list) and len(valor) == 0:
            dados_json[campo] = None
        
        # Normalizar recursivamente objetos aninhados
        elif isinstance(valor, dict):
            dados_json[campo] = _normalizar_placeholders_venda(valor)
        
        # Normalizar recursivamente listas de objetos
        elif isinstance(valor, list) and valor is not None:
            dados_json[campo] = [
                _normalizar_placeholders_venda(item) if isinstance(item, dict) else item
                for item in valor
            ]
    
    return dados_json

# =====================================================
# 1. CRIANDO A CLASSE PARA EXTRAÇÃO DE VENDAS
# =====================================================

class VendasExtractor(BaseExtractor):
    
    """
    Extrator específico para vendas da API Bling
    Herda toda a lógica comum da BaseExtractor e adiciona só o que é específico
    
    MODOS DE EXTRAÇÃO:
    - FULL: Extrai desde 2024-01-01 usando dataInicial + Limpeza de órfãos ATIVA
    - INCREMENTAL: Extrai últimos 7 dias usando dataAlteração + Limpeza DESABILITADA
    
    NORMALIZAÇÃO ESPECIALIZADA:
    - Remove inconsistências de payload "lista" (datas 0000-00-00, objetos vazios, etc)
    - Remove campo "itens" para não conflitar com VendasDetalhesExtractor
    - Reduz drasticamente falsos positivos na comparação
    
    IMPORTANTE: Vendas possui filtro de dataAlteração na API Bling!
    Isso permite capturar mudanças em vendas antigas (ex: venda de 2023 
    com status alterado hoje será capturada no incremental).
    """
    
    def __init__(self, api_key, empresa_id):
        """
        Inicializa o extrator de vendas
        Passa para a classe pai (BaseExtractor) a URL e modelo específicos
        
        Args:
            api_key: Token de autenticação da API Bling
            empresa_id: ID da empresa na tabela dim_empresas
        """
        super().__init__(endpoints['vendas'], VendasRaw)
        self.empresa_id = empresa_id
        
        # Sobrescrever headers do base_extractor com a API key específica
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    
    def executar_extracao_completa(self, extraction_mode=ExtractionMode.FULL):
        """
        Executa o processo completo de extração de vendas
        
        Args:
            extraction_mode: ExtractionMode.FULL ou ExtractionMode.INCREMENTAL
                - FULL: dataInicial desde 2024-01-01 + Remove órfãos
                - INCREMENTAL: dataAlteracao últimos 7 dias (sem remoção)
        """
        try:
            inicio_extracao = datetime.now()
            
            # =====================================================
            # DEFINIR JANELA DE EXTRAÇÃO BASEADA NO MODO
            # =====================================================
            if extraction_mode == ExtractionMode.INCREMENTAL:
                # MODO INCREMENTAL: Vendas ALTERADAS nos últimos 7 dias
                # Usa dataAlteracao para capturar mudanças em vendas de qualquer época
                print(f"\n⚡ MODO INCREMENTAL: Vendas")
                print(f"📅 Período: Alteradas nos últimos 7 dias")
                print(f"🔍 Filtro API: dataAlteracao")
                print(f"🛡️ Limpeza de órfãos: DESABILITADA")
                
                data_inicial = (datetime.now() - timedelta(days=7)).date()
                usar_filtro_alteracao = True
                limpar_orfaos = False
                
            else:
                # MODO FULL: Todas as vendas desde 2024 (sincronização completa)
                # Usa dataInicial para extração completa do período
                print(f"\n📊 MODO FULL: Vendas")
                print(f"📅 Período: Desde 2024-01-01")
                print(f"🔍 Filtro API: dataInicial")
                print(f"🧹 Limpeza de órfãos: ATIVA")
                
                data_inicial = datetime(2024, 1, 1).date()
                usar_filtro_alteracao = False
                limpar_orfaos = True
            
            # ⚠️ CRÍTICO: Informar período de extração ao base_extractor
            # Usado para limpeza de órfãos APENAS dentro do escopo de data
            # Converter date para datetime para compatibilidade
            self.data_inicial_extracao = datetime.combine(data_inicial, datetime.min.time()) if limpar_orfaos else None
            
            # ✅ CORREÇÃO CRÍTICA: Campo de data no JSONB usado para filtrar registros existentes
            # Vendas usa 'data' (NÃO 'dataInclusao')
            self.campo_data_filtro = 'data'

            print(f"\n💰 EXTRAÇÃO: VENDAS (Empresa ID: {self.empresa_id})")
            print("=" * 60)

            # =====================================================
            # EXTRAÇÃO COM JANELAS DE DATAS (SEGURANÇA API)
            # =====================================================
            # API Bling aceita dois filtros de data:
            # - dataInicial/dataFinal: quando a venda foi criada
            # - dataAlteracaoInicial/dataAlteracaoFinal: quando a venda foi modificada
            # Formato: "YYYY-MM-DD" (sem hora)
            #
            # Janelas de 360 dias evitam erro de intervalo (máx 365 dias)
            
            print("Extraindo vendas da API...")
            
            data_final = datetime.now().date()
            janela_dias = 360  # Margem de segurança (< 365)

            todas_vendas = []
            ids_vistos = set()

            inicio_janela = data_inicial
            
            while inicio_janela <= data_final:
                # Calcula fim da janela (máximo 360 dias ou até data_final)
                fim_janela = min(inicio_janela + timedelta(days=janela_dias), data_final)

                # =====================================================
                # FILTROS DIFERENTES POR MODO
                # =====================================================
                if usar_filtro_alteracao:
                    # INCREMENTAL: Vendas ALTERADAS (pega mudanças em vendas antigas)
                    filtros_adicionais = {
                        "dataAlteracaoInicial": inicio_janela.strftime("%Y-%m-%d"),
                        "dataAlteracaoFinal": fim_janela.strftime("%Y-%m-%d"),
                    }
                    print(
                        f"\n📅 Janela ALTERAÇÃO: "
                        f"{filtros_adicionais['dataAlteracaoInicial']} → "
                        f"{filtros_adicionais['dataAlteracaoFinal']}"
                    )
                else:
                    # FULL: Vendas INCLUÍDAS (extração completa do período)
                    filtros_adicionais = {
                        "dataInicial": inicio_janela.strftime("%Y-%m-%d"),
                        "dataFinal": fim_janela.strftime("%Y-%m-%d"),
                    }
                    print(
                        f"\n📅 Janela INCLUSÃO: "
                        f"{filtros_adicionais['dataInicial']} → "
                        f"{filtros_adicionais['dataFinal']}"
                    )

                # Extração paginada desta janela
                vendas_janela = self.extract_dados_bling_paginado(
                    limite_por_pagina=100,       # Máximo permitido pela API
                    delay_entre_requests=0.35,   # Delay mínimo, com margem de segurança
                    max_paginas=1000,            # Limite de segurança
                    max_tentativas=3,            # 3 tentativas antes de parar tudo
                    filtros_adicionais=filtros_adicionais,
                )

                # Deduplica registros (evita duplicatas entre janelas)
                for venda in vendas_janela:
                    venda_id = venda.get("id")
                    if venda_id is None or venda_id in ids_vistos:
                        continue
                    ids_vistos.add(venda_id)
                    todas_vendas.append(venda)

                # Próxima janela (evita sobreposição - +1 dia)
                inicio_janela = fim_janela + timedelta(days=1)

            fim_extracao = datetime.now()
            tempo_extracao = fim_extracao - inicio_extracao

            # =====================================================
            # VALIDAÇÃO DOS DADOS EXTRAÍDOS
            # =====================================================
            if not todas_vendas:
                if extraction_mode == ExtractionMode.INCREMENTAL:
                    print("✨ Nenhuma venda alterada nos últimos 7 dias.")
                    print("   Isso é normal no modo incremental.")
                else:
                    print("⚠️ Nenhuma venda foi extraída.")
                    print("   Possíveis causas:")
                    print("   • Não há vendas no período especificado")
                    print("   • Problema de conectividade com a API")
                return
            
            print(f"\n📊 EXTRAÇÃO CONCLUÍDA:")
            print(f"⏱️  Tempo de extração: {tempo_extracao}")
            print(f"📈 Vendas extraídas: {len(todas_vendas)}")
            print(f"🚀 Velocidade: {len(todas_vendas)/tempo_extracao.total_seconds():.1f} vendas/segundo")

            # =====================================================
            # PREPARAR DADOS PARA SALVAMENTO (COM NORMALIZAÇÃO)
            # =====================================================
            print("\n📝 Preparando dados para salvamento...")
            dados_para_salvar = []
            
            for venda in todas_vendas:
                # IMPORTANTE:
                # O endpoint de "lista" pode conter campos que sobrescrevem/enfraquecem
                # o JSON detalhado salvo depois (ex.: itens).
                # Para permitir RETOMADA/INCREMENTAL no `VendasDetalhesExtractor`,
                # não devemos trazer `itens` da lista para dentro do `dados_json`.
                
                # ✅ NORMALIZAÇÃO É APLICADA AQUI (1x só, na extração)
                # Remove inconsistências que causam falsos positivos
                dados_json = _normalizar_placeholders_venda(dict(venda))
                dados_json.pop('itens', None)  # Remove itens da lista

                dados_formatados = {
                    'bling_id': venda['id'],
                    'empresa_id': self.empresa_id,
                    'dados_json': dados_json  # ✅ Dados já normalizados!
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
            print(f"🚀 Performance geral: {len(todas_vendas)/tempo_total.total_seconds():.1f} vendas/segundo")
            
            # Eficiência do algoritmo de comparação
            if stats['total'] > 0:
                eficiencia = (stats['ignorados'] / stats['total']) * 100
                print(f"⚡ Eficiência: {eficiencia:.1f}% dos registros eram idênticos")
                print(f"   (Evitou {stats['ignorados']} escritas desnecessárias)")
                
                # ✅ MONITORAMENTO: Alerta se normalização não está funcionando
                if eficiencia < 90:
                    print(f"\n⚠️  ATENÇÃO: Taxa de idênticos BAIXA!")
                    print(f"   📊 Esperado: >90% (normalização funcionando)")
                    print(f"   📉 Obtido: {eficiencia:.1f}%")
                    print(f"   🔍 Verifique se houve mudanças reais nos dados ou se a normalização precisa ajuste")

            print(f"\n✅ Vendas extraídas e salvas com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️  Execução interrompida pelo usuário")
            print("💾 Dados processados até este ponto foram preservados")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
            print("Script interrompido para análise do erro")
            print("Todos os dados extraídos até este ponto foram preservados")
            import traceback
            traceback.print_exc()
            raise