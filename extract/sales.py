# Responsável por: orquestrar a extração de contatos especificamente

from datetime import datetime, timedelta
from core.base_extractor import BaseExtractor
from models.sales_raw import VendasRaw
from config.settings import endpoints

# =====================================================
# NORMALIZAÇÃO (ANTI "FALSO POSITIVO" NA LISTA DE VENDAS)
# =====================================================

def _normalizar_placeholders_venda(dados_json: dict) -> dict:
    """
    Normaliza inconsistências comuns do payload "lista" do Bling para
    reduzir falsos positivos na comparação (ex.: "0000-00-00" vs null).
    
    ✅ VERSÃO CORRIGIDA: Agora é RECURSIVA e normaliza listas vazias!
    
    Importante: isso NÃO mexe em `itens` (que já é removido abaixo).
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
    Herda toda a lógica comum da BaseExtractor e adiciona só o que é específico de vendas
    """
    
    def __init__(self, api_key, empresa_id): # Essa é a função que inicializa a classe
        """
        Inicializa o extrator de vendas
        Passa para a classe pai (BaseExtractor) a URL e modelo específicos de vendas
        
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
    
    def executar_extracao_completa(self):
        """
        Executa o processo completo de extração de vendas
        """
        try:
            # Extrai TODOS os dados da API usando paginação
            print(f"Extraindo todos as vendas da API (Empresa ID: {self.empresa_id})...")
            inicio_extracao = datetime.now()

            # Filtro correto conforme OpenAPI do Bling (GET /pedidos/vendas):
            # - dataInicial / dataFinal (format: "YYYY-MM-DD")
            #
            # Observação: para evitar erro de intervalo (ex.: > 1 ano),
            # extraímos em janelas de datas.
            data_inicial = datetime(2024, 1, 1).date()
            data_final = datetime.now().date()
            janela_dias = 360  # margem de segurança (< 365)

            todas_vendas = []
            ids_vistos = set()

            inicio_janela = data_inicial
            while inicio_janela <= data_final:
                fim_janela = min(inicio_janela + timedelta(days=janela_dias), data_final)

                filtros_adicionais = {
                    "dataInicial": inicio_janela.strftime("%Y-%m-%d"),
                    "dataFinal": fim_janela.strftime("%Y-%m-%d"),
                }

                print(
                    f"\n📅 Janela de vendas: "
                    f"{filtros_adicionais['dataInicial']} → {filtros_adicionais['dataFinal']}"
                )

                vendas_janela = self.extract_dados_bling_paginado(
                    limite_por_pagina=100,       # Máximo permitido pela API
                    delay_entre_requests=0.35,   # Deley mínimo, com margem de segurança. Segundo documentação da API são 3 requisições por segundo
                    max_paginas=1000,            # Limite de segurança
                    max_tentativas=3,            # 3 tentativas antes de parar tudo
                    filtros_adicionais=filtros_adicionais,
                )

                for venda in vendas_janela:
                    venda_id = venda.get("id")
                    if venda_id is None or venda_id in ids_vistos:
                        continue
                    ids_vistos.add(venda_id)
                    todas_vendas.append(venda)

                # Próxima janela (evita sobreposição)
                inicio_janela = fim_janela + timedelta(days=1)

            fim_extracao = datetime.now()
            tempo_extracao = fim_extracao - inicio_extracao

            if not todas_vendas:
                print("❌ Nenhuma venda foi extraído. Verificar API ou configurações.")
                exit()
            
            print(f"\n📊 EXTRAÇÃO CONCLUÍDA:")
            print(f"⏱️ Tempo de extração: {tempo_extracao}")
            print(f"📈 Contatos extraídos: {len(todas_vendas)}")
            print(f"🚀 Velocidade: {len(todas_vendas)/tempo_extracao.total_seconds():.1f} vendas/segundo")

            # Preparar dados
            print("\n📝 Preparando dados para salvamento...")
            dados_para_salvar = []
            
            for venda in todas_vendas:
                # IMPORTANTE:
                # O endpoint de "lista" pode conter campos que sobrescrevem/enfraquecem
                # o JSON detalhado salvo depois (ex.: itens).
                # Para permitir RETOMADA/INCREMENTAL no `VendasDetalhesExtractor`,
                # não devemos trazer `itens` da lista para dentro do `dados_json`.
                
                # ✅ NORMALIZAÇÃO É APLICADA AQUI (1x só, na extração)
                dados_json = _normalizar_placeholders_venda(dict(venda))
                dados_json.pop('itens', None)

                dados_formatados = {
                    'bling_id': venda['id'],
                    'empresa_id': self.empresa_id,  
                    'dados_json': dados_json  # ✅ Dados já normalizados!
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
            print(f"🚀 Performance geral: {len(todas_vendas)/tempo_total.total_seconds():.1f} vendas/segundo")
            
            # Eficiência do algoritmo
            if stats['total'] > 0:
                eficiencia = (stats['ignorados'] / stats['total']) * 100
                print(f"⚡ Eficiência: {eficiencia:.1f}% dos registros eram idênticos (evitou escritas desnecessárias)")
                
                # ✅ MONITORAMENTO: Alerta se normalização não está funcionando
                if eficiencia < 90:
                    print(f"\n⚠️  ATENÇÃO: Taxa de idênticos BAIXA!")
                    print(f"   📊 Esperado: >90% (normalização funcionando)")
                    print(f"   📉 Obtido: {eficiencia:.1f}%")
                    print(f"   🔍 Verifique se houve mudanças reais nos dados ou se a normalização precisa ajuste")

            print("\n🎉 Script de vendas executado com sucesso!")
            
        except KeyboardInterrupt:
            print("\n⚠️ Execução interrompida pelo usuário")
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
            print("Script interrompido para análise do erro")
            print("Todos os dados extraídos até este ponto foram preservados")
            raise