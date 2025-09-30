# Responsável por: extrair contatos completos - INSERIR APENAS NOVOS (SEM COMPARAÇÃO)

from datetime import datetime
import time
import requests
from core.base_extractor import BaseExtractor
from models.contact_raw import ContatoRaw
from config.settings import endpoints, headers
from config.database import Session
from sqlalchemy import text

# =============================================================
# 1. CRIANDO A CLASSE PARA EXTRAÇÃO DE CLIENTES + ENDEREÇOS
# =============================================================

class ContatosCompletoExtractor(BaseExtractor):
    """
    Extrator otimizado que INSERE APENAS NOVOS contatos
    SEM comparação de dados existentes
    MUDANÇA: Não usa salvar_dados_postgres_bulk() igual os contatos e vendas.
    """
    
    def __init__(self):
        super().__init__(endpoints['contatos'], ContatoRaw)
    
    def executar_extracao_completa(self):
        """
        Processo otimizado: inserir apenas novos (SEM COMPARAÇÃO)
        """
        try:
            print("🚀 EXTRAÇÃO: CONTATOS COMPLETOS")
            print("⚡ Estratégia: Inserir apenas novos (SEM comparação de JSON)")
            print("=" * 60)
            inicio_total = datetime.now()
            
            # ETAPA 1: Extrair lista básica de contatos
            print("\n1️⃣ EXTRAINDO LISTA BÁSICA DE CONTATOS...")
            inicio_lista = datetime.now()
            
            lista_contatos = self.extract_dados_bling_paginado(
                limite_por_pagina=100,
                delay_entre_requests=0.35,
                max_paginas=1000,
                max_tentativas=3
            )
            
            fim_lista = datetime.now()
            tempo_lista = fim_lista - inicio_lista
            
            if not lista_contatos:
                print("❌ Nenhum contato extraído da API")
                return
            
            print(f"✅ {len(lista_contatos)} contatos extraídos em {tempo_lista}")
            
            # ETAPA 2: Identificar apenas novos (1 query única e rápida)
            print(f"\n2️⃣ IDENTIFICANDO CONTATOS NOVOS...")
            inicio_filtro = datetime.now()
            
            contatos_novos = self._filtrar_apenas_novos(lista_contatos)
            
            fim_filtro = datetime.now()
            
            if not contatos_novos:
                print(f"✅ Nenhum contato novo encontrado. Base já atualizada!")
                print(f"⏱️  Tempo de verificação: {fim_filtro - inicio_filtro}")
                return
            
            print(f"✅ {len(contatos_novos)} contatos novos identificados em {fim_filtro - inicio_filtro}")
            
            # ETAPA 3: Buscar detalhes apenas dos novos
            print(f"\n3️⃣ BUSCANDO DETALHES DOS NOVOS CONTATOS...")
            inicio_detalhes = datetime.now()
            
            contatos_completos = self._buscar_detalhes_otimizado(contatos_novos)
            
            fim_detalhes = datetime.now()
            tempo_detalhes = fim_detalhes - inicio_detalhes
            
            print(f"✅ Detalhes coletados em {tempo_detalhes}")
            
            # ETAPA 4: Salvar apenas novos (SEM COMPARAÇÃO - DIRETO)
            print(f"\n4️⃣ SALVANDO NOVOS CONTATOS (INSERT DIRETO)...")
            inicio_salvamento = datetime.now()
            
            # ⚡ FUNÇÃO OTIMIZADA - NÃO USA salvar_dados_postgres_bulk()
            stats = self._salvar_novos_direto(contatos_completos)
            
            fim_salvamento = datetime.now()
            fim_total = datetime.now()
            
            # RELATÓRIO FINAL
            print(f"\n🎉 EXTRAÇÃO CONCLUÍDA COM SUCESSO!")
            print(f"=" * 60)
            print(f"\n⏱️  TEMPOS:")
            print(f"   • Extração da API: {tempo_lista}")
            print(f"   • Filtro de novos: {fim_filtro - inicio_filtro}")
            print(f"   • Busca detalhes: {tempo_detalhes}")
            print(f"   • Salvamento: {fim_salvamento - inicio_salvamento}")
            print(f"   • TOTAL: {fim_total - inicio_total}")
            
            print(f"\n📊 ESTATÍSTICAS GERAIS:")
            print(f"   • Total de contatos na API: {len(lista_contatos)}")
            print(f"   • Contatos já existentes no banco: {len(lista_contatos) - len(contatos_novos)}")
            print(f"   • Contatos novos encontrados: {len(contatos_novos)}")
            
            print(f"\n💾 OPERAÇÕES NO BANCO:")
            print(f"   • Inseridos com sucesso: {stats['inseridos']}")
            print(f"   • Erros durante inserção: {stats['erros']}")
            
            print(f"\n📈 RESUMO DO BANCO:")
            # Consulta total no banco após inserção
            total_no_banco = self._contar_total_no_banco()
            print(f"   • Total de contatos no banco agora: {total_no_banco}")
            
            # Estatísticas de endereços
            self._calcular_estatisticas_enderecos(contatos_completos, len(contatos_novos))
            
            # Resumo de economia
            economia_operacoes = len(lista_contatos) - len(contatos_novos)
            if economia_operacoes > 0:
                print(f"\n⚡ ECONOMIA:")
                print(f"   • {economia_operacoes} inserções duplicadas evitadas")
            
            print(f"\n✨ Estratégia otimizada executada com sucesso!")
            
        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO: {e}")
            raise
    
    def _filtrar_apenas_novos(self, lista_contatos):
        """
        Filtra apenas contatos que NÃO existem no banco
        OTIMIZAÇÃO: Uma única query SQL para verificar todos os IDs de uma vez
        
        Args:
            lista_contatos: Lista de contatos da API
            
        Returns:
            list: Apenas contatos que não existem no banco
        """
        session = Session()
        
        try:
            # Extrai todos os IDs da API
            ids_api = [c['id'] for c in lista_contatos]
            
            if not ids_api:
                return []
            
            # Query única para buscar todos os IDs que já existem
            # Muito mais rápido que 1000 queries individuais
            query = text("""
                SELECT bling_id 
                FROM raw.contatos_raw 
                WHERE bling_id = ANY(:ids)
            """)
            
            resultado = session.execute(query, {"ids": ids_api})
            ids_existentes = set(row.bling_id for row in resultado)
            
            # Filtra apenas os que NÃO existem
            contatos_novos = [c for c in lista_contatos if c['id'] not in ids_existentes]
            
            print(f"   📊 {len(ids_existentes)} já existem no banco")
            print(f"   🆕 {len(contatos_novos)} são novos")
            
            return contatos_novos
            
        except Exception as e:
            print(f"❌ Erro ao filtrar contatos: {e}")
            raise
        finally:
            session.close()
    
    def _salvar_novos_direto(self, contatos_completos):
        """
        NOVA FUNÇÃO OTIMIZADA: Salva direto sem comparação
        
        - Sem SELECT de registros existentes
        - Sem comparação de JSON  
        - Apenas INSERT direto
        - Commits em lote para performance
        
        Args:
            contatos_completos: Lista de contatos processados
            
        Returns:
            dict: Estatísticas de inserção
        """
        if not contatos_completos:
            return {'inseridos': 0, 'erros': 0}
        
        session = Session()
        stats = {'inseridos': 0, 'erros': 0}
        
        try:
            print(f"   💾 Preparando {len(contatos_completos)} registros para inserção...")
            
            for i, contato in enumerate(contatos_completos):
                try:
                    # Cria objeto diretamente (sem verificações)
                    novo_registro = ContatoRaw(
                        bling_id=contato['id'],
                        dados_json=contato,
                        data_ingestao=datetime.now(),
                        status_processamento='pendente'
                    )
                    
                    session.add(novo_registro)
                    stats['inseridos'] += 1
                    
                    # Commit em lotes de 100 para performance
                    if (i + 1) % 100 == 0:
                        session.commit()
                        print(f"   ✅ {i + 1}/{len(contatos_completos)} registros inseridos...")
                    
                except Exception as e:
                    session.rollback()
                    stats['erros'] += 1
                    erro_msg = str(e)
                    
                    # Log apenas se não for duplicata (que seria estranho aqui)
                    if "duplicate key" not in erro_msg and "already exists" not in erro_msg:
                        print(f"   ❌ Erro no contato {contato.get('id')}: {erro_msg[:100]}")
            
            # Commit final para registros restantes
            session.commit()
            print(f"   ✅ Commit final realizado")
            
        except Exception as e:
            session.rollback()
            print(f"❌ Erro crítico no salvamento: {e}")
            raise
        finally:
            session.close()
        
        return stats
    
    def _buscar_detalhes_otimizado(self, lista_contatos):
        """
        Busca detalhes completos dos contatos (incluindo endereços)
        com delay otimizado entre requisições
        
        Args:
            lista_contatos: Lista de contatos básicos
            
        Returns:
            list: Contatos com detalhes completos e endereços estruturados
        """
        contatos_completos = []
        total = len(lista_contatos)
        
        print(f"   📡 Buscando detalhes de {total} contatos...")
        
        for i, contato in enumerate(lista_contatos):
            # Progresso a cada 50 contatos
            if (i + 1) % 50 == 0:
                print(f"   Processando {i + 1}/{total}...")
            
            # Busca detalhes do contato
            detalhes = self._buscar_detalhes_contato(contato['id'])
            
            if detalhes:
                # Processa e estrutura o endereço
                processado = self._processar_contato_detalhado(detalhes)
                contatos_completos.append(processado)
            else:
                # Se não conseguir detalhes, usa dados básicos
                contatos_completos.append(contato)
            
            # Delay pequeno para não estourar rate limit
            time.sleep(0.05)
        
        return contatos_completos
    
    def _buscar_detalhes_contato(self, contato_id):
        """
        Busca detalhes completos de um contato específico
        
        Args:
            contato_id: ID do contato no Bling
            
        Returns:
            dict: Dados completos do contato ou None se falhar
        """
        try:
            url = f"{endpoints['contatos']}/{contato_id}"
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                return response.json().get('data', {})
            else:
                return None
                
        except Exception:
            return None
    
    def _processar_contato_detalhado(self, contato_detalhado):
        """
        Processa contato adicionando estrutura de endereço formatada
        
        Args:
            contato_detalhado: Dados completos do contato da API
            
        Returns:
            dict: Contato com endereço estruturado
        """
        endereco_geral = contato_detalhado.get("endereco", {}).get("geral", {})
        contato_processado = contato_detalhado.copy()
        
        if endereco_geral:
            contato_processado['endereco_estruturado'] = {
                'tem_endereco': True,
                'endereco_completo_formatado': self._formatar_endereco_contato(endereco_geral),
                'endereco_detalhado': {
                    'logradouro': endereco_geral.get('endereco'),
                    'numero': endereco_geral.get('numero'),
                    'complemento': endereco_geral.get('complemento'),
                    'bairro': endereco_geral.get('bairro'),
                    'cidade': endereco_geral.get('municipio'),
                    'estado': endereco_geral.get('uf'),
                    'cep': endereco_geral.get('cep'),
                    'pais': endereco_geral.get('pais', 'Brasil')
                },
                'data_processamento': datetime.now().isoformat()
            }
        else:
            contato_processado['endereco_estruturado'] = {
                'tem_endereco': False,
                'endereco_completo_formatado': None,
                'endereco_detalhado': None,
                'data_processamento': datetime.now().isoformat()
            }
        
        return contato_processado
    
    def _formatar_endereco_contato(self, endereco_geral):
        """
        Formata endereço em string legível
        
        Args:
            endereco_geral: Dicionário com dados do endereço
            
        Returns:
            str: Endereço formatado ou None se vazio
        """
        partes = [
            endereco_geral.get('endereco', ''),
            endereco_geral.get('numero', ''),
            endereco_geral.get('complemento', ''),
            endereco_geral.get('bairro', ''),
            endereco_geral.get('municipio', ''),
            endereco_geral.get('uf', ''),
            endereco_geral.get('cep', '')
        ]
        
        partes_validas = [p.strip() for p in partes if p and p.strip()]
        return ', '.join(partes_validas) if partes_validas else None
    
    def _calcular_estatisticas_enderecos(self, contatos_completos, total_contatos):
        """
        Calcula e exibe estatísticas sobre endereços
        
        Args:
            contatos_completos: Lista de contatos processados
            total_contatos: Total de contatos
        """
        contatos_com_endereco = sum(
            1 for c in contatos_completos
            if c.get('endereco_estruturado', {}).get('tem_endereco', False)
        )
        
        print(f"\n🏠 ESTATÍSTICAS DE ENDEREÇOS:")
        print(f"   • Com endereços completos: {contatos_com_endereco}/{total_contatos}")
        
        if total_contatos > 0:
            taxa = (contatos_com_endereco / total_contatos) * 100
            print(f"   • Taxa de cobertura: {taxa:.1f}%")
            
            if taxa < 50:
                print(f"   ⚠️  Atenção: Mais de 50% dos contatos sem endereço")
    
    def _contar_total_no_banco(self):
        """
        Conta o total de contatos no banco após inserção
        """
        session = Session()
        try:
            query = text("SELECT COUNT(*) FROM raw.contatos_raw")
            resultado = session.execute(query)
            total = resultado.scalar()
            return total
        finally:
            session.close()