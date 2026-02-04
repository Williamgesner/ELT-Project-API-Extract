# Responsável por: executar TODOS os extratores E transformadores em sequência em Empresa ID - 1
# Este script mantém o DW sincronizado com a Bling - VERSÃO INCREMENTAL
# Inclui: Parte COMERCIAL + Parte FINANCEIRA
# MODO INCREMENTAL: Executa extração otimizada SEM limpeza de órfãos

"""
========================================
PIPELINE COMPLETO - EMPRESA 1
MODO INCREMENTAL
========================================

Responsável por: executar TODOS os extratores E transformadores em sequência
VERSÃO MULTI-CNPJ: empresa_id=1

Este script mantém o DW sincronizado com a Bling
Inclui: Parte COMERCIAL + Parte FINANCEIRA
Executar a cada 2 horas (Solicitação do cliente)

🛡️ PROTEÇÃO IMPLEMENTADA:
   • Se extração falhar → transformação NÃO roda
   • Evita deleção de dados se API Key expirar
   • Garante integridade dos dados

⚡ MODO INCREMENTAL:
   • Contacts: últimos 7 dias (dataAlteracaoInicial/Final)
   • Products: últimos 7 dias (dataAlteracaoInicial/Final)
   • Sales: últimos 7 dias (dataAlteracaoInicial/Final)
   • Accounts Payable: últimos 120 dias (janela)
   • Accounts Receivable: últimos 120 dias (janela)
   • NFe: últimos 120 dias (janela)
   • Limpeza de órfãos DESABILITADA
"""

import os
import time
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import text
from config.database import create_schema_raw, create_schema_processed, create_all_tables, Session
from config.auth_manager import obter_token_para_empresa
from config.logger import setup_logging, close_logging
from config.extraction_mode import ExtractionMode

# =====================================================
# IMPORTAÇÕES - PARTE COMERCIAL
# =====================================================
from extract.contacts import ContatosCompletoExtractor
from extract.products import ProdutosExtractor
from extract.sales import VendasExtractor
from extract.sales_details import VendasDetalhesExtractor
from transform.contacts_dw import ContatosTransformer
from transform.products_dw import ProdutosTransformer
from transform.sales_dw import VendasTransformer
from transform.items_dw import ItensTransformer 

# =====================================================
# IMPORTAÇÕES - PARTE FINANCEIRA
# =====================================================
from extract.payment_methods import FormasPagamentosExtractor
from extract.accounts_payable_categories import CategoriasExtractor
from extract.nature_operation import NaturezaOperacaoExtractor
from extract.accounts_payable import ContasPagarExtractor
from extract.accounts_payable_details import ContasPagarDetalhesExtractor
from extract.accounts_receivable import ContasReceberExtractor
from extract.nfe import NFeExtractor
from extract.nfe_details import NFeDetalhesExtractor
from transform.methods_accounts_payable_dw import FormasPagamentoTransformer
from transform.categories_payable_dw import CategoriasContasPagarTransformer
from transform.nature_operation_dw import NaturezaOperacaoTransformer
from transform.accounts_payable_dw import ContasPagarTransformer
from transform.accounts_receivable_dw import ContasReceberTransformer
from transform.nfe_dw import NFeTransformer

# =====================================================
# CONFIGURAÇÃO GLOBAL
# =====================================================

# Carregar variáveis de ambiente
load_dotenv()

# Empresa ID
EMPRESA_ID = 1

# OBTER TOKEN VÁLIDO AUTOMATICAMENTE (renova se necessário)
print("\n🔑 Obtendo token válido para Empresa 01...")
API_KEY_EMPRESA_1 = obter_token_para_empresa(EMPRESA_ID)
print(f"✅ Token obtido e validado!\n")

# =====================================================
# FUNÇÃO AUXILIAR
# =====================================================

def formatar_tempo(segundos):
    """Formata segundos em formato legível"""
    if segundos < 60:
        return f"{segundos:.2f}s"
    elif segundos < 3600:
        minutos = segundos / 60
        return f"{minutos:.2f} minutos ({segundos:.2f}s)"
    else:
        horas = segundos / 3600
        return f"{horas:.2f} horas ({segundos:.2f}s)"

# =====================================================
# 1. EXECUÇÃO COMPLETA - EXTRAÇÃO (MODO INCREMENTAL)
# =====================================================

def executar_extracao_completa():
    """
    Executa a extração de todos os endpoints em sequência
    MODO INCREMENTAL: Extração otimizada
    """
    print("\n🚀 FASE 1: EXTRAÇÃO COMPLETA DE TODOS OS ENDPOINTS")
    print("=" * 70)
    print(f"📌 Empresa ID: {EMPRESA_ID}")
    print("⚡ MODO INCREMENTAL: Extração otimizada SEM limpeza")
    print("📊 PARTE COMERCIAL + 💰 PARTE FINANCEIRA")
    print("=" * 70)
    
    inicio_extracao = time.time()
    
    # Lista dos extratores (MODO INCREMENTAL)
    extratores = [
        # === PARTE COMERCIAL ===
        ("📊 👥 CONTATOS ⚡", ContatosCompletoExtractor, 
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, 
         {'extraction_mode': ExtractionMode.INCREMENTAL}),
        
        ("📊 🏭 PRODUTOS ⚡", ProdutosExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, 
         {'extraction_mode': ExtractionMode.INCREMENTAL}),
        
        ("📊 💰 VENDAS ⚡ (Lista)", VendasExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, 
         {'extraction_mode': ExtractionMode.INCREMENTAL}),
        
        ("📊 🛒 VENDAS (Detalhes + Itens)", VendasDetalhesExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID},
         {'delay_entre_requests': 0.4, 'batch_size': 100}),
        
        # === PARTE FINANCEIRA - TABELAS DE APOIO ===
        ("💰 💳 FORMAS DE PAGAMENTO", FormasPagamentosExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, {}),
        
        ("💰 📂 CATEGORIAS (Receitas/Despesas)", CategoriasExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, {}),
        
        ("💰 🌿 NATUREZA DE OPERAÇÃO", NaturezaOperacaoExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, {}),
        
        # === PARTE FINANCEIRA - DADOS PRINCIPAIS ===
        ("💰 💵 CONTAS A PAGAR ⚡ (Lista)", ContasPagarExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, 
         {'extraction_mode': ExtractionMode.INCREMENTAL}),
        
        ("💰 🔍 CONTAS A PAGAR (Detalhes + Categoria)", ContasPagarDetalhesExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID},
         {'delay_entre_requests': 0.35, 'batch_size': 100}),
        
        ("💰 💸 CONTAS A RECEBER ⚡ (Lista)", ContasReceberExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, 
         {'extraction_mode': ExtractionMode.INCREMENTAL}),
        
        ("💰 📄 NFe ⚡ (Entrada + Saída)", NFeExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, 
         {'extraction_mode': ExtractionMode.INCREMENTAL}),
        
        ("💰 🔍 NFe (Detalhes + Enriquecimento)", NFeDetalhesExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID},
         {'delay_entre_requests': 0.35, 'batch_size': 100})
    ]
    
    resultados_extracao = []
    
    for nome_endpoint, ExtractorClass, init_params, exec_params in extratores:
        try:
            print(f"\n{nome_endpoint}")
            print("-" * 70)
            
            inicio_endpoint = time.time()
            
            # Criar extrator
            extrator = ExtractorClass(**init_params)
            
            # Verificar se precisa de parâmetros especiais
            if exec_params:
                # Extrair com parâmetros personalizados
                if ExtractorClass == VendasDetalhesExtractor:
                    extrator.executar_extracao_detalhes(**exec_params)
                elif ExtractorClass == ContasPagarDetalhesExtractor:
                    extrator.executar_extracao_detalhes(**exec_params)
                elif ExtractorClass == NFeDetalhesExtractor:
                    extrator.executar_enriquecimento_completo(**exec_params)
                else:
                    extrator.executar_extracao_completa(**exec_params)
            else:
                # Executar normalmente
                extrator.executar_extracao_completa()
            
            fim_endpoint = time.time()
            tempo_endpoint = fim_endpoint - inicio_endpoint
            
            resultados_extracao.append({
                'endpoint': nome_endpoint,
                'status': 'SUCCESS',
                'tempo': tempo_endpoint
            })
            
            print(f"✅ {nome_endpoint} concluído em {formatar_tempo(tempo_endpoint)}")
            
        except Exception as e:
            fim_endpoint = time.time()
            tempo_endpoint = fim_endpoint - inicio_endpoint
            
            resultados_extracao.append({
                'endpoint': nome_endpoint,
                'status': 'ERROR',
                'tempo': tempo_endpoint,
                'erro': str(e)
            })
            
            print(f"❌ ERRO em {nome_endpoint}: {e}")
            print("Continuando com próximo endpoint...")
    
    # Relatório extração
    fim_extracao = time.time()
    tempo_extracao = fim_extracao - inicio_extracao
    
    print(f"\n✅ EXTRAÇÃO COMPLETA FINALIZADA")
    print("=" * 70)
    print(f"⏱️ Tempo total: {formatar_tempo(tempo_extracao)}")
    print("\n📊 RESUMO DOS RESULTADOS:")
    
    sucessos = sum(1 for r in resultados_extracao if r['status'] == 'SUCCESS')
    erros = sum(1 for r in resultados_extracao if r['status'] == 'ERROR')
    
    for resultado in resultados_extracao:
        status_emoji = "✅" if resultado['status'] == 'SUCCESS' else "❌"
        print(f"{status_emoji} {resultado['endpoint']}: {formatar_tempo(resultado['tempo'])}")
        
        if resultado['status'] == 'ERROR':
            print(f"   └── Erro: {resultado.get('erro', 'N/A')}")
    
    print(f"\n🎯 ESTATÍSTICAS FINAIS DA EXTRAÇÃO:")
    print(f"✅ Sucessos: {sucessos}/{len(extratores)}")
    print(f"❌ Erros: {erros}/{len(extratores)}")
    
    return resultados_extracao

# =====================================================
# 2. EXECUÇÃO COMPLETA - TRANSFORMAÇÃO
# =====================================================

def executar_transformacao_completa():
    """
    Executa a transformação de todos os dados RAW para DW
    """
    print(f"\n{'='*70}")
    print("🔄 FASE 2: TRANSFORMAÇÃO DOS DADOS")
    print(f"{'='*70}")
    print(f"📌 Empresa ID: {EMPRESA_ID}")
    print("📊 PARTE COMERCIAL + 💰 PARTE FINANCEIRA")
    print(f"{'='*70}")
    
    inicio_transformacao = time.time()
    
    # Modo incremental - processar apenas registros 'pendente'
    print("\n▶️  Modo incremental: processando apenas registros 'pendente'...")

    session = Session()
    try:
        # Verificar quantos registros pendentes existem
        print("\n📊 VERIFICANDO REGISTROS PENDENTES (EMPRESA 1)...")
        print("-" * 70)
        
        # Parte Comercial
        contatos_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.contatos_raw WHERE status_processamento = 'pendente' AND empresa_id = :emp_id"
        ), {'emp_id': EMPRESA_ID}).scalar()
        
        produtos_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.produtos_raw WHERE status_processamento = 'pendente' AND empresa_id = :emp_id"
        ), {'emp_id': EMPRESA_ID}).scalar()
        
        vendas_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.vendas_raw WHERE status_processamento = 'pendente' AND empresa_id = :emp_id"
        ), {'emp_id': EMPRESA_ID}).scalar()
        
        # Parte Financeira
        formas_pagamento_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.formas_pagamentos_raw WHERE empresa_id = :emp_id"
        ), {'emp_id': EMPRESA_ID}).scalar() or 0
        
        categorias_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.categorias_contas_pagar_raw WHERE empresa_id = :emp_id"
        ), {'emp_id': EMPRESA_ID}).scalar() or 0
        
        natureza_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.natureza_operacao_raw WHERE empresa_id = :emp_id"
        ), {'emp_id': EMPRESA_ID}).scalar() or 0
        
        contas_pagar_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.contas_pagar_raw WHERE status_processamento = 'pendente' AND empresa_id = :emp_id"
        ), {'emp_id': EMPRESA_ID}).scalar() or 0
        
        contas_receber_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.contas_receber_raw WHERE status_processamento = 'pendente' AND empresa_id = :emp_id"
        ), {'emp_id': EMPRESA_ID}).scalar() or 0
        
        nfe_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.nfe_raw WHERE empresa_id = :emp_id"
        ), {'emp_id': EMPRESA_ID}).scalar() or 0
        
        print(f"📊 PARTE COMERCIAL:")
        print(f"   • Contatos pendentes: {contatos_pendentes}")
        print(f"   • Produtos pendentes: {produtos_pendentes}")
        print(f"   • Vendas pendentes: {vendas_pendentes}")
        
        print(f"\n💰 PARTE FINANCEIRA:")
        print(f"   • Formas de Pagamento: {formas_pagamento_pendentes}")
        print(f"   • Categorias: {categorias_pendentes}")
        print(f"   • Natureza de Operação: {natureza_pendentes}")
        print(f"   • Contas a Pagar pendentes: {contas_pagar_pendentes}")
        print(f"   • Contas a Receber pendentes: {contas_receber_pendentes}")
        print(f"   • NFe: {nfe_pendentes}")
        
        total_pendentes = (contatos_pendentes + produtos_pendentes + vendas_pendentes + 
                          contas_pagar_pendentes + contas_receber_pendentes)
        
        if total_pendentes == 0 and formas_pagamento_pendentes == 0 and categorias_pendentes == 0 and nfe_pendentes == 0:
            print("\n✨ Nenhum registro pendente - DW já está atualizado!")
            return []
            
    except Exception as e:
        print(f"⚠️  Erro ao verificar status: {e}")
    finally:
        session.close()
    
    # Executar transformações
    transformadores = [
        # === PARTE COMERCIAL - DIMENSÕES ===
        ("📊 👥 CONTATOS", ContatosTransformer, {'empresa_id': EMPRESA_ID}),
        ("📊 🏭 PRODUTOS", ProdutosTransformer, {'empresa_id': EMPRESA_ID}),
        
        # === PARTE FINANCEIRA - DIMENSÕES DE APOIO ===
        ("💰 💳 FORMAS DE PAGAMENTO", FormasPagamentoTransformer, {'empresa_id': EMPRESA_ID}),
        ("💰 📂 CATEGORIAS", CategoriasContasPagarTransformer, {'empresa_id': EMPRESA_ID}),
        ("💰 🌿 NATUREZA DE OPERAÇÃO", NaturezaOperacaoTransformer, {'empresa_id': EMPRESA_ID}),
        
        # === PARTE COMERCIAL - FATOS ===
        ("📊 💰 VENDAS", VendasTransformer, {'empresa_id': EMPRESA_ID}),
        ("📊 🛒 ITENS", ItensTransformer, {'empresa_id': EMPRESA_ID}),
        
        # === PARTE FINANCEIRA - FATOS ===
        ("💰 💵 CONTAS A PAGAR", ContasPagarTransformer, {'empresa_id': EMPRESA_ID}),
        ("💰 💸 CONTAS A RECEBER", ContasReceberTransformer, {'empresa_id': EMPRESA_ID}),
        ("💰 📄 NFe", NFeTransformer, {'empresa_id': EMPRESA_ID})
    ]
    
    resultados_transformacao = []
    
    for nome, TransformerClass, init_params in transformadores:
        try:
            print(f"\n{nome}")
            print("-" * 70)
            
            inicio_transform = time.time()
            
            # Criar transformer
            transformer = TransformerClass(**init_params)
            transformer.executar_transformacao_completa()
            
            fim_transform = time.time()
            tempo_transform = fim_transform - inicio_transform
            
            resultados_transformacao.append({
                'transformador': nome,
                'status': 'SUCCESS',
                'tempo': tempo_transform
            })
            
            print(f"✅ {nome} transformado em {formatar_tempo(tempo_transform)}")
            
        except Exception as e:
            fim_transform = time.time()
            tempo_transform = fim_transform - inicio_transform
            
            resultados_transformacao.append({
                'transformador': nome,
                'status': 'ERROR',
                'tempo': tempo_transform,
                'erro': str(e)
            })
            
            print(f"❌ ERRO ao transformar {nome}: {e}")
            print("⚠️  Continuando com próximo transformer...")
    
    # Relatório transformação
    fim_transformacao = time.time()
    tempo_transformacao = fim_transformacao - inicio_transformacao
    
    print(f"\n✅ TRANSFORMAÇÃO COMPLETA FINALIZADA")
    print("=" * 70)
    print(f"⏱️ Tempo total: {formatar_tempo(tempo_transformacao)}")
    print("\n📊 RESUMO DOS RESULTADOS:")
    
    sucessos = sum(1 for r in resultados_transformacao if r['status'] == 'SUCCESS')
    erros = sum(1 for r in resultados_transformacao if r['status'] == 'ERROR')
    
    for resultado in resultados_transformacao:
        status_emoji = "✅" if resultado['status'] == 'SUCCESS' else "❌"
        print(f"{status_emoji} {resultado['transformador']}: {formatar_tempo(resultado['tempo'])}")
        
        if resultado['status'] == 'ERROR':
            print(f"   └── Erro: {resultado.get('erro', 'N/A')}")
    
    print(f"\n🎯 ESTATÍSTICAS FINAIS DA TRANSFORMAÇÃO:")
    print(f"✅ Sucessos: {sucessos}/{len(transformadores)}")
    print(f"❌ Erros: {erros}/{len(transformadores)}")
    
    return resultados_transformacao

# =====================================================
# 3. PIPELINE COMPLETO - COM PROTEÇÃO CONTRA FALHAS
# =====================================================

def executar_pipeline_completo():
    """
    Executa o pipeline completo: Extração + Transformação
    MODO INCREMENTAL: Extração otimizada sem limpeza
    
    🛡️ PROTEÇÃO IMPLEMENTADA:
       • Verifica se extração foi bem-sucedida
       • Se houver erros críticos, ABORTA transformação
       • Evita perda de dados por API Key expirada ou erros de conexão
    """
    print("\n" + "=" * 70)
    print("🔄 PIPELINE COMPLETO - MODO INCREMENTAL: EMPRESA 1")
    print("=" * 70)
    print(f"📌 Empresa ID: {EMPRESA_ID}")
    print("📊 PARTE COMERCIAL: Contatos, Produtos, Vendas, Itens")
    print("💰 PARTE FINANCEIRA: Contas a Pagar, Receber, NFe")
    print("🛡️ PROTEÇÃO: Aborta transformação se extração falhar")
    print("⚡ MODO INCREMENTAL: Extração otimizada SEM limpeza")
    print("Executar a cada 2 horas")
    print("=" * 70)
    
    inicio_pipeline = time.time()
    
    # =====================================================
    # FASE 1: EXTRAÇÃO
    # =====================================================

    resultados_extracao = executar_extracao_completa()
    
    # =====================================================
    # 🛡️ PROTEÇÃO CRÍTICA - VERIFICAR SE EXTRAÇÃO FOI BEM-SUCEDIDA
    # =====================================================
    erros_criticos = sum(1 for r in resultados_extracao if r['status'] == 'ERROR')
    
    if erros_criticos > 0:
        print(f"\n{'='*70}")
        print(f"🚨 ATENÇÃO: {erros_criticos} EXTRAÇÃO(ÕES) FALHARAM!")
        print(f"{'='*70}")
        print(f"🛡️ ABORTANDO TRANSFORMAÇÃO para evitar perda de dados")
        print(f"\n📋 ERROS DETECTADOS:")
        
        for resultado in resultados_extracao:
            if resultado['status'] == 'ERROR':
                print(f"   ❌ {resultado['endpoint']}")
                print(f"      └── {resultado.get('erro', 'Erro desconhecido')}")
        
        print(f"\n🔧 AÇÕES NECESSÁRIAS:")
        print(f"   1. Verifique se a API Key está válida (não expirou)")
        print(f"   2. Verifique a conexão com a API do Bling")
        print(f"   3. Corrija os erros listados acima")
        print(f"   4. Execute o pipeline novamente")
        
        print(f"\n💾 DADOS PRESERVADOS:")
        print(f"   • Nenhum dado foi deletado da PROCESSED")
        print(f"   • O DW permanece com os dados anteriores")
        print(f"   • Sistema protegeu contra perda de dados")
        
        print(f"\n{'='*70}")
        print(f"⚠️ PIPELINE INTERROMPIDO COM SEGURANÇA")
        print(f"{'='*70}")
        
        # Retornar sem executar transformação
        return
    
    # =====================================================
    # FASE 2: TRANSFORMAÇÃO (SÓ RODA SE EXTRAÇÃO OK)
    # =====================================================
    
    print(f"\n{'='*70}")
    print(f"✅ EXTRAÇÃO CONCLUÍDA SEM ERROS")
    print(f"{'='*70}")
    print(f"🔄 Prosseguindo com transformação...")
    
    resultados_transformacao = executar_transformacao_completa()
    
    # =====================================================
    # RELATÓRIO FINAL CONSOLIDADO
    # =====================================================

    fim_pipeline = time.time()
    tempo_total = fim_pipeline - inicio_pipeline
    
    print(f"\n{'='*70}")
    print(f"🏁 PIPELINE COMPLETO FINALIZADO")
    print(f"{'='*70}")
    print(f"⏱️  Tempo total do pipeline: {formatar_tempo(tempo_total)}")
    
    # Calcular tempo de extração e transformação
    tempo_extracao_total = sum(r['tempo'] for r in resultados_extracao)
    tempo_transformacao_total = sum(r['tempo'] for r in resultados_transformacao) if resultados_transformacao else 0
    
    print(f"\n📊 BREAKDOWN DE TEMPO:")
    print("-" * 70)
    perc_extracao = (tempo_extracao_total/tempo_total)*100 if tempo_total > 0 else 0
    perc_transformacao = (tempo_transformacao_total/tempo_total)*100 if tempo_total > 0 else 0
    print(f"🔵 EXTRAÇÃO:      {formatar_tempo(tempo_extracao_total):>20} ({perc_extracao:5.1f}%)")
    print(f"🟢 TRANSFORMAÇÃO: {formatar_tempo(tempo_transformacao_total):>20} ({perc_transformacao:5.1f}%)")
    overhead = tempo_total - (tempo_extracao_total + tempo_transformacao_total)
    perc_overhead = (overhead/tempo_total)*100 if tempo_total > 0 else 0
    print(f"🟡 OVERHEAD:      {formatar_tempo(overhead):>20} ({perc_overhead:5.1f}%)")
    
    # Estatísticas consolidadas
    total_extracao = len(resultados_extracao)
    sucesso_extracao = sum(1 for r in resultados_extracao if r['status'] == 'SUCCESS')
    
    total_transformacao = len(resultados_transformacao) if resultados_transformacao else 0
    sucesso_transformacao = sum(1 for r in resultados_transformacao if r['status'] == 'SUCCESS') if resultados_transformacao else 0
    
    print(f"\n📊 RESUMO GERAL:")
    print(f"   • Extração: {sucesso_extracao}/{total_extracao} sucessos")
    print(f"   • Transformação: {sucesso_transformacao}/{total_transformacao} sucessos")
    
    # Identificar gargalos
    print(f"\n🔍 ANÁLISE DE GARGALOS:")
    print("-" * 70)
    
    todos_resultados = resultados_extracao + (resultados_transformacao if resultados_transformacao else [])
    todos_ordenados = sorted(todos_resultados, key=lambda x: x.get('tempo', 0), reverse=True)
    
    print(f"\n🐌 TOP 10 PROCESSOS MAIS LENTOS:")
    print("=" * 70)
    for i, resultado in enumerate(todos_ordenados[:10], 1):
        nome = resultado.get('endpoint') or resultado.get('transformador')
        tempo = resultado['tempo']
        percentual = (tempo / tempo_total) * 100 if tempo_total > 0 else 0
        status = "✅" if resultado['status'] == 'SUCCESS' else "❌"
        print(f"{i:2d}. {status} {nome}")
        print(f"    ⏱️  {formatar_tempo(tempo):>20} | {percentual:5.1f}% do total")
    
    # Estatísticas do DW
    print(f"\n📈 ESTATÍSTICAS DO DATA WAREHOUSE (EMPRESA 1):")
    print("-" * 70)
    session = Session()
    try:
        # Contatos
        query = text("SELECT COUNT(*) FROM processed.dim_contatos WHERE empresa_id = :emp_id")
        total_contatos = session.execute(query, {'emp_id': EMPRESA_ID}).scalar()
        print(f"   • dim_contatos: {total_contatos:,} registros")
        
        # Produtos
        query = text("SELECT COUNT(*) FROM processed.dim_produtos WHERE empresa_id = :emp_id")
        total_produtos = session.execute(query, {'emp_id': EMPRESA_ID}).scalar()
        print(f"   • dim_produtos: {total_produtos:,} registros")
        
        # Pedidos
        query = text("SELECT COUNT(*) FROM processed.fato_pedidos WHERE empresa_id = :emp_id")
        total_pedidos = session.execute(query, {'emp_id': EMPRESA_ID}).scalar()
        print(f"   • fato_pedidos: {total_pedidos:,} registros")
        
        # Itens
        query = text("SELECT COUNT(*) FROM processed.fato_itens_pedidos WHERE empresa_id = :emp_id")
        total_itens = session.execute(query, {'emp_id': EMPRESA_ID}).scalar()
        print(f"   • fato_itens_pedidos: {total_itens:,} registros")
        
        # Contas a Pagar
        query = text("SELECT COUNT(*) FROM processed.fato_contas_pagar WHERE empresa_id = :emp_id")
        total_contas_pagar = session.execute(query, {'emp_id': EMPRESA_ID}).scalar()
        print(f"   • fato_contas_pagar: {total_contas_pagar:,} registros")
        
        # Contas a Receber
        query = text("SELECT COUNT(*) FROM processed.fato_contas_receber WHERE empresa_id = :emp_id")
        total_contas_receber = session.execute(query, {'emp_id': EMPRESA_ID}).scalar()
        print(f"   • fato_contas_receber: {total_contas_receber:,} registros")
        
        # NFe
        query = text("SELECT COUNT(*) FROM processed.fato_nfe WHERE empresa_id = :emp_id")
        total_nfe = session.execute(query, {'emp_id': EMPRESA_ID}).scalar()
        print(f"   • fato_nfe: {total_nfe:,} registros")
        
    except Exception as e:
        print(f"   ⚠️  Erro ao coletar estatísticas: {e}")
    finally:
        session.close()
    
    if sucesso_extracao == total_extracao and sucesso_transformacao == total_transformacao:
        print(f"\n🎉 TODOS OS PROCESSOS EXECUTADOS COM SUCESSO!")
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. DW atualizado com dados incrementais (Empresa 1)")
        print(f"   2. Power BI pode ser atualizado")
        print(f"   3. Execute pipeline FULL semanalmente para sincronização completa")
    else:
        print(f"\n⚠️  Alguns processos falharam. Verifique os logs acima.")

# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    log_file = setup_logging(empresa_id=EMPRESA_ID)
    
    try:
        # Cria os schemas se não existirem
        create_schema_raw()
        create_schema_processed()

        # Cria as tabelas
        create_all_tables()

        # Executar pipeline completo
        executar_pipeline_completo()
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
        print("💾 Dados processados até este ponto foram preservados")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally: 
        # ===== FECHAR LOGGING (SEMPRE) =====
        close_logging()
        print(f"\n📁 Log completo salvo em: {log_file}")
        # ====================================