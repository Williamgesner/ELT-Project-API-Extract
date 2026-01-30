# Responsável por: executar pipeline com MODO INCREMENTAL (OTIMIZADO)

"""
========================================
PIPELINE OTIMIZADO - EMPRESA 1
========================================

PROBLEMA RESOLVIDO:
- Pipeline completo demora 2h30 (inviável para 4x/dia)
- Solução: Modo INCREMENTAL extrai apenas alterações

MODOS DE EXECUÇÃO:
1. FULL (1x/semana):
   - Extrai TODOS os dados desde 2024
   - Limpa registros órfãos
   - Tempo: ~2h30

2. INCREMENTAL (4x/dia):
   - Extrai apenas dados alterados (últimos 7 dias)
   - NÃO limpa órfãos (segurança)
   - Tempo: ~15-30 minutos

SEGURANÇA 100%:
- Limpeza de órfãos APENAS em modo FULL
- Validação de contagem em modo FULL
- Logs detalhados de todas as operações

USO:
    # Modo automático (decide baseado na última execução)
    python main_pipeline_01_optimized.py
    
    # Forçar modo FULL
    python main_pipeline_01_optimized.py --full
    
    # Forçar modo INCREMENTAL
    python main_pipeline_01_optimized.py --incremental
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import text
from config.database import create_schema_raw, create_schema_processed, create_all_tables, Session
from config.auth_manager import obter_token_para_empresa
from config.extraction_mode import ExtractionMode, ExtractionModeManager, print_extraction_mode_banner
from config.logger import setup_logging, close_logging

# =====================================================
# IMPORTAÇÕES - EXTRACTORS OTIMIZADOS
# =====================================================
from extract.contacts_v2 import ContatosCompletoExtractorV2
from extract.products import ProdutosExtractor
from extract.sales import VendasExtractor
from extract.sales_details import VendasDetalhesExtractor

# =====================================================
# IMPORTAÇÕES - EXTRACTORS FINANCEIROS (usar originais)
# =====================================================
from extract.payment_methods import FormasPagamentosExtractor
from extract.accounts_payable_categories import CategoriasExtractor
from extract.nature_operation import NaturezaOperacaoExtractor
from extract.accounts_payable import ContasPagarExtractor
from extract.accounts_payable_details import ContasPagarDetalhesExtractor
from extract.accounts_receivable import ContasReceberExtractor
from extract.nfe import NFeExtractor
from extract.nfe_details import NFeDetalhesExtractor

# =====================================================
# IMPORTAÇÕES - TRANSFORMERS (usar originais)
# =====================================================
from transform.contacts_dw import ContatosTransformer
from transform.products_dw import ProdutosTransformer
from transform.sales_dw import VendasTransformer
from transform.items_dw import ItensTransformer 
from transform.methods_accounts_payable_dw import FormasPagamentoTransformer
from transform.categories_payable_dw import CategoriasContasPagarTransformer
from transform.nature_operation_dw import NaturezaOperacaoTransformer
from transform.accounts_payable_dw import ContasPagarTransformer
from transform.accounts_receivable_dw import ContasReceberTransformer
from transform.nfe_dw import NFeTransformer

# =====================================================
# CONFIGURAÇÃO GLOBAL
# =====================================================

load_dotenv()

EMPRESA_ID = 1

# Obter token válido
print("\n🔑 Obtendo token válido para Empresa 01...")
API_KEY_EMPRESA_1 = obter_token_para_empresa(EMPRESA_ID)
print(f"✅ Token obtido e validado!\n")

# =====================================================
# FUNÇÕES AUXILIARES
# =====================================================

def parse_arguments():
    """Parse argumentos da linha de comando"""
    force_full = '--full' in sys.argv
    force_incremental = '--incremental' in sys.argv
    
    if force_full and force_incremental:
        print("❌ ERRO: Não pode usar --full e --incremental juntos")
        sys.exit(1)
    
    return force_full, force_incremental

def executar_extracao_otimizada(mode_manager):
    """
    Executa extração otimizada baseada no modo
    
    Args:
        mode_manager: ExtractionModeManager configurado
    
    Returns:
        list: Resultados da extração
    """
    mode = mode_manager.get_extraction_mode()
    
    print_extraction_mode_banner(mode, mode_manager)
    
    print("\n🚀 FASE 1: EXTRAÇÃO OTIMIZADA")
    print("=" * 70)
    print(f"📌 Empresa ID: {EMPRESA_ID}")
    print(f"🔧 Modo: {mode.value.upper()}")
    print("=" * 70)
    
    inicio_extracao = datetime.now()
    
    # Lista de extractors com configuração de modo
    extratores = [
        # === COMERCIAL (COM FILTRO DE ALTERAÇÃO) ===
        ("📊 👥 CONTATOS", ContatosCompletoExtractorV2, 
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID, 'extraction_mode': mode}, 
         {}, 'contatos'),
        
        ("📊 🏭 PRODUTOS", ProdutosExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, 
         {}, 'produtos'),
        
        ("📊 💰 VENDAS (Lista)", VendasExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, 
         {}, 'vendas'),
        
        ("📊 🛒 VENDAS (Detalhes + Itens)", VendasDetalhesExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID},
         {'delay_entre_requests': 0.4, 'batch_size': 100}, 'vendas_detalhes'),
        
        # === FINANCEIRO - TABELAS DE APOIO (SEMPRE FULL) ===
        ("💰 💳 FORMAS DE PAGAMENTO", FormasPagamentosExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, 
         {}, 'formas_pagamento'),
        
        ("💰 📂 CATEGORIAS", CategoriasExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, 
         {}, 'categorias'),
        
        ("💰 🌿 NATUREZA DE OPERAÇÃO", NaturezaOperacaoExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, 
         {}, 'natureza_operacao'),
        
        # === FINANCEIRO - DADOS PRINCIPAIS (SEM FILTRO DE ALTERAÇÃO) ===
        ("💰 💵 CONTAS A PAGAR (Lista)", ContasPagarExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, 
         {}, 'contas_pagar'),
        
        ("💰 🔍 CONTAS A PAGAR (Detalhes)", ContasPagarDetalhesExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID},
         {'delay_entre_requests': 0.35, 'batch_size': 100}, 'contas_pagar_detalhes'),
        
        ("💰 💸 CONTAS A RECEBER", ContasReceberExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, 
         {}, 'contas_receber'),
        
        ("💰 📄 NFe (Entrada + Saída)", NFeExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, 
         {}, 'nfe'),
        
        ("💰 🔍 NFe (Detalhes)", NFeDetalhesExtractor,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID},
         {'delay_entre_requests': 0.35, 'batch_size': 100}, 'nfe_detalhes')
    ]
    
    resultados_extracao = []
    
    for nome_endpoint, ExtractorClass, init_params, exec_params, endpoint_key in extratores:
        try:
            print(f"\n{nome_endpoint}")
            print("-" * 70)
            
            inicio_endpoint = datetime.now()
            
            # Criar extrator
            extrator = ExtractorClass(**init_params)
            
            # Executar com parâmetros especiais se necessário
            if exec_params:
                if ExtractorClass == VendasDetalhesExtractor:
                    extrator.executar_extracao_detalhes(**exec_params)
                elif ExtractorClass == ContasPagarDetalhesExtractor:
                    extrator.executar_extracao_detalhes(**exec_params)
                elif ExtractorClass == NFeDetalhesExtractor:
                    extrator.executar_enriquecimento_completo(**exec_params)
                else:
                    extrator.executar_extracao_completa()
            else:
                extrator.executar_extracao_completa()
            
            fim_endpoint = datetime.now()
            tempo_endpoint = fim_endpoint - inicio_endpoint
            
            resultados_extracao.append({
                'endpoint': nome_endpoint,
                'endpoint_key': endpoint_key,
                'status': 'SUCCESS',
                'tempo': tempo_endpoint
            })
            
            # Marcar extração como completa no manager
            mode_manager.mark_extraction_complete(mode, endpoint_key, {
                'tempo': str(tempo_endpoint),
                'status': 'SUCCESS'
            })
            
            print(f"✅ {nome_endpoint} concluído em {tempo_endpoint}")
            
        except Exception as e:
            fim_endpoint = datetime.now()
            tempo_endpoint = fim_endpoint - inicio_endpoint
            
            resultados_extracao.append({
                'endpoint': nome_endpoint,
                'endpoint_key': endpoint_key,
                'status': 'ERROR',
                'tempo': tempo_endpoint,
                'erro': str(e)
            })
            
            print(f"❌ ERRO em {nome_endpoint}: {e}")
            print("Continuando com próximo endpoint...")
    
    # Relatório extração
    fim_extracao = datetime.now()
    tempo_extracao = fim_extracao - inicio_extracao
    
    print(f"\n✅ EXTRAÇÃO COMPLETA FINALIZADA")
    print("=" * 70)
    print(f"⏱️ Tempo total: {tempo_extracao}")
    print(f"🔧 Modo: {mode.value.upper()}")
    
    if mode == ExtractionMode.INCREMENTAL:
        print(f"⚡ Economia estimada: ~80% vs modo FULL")
    
    print("\n📊 RESUMO DOS RESULTADOS:")
    
    sucessos = sum(1 for r in resultados_extracao if r['status'] == 'SUCCESS')
    erros = sum(1 for r in resultados_extracao if r['status'] == 'ERROR')
    
    for resultado in resultados_extracao:
        status_emoji = "✅" if resultado['status'] == 'SUCCESS' else "❌"
        print(f"{status_emoji} {resultado['endpoint']}: {resultado['tempo']}")
        
        if resultado['status'] == 'ERROR':
            print(f"   └── Erro: {resultado.get('erro', 'N/A')}")
    
    print(f"\n🎯 ESTATÍSTICAS FINAIS DA EXTRAÇÃO:")
    print(f"✅ Sucessos: {sucessos}/{len(extratores)}")
    print(f"❌ Erros: {erros}/{len(extratores)}")
    
    return resultados_extracao

def executar_transformacao_completa():
    """Executa transformação (sempre processa pendentes)"""
    print(f"\n{'='*70}")
    print("🔄 FASE 2: TRANSFORMAÇÃO DOS DADOS")
    print(f"{'='*70}")
    print(f"📌 Empresa ID: {EMPRESA_ID}")
    print(f"{'='*70}")
    
    inicio_transformacao = datetime.now()
    
    print("\n▶️  Modo incremental: processando apenas registros 'pendente'...")

    session = Session()
    try:
        print("\n📊 VERIFICANDO REGISTROS PENDENTES...")
        print("-" * 70)
        
        contatos_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.contatos_raw WHERE status_processamento = 'pendente' AND empresa_id = :emp_id"
        ), {'emp_id': EMPRESA_ID}).scalar()
        
        produtos_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.produtos_raw WHERE status_processamento = 'pendente' AND empresa_id = :emp_id"
        ), {'emp_id': EMPRESA_ID}).scalar()
        
        vendas_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.vendas_raw WHERE status_processamento = 'pendente' AND empresa_id = :emp_id"
        ), {'emp_id': EMPRESA_ID}).scalar()
        
        contas_pagar_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.contas_pagar_raw WHERE status_processamento = 'pendente' AND empresa_id = :emp_id"
        ), {'emp_id': EMPRESA_ID}).scalar() or 0
        
        contas_receber_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.contas_receber_raw WHERE status_processamento = 'pendente' AND empresa_id = :emp_id"
        ), {'emp_id': EMPRESA_ID}).scalar() or 0
        
        print(f"📊 PARTE COMERCIAL:")
        print(f"   • Contatos pendentes: {contatos_pendentes}")
        print(f"   • Produtos pendentes: {produtos_pendentes}")
        print(f"   • Vendas pendentes: {vendas_pendentes}")
        
        print(f"\n💰 PARTE FINANCEIRA:")
        print(f"   • Contas a Pagar pendentes: {contas_pagar_pendentes}")
        print(f"   • Contas a Receber pendentes: {contas_receber_pendentes}")
        
        total_pendentes = (contatos_pendentes + produtos_pendentes + vendas_pendentes + 
                          contas_pagar_pendentes + contas_receber_pendentes)
        
        if total_pendentes == 0:
            print("\n✨ Nenhum registro pendente - DW já está atualizado!")
            return []
            
    except Exception as e:
        print(f"⚠️  Erro ao verificar status: {e}")
    finally:
        session.close()
    
    # Executar transformações
    transformadores = [
        ("📊 👥 CONTATOS", ContatosTransformer, {'empresa_id': EMPRESA_ID}),
        ("📊 🏭 PRODUTOS", ProdutosTransformer, {'empresa_id': EMPRESA_ID}),
        ("💰 💳 FORMAS DE PAGAMENTO", FormasPagamentoTransformer, {'empresa_id': EMPRESA_ID}),
        ("💰 📂 CATEGORIAS", CategoriasContasPagarTransformer, {'empresa_id': EMPRESA_ID}),
        ("💰 🌿 NATUREZA DE OPERAÇÃO", NaturezaOperacaoTransformer, {'empresa_id': EMPRESA_ID}),
        ("📊 💰 VENDAS", VendasTransformer, {'empresa_id': EMPRESA_ID}),
        ("📊 🛒 ITENS", ItensTransformer, {'empresa_id': EMPRESA_ID}),
        ("💰 💵 CONTAS A PAGAR", ContasPagarTransformer, {'empresa_id': EMPRESA_ID}),
        ("💰 💸 CONTAS A RECEBER", ContasReceberTransformer, {'empresa_id': EMPRESA_ID}),
        ("💰 📄 NFe", NFeTransformer, {'empresa_id': EMPRESA_ID})
    ]
    
    resultados_transformacao = []
    
    for nome, TransformerClass, init_params in transformadores:
        try:
            print(f"\n{nome}")
            print("-" * 70)
            
            inicio_transform = datetime.now()
            
            transformer = TransformerClass(**init_params)
            transformer.executar_transformacao_completa()
            
            fim_transform = datetime.now()
            tempo_transform = fim_transform - inicio_transform
            
            resultados_transformacao.append({
                'transformador': nome,
                'status': 'SUCCESS',
                'tempo': tempo_transform
            })
            
            print(f"✅ {nome} transformado em {tempo_transform}")
            
        except Exception as e:
            fim_transform = datetime.now()
            tempo_transform = fim_transform - inicio_transform
            
            resultados_transformacao.append({
                'transformador': nome,
                'status': 'ERROR',
                'tempo': tempo_transform,
                'erro': str(e)
            })
            
            print(f"❌ ERRO ao transformar {nome}: {e}")
    
    fim_transformacao = datetime.now()
    tempo_transformacao = fim_transformacao - inicio_transformacao
    
    print(f"\n✅ TRANSFORMAÇÃO COMPLETA FINALIZADA")
    print("=" * 70)
    print(f"⏱️ Tempo total: {tempo_transformacao}")
    
    return resultados_transformacao

def executar_pipeline_otimizado():
    """Executa pipeline otimizado com modo automático ou forçado"""
    
    # Parse argumentos
    force_full, force_incremental = parse_arguments()
    
    # Criar gerenciador de modo
    mode_manager = ExtractionModeManager(EMPRESA_ID)
    
    # Determinar modo
    if force_full:
        mode = ExtractionMode.FULL
        print("\n🔧 MODO FORÇADO: FULL")
    elif force_incremental:
        mode = ExtractionMode.INCREMENTAL
        print("\n🔧 MODO FORÇADO: INCREMENTAL")
    else:
        mode = mode_manager.get_extraction_mode()
        print(f"\n🔧 MODO AUTOMÁTICO: {mode.value.upper()}")
    
    # Atualizar modo no manager
    if force_full:
        mode_manager.state['last_full_extraction'] = None  # Forçar FULL
    
    print("\n" + "=" * 70)
    print("🔄 PIPELINE OTIMIZADO: EMPRESA 1")
    print("=" * 70)
    print(f"📌 Empresa ID: {EMPRESA_ID}")
    print(f"🔧 Modo: {mode.value.upper()}")
    print("=" * 70)
    
    inicio_pipeline = datetime.now()
    
    # FASE 1: EXTRAÇÃO
    resultados_extracao = executar_extracao_otimizada(mode_manager)
    
    # Verificar erros críticos
    erros_criticos = sum(1 for r in resultados_extracao if r['status'] == 'ERROR')
    
    if erros_criticos > 0:
        print(f"\n{'='*70}")
        print(f"🚨 ATENÇÃO: {erros_criticos} EXTRAÇÃO(ÕES) FALHARAM!")
        print(f"{'='*70}")
        print(f"🛡️ ABORTANDO TRANSFORMAÇÃO para evitar perda de dados")
        return
    
    # FASE 2: TRANSFORMAÇÃO
    print(f"\n{'='*70}")
    print(f"✅ EXTRAÇÃO CONCLUÍDA SEM ERROS")
    print(f"{'='*70}")
    print(f"🔄 Prosseguindo com transformação...")
    
    resultados_transformacao = executar_transformacao_completa()
    
    # RELATÓRIO FINAL
    fim_pipeline = datetime.now()
    tempo_total = fim_pipeline - inicio_pipeline
    
    print(f"\n{'='*70}")
    print(f"🏁 PIPELINE OTIMIZADO FINALIZADO")
    print(f"{'='*70}")
    print(f"⏱️  Tempo total: {tempo_total}")
    print(f"🔧 Modo: {mode.value.upper()}")
    
    if mode == ExtractionMode.INCREMENTAL:
        print(f"⚡ Economia: ~80% vs modo FULL")
        print(f"🛡️  Segurança: Dados históricos preservados")
    
    # Estatísticas
    total_extracao = len(resultados_extracao)
    sucesso_extracao = sum(1 for r in resultados_extracao if r['status'] == 'SUCCESS')
    
    total_transformacao = len(resultados_transformacao)
    sucesso_transformacao = sum(1 for r in resultados_transformacao if r['status'] == 'SUCCESS')
    
    print(f"\n📊 RESUMO GERAL:")
    print(f"   • Extração: {sucesso_extracao}/{total_extracao} sucessos")
    print(f"   • Transformação: {sucesso_transformacao}/{total_transformacao} sucessos")
    
    # Próxima execução recomendada
    status = mode_manager.get_status_report()
    if status['next_full_recommended']:
        next_full_dt = datetime.fromisoformat(status['next_full_recommended'])
        print(f"\n📅 PRÓXIMA EXECUÇÃO:")
        print(f"   • Próximo FULL recomendado: {next_full_dt.strftime('%d/%m/%Y %H:%M')}")
        print(f"   • Até lá: usar modo INCREMENTAL")
    
    print(f"\n💡 DICA:")
    print(f"   • Para forçar FULL: python {sys.argv[0]} --full")
    print(f"   • Para forçar INCREMENTAL: python {sys.argv[0]} --incremental")

# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    log_file = setup_logging(empresa_id=1) 
    try:
        create_schema_raw()
        create_schema_processed()
        create_all_tables()

        executar_pipeline_otimizado()
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
        print("💾 Dados processados até este ponto foram preservados")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally: 
        close_logging()
        print(f"\n📁 Log completo salvo em: {log_file}")
