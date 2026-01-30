# PIPELINE DE TESTE - VERSÃO OTIMIZADA
# Testa apenas: PRODUTOS (7 dias) + CONTAS A RECEBER (120 dias)
# IDÊNTICO ao main_pipeline_01_com_logs.py mas com extratores otimizados

"""
========================================
PIPELINE DE TESTE - VERSÃO OTIMIZADA
========================================

🎯 OBJETIVO: Comparar performance FULL vs INCREMENTAL

📊 ENDPOINTS TESTADOS:
   • Produtos (modo incremental: 7 dias)
   • Contas a Receber (modo incremental: 120 dias)

⚡ OTIMIZAÇÕES:
   • Extração incremental (janelas reduzidas)
   • Comparação inteligente SEMPRE ativa
   • Limpeza de órfãos DESABILITADA

📈 MÉTRICAS: Mesmos logs do pipeline original para comparação
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
# IMPORTAÇÕES - VERSÕES OTIMIZADAS
# =====================================================
from extract.products_v2 import ProdutosExtractor
from extract.accounts_receivable_v2 import ContasReceberExtractorV2
from transform.products_dw import ProdutosTransformer
from transform.accounts_receivable_dw import ContasReceberTransformer

# =====================================================
# CONFIGURAÇÃO GLOBAL
# =====================================================

load_dotenv()

EMPRESA_ID = 1

print("\n🔑 Obtendo token válido para Empresa 01...")
API_KEY_EMPRESA_1 = obter_token_para_empresa(EMPRESA_ID)
print(f"✅ Token obtido e validado!\n")

# =====================================================
# FUNÇÕES AUXILIARES PARA MÉTRICAS
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
# 1. EXECUÇÃO COMPLETA - EXTRAÇÃO
# =====================================================

def executar_extracao_completa():
    """
    Executa a extração de PRODUTOS e CONTAS A RECEBER em modo INCREMENTAL
    """
    print("\n🚀 FASE 1: EXTRAÇÃO OTIMIZADA (TESTE)")
    print("=" * 70)
    print(f"📌 Empresa ID: {EMPRESA_ID}")
    print("⚡ MODO: INCREMENTAL")
    print("📊 Produtos: 7 dias | 💰 Contas a Receber: 120 dias")
    print("=" * 70)
    
    inicio_extracao_geral = time.time()
    
    # Lista dos extratores OTIMIZADOS
    extratores = [
        ("📊 🏭 PRODUTOS (7 dias)", ProdutosExtractor, 
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID}, 
         {'extraction_mode': ExtractionMode.INCREMENTAL}),
        
        ("💰 💸 CONTAS A RECEBER (120 dias)", ContasReceberExtractorV2,
         {'api_key': API_KEY_EMPRESA_1, 'empresa_id': EMPRESA_ID, 'extraction_mode': ExtractionMode.INCREMENTAL}, 
         {}),
    ]
    
    resultados_extracao = []
    
    for nome_endpoint, ExtractorClass, init_params, exec_params in extratores:
        try:
            print(f"\n{'='*70}")
            print(f"▶️  INICIANDO: {nome_endpoint}")
            print(f"{'='*70}")
            
            inicio_endpoint = time.time()
            
            extrator = ExtractorClass(**init_params)
            
            if exec_params:
                extrator.executar_extracao_completa(**exec_params)
            else:
                extrator.executar_extracao_completa()
            
            fim_endpoint = time.time()
            tempo_endpoint = fim_endpoint - inicio_endpoint
            
            print(f"\n{'='*70}")
            print(f"✅ CONCLUÍDO: {nome_endpoint}")
            print(f"{'='*70}")
            print(f"⏱️  Tempo: {formatar_tempo(tempo_endpoint)}")
            print(f"{'='*70}\n")
            
            resultados_extracao.append({
                'endpoint': nome_endpoint,
                'status': 'SUCCESS',
                'tempo': tempo_endpoint
            })
            
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
    fim_extracao_geral = time.time()
    tempo_extracao_geral = fim_extracao_geral - inicio_extracao_geral
    
    print(f"\n{'='*70}")
    print(f"✅ EXTRAÇÃO COMPLETA FINALIZADA")
    print(f"{'='*70}")
    print(f"⏱️  Tempo total da EXTRAÇÃO: {formatar_tempo(tempo_extracao_geral)}")
    print(f"\n📊 RESUMO DOS RESULTADOS:")
    
    sucessos = sum(1 for r in resultados_extracao if r['status'] == 'SUCCESS')
    erros = sum(1 for r in resultados_extracao if r['status'] == 'ERROR')
    
    # Ordenar por tempo (mais lento primeiro)
    resultados_ordenados = sorted(resultados_extracao, key=lambda x: x['tempo'], reverse=True)
    
    print(f"\n🐌 ENDPOINTS POR TEMPO:")
    print("-" * 70)
    for i, resultado in enumerate(resultados_ordenados, 1):
        status_emoji = "✅" if resultado['status'] == 'SUCCESS' else "❌"
        tempo_formatado = formatar_tempo(resultado['tempo'])
        percentual = (resultado['tempo'] / tempo_extracao_geral) * 100
        print(f"{i}. {status_emoji} {resultado['endpoint']}")
        print(f"   ⏱️  Tempo: {tempo_formatado} ({percentual:.1f}% do total)")
        
        if resultado['status'] == 'ERROR':
            print(f"   └── Erro: {resultado.get('erro', 'N/A')}")
    
    print(f"\n🎯 ESTATÍSTICAS FINAIS DA EXTRAÇÃO:")
    print(f"✅ Sucessos: {sucessos}/{len(extratores)}")
    print(f"❌ Erros: {erros}/{len(extratores)}")
    print(f"{'='*70}\n")
    
    return resultados_extracao

# =====================================================
# 2. EXECUÇÃO COMPLETA - TRANSFORMAÇÃO
# =====================================================

def executar_transformacao_completa():
    """
    Executa a transformação de PRODUTOS e CONTAS A RECEBER
    """
    print(f"\n{'='*70}")
    print("🔄 FASE 2: TRANSFORMAÇÃO DOS DADOS")
    print(f"{'='*70}")
    print(f"📌 Empresa ID: {EMPRESA_ID}")
    print("📊 Produtos + 💰 Contas a Receber")
    print(f"{'='*70}")
    
    inicio_transformacao_geral = time.time()
    
    print("\n▶️  Modo incremental: processando apenas registros 'pendente'...")

    session = Session()
    try:
        print("\n📊 VERIFICANDO REGISTROS PENDENTES (EMPRESA 1)...")
        print("-" * 70)
        
        produtos_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.produtos_raw WHERE status_processamento = 'pendente' AND empresa_id = :emp_id"
        ), {'emp_id': EMPRESA_ID}).scalar()
        
        contas_receber_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.contas_receber_raw WHERE status_processamento = 'pendente' AND empresa_id = :emp_id"
        ), {'emp_id': EMPRESA_ID}).scalar() or 0
        
        print(f"📊 PRODUTOS:")
        print(f"   • Produtos pendentes: {produtos_pendentes:,}")
        
        print(f"\n💰 CONTAS A RECEBER:")
        print(f"   • Contas a Receber pendentes: {contas_receber_pendentes:,}")
        
        total_pendentes = produtos_pendentes + contas_receber_pendentes
        
        if total_pendentes == 0:
            print("\n✨ Nenhum registro pendente - DW já está atualizado!")
            return []
            
    except Exception as e:
        print(f"⚠️  Erro ao verificar status: {e}")
    finally:
        session.close()
    
    # Executar transformações
    transformadores = [
        ("📊 🏭 PRODUTOS", ProdutosTransformer, {'empresa_id': EMPRESA_ID}),
        ("💰 💸 CONTAS A RECEBER", ContasReceberTransformer, {'empresa_id': EMPRESA_ID}),
    ]
    
    resultados_transformacao = []
    
    for nome, TransformerClass, init_params in transformadores:
        try:
            print(f"\n{'='*70}")
            print(f"▶️  INICIANDO: {nome}")
            print(f"{'='*70}")
            
            inicio_transform = time.time()
            
            transformer = TransformerClass(**init_params)
            transformer.executar_transformacao_completa()
            
            fim_transform = time.time()
            tempo_transform = fim_transform - inicio_transform
            
            print(f"\n{'='*70}")
            print(f"✅ CONCLUÍDO: {nome}")
            print(f"{'='*70}")
            print(f"⏱️  Tempo: {formatar_tempo(tempo_transform)}")
            print(f"{'='*70}\n")
            
            resultados_transformacao.append({
                'transformador': nome,
                'status': 'SUCCESS',
                'tempo': tempo_transform
            })
            
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
    fim_transformacao_geral = time.time()
    tempo_transformacao_geral = fim_transformacao_geral - inicio_transformacao_geral
    
    print(f"\n{'='*70}")
    print(f"✅ TRANSFORMAÇÃO COMPLETA FINALIZADA")
    print(f"{'='*70}")
    print(f"⏱️  Tempo total da TRANSFORMAÇÃO: {formatar_tempo(tempo_transformacao_geral)}")
    print(f"\n📊 RESUMO DOS RESULTADOS:")
    
    sucessos = sum(1 for r in resultados_transformacao if r['status'] == 'SUCCESS')
    erros = sum(1 for r in resultados_transformacao if r['status'] == 'ERROR')
    
    # Ordenar por tempo (mais lento primeiro)
    resultados_ordenados = sorted(resultados_transformacao, key=lambda x: x['tempo'], reverse=True)
    
    print(f"\n🐌 TRANSFORMAÇÕES POR TEMPO:")
    print("-" * 70)
    for i, resultado in enumerate(resultados_ordenados, 1):
        status_emoji = "✅" if resultado['status'] == 'SUCCESS' else "❌"
        tempo_formatado = formatar_tempo(resultado['tempo'])
        percentual = (resultado['tempo'] / tempo_transformacao_geral) * 100 if tempo_transformacao_geral > 0 else 0
        print(f"{i}. {status_emoji} {resultado['transformador']}")
        print(f"   ⏱️  Tempo: {tempo_formatado} ({percentual:.1f}% do total)")
        
        if resultado['status'] == 'ERROR':
            print(f"   └── Erro: {resultado.get('erro', 'N/A')}")
    
    print(f"\n🎯 ESTATÍSTICAS FINAIS DA TRANSFORMAÇÃO:")
    print(f"✅ Sucessos: {sucessos}/{len(transformadores)}")
    print(f"❌ Erros: {erros}/{len(transformadores)}")
    print(f"{'='*70}\n")
    
    return resultados_transformacao

# =====================================================
# 3. PIPELINE COMPLETO - COM PROTEÇÃO CONTRA FALHAS
# =====================================================

def executar_pipeline_completo():
    """
    Executa o pipeline de teste: Extração + Transformação
    VERSÃO OTIMIZADA para comparação de performance
    """
    print("\n" + "=" * 70)
    print("🔄 PIPELINE DE TESTE - VERSÃO OTIMIZADA")
    print("=" * 70)
    print(f"📌 Empresa ID: {EMPRESA_ID}")
    print("⚡ MODO: INCREMENTAL")
    print("📊 Produtos (7 dias) + 💰 Contas a Receber (120 dias)")
    print("🛡️ PROTEÇÃO: Aborta transformação se extração falhar")
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
        
        print(f"\n{'='*70}")
        print(f"⚠️ PIPELINE INTERROMPIDO COM SEGURANÇA")
        print(f"{'='*70}")
        
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
    print(f"🏁 PIPELINE DE TESTE FINALIZADO")
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
    
    print(f"\n🐌 PROCESSOS POR TEMPO:")
    print("=" * 70)
    for i, resultado in enumerate(todos_ordenados, 1):
        nome = resultado.get('endpoint') or resultado.get('transformador')
        tempo = resultado['tempo']
        percentual = (tempo / tempo_total) * 100 if tempo_total > 0 else 0
        status = "✅" if resultado['status'] == 'SUCCESS' else "❌"
        print(f"{i:2d}. {status} {nome}")
        print(f"    ⏱️  {formatar_tempo(tempo):>20} | {percentual:5.1f}% do total")
    
    # Estatísticas do DW (apenas empresa 1)
    print(f"\n📈 ESTATÍSTICAS DO DATA WAREHOUSE (EMPRESA 1):")
    print("-" * 70)
    session = Session()
    try:
        # Produtos
        query = text("SELECT COUNT(*) FROM processed.dim_produtos WHERE empresa_id = :emp_id")
        total_produtos = session.execute(query, {'emp_id': EMPRESA_ID}).scalar()
        print(f"   • dim_produtos: {total_produtos:,} registros")
        
        # Contas a Receber
        query = text("SELECT COUNT(*) FROM processed.fato_contas_receber WHERE empresa_id = :emp_id")
        total_contas_receber = session.execute(query, {'emp_id': EMPRESA_ID}).scalar()
        print(f"   • fato_contas_receber: {total_contas_receber:,} registros")
        
    except Exception as e:
        print(f"   ⚠️  Erro ao coletar estatísticas: {e}")
    finally:
        session.close()
    
    if sucesso_extracao == total_extracao and sucesso_transformacao == total_transformacao:
        print(f"\n🎉 TESTE CONCLUÍDO COM SUCESSO!")
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. Compare o tempo total com o pipeline FULL")
        print(f"   2. Verifique se os dados estão corretos no DW")
        print(f"   3. Se OK, implemente para todos os endpoints")
    else:
        print(f"\n⚠️  Alguns processos falharam. Verifique os logs acima.")


# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    log_file = setup_logging(empresa_id=1) 
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
