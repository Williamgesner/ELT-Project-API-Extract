# Responsável por: executar TODOS os extratores E transformadores em sequência em Empresa ID - 4
# Este script mantém o DW sincronizado com a Bling - VERSÃO COMPLETA
# Inclui: Parte COMERCIAL + Parte FINANCEIRA
# Na fase de gerar os fluxos de trabalho (workflows), esse script será executado a cada 2 horas ou mais (Solicitação do cliente)

"""
========================================
PIPELINE COMPLETO - EMPRESA 4
========================================

Responsável por: executar TODOS os extratores E transformadores em sequência
VERSÃO MULTI-CNPJ: empresa_id=4

Este script mantém o DW sincronizado com a Bling
Inclui: Parte COMERCIAL + Parte FINANCEIRA
Executar a cada 2 horas (Solicitação do cliente)

🛡️ PROTEÇÃO IMPLEMENTADA:
   • Se extração falhar → transformação NÃO roda
   • Evita deleção de dados se API Key expirar
   • Garante integridade dos dados
"""

import os
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import text
from config.database import create_schema_raw, create_schema_processed, create_all_tables, Session
from config.auth_manager import obter_token_para_empresa #IMPORTAR O GERENCIADOR DE TOKENS

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
EMPRESA_ID = 4

# OBTER TOKEN VÁLIDO AUTOMATICAMENTE (renova se necessário)
print("\n🔑 Obtendo token válido para Empresa 04...")
API_KEY_EMPRESA_4 = obter_token_para_empresa(EMPRESA_ID)
print(f"✅ Token obtido e validado!\n")

# =====================================================
# 1. EXECUÇÃO COMPLETA - EXTRAÇÃO
# =====================================================

def executar_extracao_completa():
    """
    Executa a extração de todos os endpoints em sequência
    TODOS os extractors recebem empresa_id=4
    """
    print("\n🚀 FASE 1: EXTRAÇÃO COMPLETA DE TODOS OS ENDPOINTS")
    print("=" * 70)
    print(f"📌 Empresa ID: {EMPRESA_ID}")
    print("📊 PARTE COMERCIAL + 💰 PARTE FINANCEIRA")
    print("=" * 70)
    
    inicio_extracao = datetime.now()
    
    # Lista dos extratores (COM EMPRESA_ID!)
    extratores = [
        # === PARTE COMERCIAL ===
        ("📊 👥 CONTATOS", ContatosCompletoExtractor, 
         {'api_key': API_KEY_EMPRESA_4, 'empresa_id': EMPRESA_ID}, {}),
        
        ("📊 🏭 PRODUTOS", ProdutosExtractor,
         {'api_key': API_KEY_EMPRESA_4, 'empresa_id': EMPRESA_ID}, {}),
        
        ("📊 💰 VENDAS (Lista)", VendasExtractor,
         {'api_key': API_KEY_EMPRESA_4, 'empresa_id': EMPRESA_ID}, {}),
        
        ("📊 🛒 VENDAS (Detalhes + Itens)", VendasDetalhesExtractor,
         {'api_key': API_KEY_EMPRESA_4, 'empresa_id': EMPRESA_ID},
         {'delay_entre_requests': 0.4, 'batch_size': 100}),
        
        # === PARTE FINANCEIRA - TABELAS DE APOIO ===
        ("💰 💳 FORMAS DE PAGAMENTO", FormasPagamentosExtractor,
         {'api_key': API_KEY_EMPRESA_4, 'empresa_id': EMPRESA_ID}, {}),
        
        ("💰 📂 CATEGORIAS (Receitas/Despesas)", CategoriasExtractor,
         {'api_key': API_KEY_EMPRESA_4, 'empresa_id': EMPRESA_ID}, {}),
        
        ("💰 🌿 NATUREZA DE OPERAÇÃO", NaturezaOperacaoExtractor,
         {'api_key': API_KEY_EMPRESA_4, 'empresa_id': EMPRESA_ID}, {}),
        
        # === PARTE FINANCEIRA - DADOS PRINCIPAIS ===
        ("💰 💵 CONTAS A PAGAR (Lista)", ContasPagarExtractor,
         {'api_key': API_KEY_EMPRESA_4, 'empresa_id': EMPRESA_ID}, {}),
        
        ("💰 🔍 CONTAS A PAGAR (Detalhes + Categoria)", ContasPagarDetalhesExtractor,
         {'api_key': API_KEY_EMPRESA_4, 'empresa_id': EMPRESA_ID},
         {'delay_entre_requests': 0.35, 'batch_size': 100}),
        
        ("💰 💸 CONTAS A RECEBER (Lista)", ContasReceberExtractor,
         {'api_key': API_KEY_EMPRESA_4, 'empresa_id': EMPRESA_ID}, {}),
        
        ("💰 📄 NFe (Entrada + Saída)", NFeExtractor,
         {'api_key': API_KEY_EMPRESA_4, 'empresa_id': EMPRESA_ID}, {}),
        
        ("💰 🔍 NFe (Detalhes + Enriquecimento)", NFeDetalhesExtractor,
         {'api_key': API_KEY_EMPRESA_4, 'empresa_id': EMPRESA_ID},
         {'delay_entre_requests': 0.35, 'batch_size': 100})
    ]
    
    resultados_extracao = []
    
    for nome_endpoint, ExtractorClass, init_params, exec_params in extratores:
        try:
            print(f"\n{nome_endpoint}")
            print("-" * 70)
            
            inicio_endpoint = datetime.now()
            
            # Criar extrator COM empresa_id
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
                    extrator.executar_extracao_completa()
            else:
                # Executar normalmente
                extrator.executar_extracao_completa()
            
            fim_endpoint = datetime.now()
            tempo_endpoint = fim_endpoint - inicio_endpoint
            
            resultados_extracao.append({
                'endpoint': nome_endpoint,
                'status': 'SUCCESS',
                'tempo': tempo_endpoint
            })
            
            print(f"✅ {nome_endpoint} concluído em {tempo_endpoint}")
            
        except Exception as e:
            fim_endpoint = datetime.now()
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
    fim_extracao = datetime.now()
    tempo_extracao = fim_extracao - inicio_extracao
    
    print(f"\n✅ EXTRAÇÃO COMPLETA FINALIZADA")
    print("=" * 70)
    print(f"⏱️ Tempo total: {tempo_extracao}")
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

# =====================================================
# 2. EXECUÇÃO COMPLETA - TRANSFORMAÇÃO
# =====================================================

def executar_transformacao_completa():
    """
    Executa a transformação de todos os dados RAW para DW
    TODOS os transformers recebem empresa_id=4
    """
    print(f"\n{'='*70}")
    print("🔄 FASE 2: TRANSFORMAÇÃO DOS DADOS")
    print(f"{'='*70}")
    print(f"📌 Empresa ID: {EMPRESA_ID}")
    print("📊 PARTE COMERCIAL + 💰 PARTE FINANCEIRA")
    print(f"{'='*70}")
    
    inicio_transformacao = datetime.now()
    
    # Modo incremental - processar apenas registros 'pendente'
    print("\n▶️  Modo incremental: processando apenas registros 'pendente'...")

    session = Session()
    try:
        # Verificar quantos registros pendentes existem
        print("\n📊 VERIFICANDO REGISTROS PENDENTES (EMPRESA 4)...")
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
    
    # Executar transformações (COM EMPRESA_ID!)
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
            
            inicio_transform = datetime.now()
            
            # Criar transformer COM empresa_id
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
            print("⚠️  Continuando com próximo transformer...")
    
    # Relatório transformação
    fim_transformacao = datetime.now()
    tempo_transformacao = fim_transformacao - inicio_transformacao
    
    print(f"\n✅ TRANSFORMAÇÃO COMPLETA FINALIZADA")
    print("=" * 70)
    print(f"⏱️ Tempo total: {tempo_transformacao}")
    print("\n📊 RESUMO DOS RESULTADOS:")
    
    sucessos = sum(1 for r in resultados_transformacao if r['status'] == 'SUCCESS')
    erros = sum(1 for r in resultados_transformacao if r['status'] == 'ERROR')
    
    for resultado in resultados_transformacao:
        status_emoji = "✅" if resultado['status'] == 'SUCCESS' else "❌"
        print(f"{status_emoji} {resultado['transformador']}: {resultado['tempo']}")
        
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
    VERSÃO MULTI-CNPJ para EMPRESA 4

    🛡️ PROTEÇÃO IMPLEMENTADA:
       • Verifica se extração foi bem-sucedida
       • Se houver erros críticos, ABORTA transformação
       • Evita perda de dados por API Key expirada ou erros de conexão    
    """
    print("\n" + "=" * 70)
    print("🔄 PIPELINE COMPLETO: EMPRESA 4")
    print("=" * 70)
    print(f"📌 Empresa ID: {EMPRESA_ID}")
    print("📊 PARTE COMERCIAL: Contatos, Produtos, Vendas, Itens")
    print("💰 PARTE FINANCEIRA: Contas a Pagar, Receber, NFe")
    print("🛡️ PROTEÇÃO: Aborta transformação se extração falhar")
    print("Executar a cada 2 horas")
    print("=" * 70)
    
    inicio_pipeline = datetime.now()
    
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

    fim_pipeline = datetime.now()
    tempo_total = fim_pipeline - inicio_pipeline
    
    print(f"\n{'='*70}")
    print(f"🏁 PIPELINE COMPLETO FINALIZADO")
    print(f"{'='*70}")
    print(f"⏱️  Tempo total do pipeline: {tempo_total}")
    
    # Estatísticas consolidadas
    total_extracao = len(resultados_extracao)
    sucesso_extracao = sum(1 for r in resultados_extracao if r['status'] == 'SUCCESS')
    
    total_transformacao = len(resultados_transformacao)
    sucesso_transformacao = sum(1 for r in resultados_transformacao if r['status'] == 'SUCCESS')
    
    print(f"\n📊 RESUMO GERAL:")
    print(f"   • Extração: {sucesso_extracao}/{total_extracao} sucessos")
    print(f"   • Transformação: {sucesso_transformacao}/{total_transformacao} sucessos")
    
    # Estatísticas do DW (apenas empresa 4)
    print(f"\n📈 ESTATÍSTICAS DO DATA WAREHOUSE (EMPRESA 4):")
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
        
    except Exception as e:
        print(f"   ⚠️  Erro ao coletar estatísticas: {e}")
    finally:
        session.close()
    
    if sucesso_extracao == total_extracao and sucesso_transformacao == total_transformacao:
        print(f"\n🎉 TODOS OS PROCESSOS EXECUTADOS COM SUCESSO!")
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. DW sincronizado com a Bling (Empresa 4)")
        print(f"   2. Power BI pode ser atualizado")
        print(f"   3. Para adicionar outras empresas, crie main_pipeline_5.py, etc.")
    else:
        print(f"\n⚠️  Alguns processos falharam. Verifique os logs acima.")

# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
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