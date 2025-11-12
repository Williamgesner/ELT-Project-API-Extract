# Responsável por: executar TODOS os extratores E transformadores em sequência
# Este script mantém o DW sincronizado com a Bling - VERSÃO COMPLETA
# Inclui: Parte COMERCIAL + Parte FINANCEIRA
# Na fase de gerar os fluxos de trabalho (workflows), esse script será executado a cada 2 horas ou mais (Solicitação do cliente)

from datetime import datetime
from sqlalchemy import text
from config.database import create_schema_raw, create_schema_processed, create_all_tables, Session

# =====================================================
# IMPORTAÇÕES - PARTE COMERCIAL (JÁ EXISTENTE)
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
# IMPORTAÇÕES - PARTE FINANCEIRA (NOVAS)
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
# 1. EXECUÇÃO COMPLETA - EXTRAÇÃO
# =====================================================

def executar_extracao_completa():
    """
    Executa a extração de todos os endpoints em sequência
    
    FLUXO DE EXTRAÇÃO:
    
    📊 PARTE COMERCIAL:
    1. Contatos (lista + detalhes individuais)
    2. Produtos (lista completa)
    3. Vendas (lista resumida)
    4. Vendas Detalhes (itens de cada pedido)
    
    💰 PARTE FINANCEIRA:
    5. Formas de Pagamento (tabela de apoio)
    6. Categorias de Receitas/Despesas (tabela de apoio)
    7. Natureza de Operação (tabela de apoio)
    8. Contas a Pagar (lista)
    9. Contas a Pagar Detalhes (enriquecer com categoria)
    10. Contas a Receber (lista)
    11. NFe - Entrada e Saída (lista)
    12. NFe Detalhes (enriquecer com valorNota e valorFrete)
    """
    print("\n🚀 FASE 1: EXTRAÇÃO COMPLETA DE TODOS OS ENDPOINTS")
    print("=" * 70)
    print("📊 PARTE COMERCIAL + 💰 PARTE FINANCEIRA")
    print("=" * 70)
    
    inicio_extracao = datetime.now()
    
    # Lista dos extratores para executar (NA ORDEM CORRETA DE DEPENDÊNCIAS)
    extratores = [
        # === PARTE COMERCIAL ===
        ("📊 👥 CONTATOS", ContatosCompletoExtractor, {}),
        ("📊 🏭 PRODUTOS", ProdutosExtractor, {}), 
        ("📊 💰 VENDAS (Lista)", VendasExtractor, {}),
        ("📊 🛒 VENDAS (Detalhes + Itens)", VendasDetalhesExtractor, {
            'delay_entre_requests': 0.4,
            'batch_size': 100
        }),
        
        # === PARTE FINANCEIRA - TABELAS DE APOIO ===
        ("💰 💳 FORMAS DE PAGAMENTO", FormasPagamentosExtractor, {}),
        ("💰 📂 CATEGORIAS (Receitas/Despesas)", CategoriasExtractor, {}),
        ("💰 🌿 NATUREZA DE OPERAÇÃO", NaturezaOperacaoExtractor, {}),
        
        # === PARTE FINANCEIRA - DADOS PRINCIPAIS ===
        ("💰 💵 CONTAS A PAGAR (Lista)", ContasPagarExtractor, {}),
        ("💰 🔍 CONTAS A PAGAR (Detalhes + Categoria)", ContasPagarDetalhesExtractor, {
            'delay_entre_requests': 0.35,
            'batch_size': 100
        }),
        ("💰 💸 CONTAS A RECEBER (Lista)", ContasReceberExtractor, {}),
        ("💰 📄 NFe (Entrada + Saída)", NFeExtractor, {}),
        ("💰 🔍 NFe (Detalhes + Enriquecimento)", NFeDetalhesExtractor, {
            'delay_entre_requests': 0.35,
            'batch_size': 100
        })
    ]
    
    resultados_extracao = []
    
    for nome_endpoint, ExtractorClass, params in extratores:
        try:
            print(f"\n{nome_endpoint}")
            print("-" * 70)
            
            inicio_endpoint = datetime.now()
            
            # Criar e executar o extrator
            extrator = ExtractorClass()
            
            # Verificar se precisa de parâmetros especiais
            if params:
                # Extrair com parâmetros personalizados
                if ExtractorClass == VendasDetalhesExtractor:
                    extrator.executar_extracao_detalhes(**params)
                elif ExtractorClass == ContasPagarDetalhesExtractor:
                    extrator.executar_extracao_detalhes(**params)
                elif ExtractorClass == NFeDetalhesExtractor:
                    extrator.executar_enriquecimento_completo(**params)
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
    
    sucessos = 0
    erros = 0
    
    for resultado in resultados_extracao:
        status_emoji = "✅" if resultado['status'] == 'SUCCESS' else "❌"
        print(f"{status_emoji} {resultado['endpoint']}: {resultado['tempo']}")
        
        if resultado['status'] == 'SUCCESS':
            sucessos += 1
        else:
            erros += 1
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
    
    FLUXO DE TRANSFORMAÇÃO:
    
    📊 PARTE COMERCIAL:
    1. Contatos → dim_contatos
    2. Produtos → dim_produtos
    
    💰 PARTE FINANCEIRA - DIMENSÕES DE APOIO (PRIMEIRO!):
    3. Formas de Pagamento → dim_formas_pagamento
    4. Categorias → dim_categorias_contas_pagar
    5. Natureza de Operação → dim_natureza_operacao
    
    📊 PARTE COMERCIAL - FATOS:
    6. Vendas → fato_pedidos
    7. Itens → fato_itens_pedidos
    
    💰 PARTE FINANCEIRA - FATOS:
    8. Contas a Pagar → fato_contas_pagar
    9. Contas a Receber → fato_contas_receber
    10. NFe → fato_nfe
    """
    print(f"\n{'='*70}")
    print("🔄 FASE 2: TRANSFORMAÇÃO DOS DADOS")
    print(f"{'='*70}")
    print("📊 PARTE COMERCIAL + 💰 PARTE FINANCEIRA")
    print(f"{'='*70}")
    
    inicio_transformacao = datetime.now()
    
    # Modo incremental - processar apenas registros 'pendente'
    print("\n▶️  Modo incremental: processando apenas registros 'pendente'...")

    session = Session()
    try:
        # Verificar quantos registros pendentes existem
        print("\n📊 VERIFICANDO REGISTROS PENDENTES...")
        print("-" * 70)
        
        # Parte Comercial
        contatos_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.contatos_raw WHERE status_processamento = 'pendente'"
        )).scalar()
        
        produtos_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.produtos_raw WHERE status_processamento = 'pendente'"
        )).scalar()
        
        vendas_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.vendas_raw WHERE status_processamento = 'pendente'"
        )).scalar()
        
        # Parte Financeira
        formas_pagamento_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.formas_pagamentos_raw"
        )).scalar() or 0
        
        categorias_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.categorias_contas_pagar_raw"
        )).scalar() or 0
        
        natureza_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.natureza_operacao_raw"
        )).scalar() or 0
        
        contas_pagar_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.contas_pagar_raw WHERE status_processamento = 'pendente'"
        )).scalar() or 0
        
        contas_receber_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.contas_receber_raw WHERE status_processamento = 'pendente'"
        )).scalar() or 0
        
        nfe_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.nfe_raw"
        )).scalar() or 0
        
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
    # IMPORTANTE: A ordem importa! Dimensões ANTES de Fatos, e Fatos com dependências no final
    transformadores = [
        # === PARTE COMERCIAL - DIMENSÕES ===
        ("📊 👥 CONTATOS", ContatosTransformer),
        ("📊 🏭 PRODUTOS", ProdutosTransformer),
        
        # === PARTE FINANCEIRA - DIMENSÕES DE APOIO (ANTES DOS FATOS!) ===
        ("💰 💳 FORMAS DE PAGAMENTO", FormasPagamentoTransformer),
        ("💰 📂 CATEGORIAS", CategoriasContasPagarTransformer),
        ("💰 🌿 NATUREZA DE OPERAÇÃO", NaturezaOperacaoTransformer),
        
        # === PARTE COMERCIAL - FATOS ===
        ("📊 💰 VENDAS", VendasTransformer),
        ("📊 🛒 ITENS", ItensTransformer),
        
        # === PARTE FINANCEIRA - FATOS ===
        ("💰 💵 CONTAS A PAGAR", ContasPagarTransformer),
        ("💰 💸 CONTAS A RECEBER", ContasReceberTransformer),
        ("💰 📄 NFe", NFeTransformer)
    ]
    
    resultados_transformacao = []
    
    for nome, Transformer in transformadores:
        try:
            print(f"\n{nome}")
            print("-" * 70)
            
            inicio_transform = datetime.now()
            
            transformer = Transformer()
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
            
            # Se falhar em ITENS ou NFe, avisar mas continuar
            if "ITENS" in nome or "NFe" in nome:
                print("⚠️  Falha não interrompe o pipeline - continuando...")
    
    # Relatório transformação
    fim_transformacao = datetime.now()
    tempo_transformacao = fim_transformacao - inicio_transformacao
    
    print(f"\n✅ TRANSFORMAÇÃO COMPLETA FINALIZADA")
    print("=" * 70)
    print(f"⏱️ Tempo total: {tempo_transformacao}")
    print("\n📊 RESUMO DOS RESULTADOS:")
    
    sucessos = 0
    erros = 0
    
    for resultado in resultados_transformacao:
        status_emoji = "✅" if resultado['status'] == 'SUCCESS' else "❌"
        print(f"{status_emoji} {resultado['transformador']}: {resultado['tempo']}")
        
        if resultado['status'] == 'SUCCESS':
            sucessos += 1
        else:
            erros += 1
            print(f"   └── Erro: {resultado.get('erro', 'N/A')}")
    
    print(f"\n🎯 ESTATÍSTICAS FINAIS DA TRANSFORMAÇÃO:")
    print(f"✅ Sucessos: {sucessos}/{len(transformadores)}")
    print(f"❌ Erros: {erros}/{len(transformadores)}")
    
    return resultados_transformacao

# =====================================================
# 3. PIPELINE COMPLETO
# =====================================================

def executar_pipeline_completo():
    """
    Executa o pipeline completo: Extração + Transformação
    Este é o script principal para manter o DW atualizado
    VERSÃO COMPLETA: Parte Comercial + Parte Financeira
    """
    print("\n" + "=" * 70)
    print("🔄 PIPELINE COMPLETO: EXTRAÇÃO + TRANSFORMAÇÃO")
    print("=" * 70)
    print("Mantém o Data Warehouse sincronizado com a Bling")
    print("📊 PARTE COMERCIAL: Contatos, Produtos, Vendas, Itens")
    print("💰 PARTE FINANCEIRA: Contas a Pagar, Receber, NFe")
    print("Recomendado: Executar a cada 2 horas - Solicitação do cliente")
    print("=" * 70)
    
    inicio_pipeline = datetime.now()
    
    # FASE 1: Extração
    resultados_extracao = executar_extracao_completa()
    
    # FASE 2: Transformação
    resultados_transformacao = executar_transformacao_completa()
    
    # Relatório final consolidado
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
    
    # Estatísticas detalhadas do DW
    print(f"\n📈 ESTATÍSTICAS DO DATA WAREHOUSE:")
    print("-" * 70)
    session = Session()
    try:
        # === DIMENSÕES ===
        print("\n🔷 DIMENSÕES:")
        
        # Contatos
        query = text("SELECT COUNT(*) FROM processed.dim_contatos")
        total_contatos = session.execute(query).scalar()
        print(f"   • dim_contatos: {total_contatos:,} registros")
        
        # Produtos
        query = text("SELECT COUNT(*) FROM processed.dim_produtos")
        total_produtos = session.execute(query).scalar()
        print(f"   • dim_produtos: {total_produtos:,} registros")
        
        # Formas de Pagamento
        query = text("SELECT COUNT(*) FROM processed.dim_formas_pagamento")
        total_formas = session.execute(query).scalar() or 0
        print(f"   • dim_formas_pagamento: {total_formas:,} registros")
        
        # Categorias
        query = text("SELECT COUNT(*) FROM processed.dim_categorias_contas_pagar")
        total_categorias = session.execute(query).scalar() or 0
        print(f"   • dim_categorias_contas_pagar: {total_categorias:,} registros")
        
        # Natureza de Operação
        query = text("SELECT COUNT(*) FROM processed.dim_natureza_operacao")
        total_natureza = session.execute(query).scalar() or 0
        print(f"   • dim_natureza_operacao: {total_natureza:,} registros")
        
        # === FATOS - COMERCIAL ===
        print("\n📊 FATOS - PARTE COMERCIAL:")
        
        # Pedidos
        query = text("SELECT COUNT(*) FROM processed.fato_pedidos")
        total_pedidos = session.execute(query).scalar()
        print(f"   • fato_pedidos: {total_pedidos:,} registros")
        
        # Itens
        query = text("""
            SELECT 
                COUNT(*) as total,
                COUNT(produto_id) as com_produto,
                ROUND(100.0 * COUNT(produto_id) / NULLIF(COUNT(*), 0), 1) as taxa
            FROM processed.fato_itens_pedidos
        """)
        resultado = session.execute(query).fetchone()
        if resultado and resultado.total > 0:
            print(f"   • fato_itens_pedidos: {resultado.total:,} registros")
            print(f"     └─ Mapeamento: {resultado.taxa}% com produto_id")
            
            # Alerta se taxa baixa
            if resultado.taxa < 95:
                print(f"\n   🚨 ALERTA: Taxa de mapeamento de produtos abaixo de 95%!")
                print(f"   💡 Considere executar: python main_product.py")
        
        # === FATOS - FINANCEIRO ===
        print("\n💰 FATOS - PARTE FINANCEIRA:")
        
        # Contas a Pagar
        query = text("""
            SELECT 
                COUNT(*) as total,
                SUM(valor) as valor_total,
                SUM(CASE WHEN situacao IN ('Em aberto', 'Atrasada', 'Vencendo hoje') THEN valor ELSE 0 END) as valor_aberto
            FROM processed.fato_contas_pagar
        """)
        resultado = session.execute(query).fetchone()
        if resultado and resultado.total > 0:
            print(f"   • fato_contas_pagar: {resultado.total:,} registros")
            print(f"     └─ Valor total: R$ {resultado.valor_total:,.2f}")
            print(f"     └─ Valor em aberto: R$ {resultado.valor_aberto:,.2f}")
        else:
            print(f"   • fato_contas_pagar: 0 registros")
        
        # Contas a Receber
        query = text("""
            SELECT 
                COUNT(*) as total,
                SUM(valor) as valor_total,
                SUM(CASE WHEN situacao IN ('Em aberto', 'Atrasada', 'Vencendo hoje') THEN valor ELSE 0 END) as valor_aberto
            FROM processed.fato_contas_receber
        """)
        resultado = session.execute(query).fetchone()
        if resultado and resultado.total > 0:
            print(f"   • fato_contas_receber: {resultado.total:,} registros")
            print(f"     └─ Valor total: R$ {resultado.valor_total:,.2f}")
            print(f"     └─ Valor a receber: R$ {resultado.valor_aberto:,.2f}")
        else:
            print(f"   • fato_contas_receber: 0 registros")
        
        # NFe
        query = text("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN tipo = 'Entrada' THEN 1 END) as entradas,
                COUNT(CASE WHEN tipo = 'Saída' THEN 1 END) as saidas
            FROM processed.fato_nfe
        """)
        resultado = session.execute(query).fetchone()
        if resultado and resultado.total > 0:
            print(f"   • fato_nfe: {resultado.total:,} registros")
            print(f"     └─ Entradas: {resultado.entradas:,}")
            print(f"     └─ Saídas: {resultado.saidas:,}")
        else:
            print(f"   • fato_nfe: 0 registros")
        
    except Exception as e:
        print(f"   ⚠️  Erro ao coletar estatísticas: {e}")
    finally:
        session.close()
    
    if sucesso_extracao == total_extracao and sucesso_transformacao == total_transformacao:
        print(f"\n🎉 TODOS OS PROCESSOS EXECUTADOS COM SUCESSO!")
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. Dados estão sincronizados com a Bling")
        print(f"   2. Power BI pode ser atualizado")
        print(f"   3. Execute novamente em 2 horas para manter atualizado")
        print(f"\n📊 DASHBOARDS DISPONÍVEIS:")
        print(f"   • Dashboard Comercial: Vendas, Produtos, Clientes")
        print(f"   • Dashboard Financeiro: Contas a Pagar/Receber, NFe")
    else:
        print(f"\n⚠️  Alguns processos falharam. Verifique os logs acima.")

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
        print("Você pode continuar executando novamente este script")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        raise