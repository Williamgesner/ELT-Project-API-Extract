# Responsável por: executar TODOS os extratores E transformadores em sequência
# Este script mantém o DW sincronizado com a Bling
# Na fase de gerar o fluxos de trabalho (workflows), esse scrip será executado a cada 2 horas (Solicitação do cliente)

from datetime import datetime
from sqlalchemy import text
from config.database import create_schema_raw, create_schema_processed, create_all_tables, Session
from extract.contacts import ContatosCompletoExtractor
from extract.products import ProdutosExtractor
from extract.sales import VendasExtractor
from extract.sales_details import VendasDetalhesExtractor
from transform.contacts_dw import ContatosTransformer
from transform.products_dw import ProdutosTransformer
from transform.sales_dw import VendasTransformer
from transform.items_dw import ItensTransformer 

# =====================================================
# 1. EXECUÇÃO COMPLETA - EXTRAÇÃO
# =====================================================

def executar_extracao_completa():
    """
    Executa a extração de todos os endpoints em sequência
    
    FLUXO DE EXTRAÇÃO:
    1. Contatos (lista + detalhes individuais)
    2. Produtos (lista completa)
    3. Vendas (lista resumida)
    4. Vendas Detalhes (itens de cada pedido)
    """
    print("\n🚀 FASE 1: EXTRAÇÃO COMPLETA DE TODOS OS ENDPOINTS")
    print("=" * 60)
    
    inicio_extracao = datetime.now()
    
    # Lista dos extratores para executar
    extratores = [
        ("👥 CONTATOS", ContatosCompletoExtractor),
        ("🏭 PRODUTOS", ProdutosExtractor), 
        ("💰 VENDAS (Lista)", VendasExtractor),
        ("🛒 VENDAS (Detalhes + Itens)", VendasDetalhesExtractor)
    ]
    
    resultados_extracao = []
    
    for nome_endpoint, ExtractorClass in extratores:
        try:
            print(f"\n{nome_endpoint}")
            print("-" * 50)
            
            inicio_endpoint = datetime.now()
            
            # Criar e executar o extrator
            extrator = ExtractorClass()
            
            # Verificar se é o extrator de detalhes de vendas
            if ExtractorClass == VendasDetalhesExtractor:
                # Executar com configurações específicas
                extrator.executar_extracao_detalhes(
                    delay_entre_requests=0.4,
                    batch_size=100
                )
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
    print("=" * 60)
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
    1. Resetar status_processamento (reprocessar tudo)
    2. Transformar Contatos → dim_contatos
    3. Transformar Produtos → dim_produtos
    4. Transformar Vendas → fato_pedidos
    5. Transformar Itens → fato_itens_pedidos  ← NOVO!
    """
    print(f"\n{'='*60}")
    print("🔄 FASE 2: TRANSFORMAÇÃO DOS DADOS")
    print(f"{'='*60}")
    
    inicio_transformacao = datetime.now()
    
    # Resetar status para reprocessar todos os registros (Apenas quando for executar pela primeira vez)
    # print("\n▶️  Resetando status_processamento...")
    # session = Session()
    # try:
    #     session.execute(text("UPDATE raw.contatos_raw SET status_processamento = 'pendente'"))
    #     session.execute(text("UPDATE raw.produtos_raw SET status_processamento = 'pendente'"))
    #     session.execute(text("UPDATE raw.vendas_raw SET status_processamento = 'pendente'"))
    #     session.commit()
    #     print("✅ Status resetado - todos os registros serão reprocessados")
    # except Exception as e:
    #     print(f"⚠️  Erro ao resetar status: {e}")
    #     session.rollback()
    # finally:
    #     session.close()

    # NÃO resetar status - processamento incremental (A partir da segunda vez que for rodar, será essa parte aqui)
    print("\n▶️  Modo incremental: processando apenas registros 'pendente'...")

    session = Session()
    try:
        # Verificar quantos registros pendentes existem
        contatos_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.contatos_raw WHERE status_processamento = 'pendente'"
        )).scalar()
        
        produtos_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.produtos_raw WHERE status_processamento = 'pendente'"
        )).scalar()
        
        vendas_pendentes = session.execute(text(
            "SELECT COUNT(*) FROM raw.vendas_raw WHERE status_processamento = 'pendente'"
        )).scalar()
        
        print(f"   • Contatos pendentes: {contatos_pendentes}")
        print(f"   • Produtos pendentes: {produtos_pendentes}")
        print(f"   • Vendas pendentes: {vendas_pendentes}")
        
        total_pendentes = contatos_pendentes + produtos_pendentes + vendas_pendentes
        
        if total_pendentes == 0:
            print("\n✨ Nenhum registro pendente - DW já está atualizado!")
            return []
            
    except Exception as e:
        print(f"⚠️  Erro ao verificar status: {e}")
    finally:
        session.close()
    
    # Executar transformações
    # IMPORTANTE: A ordem importa! Itens dependem de pedidos e produtos
    transformadores = [
        ("👥 CONTATOS", ContatosTransformer),
        ("🏭 PRODUTOS", ProdutosTransformer),
        ("💰 VENDAS", VendasTransformer),
        ("🛒 ITENS", ItensTransformer)  # ← ADICIONAR ESTA LINHA
    ]
    
    resultados_transformacao = []
    
    for nome, Transformer in transformadores:
        try:
            print(f"\n{nome}")
            print("-" * 50)
            
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
            
            # Se falhar em ITENS, avisar mas continuar
            if "ITENS" in nome:
                print("⚠️  Falha em ITENS não interrompe o pipeline")
    
    # Relatório transformação
    fim_transformacao = datetime.now()
    tempo_transformacao = fim_transformacao - inicio_transformacao
    
    print(f"\n✅ TRANSFORMAÇÃO COMPLETA FINALIZADA")
    print("=" * 60)
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
    """
    print("\n" + "=" * 70)
    print("🔄 PIPELINE COMPLETO: EXTRAÇÃO + TRANSFORMAÇÃO")
    print("=" * 70)
    print("Mantém o Data Warehouse sincronizado com a Bling")
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
    session = Session()
    try:
        # Contatos
        query = text("SELECT COUNT(*) FROM processed.dim_contatos")
        total_contatos = session.execute(query).scalar()
        print(f"   • dim_contatos: {total_contatos:,} registros")
        
        # Produtos
        query = text("SELECT COUNT(*) FROM processed.dim_produtos")
        total_produtos = session.execute(query).scalar()
        print(f"   • dim_produtos: {total_produtos:,} registros")
        
        # Pedidos
        query = text("SELECT COUNT(*) FROM processed.fato_pedidos")
        total_pedidos = session.execute(query).scalar()
        print(f"   • fato_pedidos: {total_pedidos:,} registros")
        
        # Itens
        query = text("""
            SELECT 
                COUNT(*) as total,
                COUNT(produto_id) as com_produto,
                ROUND(100.0 * COUNT(produto_id) / COUNT(*), 1) as taxa
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