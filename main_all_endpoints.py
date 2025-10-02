# Responsável por: executar TODOS os extratores em sequência

from datetime import datetime
from config.database import create_schema_raw, create_all_tables
from extract.contacts import ContatosCompletoExtractor
from extract.products import ProdutosExtractor
from extract.sales import VendasExtractor
from extract.sales_details import VendasDetalhesExtractor
# from extract.stocks import EstoqueExtractor - Vamos usar somente no Bling do G4 (que é onde contem as inforações de depósito)

# =====================================================
# 1. EXECUÇÃO COMPLETA - TODOS OS ENDPOINTS
# =====================================================

def executar_extracao_completa():
    """
    Executa a extração de todos os endpoints em sequência
    
    FLUXO DE EXTRAÇÃO:
    1. Contatos (lista + detalhes individuais)
    2. Produtos (lista completa)
    3. Vendas (lista resumida)
    4. Vendas Detalhes (itens de cada pedido) ← NOVO!
    """
    print("\n🚀 INICIANDO EXTRAÇÃO COMPLETA DE TODOS OS ENDPOINTS")
    print("=" * 60)
    
    inicio_total = datetime.now()
    
    # Lista dos extratores para executar
    extratores = [
        ("👥 CONTATOS", ContatosCompletoExtractor),
        ("🏭 PRODUTOS", ProdutosExtractor), 
        ("💰 VENDAS (Lista)", VendasExtractor),
        ("🛒 VENDAS (Detalhes + Itens)", VendasDetalhesExtractor), 
      # ("📦 ESTOQUES", EstoqueExtractor) - Vamos usar somente no Bling do G4 (que é onde contem as inforações de depósito)
    ]
    
    resultados = []
    
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
            
            resultados.append({
                'endpoint': nome_endpoint,
                'status': 'SUCCESS',
                'tempo': tempo_endpoint
            })
            
            print(f"✅ {nome_endpoint} concluído em {tempo_endpoint}")
            
        except Exception as e:
            fim_endpoint = datetime.now()
            tempo_endpoint = fim_endpoint - inicio_endpoint
            
            resultados.append({
                'endpoint': nome_endpoint,
                'status': 'ERROR',
                'tempo': tempo_endpoint,
                'erro': str(e)
            })
            
            print(f"❌ ERRO em {nome_endpoint}: {e}")
            print("Continuando com próximo endpoint...")
    
    # Relatório final
    fim_total = datetime.now()
    tempo_total = fim_total - inicio_total
    
    print(f"\n🏁 EXTRAÇÃO COMPLETA FINALIZADA")
    print("=" * 60)
    print(f"⏱️ Tempo total: {tempo_total}")
    print("\n📊 RESUMO DOS RESULTADOS:")
    
    sucessos = 0
    erros = 0
    
    for resultado in resultados:
        status_emoji = "✅" if resultado['status'] == 'SUCCESS' else "❌"
        print(f"{status_emoji} {resultado['endpoint']}: {resultado['tempo']}")
        
        if resultado['status'] == 'SUCCESS':
            sucessos += 1
        else:
            erros += 1
            print(f"   └── Erro: {resultado.get('erro', 'N/A')}")
    
    print(f"\n🎯 ESTATÍSTICAS FINAIS:")
    print(f"✅ Sucessos: {sucessos}/{len(extratores)}")
    print(f"❌ Erros: {erros}/{len(extratores)}")
    
    if erros == 0:
        print(f"🎉 TODOS OS ENDPOINTS EXTRAÍDOS COM SUCESSO!")
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. Validar dados extraídos (execute analyze_raw_data.py)")
        print(f"   2. Iniciar transformação para modelo dimensional")
    else:
        print(f"⚠️ {erros} endpoint(s) com erro. Verifique os logs acima.")

if __name__ == "__main__":
    try:
        # Cria o schema se não existir
        create_schema_raw()

        # Cria as tabelas
        create_all_tables()

        # Executar extração completa
        executar_extracao_completa()
        
    except KeyboardInterrupt:
        print("\n⚠️ Execução interrompida pelo usuário")
        print("💾 Dados processados até este ponto foram preservados")
        print("Você pode continuar executando novamente este script")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante execução: {e}")
        print("Script interrompido para análise do erro")
        raise