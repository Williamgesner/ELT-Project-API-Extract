# Responsável por: executar o enriquecimento de contas a pagar (adicionar categoria.id)

from datetime import datetime
from extract.accounts_payable_details import ContasPagarDetalhesExtractor
from config.settings import empresas

# =====================================================
# 1. EXECUÇÃO DO ENRIQUECIMENTO
# =====================================================

if __name__ == "__main__":
    try:
        print("\n" + "=" * 70)
        print("🔄 ENRIQUECIMENTO: CONTAS A PAGAR RAW → ADICIONAR CATEGORIA")
        print("=" * 70)
        print("\n⚠️  IMPORTANTE:")
        print("   Este script busca detalhes de CADA conta individualmente na API")
        print("   para adicionar a informação de categoria.id no JSON")
        print("=" * 70)
        print("\n📋 ETAPAS:")
        print("   1. Buscar todas as contas em contas_pagar_raw")
        print("   2. Identificar quais não têm categoria")
        print("   3. Para cada conta SEM categoria:")
        print("      • Buscar detalhes na API: GET /contas/pagar/{id}")
        print("      • Extrair categoria.id")
        print("      • Atualizar o JSON no banco")
        print("   4. Marcar como 'pendente' para reprocessar")
        print("=" * 70)
        
        inicio = datetime.now()
        
        # Processar cada empresa
        print("\n🚀 Iniciando enriquecimento...")

        for empresa_config in empresas:
            empresa_id = empresa_config['empresa_id']
            api_key = empresa_config['api_key']
            nome = empresa_config['nome']
            
            print(f"\n🏢 Processando: {nome} (ID: {empresa_id})")
            
            extrator = ContasPagarDetalhesExtractor(api_key, empresa_id)
            extrator.executar_extracao_detalhes(
                delay_entre_requests=0.35,
                batch_size=100
            )
        
        fim = datetime.now()
        tempo_total = fim - inicio
        
        print(f"\n{'='*70}")
        print(f"✅ ENRIQUECIMENTO CONCLUÍDO COM SUCESSO!")
        print(f"⏱️  Tempo total: {tempo_total}")
        print(f"{'='*70}")
        
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. ✅ Processar categorias (se ainda não processou):")
        print(f"      python main_transform_categories_payable.py")
        print(f"")
        print(f"   2. ✅ Reprocessar contas a pagar (agora com categoria!):")
        print(f"      python main_transform_accounts_payable.py")
        print(f"")
        print(f"   3. ✅ Verificar resultado:")
        print(f"      SELECT")
        print(f"          fcp.data_vencimento,")
        print(f"          dc.nome AS empresa,")
        print(f"          fcp.valor,")
        print(f"          fcp.situacao,")
        print(f"          dcc.descricao AS categoria")
        print(f"      FROM processed.fato_contas_pagar fcp")
        print(f"      LEFT JOIN processed.dim_contatos dc")
        print(f"          ON fcp.bling_cliente_id = dc.bling_contato_id")
        print(f"      LEFT JOIN processed.dim_categorias_contas_pagar dcc")
        print(f"          ON fcp.categoria_id = dcc.categoria_id")
        print(f"      WHERE EXTRACT(MONTH FROM fcp.data_vencimento) = 9;")
        
    except KeyboardInterrupt:
        print("\n⚠️ Enriquecimento interrompido pelo usuário")
        print("💾 Dados processados até este ponto foram preservados")
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO durante enriquecimento: {e}")
        print("Script interrompido para análise do erro")
        import traceback
        traceback.print_exc()
        raise   