"""
INVESTIGAÇÃO: ONDE ESTÃO OS ITENS DOS PEDIDOS?
==============================================
Este script vai investigar profundamente a estrutura
das vendas para encontrar os itens dos pedidos
"""

import json
from config.database import Session
from sqlalchemy import text

def investigar_estrutura_completa_vendas():
    """
    Busca um pedido e mostra sua estrutura COMPLETA
    """
    print("\n" + "="*70)
    print("🔍 INVESTIGAÇÃO PROFUNDA: ESTRUTURA COMPLETA DE VENDAS")
    print("="*70)
    
    session = Session()
    
    try:
        # Buscar 3 vendas diferentes
        query = text("""
            SELECT 
                bling_id,
                dados_json
            FROM raw.vendas_raw
            LIMIT 3
        """)
        
        resultado = session.execute(query)
        registros = resultado.fetchall()
        
        if not registros:
            print("❌ Nenhum registro encontrado")
            return
        
        for i, registro in enumerate(registros, 1):
            print(f"\n{'='*70}")
            print(f"📦 VENDA #{i} - ID: {registro.bling_id}")
            print(f"{'='*70}")
            
            # Mostrar JSON completo de forma legível
            json_formatado = json.dumps(registro.dados_json, indent=2, ensure_ascii=False)
            print(json_formatado)
            
            # Verificar especificamente se há campo 'itens'
            if 'itens' in registro.dados_json:
                print(f"\n✅ Campo 'itens' encontrado!")
                print(f"   Quantidade de itens: {len(registro.dados_json['itens'])}")
                
                if registro.dados_json['itens']:
                    print(f"\n   📋 Estrutura do primeiro item:")
                    primeiro_item = registro.dados_json['itens'][0]
                    print(json.dumps(primeiro_item, indent=4, ensure_ascii=False))
            else:
                print(f"\n❌ Campo 'itens' NÃO encontrado no JSON")
                print(f"\n📋 Campos disponíveis no JSON:")
                for campo in registro.dados_json.keys():
                    print(f"   • {campo}")
        
        # Estatística geral
        print(f"\n{'='*70}")
        print(f"📊 ESTATÍSTICA GERAL DE ITENS")
        print(f"{'='*70}")
        
        query_stats = text("""
            SELECT 
                COUNT(*) as total_vendas,
                SUM(CASE WHEN jsonb_array_length(dados_json->'itens') > 0 THEN 1 ELSE 0 END) as vendas_com_itens,
                AVG(jsonb_array_length(dados_json->'itens')) as media_itens
            FROM raw.vendas_raw
            WHERE dados_json ? 'itens'
        """)
        
        stats = session.execute(query_stats).fetchone()
        
        if stats:
            print(f"   • Total de vendas no banco: {stats.total_vendas}")
            print(f"   • Vendas com itens: {stats.vendas_com_itens}")
            print(f"   • Média de itens por venda: {stats.media_itens:.2f}")
        
        # Verificar se existe campo itens mas está vazio
        query_vazios = text("""
            SELECT COUNT(*) as vendas_com_itens_vazios
            FROM raw.vendas_raw
            WHERE dados_json ? 'itens' 
            AND jsonb_array_length(dados_json->'itens') = 0
        """)
        
        vazios = session.execute(query_vazios).scalar()
        print(f"   • Vendas com campo 'itens' vazio: {vazios}")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()


def verificar_endpoint_detalhes_pedido():
    """
    Verifica se a API tem um endpoint para buscar detalhes de pedidos
    """
    print(f"\n{'='*70}")
    print(f"💡 SUGESTÕES PARA PRÓXIMOS PASSOS")
    print(f"{'='*70}")
    
    sugestoes = """
    
Se os itens NÃO estão no JSON:
    
1. A API Bling pode ter um endpoint separado para itens:
   GET https://api.bling.com.br/Api/v3/pedidos/vendas/{id}
   
   Esse endpoint pode retornar os detalhes COMPLETOS do pedido,
   incluindo os itens.

2. SOLUÇÃO PROPOSTA:
   a) Criar um novo extrator que busca os detalhes de cada pedido
   b) Atualizar o JSON na tabela vendas_raw com os itens
   c) Ou criar uma tabela raw.itens_pedidos_raw separada
   
3. ALTERNATIVA RÁPIDA:
   Se você tem acesso à documentação da API, verificar:
   - O endpoint /pedidos/vendas retorna itens?
   - Precisa adicionar algum parâmetro na URL?
   - Existe um endpoint específico para itens?

📚 DOCUMENTAÇÃO API BLING:
   https://developer.bling.com.br/aplicativos#pedidos-vendas

⚠️  IMPORTANTE:
   Sem os itens dos pedidos, não conseguiremos popular:
   - fato_itens_pedidos (que é essencial para análises)
   - Quantidade total de itens por pedido
   - Análise de produtos mais vendidos
   - Margem de lucro por item
    """
    
    print(sugestoes)


if __name__ == "__main__":
    try:
        investigar_estrutura_completa_vendas()
        verificar_endpoint_detalhes_pedido()
        
        print(f"\n{'='*70}")
        print("✅ INVESTIGAÇÃO CONCLUÍDA")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        raise