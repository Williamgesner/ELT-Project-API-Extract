# Responsável por: executar TODOS os pipelines de TODAS as empresas
# Este é o ORQUESTRADOR PRINCIPAL que executa os 6 pipelines em sequência

"""
========================================
PIPELINE COMPLETO - TODAS AS EMPRESAS
========================================

ORQUESTRADOR PRINCIPAL
Executa os 6 pipelines em sequência:
- Empresa 1 (main_pipeline_01)
- Empresa 2 (main_pipeline_02)
- Empresa 3 (main_pipeline_03)
- Empresa 4 (main_pipeline_04)
- Empresa 5 (main_pipeline_05)
- Empresa 6 (main_pipeline_06)

Inclui: Parte COMERCIAL + Parte FINANCEIRA de todas as empresas
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import text
from config.database import create_schema_raw, create_schema_processed, create_all_tables, Session

# Carregar variáveis de ambiente
load_dotenv()

# =====================================================
# CONFIGURAÇÃO DE EMPRESAS ATIVAS
# =====================================================

# Lista de empresas para processar (adicione/remova conforme necessário)
EMPRESAS_ATIVAS = [1, 2, 3, 4, 5, 6]

# Mapeamento de módulos de pipeline por empresa
PIPELINE_MODULES = {
    1: 'main_pipeline_01',
    2: 'main_pipeline_02',
    3: 'main_pipeline_03',
    4: 'main_pipeline_04',
    5: 'main_pipeline_05',
    6: 'main_pipeline_06'
}

# =====================================================
# FUNÇÕES AUXILIARES
# =====================================================

def verificar_api_keys():
    """Verifica se todas as API Keys das empresas ativas estão configuradas"""
    print("\n🔑 VERIFICANDO API KEYS...")
    print("=" * 70)
    
    keys_faltando = []
    
    for empresa_id in EMPRESAS_ATIVAS:
        key_name = f'API_KEY_{empresa_id:02d}'
        api_key = os.getenv(key_name)
        
        if not api_key:
            keys_faltando.append(key_name)
            print(f"❌ {key_name} não encontrada")
        else:
            # Mostrar apenas primeiros e últimos caracteres por segurança
            key_preview = f"{api_key[:8]}...{api_key[-4:]}" if len(api_key) > 12 else "***"
            print(f"✅ {key_name}: {key_preview}")
    
    if keys_faltando:
        print(f"\n❌ ERRO: {len(keys_faltando)} API Key(s) não encontrada(s) no .env:")
        for key in keys_faltando:
            print(f"   • {key}")
        return False
    
    print(f"\n✅ Todas as {len(EMPRESAS_ATIVAS)} API Keys estão configuradas!")
    return True

def executar_pipeline_empresa(empresa_id):
    """
    Executa o pipeline de uma empresa específica
    Retorna um dicionário com os resultados
    """
    print(f"\n{'='*70}")
    print(f"🚀 INICIANDO PIPELINE DA EMPRESA {empresa_id}")
    print(f"{'='*70}")
    
    inicio = datetime.now()
    
    try:
        # Importar o módulo do pipeline dinamicamente
        module_name = PIPELINE_MODULES[empresa_id]
        pipeline_module = __import__(module_name)
        
        # Executar a função principal do pipeline
        pipeline_module.executar_pipeline_completo()
        
        fim = datetime.now()
        tempo_total = fim - inicio
        
        return {
            'empresa_id': empresa_id,
            'status': 'SUCCESS',
            'tempo': tempo_total,
            'inicio': inicio,
            'fim': fim
        }
        
    except Exception as e:
        fim = datetime.now()
        tempo_total = fim - inicio
        
        print(f"\n❌ ERRO ao executar pipeline da Empresa {empresa_id}: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            'empresa_id': empresa_id,
            'status': 'ERROR',
            'tempo': tempo_total,
            'inicio': inicio,
            'fim': fim,
            'erro': str(e)
        }

def coletar_estatisticas_finais():
    """Coleta estatísticas consolidadas de todas as empresas do DW"""
    print(f"\n{'='*70}")
    print(f"📊 ESTATÍSTICAS CONSOLIDADAS DO DATA WAREHOUSE")
    print(f"{'='*70}")
    
    session = Session()
    estatisticas = {}
    
    try:
        for empresa_id in EMPRESAS_ATIVAS:
            print(f"\n📌 Empresa {empresa_id}:")
            print("-" * 70)
            
            # Contatos
            query = text("SELECT COUNT(*) FROM processed.dim_contatos WHERE empresa_id = :emp_id")
            total_contatos = session.execute(query, {'emp_id': empresa_id}).scalar() or 0
            
            # Produtos
            query = text("SELECT COUNT(*) FROM processed.dim_produtos WHERE empresa_id = :emp_id")
            total_produtos = session.execute(query, {'emp_id': empresa_id}).scalar() or 0
            
            # Pedidos
            query = text("SELECT COUNT(*) FROM processed.fato_pedidos WHERE empresa_id = :emp_id")
            total_pedidos = session.execute(query, {'emp_id': empresa_id}).scalar() or 0
            
            # Contas a Pagar
            query = text("SELECT COUNT(*) FROM processed.fato_contas_pagar WHERE empresa_id = :emp_id")
            total_contas_pagar = session.execute(query, {'emp_id': empresa_id}).scalar() or 0
            
            # Contas a Receber
            query = text("SELECT COUNT(*) FROM processed.fato_contas_receber WHERE empresa_id = :emp_id")
            total_contas_receber = session.execute(query, {'emp_id': empresa_id}).scalar() or 0
            
            # NFe
            query = text("SELECT COUNT(*) FROM processed.fato_nfe WHERE empresa_id = :emp_id")
            total_nfe = session.execute(query, {'emp_id': empresa_id}).scalar() or 0
            
            print(f"   📊 COMERCIAL:")
            print(f"      • Contatos: {total_contatos:,}")
            print(f"      • Produtos: {total_produtos:,}")
            print(f"      • Pedidos: {total_pedidos:,}")
            
            print(f"   💰 FINANCEIRO:")
            print(f"      • Contas a Pagar: {total_contas_pagar:,}")
            print(f"      • Contas a Receber: {total_contas_receber:,}")
            print(f"      • NFe: {total_nfe:,}")
            
            estatisticas[empresa_id] = {
                'contatos': total_contatos,
                'produtos': total_produtos,
                'pedidos': total_pedidos,
                'contas_pagar': total_contas_pagar,
                'contas_receber': total_contas_receber,
                'nfe': total_nfe
            }
        
        # Totais gerais
        print(f"\n{'='*70}")
        print(f"🎯 TOTAIS GERAIS (TODAS AS EMPRESAS)")
        print(f"{'='*70}")
        
        total_geral_contatos = sum(e['contatos'] for e in estatisticas.values())
        total_geral_produtos = sum(e['produtos'] for e in estatisticas.values())
        total_geral_pedidos = sum(e['pedidos'] for e in estatisticas.values())
        total_geral_contas_pagar = sum(e['contas_pagar'] for e in estatisticas.values())
        total_geral_contas_receber = sum(e['contas_receber'] for e in estatisticas.values())
        total_geral_nfe = sum(e['nfe'] for e in estatisticas.values())
        
        print(f"📊 COMERCIAL:")
        print(f"   • Total de Contatos: {total_geral_contatos:,}")
        print(f"   • Total de Produtos: {total_geral_produtos:,}")
        print(f"   • Total de Pedidos: {total_geral_pedidos:,}")
        
        print(f"\n💰 FINANCEIRO:")
        print(f"   • Total de Contas a Pagar: {total_geral_contas_pagar:,}")
        print(f"   • Total de Contas a Receber: {total_geral_contas_receber:,}")
        print(f"   • Total de NFe: {total_geral_nfe:,}")
        
    except Exception as e:
        print(f"⚠️  Erro ao coletar estatísticas: {e}")
    finally:
        session.close()
    
    return estatisticas

# =====================================================
# EXECUÇÃO PRINCIPAL
# =====================================================

def executar_todos_pipelines():
    """
    Executa todos os pipelines em sequência
    """
    print("\n" + "=" * 70)
    print("🌐 PIPELINE COMPLETO - TODAS AS EMPRESAS")
    print("=" * 70)
    print(f"📅 Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"🏢 Empresas ativas: {len(EMPRESAS_ATIVAS)}")
    print(f"📋 IDs: {', '.join(str(e) for e in EMPRESAS_ATIVAS)}")
    print("=" * 70)
    
    # Verificar API Keys
    if not verificar_api_keys():
        print("\n❌ Execução abortada por falta de API Keys")
        return
    
    # Criar schemas e tabelas (uma única vez)
    print("\n🔧 PREPARANDO BANCO DE DADOS...")
    try:
        create_schema_raw()
        create_schema_processed()
        create_all_tables()
        print("✅ Schemas e tabelas verificados/criados")
    except Exception as e:
        print(f"❌ Erro ao preparar banco de dados: {e}")
        return
    
    # Registrar início geral
    inicio_geral = datetime.now()
    
    # Executar cada pipeline
    resultados = []
    
    for empresa_id in EMPRESAS_ATIVAS:
        resultado = executar_pipeline_empresa(empresa_id)
        resultados.append(resultado)
        
        # Pequena pausa entre empresas para não sobrecarregar
        if empresa_id != EMPRESAS_ATIVAS[-1]:  # Não pausar após a última
            print("\n⏸️  Aguardando 5 segundos antes da próxima empresa...")
            import time
            time.sleep(5)
    
    # Registrar fim geral
    fim_geral = datetime.now()
    tempo_total_geral = fim_geral - inicio_geral
    
    # Coletar estatísticas finais
    estatisticas = coletar_estatisticas_finais()
    
    # =====================================================
    # RELATÓRIO FINAL CONSOLIDADO
    # =====================================================
    
    print(f"\n{'='*70}")
    print(f"🏁 EXECUÇÃO COMPLETA FINALIZADA")
    print(f"{'='*70}")
    print(f"⏱️  Tempo total: {tempo_total_geral}")
    print(f"📅 Início: {inicio_geral.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"📅 Fim: {fim_geral.strftime('%d/%m/%Y %H:%M:%S')}")
    
    # Resumo por empresa
    print(f"\n📊 RESUMO POR EMPRESA:")
    print("-" * 70)
    
    sucessos = 0
    erros = 0
    
    for resultado in resultados:
        empresa_id = resultado['empresa_id']
        status = resultado['status']
        tempo = resultado['tempo']
        
        status_emoji = "✅" if status == 'SUCCESS' else "❌"
        
        print(f"{status_emoji} Empresa {empresa_id}: {status} - Tempo: {tempo}")
        
        if status == 'SUCCESS':
            sucessos += 1
        else:
            erros += 1
            if 'erro' in resultado:
                print(f"   └── Erro: {resultado['erro'][:100]}...")
    
    # Estatísticas finais
    print(f"\n🎯 ESTATÍSTICAS FINAIS:")
    print(f"✅ Sucessos: {sucessos}/{len(EMPRESAS_ATIVAS)}")
    print(f"❌ Erros: {erros}/{len(EMPRESAS_ATIVAS)}")
    print(f"📈 Taxa de sucesso: {(sucessos/len(EMPRESAS_ATIVAS)*100):.1f}%")
    
    # Mensagem final
    if sucessos == len(EMPRESAS_ATIVAS):
        print(f"\n🎉 TODOS OS PIPELINES EXECUTADOS COM SUCESSO!")
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. DW sincronizado com a Bling (Todas as {len(EMPRESAS_ATIVAS)} empresas)")
        print(f"   2. Power BI pode ser atualizado")
        print(f"   3. Sistema pronto para análises consolidadas")
    elif sucessos > 0:
        print(f"\n⚠️  EXECUÇÃO PARCIAL:")
        print(f"   • {sucessos} empresa(s) processada(s) com sucesso")
        print(f"   • {erros} empresa(s) com erro - Verifique os logs acima")
    else:
        print(f"\n❌ TODAS AS EMPRESAS FALHARAM")
        print(f"   • Verifique conexões, API Keys e logs de erro")
    
    print(f"\n{'='*70}")
    print(f"📝 Log salvo automaticamente pelo Python")
    print(f"{'='*70}\n")

# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    try:
        print("""
        ╔═══════════════════════════════════════════════════════════════╗
        ║                                                               ║
        ║           🌐 ORQUESTRADOR PRINCIPAL - MULTI EMPRESA          ║
        ║                                                               ║
        ║   Este script executa TODOS os pipelines em sequência:       ║
        ║   • Empresa 1, 2, 3, 4, 5 e 6                                ║
        ║   • Parte Comercial + Financeira                             ║
        ║   • Extração + Transformação completas                       ║
        ║                                                               ║
        ╚═══════════════════════════════════════════════════════════════╝
        """)
        
        # Executar todos os pipelines
        executar_todos_pipelines()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  EXECUÇÃO INTERROMPIDA PELO USUÁRIO")
        print("💾 Dados processados até este ponto foram preservados")
        print("🔄 Para retomar, execute o script novamente")
        sys.exit(0)
        
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO NO ORQUESTRADOR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)