# Responsável por: executar TODOS os pipelines FULL de TODAS as empresas
# Este é o ORQUESTRADOR PRINCIPAL FULL que executa os 6 pipelines em sequência
# MODO FULL: Extração completa + Limpeza de órfãos (RODAR SOMENTE AOS FINAIS DE SEMANAS)

"""
========================================
PIPELINE COMPLETO - TODAS AS EMPRESAS
MODO FULL
========================================

ORQUESTRADOR PRINCIPAL - MODO FULL
Executa os 6 pipelines FULL em sequência:
- Empresa 1 (main_pipeline_01_FULL)
- Empresa 2 (main_pipeline_02_FULL)
- Empresa 3 (main_pipeline_03_FULL)
- Empresa 4 (main_pipeline_04_FULL)
- Empresa 5 (main_pipeline_05_FULL)
- Empresa 6 (main_pipeline_06_FULL)

Inclui: Parte COMERCIAL + Parte FINANCEIRA de todas as empresas

⚡ MODO FULL:
   • Extração completa desde 2024
   • Limpeza de órfãos ATIVA
   • Sincronização completa com Bling
   • Executar SEMANALMENTE (finais de semana)

🔐 SEGURANÇA:
   • Gera API Keys automaticamente antes de executar
   • Verifica credenciais OAuth (CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
   • Mantém segurança: ABORTA se faltar credenciais
"""

import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import text
from config.database import create_schema_raw, create_schema_processed, create_all_tables, Session
from config.auth_manager import obter_token_para_empresa
import importlib  # para importar os módulos dos pipelines
import boto3      # Para enviar notificações por email

AVISOS_TOKEN = []  # Acumula erros de token durante a execução (compartilhado com pipelines)

# Garantir que todos os pipelines FULL importem o MESMO módulo (e a mesma lista AVISOS_TOKEN),
# mesmo quando este arquivo é executado como script principal (__main__).
sys.modules.setdefault("main_pipeline_complete_FULL", sys.modules[__name__])

# Carregar variáveis de ambiente
load_dotenv()

# =====================================================
# CONFIGURAÇÃO SNS (Notificações por Email)
# =====================================================
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:892789742514:ETL-DiasBike-Notifications'

def notify_success(pipeline_type, tempo_total_segundos, total_empresas, avisos_token=None):
    """Envia notificação de sucesso via SNS"""
    try:
        sns_client = boto3.client('sns', region_name='us-east-1')
        tempo_formatado = formatar_tempo(tempo_total_segundos)

        secao_avisos = ""
        if avisos_token:
            linhas = "\n".join(f"   ⚠️  {a}" for a in avisos_token)
            secao_avisos = f"""

⚠️  AVISOS DE TOKEN ({len(avisos_token)} ocorrência(s)):
{linhas}

🔧 AÇÃO RECOMENDADA: Verifique/renove os refresh tokens das empresas listadas acima.
"""

        message = f"""
✅ ETL DiasBike - SUCESSO

Pipeline: {pipeline_type}
Empresas: {total_empresas}
Horário: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
Tempo Total: {tempo_formatado}
Status: Concluído com sucesso!

✅ Todas as {total_empresas} empresas processadas
✅ Data Warehouse atualizado
✅ Power BI pode ser atualizado{secao_avisos}
        """
        
        # Para indicar avisos quando houver
        subject_suffix = " (com avisos de token)" if avisos_token else ""
        
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f'✅ ETL DiasBike {pipeline_type} - SUCESSO{subject_suffix}',
            Message=message
        )
        print("\n📧 Notificação de SUCESSO enviada por email!")
        
    except Exception as e:
        print(f"\n⚠️  Erro ao enviar notificação de sucesso: {e}")

def notify_error(pipeline_type, error_message, tempo_total_segundos=0, avisos_token=None):
    """Envia notificação de erro via SNS, incluindo avisos de token quando existirem"""
    try:
        sns_client = boto3.client('sns', region_name='us-east-1')
        tempo_formatado = formatar_tempo(tempo_total_segundos) if tempo_total_segundos > 0 else "N/A"

        secao_avisos = ""
        if avisos_token:
            linhas = "\n".join(f"   ⚠️  {a}" for a in avisos_token)
            secao_avisos = f"""

⚠️  AVISOS DE TOKEN ({len(avisos_token)} ocorrência(s)):
{linhas}

🔧 AÇÃO RECOMENDADA: Verifique/renove os refresh tokens e API Keys das empresas listadas acima.
"""

        message = f"""
❌ ETL DiasBike - ERRO

Pipeline: {pipeline_type}
Horário: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
Tempo até erro: {tempo_formatado}
Status: ERRO

Erro: {error_message[:500]}
{secao_avisos}
⚠️  Verifique os logs no servidor
⚠️  Pipeline pode precisar ser executado novamente
        """
        
        subject_suffix = " (com avisos de token)" if avisos_token else ""

        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f'❌ ETL DiasBike {pipeline_type} - ERRO{subject_suffix}',
            Message=message
        )
        print("\n📧 Notificação de ERRO enviada por email!")
        
    except Exception as e:
        print(f"\n⚠️  Erro ao enviar notificação de erro: {e}")

# =====================================================
# CONFIGURAÇÃO DE EMPRESAS ATIVAS
# =====================================================

# Lista de empresas para processar (adicione/remova conforme necessário)
EMPRESAS_ATIVAS = [1, 2, 3, 4, 5, 6]

# Mapeamento de módulos de pipeline por empresa (MODO FULL)
PIPELINE_MODULES = {
    1: 'main_pipeline_01_FULL',
    2: 'main_pipeline_02_FULL',
    3: 'main_pipeline_03_FULL',
    4: 'main_pipeline_04_FULL',
    5: 'main_pipeline_05_FULL',
    6: 'main_pipeline_06_FULL'
}

# =====================================================
# FUNÇÕES AUXILIARES - SEGURANÇA
# =====================================================

def verificar_credenciais_empresas():
    """
    🔐 VERIFICAÇÃO DE SEGURANÇA
    
    Verifica se TODAS as credenciais OAuth necessárias estão configuradas
    para cada empresa ativa.
    
    CREDENCIAIS OBRIGATÓRIAS por empresa:
    - CLIENT_ID_XX
    - CLIENT_SECRET_XX
    - REFRESH_TOKEN_XX
    
    NOTA: API_KEY_XX será gerada automaticamente pelo auth_manager
    
    Returns:
        bool: True se todas as credenciais estão ok, False caso contrário
    """
    print("\n🔐 VERIFICANDO CREDENCIAIS DE AUTENTICAÇÃO...")
    print("=" * 70)
    print("📋 Verificando CLIENT_ID, CLIENT_SECRET e REFRESH_TOKEN")
    print("-" * 70)
    
    credenciais_faltando = []
    empresas_ok = []
    
    for empresa_id in EMPRESAS_ATIVAS:
        print(f"\n📌 Empresa {empresa_id:02d}:")
        
        # Verificar as 3 credenciais obrigatórias
        client_id = os.getenv(f'CLIENT_ID_{empresa_id:02d}')
        client_secret = os.getenv(f'CLIENT_SECRET_{empresa_id:02d}')
        refresh_token = os.getenv(f'REFRESH_TOKEN_{empresa_id:02d}')
        
        status_client_id = "✅" if client_id else "❌"
        status_client_secret = "✅" if client_secret else "❌"
        status_refresh_token = "✅" if refresh_token else "❌"
        
        print(f"   {status_client_id} CLIENT_ID_{empresa_id:02d}")
        print(f"   {status_client_secret} CLIENT_SECRET_{empresa_id:02d}")
        print(f"   {status_refresh_token} REFRESH_TOKEN_{empresa_id:02d}")
        
        # Se faltar alguma credencial, registrar
        if not all([client_id, client_secret, refresh_token]):
            if not client_id:
                credenciais_faltando.append(f"CLIENT_ID_{empresa_id:02d}")
            if not client_secret:
                credenciais_faltando.append(f"CLIENT_SECRET_{empresa_id:02d}")
            if not refresh_token:
                credenciais_faltando.append(f"REFRESH_TOKEN_{empresa_id:02d}")
        else:
            empresas_ok.append(empresa_id)
    
    print("\n" + "=" * 70)
    
    # 🔴 VERIFICAÇÃO DE SEGURANÇA: Se faltar credenciais, ABORTAR!
    if credenciais_faltando:
        print(f"\n❌ ERRO CRÍTICO: {len(credenciais_faltando)} CREDENCIAL(IS) FALTANDO!")
        print("=" * 70)
        print("\n📋 Credenciais não encontradas no .env:")
        for cred in credenciais_faltando:
            print(f"   • {cred}")
        
        print("\n🔧 AÇÕES NECESSÁRIAS:")
        print("   1. Abra o arquivo .env")
        print("   2. Adicione as credenciais listadas acima")
        print("   3. Obtenha as credenciais no Postman (Get New Access Token)")
        print("   4. Execute o pipeline novamente")
        
        print("\n🛡️ SEGURANÇA: Execução ABORTADA para proteger dados")
        print("=" * 70)
        
        return False
    
    # ✅ TODAS as credenciais estão OK!
    print(f"\n✅ VERIFICAÇÃO CONCLUÍDA COM SUCESSO!")
    print(f"✅ Todas as {len(EMPRESAS_ATIVAS)} empresas têm credenciais completas!")
    print(f"✅ API Keys serão geradas automaticamente")
    print("=" * 70)
    
    return True


def obter_tokens_empresas():
    """
    🔑 GERAÇÃO AUTOMÁTICA DE TOKENS
    
    Obtém/renova tokens válidos para TODAS as empresas ativas
    antes de executar os pipelines.
    
    Isso garante que:
    - Todos os tokens estão válidos (não expirados)
    - Arquivo .env está atualizado
    - Pipelines não falharão por token inválido
    
    Returns:
        dict: Dicionário com tokens de cada empresa {empresa_id: token}
    """
    print("\n" + "=" * 70)
    print("🔑 OBTENDO TOKENS VÁLIDOS PARA TODAS AS EMPRESAS")
    print("=" * 70)
    print("⚙️  Esta etapa pode demorar alguns segundos...")
    print("-" * 70)
    
    tokens = {}
    erros = []
    
    for empresa_id in EMPRESAS_ATIVAS:
        try:
            print(f"\n📋 Empresa {empresa_id:02d}:")
            
            # Chamar o auth_manager para obter/renovar token
            token = obter_token_para_empresa(empresa_id)
            
            if token:
                tokens[empresa_id] = token
                # Mostrar preview do token (primeiros e últimos caracteres)
                token_preview = f"{token[:10]}...{token[-6:]}" if len(token) > 16 else "***"
                print(f"✅ Token obtido: {token_preview}")
            else:
                erros.append(empresa_id)
                print(f"❌ Falha ao obter token da Empresa {empresa_id:02d}")
                
        except Exception as e:
            erros.append(empresa_id)
            print(f"❌ Erro ao obter token da Empresa {empresa_id:02d}: {e}")
    
    print("\n" + "=" * 70)
    
    # 🔴 VERIFICAÇÃO DE SEGURANÇA: Se algum token falhou, ABORTAR!
    if erros:
        print(f"\n❌ ERRO CRÍTICO: Falha ao obter {len(erros)} token(s)!")
        print("=" * 70)
        print("\n📋 Empresas com erro:")
        for emp_id in erros:
            print(f"   • Empresa {emp_id:02d}")
        
        print("\n🔧 POSSÍVEIS CAUSAS:")
        print("   • Refresh token expirado (renovar no Postman)")
        print("   • Client ID ou Client Secret incorretos")
        print("   • Problemas de conexão com API do Bling")
        print("   • Credenciais inválidas ou revogadas")
        
        print("\n🛡️ SEGURANÇA: Execução ABORTADA para proteger dados")
        print("=" * 70)
        
        return None
    
    # ✅ TODOS os tokens obtidos com sucesso!
    print(f"\n✅ TODOS OS {len(tokens)} TOKENS OBTIDOS COM SUCESSO!")
    print(f"✅ Arquivo .env atualizado com novos tokens")
    print(f"✅ Sistema pronto para extrair dados")
    print("=" * 70)
    
    return tokens


# =====================================================
# FUNÇÕES AUXILIARES - EXECUÇÃO
# =====================================================

def executar_pipeline_empresa(empresa_id):
    """
    Executa o pipeline FULL de uma empresa específica
    Retorna um dicionário com os resultados
    """
    print(f"\n{'='*70}")
    print(f"🚀 INICIANDO PIPELINE FULL DA EMPRESA {empresa_id}")
    print(f"{'='*70}")
    
    inicio = datetime.now()
    inicio_timestamp = time.time()
    
    try:
        # 🔑 Renovar token desta empresa ANTES de importar o pipeline
        # Garante que .env e memória tenham token fresco (evita 401 na Empresa 3, etc.)
        print(f"\n🔑 Renovando token da Empresa {empresa_id:02d} antes de executar pipeline...")
        obter_token_para_empresa(empresa_id)
        print(f"✅ Token da Empresa {empresa_id:02d} validado.\n")

        # Importar o módulo do pipeline dinamicamente
        module_name = f"main.pipeline_complete_FULL.{PIPELINE_MODULES[empresa_id]}"
        pipeline_module = importlib.import_module(module_name)
        
        # Executar a função principal do pipeline
        pipeline_module.executar_pipeline_completo()
        
        fim = datetime.now()
        fim_timestamp = time.time()
        tempo_total_segundos = fim_timestamp - inicio_timestamp
        tempo_total = fim - inicio
        
        return {
            'empresa_id': empresa_id,
            'status': 'SUCCESS',
            'tempo': tempo_total,
            'tempo_segundos': tempo_total_segundos,
            'inicio': inicio,
            'fim': fim
        }
        
    except Exception as e:
        fim = datetime.now()
        fim_timestamp = time.time()
        tempo_total_segundos = fim_timestamp - inicio_timestamp
        tempo_total = fim - inicio
        
        print(f"\n❌ ERRO ao executar pipeline FULL da Empresa {empresa_id}: {e}")
        import traceback
        traceback.print_exc()

        erro_str = str(e).lower()
        keywords_token = ['401', 'invalid_token', 'invalid_grant', 'token', 'expirado', 'unauthorized']
        if any(k in erro_str for k in keywords_token):
            aviso = f"Empresa {empresa_id:02d} - Erro de autenticação: {str(e)[:120]}"
            AVISOS_TOKEN.append(aviso)
            print(f"   ⚠️  Aviso de token registrado para notificação por email")
        
        return {
            'empresa_id': empresa_id,
            'status': 'ERROR',
            'tempo': tempo_total,
            'tempo_segundos': tempo_total_segundos,
            'inicio': inicio,
            'fim': fim,
            'erro': str(e)
        }

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
            
            # Itens
            query = text("SELECT COUNT(*) FROM processed.fato_itens_pedidos WHERE empresa_id = :emp_id")
            total_itens = session.execute(query, {'emp_id': empresa_id}).scalar() or 0
            
            # Contas a Pagar
            query = text("SELECT COUNT(*) FROM processed.fato_contas_pagar WHERE empresa_id = :emp_id")
            total_contas_pagar = session.execute(query, {'emp_id': empresa_id}).scalar() or 0
            
            # Contas a Receber
            # query = text("SELECT COUNT(*) FROM processed.fato_contas_receber WHERE empresa_id = :emp_id")
            # total_contas_receber = session.execute(query, {'emp_id': empresa_id}).scalar() or 0
            
            # NFe
            query = text("SELECT COUNT(*) FROM processed.fato_nfe WHERE empresa_id = :emp_id")
            total_nfe = session.execute(query, {'emp_id': empresa_id}).scalar() or 0
            
            print(f"   📊 COMERCIAL:")
            print(f"      • Contatos: {total_contatos:,}")
            print(f"      • Produtos: {total_produtos:,}")
            print(f"      • Pedidos: {total_pedidos:,}")
            print(f"      • Itens de Pedidos: {total_itens:,}")
            
            print(f"   💰 FINANCEIRO:")
            print(f"      • Contas a Pagar: {total_contas_pagar:,}")
            # print(f"      • Contas a Receber: {total_contas_receber:,}")
            print(f"      • NFe: {total_nfe:,}")
            
            estatisticas[empresa_id] = {
                'contatos': total_contatos,
                'produtos': total_produtos,
                'pedidos': total_pedidos,
                'itens': total_itens,
                'contas_pagar': total_contas_pagar,
                # 'contas_receber': total_contas_receber,
                'nfe': total_nfe
            }
        
        # Totais gerais
        print(f"\n{'='*70}")
        print(f"🎯 TOTAIS GERAIS (TODAS AS EMPRESAS)")
        print(f"{'='*70}")
        
        total_geral_contatos = sum(e['contatos'] for e in estatisticas.values())
        total_geral_produtos = sum(e['produtos'] for e in estatisticas.values())
        total_geral_pedidos = sum(e['pedidos'] for e in estatisticas.values())
        total_geral_itens = sum(e['itens'] for e in estatisticas.values())
        total_geral_contas_pagar = sum(e['contas_pagar'] for e in estatisticas.values())
        # total_geral_contas_receber = sum(e['contas_receber'] for e in estatisticas.values())
        total_geral_nfe = sum(e['nfe'] for e in estatisticas.values())
        
        print(f"📊 COMERCIAL:")
        print(f"   • Total de Contatos: {total_geral_contatos:,}")
        print(f"   • Total de Produtos: {total_geral_produtos:,}")
        print(f"   • Total de Pedidos: {total_geral_pedidos:,}")
        print(f"   • Total de Itens: {total_geral_itens:,}")
        
        print(f"\n💰 FINANCEIRO:")
        print(f"   • Total de Contas a Pagar: {total_geral_contas_pagar:,}")
        # print(f"   • Total de Contas a Receber: {total_geral_contas_receber:,}")
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
    Executa todos os pipelines FULL em sequência
    
    🔐 SEGURANÇA:
       • Verifica credenciais OAuth antes de começar
       • Gera todos os tokens antes de executar
       • ABORTA se faltar credenciais ou tokens falharem
    """
    print("\n" + "=" * 70)
    print("🌐 PIPELINE COMPLETO - TODAS AS EMPRESAS - MODO FULL")
    print("=" * 70)
    print(f"📅 Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"🏢 Empresas ativas: {len(EMPRESAS_ATIVAS)}")
    print(f"📋 IDs: {', '.join(str(e) for e in EMPRESAS_ATIVAS)}")
    print(f"⚡ MODO: FULL (Extração completa + Limpeza de órfãos)")
    print(f"📅 PROGRAMAÇÃO: Executar SEMANALMENTE (finais de semana)")
    print("=" * 70)
    
    # =====================================================
    # 🔐 PASSO 1: VERIFICAR CREDENCIAIS (SEGURANÇA CRÍTICA!)
    # =====================================================
    if not verificar_credenciais_empresas():
        print("\n🛡️ EXECUÇÃO ABORTADA: Credenciais incompletas")
        print("💡 Adicione as credenciais no .env e tente novamente")
        return
    
    # =====================================================
    # 🔑 PASSO 2: OBTER/RENOVAR TOKENS (ANTES DE TUDO!)
    # =====================================================
    tokens = obter_tokens_empresas()
    
    if not tokens:
        print("\n🛡️ EXECUÇÃO ABORTADA: Falha ao obter tokens")
        print("💡 Verifique as credenciais e a conexão com API do Bling")
        return
    
    # =====================================================
    # 🔧 PASSO 3: PREPARAR BANCO DE DADOS
    # =====================================================
    print("\n🔧 PREPARANDO BANCO DE DADOS...")
    try:
        create_schema_raw()
        create_schema_processed()
        create_all_tables()
        print("✅ Schemas e tabelas verificados/criados")
    except Exception as e:
        print(f"❌ Erro ao preparar banco de dados: {e}")
        return
    
    # =====================================================
    # 🚀 PASSO 4: EXECUTAR PIPELINES FULL
    # =====================================================
    
    # Registrar início geral
    inicio_geral = datetime.now()
    inicio_geral_timestamp = time.time()
    
    # Executar cada pipeline
    resultados = []
    
    for empresa_id in EMPRESAS_ATIVAS:
        resultado = executar_pipeline_empresa(empresa_id)
        resultados.append(resultado)
        
        # Pequena pausa entre empresas para não sobrecarregar
        if empresa_id != EMPRESAS_ATIVAS[-1]:  # Não pausar após a última
            print("\n⏸️  Aguardando 5 segundos antes da próxima empresa...")
            time.sleep(5)
    
    # Registrar fim geral
    fim_geral = datetime.now()
    fim_geral_timestamp = time.time()
    tempo_total_geral = fim_geral - inicio_geral
    tempo_total_geral_segundos = fim_geral_timestamp - inicio_geral_timestamp
    
    # =====================================================
    # 📊 PASSO 5: COLETAR ESTATÍSTICAS
    # =====================================================
    estatisticas = coletar_estatisticas_finais()
    
    # =====================================================
    # 📋 RELATÓRIO FINAL CONSOLIDADO
    # =====================================================
    
    print(f"\n{'='*70}")
    print(f"🏁 EXECUÇÃO COMPLETA FINALIZADA - MODO FULL")
    print(f"{'='*70}")
    print(f"⏱️  Tempo total: {formatar_tempo(tempo_total_geral_segundos)}")
    print(f"📅 Início: {inicio_geral.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"📅 Fim: {fim_geral.strftime('%d/%m/%Y %H:%M:%S')}")
    
    # Resumo por empresa
    print(f"\n📊 RESUMO POR EMPRESA:")
    print("-" * 70)
    
    sucessos = 0
    erros = 0
    erros_detalhes = []
    
    for resultado in resultados:
        empresa_id = resultado['empresa_id']
        status = resultado['status']
        tempo_segundos = resultado['tempo_segundos']
        
        status_emoji = "✅" if status == 'SUCCESS' else "❌"
        
        print(f"{status_emoji} Empresa {empresa_id}: {status} - Tempo: {formatar_tempo(tempo_segundos)}")
        
        if status == 'SUCCESS':
            sucessos += 1
        else:
            erros += 1
            if 'erro' in resultado:
                erro_msg = resultado['erro'][:100]
                print(f"   └── Erro: {erro_msg}...")
                erros_detalhes.append(f"Empresa {empresa_id}: {erro_msg}")
    
    # Estatísticas finais
    print(f"\n🎯 ESTATÍSTICAS FINAIS:")
    print(f"✅ Sucessos: {sucessos}/{len(EMPRESAS_ATIVAS)}")
    print(f"❌ Erros: {erros}/{len(EMPRESAS_ATIVAS)}")
    print(f"📈 Taxa de sucesso: {(sucessos/len(EMPRESAS_ATIVAS)*100):.1f}%")
    
    # Mensagem final
    if sucessos == len(EMPRESAS_ATIVAS):
        print(f"\n🎉 TODOS OS PIPELINES FULL EXECUTADOS COM SUCESSO!")
        notify_success("FULL", tempo_total_geral_segundos, len(EMPRESAS_ATIVAS), AVISOS_TOKEN)
        print(f"\n💡 PRÓXIMOS PASSOS:")
        print(f"   1. DW sincronizado COMPLETAMENTE com a Bling (Todas as {len(EMPRESAS_ATIVAS)} empresas)")
        print(f"   2. Limpeza de órfãos executada em todas as empresas")
        print(f"   3. Power BI pode ser atualizado")
        print(f"   4. Execute pipeline INCREMENTAL durante a semana")
    elif sucessos > 0:
        erro_resumo = f"{erros} empresa(s) com erro: " + "; ".join(erros_detalhes)
        notify_error("FULL", erro_resumo, tempo_total_geral_segundos, AVISOS_TOKEN)
        print(f"\n⚠️  EXECUÇÃO PARCIAL:")
        print(f"   • {sucessos} empresa(s) processada(s) com sucesso")
        print(f"   • {erros} empresa(s) com erro - Verifique os logs acima")
    else:
        erro_resumo = "TODAS AS EMPRESAS FALHARAM: " + "; ".join(erros_detalhes)
        notify_error("FULL", erro_resumo, tempo_total_geral_segundos, AVISOS_TOKEN)
        print(f"\n❌ TODAS AS EMPRESAS FALHARAM")
        print(f"   • Verifique conexões, credenciais e logs de erro")
    
    print(f"\n{'='*70}")
    print(f"📝 Log salvo automaticamente pelo Python")
    print(f"📧 Notificação enviada por email")
    print(f"{'='*70}\n")


# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    try:
        print("""
        ╔═══════════════════════════════════════════════════════════════╗
        ║                                                               ║
        ║      🌐 ORQUESTRADOR PRINCIPAL - MULTI EMPRESA - FULL         ║
        ║                                                               ║
        ║   Este script executa TODOS os pipelines FULL em sequência:   ║
        ║   • Empresa 1, 2, 3, 4, 5 e 6                                 ║
        ║   • Parte Comercial + Financeira                              ║
        ║   • Extração COMPLETA + Transformação                         ║
        ║   • Limpeza de órfãos ATIVA                                   ║
        ║                                                               ║
        ║   ⚡ MODO FULL:                                                ║
        ║   • Extrai TUDO desde 2024                                    ║
        ║   • Remove registros órfãos do DW                             ║
        ║   • Sincronização COMPLETA com Bling                          ║
        ║                                                               ║
        ║   🔐 SEGURANÇA:                                               ║ 
        ║   • Verifica credenciais antes de começar                     ║
        ║   • Gera tokens automaticamente                               ║
        ║   • Aborta se faltar credenciais ou tokens                    ║
        ║                                                               ║
        ║   📅 PROGRAMAÇÃO RECOMENDADA:                                 ║
        ║   • Executar SEMANALMENTE (finais de semana)                  ║
        ║                                                               ║
        ╚═══════════════════════════════════════════════════════════════╝
        """)
        
        # Executar todos os pipelines
        executar_todos_pipelines()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  EXECUÇÃO INTERROMPIDA PELO USUÁRIO")
        print("💾 Dados processados até este ponto foram preservados")
        print("🔄 Para retomar, execute o script novamente")
        notify_error("FULL", "Execução interrompida pelo usuário", avisos_token=AVISOS_TOKEN)
        sys.exit(0)
        
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO NO ORQUESTRADOR FULL: {e}")
        import traceback
        traceback.print_exc()
        notify_error("FULL", f"Erro crítico: {str(e)}", avisos_token=AVISOS_TOKEN)
        sys.exit(1)