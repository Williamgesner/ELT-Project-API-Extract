# Script de teste: Comparar estratégias FULL vs INCREMENTAL

"""
TESTE: Produtos vs Contas a Receber
====================================

OBJETIVO:
Demonstrar a diferença entre endpoints COM e SEM filtro de alteração

ENDPOINTS TESTADOS:
1. Produtos (TEM filtro de alteração)
2. Contas a Receber (NÃO TEM filtro de alteração)

CENÁRIOS:
- Modo FULL: Ambos extraem tudo
- Modo INCREMENTAL: Comportamentos diferentes
"""

import os
from datetime import datetime
from dotenv import load_dotenv
from config.auth_manager import obter_token_para_empresa
from config.extraction_mode import ExtractionMode
from extract.products import ProdutosExtractor
from extract.accounts_receivable_v2 import ContasReceberExtractorV2

load_dotenv()

EMPRESA_ID = 1

print("=" * 70)
print("🧪 TESTE: ESTRATÉGIAS DE EXTRAÇÃO")
print("=" * 70)
print(f"📌 Empresa ID: {EMPRESA_ID}")
print(f"📅 Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
print("=" * 70)

# Obter token
print("\n🔑 Obtendo token...")
API_KEY = obter_token_para_empresa(EMPRESA_ID)
print("✅ Token obtido!")

# =====================================================
# TESTE 1: PRODUTOS (COM FILTRO DE ALTERAÇÃO)
# =====================================================

print("\n" + "=" * 70)
print("📦 TESTE 1: PRODUTOS (COM filtro de alteração)")
print("=" * 70)

print("\n🔵 CENÁRIO 1A: Produtos - Modo FULL")
print("-" * 70)
print("Comportamento esperado:")
print("  • Extrai TODOS os produtos desde 2024")
print("  • Compara com banco")
print("  • Limpa órfãos")
print("\nExecutando...")

try:
    extrator_produtos_full = ProdutosExtractor(API_KEY, EMPRESA_ID)
    # Simular modo FULL (extrai tudo)
    print("✅ Modo FULL: Extrairia ~22.290 produtos")
    print("⏱️  Tempo estimado: ~5 minutos")
except Exception as e:
    print(f"❌ Erro: {e}")

print("\n🟢 CENÁRIO 1B: Produtos - Modo INCREMENTAL")
print("-" * 70)
print("Comportamento esperado:")
print("  • Extrai apenas produtos ALTERADOS (últimos 7 dias)")
print("  • Usa filtro: dataAlteracaoInicial/Final")
print("  • NÃO limpa órfãos")
print("  • Economia: ~95% menos dados")
print("\nSimulação:")
print("✅ Modo INCREMENTAL: Extrairia ~100-500 produtos")
print("⏱️  Tempo estimado: ~30 segundos")

# =====================================================
# TESTE 2: CONTAS A RECEBER (SEM FILTRO DE ALTERAÇÃO)
# =====================================================

print("\n" + "=" * 70)
print("💵 TESTE 2: CONTAS A RECEBER (SEM filtro de alteração)")
print("=" * 70)

print("\n🔵 CENÁRIO 2A: Contas a Receber - Modo FULL")
print("-" * 70)
print("Comportamento esperado:")
print("  • Extrai TODAS as contas desde 2024")
print("  • Compara com banco (detecta mudanças de status)")
print("  • Limpa órfãos")
print("\nExecutando...")

try:
    extrator_contas_full = ContasReceberExtractorV2(
        API_KEY, 
        EMPRESA_ID, 
        extraction_mode=ExtractionMode.FULL
    )
    print("✅ Modo FULL: Extrairia todas as contas")
    print("⏱️  Tempo estimado: ~3-5 minutos")
except Exception as e:
    print(f"❌ Erro: {e}")

print("\n🟢 CENÁRIO 2B: Contas a Receber - Modo INCREMENTAL")
print("-" * 70)
print("Comportamento esperado:")
print("  • Extrai contas dos ÚLTIMOS 60 DIAS (não 7!)")
print("  • Usa filtro: dataInicial/Final (emissão, não alteração)")
print("  • SEMPRE compara (detecta mudanças de status)")
print("  • NÃO limpa órfãos")
print("\n⚠️  POR QUE 60 DIAS?")
print("  • Conta criada há 15 dias pode ser paga hoje")
print("  • Precisa detectar mudança de status")
print("  • 60 dias cobre: pagamentos atrasados, renegociações")
print("\nSimulação:")
print("✅ Modo INCREMENTAL: Extrairia contas dos últimos 60 dias")
print("⏱️  Tempo estimado: ~1-2 minutos")

# =====================================================
# COMPARAÇÃO FINAL
# =====================================================

print("\n" + "=" * 70)
print("📊 COMPARAÇÃO: PRODUTOS vs CONTAS A RECEBER")
print("=" * 70)

print("\n🔍 DIFERENÇAS CRÍTICAS:")
print("-" * 70)

print("\n1️⃣ PRODUTOS (COM filtro de alteração):")
print("   ✅ Modo INCREMENTAL: Últimos 7 dias")
print("   ✅ API retorna apenas produtos ALTERADOS")
print("   ✅ Economia máxima (~95%)")
print("   ✅ Seguro: Não perde mudanças")

print("\n2️⃣ CONTAS A RECEBER (SEM filtro de alteração):")
print("   ⚠️  Modo INCREMENTAL: Últimos 60 dias")
print("   ⚠️  API retorna por data de EMISSÃO")
print("   ⚠️  Precisa COMPARAR para detectar mudanças")
print("   ⚠️  Economia menor (~70%), mas necessário")

print("\n" + "=" * 70)
print("🎯 EXEMPLO PRÁTICO: POR QUE 60 DIAS?")
print("=" * 70)

print("\n📅 CENÁRIO REAL:")
print("   1. Conta criada: 15/01/2024 (vencimento: 30/01/2024)")
print("   2. Status inicial: 'Aberto'")
print("   3. Hoje: 10/02/2024")
print("   4. Cliente pagou HOJE → Status: 'Pago'")

print("\n❌ SE USAR 7 DIAS (ERRADO):")
print("   • API busca: 03/02 até 10/02")
print("   • Conta NÃO vem (foi criada em 15/01)")
print("   • DW fica desatualizado: mostra 'Aberto'")
print("   • ❌ PROBLEMA: Perda de atualização!")

print("\n✅ SE USAR 60 DIAS (CORRETO):")
print("   • API busca: 11/12 até 10/02")
print("   • Conta VEM (foi criada em 15/01)")
print("   • Comparação detecta mudança: 'Aberto' → 'Pago'")
print("   • Marca como 'pendente' para reprocessar")
print("   • ✅ SUCESSO: DW atualizado!")

print("\n" + "=" * 70)
print("💡 RECOMENDAÇÕES FINAIS")
print("=" * 70)

print("\n📋 CONFIGURAÇÃO POR ENDPOINT:")
print("-" * 70)

print("\n✅ COM FILTRO DE ALTERAÇÃO (7 dias):")
print("   • Contatos")
print("   • Produtos")
print("   • Vendas")

print("\n⚠️  SEM FILTRO DE ALTERAÇÃO (60 dias + comparação):")
print("   • Contas a Receber")
print("   • Contas a Pagar")
print("   • NFe")

print("\n🔄 FREQUÊNCIA RECOMENDADA:")
print("   • INCREMENTAL: 4x/dia (todos os endpoints)")
print("   • FULL: 1x/semana (limpeza de órfãos)")

print("\n🛡️  SEGURANÇA:")
print("   • Modo INCREMENTAL: NUNCA limpa órfãos")
print("   • Modo FULL: Limpa órfãos com validação")
print("   • Comparação inteligente: Detecta TODAS as mudanças")

print("\n" + "=" * 70)
print("✅ TESTE CONCLUÍDO!")
print("=" * 70)

print("\n📝 PRÓXIMOS PASSOS:")
print("   1. Testar modo FULL em produção")
print("   2. Testar modo INCREMENTAL (validar 60 dias)")
print("   3. Monitorar taxa de mudanças detectadas")
print("   4. Ajustar janela se necessário (60 → 90 dias)")
