# Pipeline ETL Multi-Empresa do setor de e-commerce | Bling → PostgreSQL → Power BI

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-316192.svg)](https://www.postgresql.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Import-F2C811.svg)](https://powerbi.microsoft.com/)
[![AWS](https://img.shields.io/badge/AWS-RDS%20%7C%20EC2%20%7C%20Lambda%20%7C%20SNS-FF9900.svg)](https://aws.amazon.com/)

> **Pipeline ETL automatizado** para consolidação de dados de 6 CNPJs do ERP Bling em um Data Warehouse único, com dashboards gerenciais em Power BI e orquestração AWS.

---

## 📋 Índice

- [O Problema](#-o-problema)
- [A Solução](#-a-solução)
- [Destaques Técnicos](#-destaques-técnicos)
- [Arquitetura](#-arquitetura)
- [Otimização INCREMENTAL](#-otimização-incremental)
- [Automação AWS](#-automação-aws)
- [Stack Tecnológica](#-stack-tecnológica)
- [Complexidade e Escala](#-complexidade-e-escala)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Como Executar](#-como-executar)
- [Dashboards](#-dashboards)
- [Resultados](#-resultados)
- [Status de Implementação](#-status-de-implementação)

---

## ❌ O Problema

**Cliente:** Empresa de e-commerce multi-CNPJ (setor de bicicletas)

**Dores identificadas:**
- 🏢 **6 CNPJs diferentes** no ERP Bling (dados isolados por empresa)
- 📊 **Impossível ter visão consolidada** de vendas e finanças
- ⏰ **Relatórios manuais** demorados e propensos a erro
- 📉 **Falta de KPIs em tempo real** para tomada de decisão
- 🔄 Necessidade de **atualização frequente** (mínimo 3x/dia)
- 💰 **Custos de infraestrutura** para servidor rodando 24/7 sem necessidade — $182/ano desperdiçados


**Impacto no negócio:**
- Decisões estratégicas baseadas em dados desatualizados
- Tempo desperdiçado consolidando planilhas manualmente
- Perda de oportunidades por falta de visibilidade de performance

---

## ✅ A Solução

Pipeline ETL automatizado que:

### **1. Extrai** 📥
- Conecta via **API** do Bling (OAuth 2.0)
- Coleta dados de **12 endpoints** (vendas, produtos, contas, NFe, etc.)
- Processa os dados das **6 empresas em sequência, devido a limitações de requests da API**

### **2. Transforma** ⚙️
- Limpa e padroniza dados brutos
- Aplica **modelagem dimensional** (Star Schema - Kimball)

### **3. Carrega** 📤
- Armazena em **PostgreSQL (AWS RDS)** otimizado
- 2 schemas: `raw` (dados brutos) + `processed` (DW)
- **15 tabelas dimensionais** prontas para análise

### **4. Automatiza** 🤖
- **Orquestração AWS** (Lambda + EventBridge + EC2)
- Servidor **liga/desliga automaticamente** em horários programados
- Execuções agendadas via **Windows Task Scheduler**
- **Notificações por email** (AWS SNS) em caso de sucesso/erro

### **5. Visualiza** 📊
- Dashboards **Power BI** (Import)
- 2 painéis: **Comercial** e **Financeiro**
- Atualização **a cada 2 horas** durante dias úteis (Início 07h30; Término 19h30)


**Resultado:**
- ✅ Visão consolidada de 6 empresas em **um único dashboard**
- ✅ Decisões baseadas em dados **atualizados e confiáveis**
- ✅ Economia de **~10 horas/semana** em consolidação dos dados manual
- ✅ Servidor **sob demanda**: 31 horas/semana (137 horas economizadas com servidor)
- ✅ Redução de **81.5% nos custos de infraestrutura** (EC2 + RDS ~$25/mês)
- ✅ **Zero intervenção manual** - tudo automatizado

---

## 🏆 Destaques Técnicos

Este projeto demonstra habilidades em:

✅ **Engenharia de Dados:**
- Modelagem dimensional (Star Schema)
- ETL em Python (Pandas + SQLAlchemy)
- Otimização de performance (FULL → INCREMENTAL)

✅ **DevOps / Cloud:**
- Automação AWS (EC2, Lambda, EventBridge, SNS)
- Infraestrutura como Código (IAM, schedules)
- Redução de custos (81.5% de economia)

✅ **Integração de Sistemas:**
- OAuth 2.0 (renovação automática de tokens)
- APIs REST (rate limiting, paginação)
- Windows Task Scheduler

✅ **Visualização de Dados:**
- Power BI (dashboards executivos)
- Import Mode (Importação otimizada de dados)
- KPIs de negócio (comercial + financeiro)

## 🔧 Arquitetura

```
┌──────────────────────────────────────────────────────────────────────────┐
│                     ARQUITETURA COMPLETA DO SISTEMA                      │
└──────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                          CAMADA DE ORQUESTRAÇÃO                         │
└─────────────────────────────────────────────────────────────────────────┘

   AWS EventBridge (22 Schedules)
   ├─ START: Seg-Sex 07h25, 09h25, ... (7x)
   ├─ START: Sábado 07h25, 11h25, 15h25 (3x)
   ├─ START: Domingo 21h25 (1x)
   ├─ STOP:  Seg-Sex 08h10, 10h10, ... (7x)
   ├─ STOP:  Sábado 08h10, 12h10, 16h10 (3x)
   └─ STOP:  Domingo 23h55 (1x)
         │
         ▼
   AWS Lambda Functions
   ├─ StartEC2-Projeto ──┐
   └─ StopEC2-Projeto    │
         │                │
         ▼                ▼
   ┌─────────────────────────────┐
   │  EC2 Windows t3.small       │
   │  • 31 horas/semana          │
   │  • Liga/Desliga Automático  │
   │  • Windows Server 2022      │
   └─────────────────────────────┘
         │
         ▼
   Windows Task Scheduler (11 Tasks)
   ├─ Seg-Sex 07h30, 09h30, ... (7x) → INCREMENTAL
   ├─ Sábado  07h30, 11h30, 15h30 (3x) → INCREMENTAL
   └─ Domingo 21h30 (1x) → FULL
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          CAMADA DE PROCESSAMENTO                        │
└─────────────────────────────────────────────────────────────────────────┘

   Python ETL Pipeline
   ├─ main_pipeline_complete_INCREMENTAL.py (~20 min)
   └─ main_pipeline_complete_FULL.py (~2h15 min)
         │
         ▼
   API BLING (6 Empresas)
   ├─ OAuth 2.0 Authentication
   ├─ Vendas, Produtos, Contatos
   ├─ NFe, Contas a Pagar/Receber
   └─ 12 endpoints diferentes
         │
         ▼
   Data Processing
   ├─ extract/  → 14 extratores
   ├─ transform/ → 12 transformadores
   └─ Pandas + SQLAlchemy
         │
         │ ┌─ SUCCESS ──→ AWS SNS
         │ └─ ERROR ───→ AWS SNS (Email Notification)
         ▼
```         

**15 Tabelas no Schema `processed`:**

**Dimensões (10):**
- `dim_canais` - Canais de venda (Amazon, Shopee, Via Varejo, etc.)
- `dim_categorias_contas_pagar` - Categorias de despesas
- `dim_contatos` - Clientes/Fornecedores
- `dim_empresas` - 6 CNPJs
- `dim_formas_pagamento` - Métodos de pagamento
- `dim_metas_empresas` - KPIs e metas por empresa/mês
- `dim_natureza_operacao` - Natureza fiscal
- `dim_produtos` - Catálogo de produtos
- `dim_situacao` - Status de pedidos (Atendido, Em aberto, Cancelado, etc.)
- `dim_tempo` - Calendário (dimensão da data)

**Fatos (5):**
- `fato_pedidos` - Transações de vendas
- `fato_itens_pedidos` - Produtos vendidos
- `fato_contas_pagar` - Contas a pagar
- `fato_contas_receber` - Contas a receber
- `fato_nfe` - Notas fiscais emitidas

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          CAMADA DE VISUALIZAÇÃO                         │
└─────────────────────────────────────────────────────────────────────────┘

   Power BI (Import)
   ├─ Dashboard Comercial
   │  ├─ Overview Executivo
   │  ├─ Canais & CNPJ
   │  └─ Geografia & Tempo
   │
   └─ Dashboard Financeiro
      ├─ Contas a Pagar - Overview
      ├─ Fornecedores & Categorias
      └─ Produção & Despacho
```

---

## ⚡ Otimização INCREMENTAL

### **Problema Inicial:**
- Pipeline executava extração **COMPLETA** (desde o início) a cada run
- ⏱️ Tempo: **2h00-2h30 horas** por execução
- ❌ **Impossível** atender requisito de 3x/dia com atualizações a cada 2 horas

### **Solução Implementada:**

Criação de **2 modos de extração** + filtro para extrair dados a partir de 2024 (a pedido do cliente):

| Modo | Janela Temporal | Limpa Órfãos? | Frequência | Duração |
|------|-----------------|---------------|------------|---------|
| **INCREMENTAL** | 7 dias (cadastros)<br>90 dias (financeiro) | ❌ Não | 10x/semana (seg-sáb) | ~15-25 min |
| **FULL** | Desde 2024-01-01 | ✅ Sim | Domingo 21h30 | ~2h15 min |

**Estratégia de Cobertura:**
```
Segunda a Sábado: INCREMENTAL captura 99% das alterações recentes
Domingo:          FULL captura 100% + limpa dados órfãos + corrige retroativos

Delay máximo para casos extremos: 6 dias (aceitável para dados históricos)
```

**Proteções Implementadas:**
- 🛡️ **Sem limpeza no INCREMENTAL**: Protege contra perda de dados se API falhar
- 🔐 **Renovação de tokens**: Validação antes de cada empresa
- 🚨 **Abort em caso de erro**: Preserva dados anteriores
- 📧 **Notificações SNS**: Email automático em caso de sucesso/erro

**Resultado:**
- ✅ Redução do pipeline de **2h00-2h30** para **15-25 minutos** (modo INCREMENTAL)
- ✅ **6x mais rápido** que o pipeline original
- ✅ Requisito de 3x/dia **superado** (agora 7x/dia)

---

## 🤖 Automação AWS

### **🎯 Objetivo**

Criar uma infraestrutura **econômica** e **totalmente automatizada** onde:
- EC2 **liga apenas quando necessário** (execuções programadas)
- **Zero intervenção manual** após configuração inicial
- **Notificações automáticas** de sucesso/erro
- **Economia de 81.5%** nos custos de computação

---

### **💰 Análise de Custos**

#### **❌ Cenário Anterior (Servidor 24/7):**
```
EC2 t3.small: $0.0208/hora
24h × 7 dias × 4.33 semanas = 730 horas/mês
Custo mensal: $0.0208 × 730 = $15.18/mês
Custo anual: $15.18 × 12 = $182.16/ano
```

#### **✅ Cenário Atual (Servidor sob Demanda):**
```
EC2 ligada apenas 31 horas/semana:
31h × 4.33 semanas = 134 horas/mês
Custo mensal: $0.0208 × 134 = $2.79/mês
Custo anual: $2.79 × 12 = $33.48/ano

ECONOMIA: $15.18 - $2.79 = $12.39/mês (~81.5%)
ECONOMIA ANUAL: $148.68/ano
```

**Custos AWS Adicionais (gratuitos dentro do free tier):**
- Lambda: FREE (1M invocações/mês grátis)
- EventBridge: FREE (execuções incluídas)
- SNS: FREE (1,000 emails/mês grátis)

**Total Real: ~ $34 /ano**

---

### **🏗️ Componentes da Automação**

#### **1. EC2 Windows (Servidor de Execução)**

**Especificações:**
- **Tipo:** t3.small (2 vCPU, 2GB RAM)
- **SO:** Windows Server 2022
- **Região:** us-east-1 (Norte da Virgínia)
- **Armazenamento:** 30GB gp3
- **Uptime:** 31 horas/semana (apenas durante execuções)

**Configuração:**
```
Instalação:
- Python 3.12.8 + venv
- Git (sincronização de código)
- boto3 (SDK AWS para SNS)
- Timezone: America/Sao_Paulo (UTC-3)

Estrutura:
C:\etl-Projeto\
├── venv\                          # Ambiente virtual Python
├── main_pipeline_complete_INCREMENTAL.py
├── main_pipeline_complete_FULL.py
├── run_incremental.bat            # Ativa venv + executa INCREMENTAL
├── run_full.bat                   # Ativa venv + executa FULL
└── .env                           # Credenciais OAuth + RDS
```

---

#### **2. AWS Lambda Functions**

**StartEC2-Projeto:**
```python
# Função: Ligar a instância EC2
# Gatilho: EventBridge (antes de cada execução)

import boto3

def lambda_handler(event, context):
    ec2 = boto3.client('ec2', region_name='us-east-1')
    instance_id = 'i-07beda8f47550fd96'
    
    response = ec2.start_instances(InstanceIds=[instance_id])
    return {'statusCode': 200, 'body': 'EC2 started'}
```

**StopEC2-Projeto:**
```python
# Função: Desligar a instância EC2
# Gatilho: EventBridge (após cada execução)

import boto3

def lambda_handler(event, context):
    ec2 = boto3.client('ec2', region_name='us-east-1')
    instance_id = 'i-07beda8f47550fd96'
    
    response = ec2.stop_instances(InstanceIds=[instance_id])
    return {'statusCode': 200, 'body': 'EC2 stopped'}
```

**Permissões IAM:**
```json
{
  "Effect": "Allow",
  "Action": [
    "ec2:StartInstances",
    "ec2:StopInstances",
    "ec2:DescribeInstances"
  ],
  "Resource": "arn:aws:ec2:us-east-1:*:instance/i-07beda8f47550fd96"
}
```

---

#### **3. AWS EventBridge (Orquestrador)**

**22 Schedules Configurados:**

| ID | Nome | Dia | Horário | Ação | Alvo |
|----|------|-----|---------|------|------|
| 1 | START-Projeto-SegSex-07h25 | Seg-Sex | 07:25 | Ligar EC2 | Lambda Start |
| 2 | STOP-Projeto-SegSex-08h10 | Seg-Sex | 08:10 | Desligar EC2 | Lambda Stop |
| ... |


**Expressão Cron (exemplo):**
```
# Seg-Sex às 07:25 (Brasília = UTC-3)
cron(25 10 ? * MON-FRI *)  # 10:25 UTC = 07:25 Brasília
```

---

#### **4. Windows Task Scheduler (Executor)**

**11 Tasks Configuradas:**

| ID | Nome | Dia | Horário | Script | Modo |
|----|------|-----|---------|--------|------|
| 1 | ETL_Full_Dom_21h30 | Domingo | 21:30 | run_full.bat | FULL |
| 2 | ETL_Incremental_SegSex_07h30 | Seg-Sex | 07:30 | run_incremental.bat | INCREMENTAL |
| ... |

---

#### **5. AWS SNS (Notificações)**

**Configuração:**
```
Tópico: ETL-Projeto-Notifications
Tipo: Standard
ARN: arn:aws:sns:us-east-1:892789742514:ETL-Projeto-Notifications
Assinatura: Email (william.gesner@outlook.com)
```

**Integração no Código Python:**
```python
import boto3

def notify_success(pipeline_type, tempo_total, total_empresas):
    sns = boto3.client('sns', region_name='us-east-1')
    sns.publish(
        TopicArn='arn:aws:sns:us-east-1:892789742514:ETL-Projeto-Notifications',
        Subject=f'✅ ETL Projeto {pipeline_type} - SUCESSO',
        Message=f'''
Pipeline: {pipeline_type}
Empresas: {total_empresas}
Tempo Total: {tempo_total}
Status: Concluído com sucesso!
        '''
    )

def notify_error(pipeline_type, error_message):
    sns = boto3.client('sns', region_name='us-east-1')
    sns.publish(
        TopicArn='arn:aws:sns:us-east-1:892789742514:ETL-Projeto-Notifications',
        Subject=f'❌ ETL Projeto {pipeline_type} - ERRO',
        Message=f'''
Pipeline: {pipeline_type}
Status: ERRO
Erro: {error_message}
⚠️  Verifique os logs no servidor
        '''
    )
```

**Exemplo de email:**
```
✅ ETL Projeto - SUCESSO

Pipeline: INCREMENTAL
Empresas: 6
Horário: 19/02/2026 07:44:48
Tempo Total: 14.34 minutos (860.49s)
Status: Concluído com sucesso!

✅ Todas as 6 empresas processadas
✅ Data Warehouse atualizado
✅ Power BI pode ser atualizado

```

---

### **⏱️ Fluxo de Execução Típico**

**Exemplo: Segunda-feira 07:30**
```
07:25:00 - EventBridge dispara schedule START-Projeto-SegSex-07h25
07:25:01 - Lambda StartEC2-Projeto executa
07:25:02 - EC2 i-07beda8f47550fd96 começa a ligar
07:25:45 - EC2 está online (Windows boot completo)

07:30:00 - Task Scheduler dispara ETL_Incremental_SegSex_07h30
07:30:01 - run_incremental.bat executa:
           1. cd C:\etl-Projeto
           2. call venv\Scripts\activate.bat
           3. python main_pipeline_complete_INCREMENTAL.py

07:30:05 - Pipeline inicia:
           • Verifica credenciais (6 empresas)
           • Renova tokens OAuth
           • Prepara schemas do banco

07:32:00 - Empresa 1: Extração + Transformação
07:35:00 - Empresa 2: Extração + Transformação
07:37:00 - Empresa 3: Extração + Transformação
07:42:00 - Empresa 4: Extração + Transformação
07:43:00 - Empresa 5: Extração + Transformação
07:44:00 - Empresa 6: Extração + Transformação

07:45:00 - Pipeline finaliza:
           • Coleta estatísticas do DW
           • Gera relatório consolidado
           • Envia notificação SNS (✅ SUCESSO)

07:45:15 - Email recebido: "✅ ETL Projeto INCREMENTAL - SUCESSO"

08:10:00 - EventBridge dispara schedule STOP-Projeto-SegSex-08h10
08:10:01 - Lambda StopEC2-Projeto executa
08:10:05 - EC2 desliga automaticamente

Total: 45 minutos de uptime (~20 min de processamento real)
```

---

### **📊 Monitoramento**

#### **1. Task Scheduler (Windows)**
```
Indicadores de Sucesso:
- Last Run Time: Data/hora da última execução
- Last Run Result: (0x0) = sucesso
- Status: "Running" durante execução

Logs:
Action → Enable All Tasks History
Event 201: Task Started
Event 200: Task Completed Successfully
Event 203: Task Failed
```

#### **2. AWS CloudWatch**
```
Lambda Logs:
/aws/lambda/StartEC2-Projeto
/aws/lambda/StopEC2-Projeto

Métricas:
- Invocations (contagem de execuções)
- Duration (tempo de execução)
- Errors (falhas)
```

#### **3. Email (SNS)**
```
Notificações Recebidas:
✅ Sucesso: "Pipeline INCREMENTAL executado em 25.3 minutos"
❌ Erro: "Falha na Empresa 3: Token expirado"

Frequência Esperada:
- 10 emails/semana (INCREMENTAL)
- 1 email/semana (FULL)
- Total: ~48 emails/mês (dentro do free tier de 1,000)
```
---

## 💻 Stack Tecnológica

### **Backend / ETL**
- **Python 3.8+** - Linguagem principal
- **Pandas 2.3.2** - Manipulação de dados
- **SQLAlchemy 2.0.41** - ORM e conexão com PostgreSQL
- **Requests + OAuth 2.0** - Comunicação com API Bling
- **boto3** - SDK AWS (SNS notifications)

### **Banco de Dados**
- **PostgreSQL 14** - Data Warehouse
- **AWS RDS** - Hospedagem cloud (us-east-1)

### **Visualização**
- **Power BI** - Dashboards interativos
- **Import Mode** - Dados importados e atualizados via agendamento

### **Infraestrutura AWS**
- **EC2 t3.small** - Servidor de execução (Windows Server 2022)
- **Lambda** - Orquestração (Start/Stop EC2)
- **EventBridge** - Agendamento (22 schedules)
- **SNS** - Notificações por email
- **IAM** - Gerenciamento de permissões
- **CloudWatch** - Logs e monitoramento

### **Ferramentas**
- **Git** - Controle de versão
- **Windows Task Scheduler** - Agendamento local (11 tasks)
- **PowerShell** - Administração Windows

---

## 📈 Complexidade e Escala

### **Dados**
- **Volume**: ~1.8GB (crescente)
- **Registros**: ~1.340.133 registros (26 tabelas)
- **Empresas**: 6 CNPJs consolidados
- **Endpoints**: 12 endpoints da API Bling

### **Processamento**
- **Tempo INCREMENTAL**: 15-25 min
- **Tempo FULL**: 2h15min
- **Frequência**: 11 execuções/semana
- **Uptime**: 31 horas/semana (EC2)

### **Automação**
- **Schedules**: 22 (EventBridge) + 11 (Task Scheduler)
- **Lambdas**: 2 (Start + Stop)
- **Notificações**: ~48 emails/mês
- **Custo**: $2.79/mês (~81.5% de economia)

### **Confiabilidade**
- **Uptime Pipeline**: 99%+
- **Monitoramento**: CloudWatch + Task Scheduler + SNS
- **Backup**: Diário (Git) + Semanal (RDS Snapshots)

---

## 📁 Estrutura do Projeto
```
PROJETO-ETL-ECOMMERCE/

│
├── config/                               # Configurações centralizadas
│   ├── auth_manager.py                   # Gerenciamento OAuth + tokens
│   ├── database.py                       # Conexão PostgreSQL + Models
│   ├── settings.py                       # Variáveis de ambiente
│   └── logger.py                         # Sistema de logs
│
├── core/                                 # Utilitários base
│   └── base_extractor.py                 # Classe base para extratores
│
├── extract/                              # 14 extratores (API → raw)
│   ├── contacts.py
│   ├── products.py
│   ├── sales.py
│   ├── nfe.py
│   └── ...
│
├── transform/                            # 12 transformadores (raw → DW)
│   ├── contacts_dw.py
│   ├── products_dw.py
│   ├── sales_dw.py
│   └── ...
│
├── models/                               # Modelos SQLAlchemy
│   ├── contact_raw.py                    # Schema raw
│   ├── product_raw.py
│   └── ...
│   └── dim_fato/                         # Schema processed
│
├── main/                                 # Orquestração
│   ├── endpoints/                        # Pipelines individuais
│   ├── pipeline_complete_FULL/           # Pipelines FULL (6 arquivos)
│   └── pipeline_complete_INCREMENTAL/    # Pipelines INCREMENTAL (6 arquivos)
│
├── data_business/                        # Dados de negócio
│   ├── empresas.csv                      # Cadastro de empresas
│   └── metas_empresas.csv                # KPIs/Metas
│
├── logs/                                 # Logs de execução
├── analysis/                             # Análises exploratórias e testes de API, Scripts e Endpoints
│
├── main_pipeline_complete_FULL.py        # Orquestrador FULL
├── main_pipeline_complete_INCREMENTAL.py # Orquestrador INCREMENTAL
├── run_incremental.bat                   # Script Windows (INCREMENTAL)
├── run_full.bat                          # Script Windows (FULL)
│
├── .env                                  # Credenciais (não versionado)
├── .gitignore                            # Controle de versão
├── requirements.txt                      # Dependências Python
└── README.md                             # Documentação
```
---

## 🚀 Como Executar

### **Pré-requisitos**

1. **Python 3.8+** instalado
2. **PostgreSQL 14** (local ou AWS RDS)
3. **Credenciais da API Bling** (OAuth 2.0)
   - Client ID, Client Secret, Refresh Token (por empresa)
4. **(Opcional)** AWS Account para automação

### **Instalação**
```bash
# 1. Clone o repositório
git clone https://github.com/Williamgesner/ELT-Project-API-Extract.git
cd ELT-Project-API-Extract

# 2. Crie ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows

# 3. Instale dependências
pip install -r requirements.txt

# 4. Configure variáveis de ambiente
cp .env.example .env
# Edite .env com suas credenciais
```

### **Configuração `.env`**
```bash
# Credenciais OAuth 2.0 (por empresa - 01 a 06)
CLIENT_ID_01=seu_client_id_empresa_01
CLIENT_SECRET_01=seu_client_secret_empresa_01
REFRESH_TOKEN_01=seu_refresh_token_empresa_01
# ... (repetir para empresas 02 a 06)

# PostgreSQL (AWS RDS ou local)
postgres_host=seu-rds-endpoint.us-east-1.rds.amazonaws.com
postgres_database=database-nome-database
postgres_username=seu-usuario
postgres_password=sua_senha_segura
postgres_port=5432
```

### **Executar Pipeline (Manual)**

**Modo INCREMENTAL** (Recomendado para uso diário):
```bash
python main_pipeline_complete_INCREMENTAL.py
```

**Modo FULL** (Executar semanalmente):
```bash
python main_pipeline_complete_FULL.py
```

### **Configurar Automação AWS** *(Opcional)*

**Veja seção [Automação AWS](#-automação-aws) para detalhes completos**

Resumo:
1. Criar EC2 Windows t3.small
2. Criar 2 Lambdas (Start + Stop)
3. Criar 22 schedules no EventBridge
4. Configurar 11 tasks no Task Scheduler
5. Configurar tópico SNS para notificações

---

## 📊 Dashboards

### **Dashboard Comercial**

**Abas:**
1. **Overview Executivo**
   - Faturamento total + comparação período anterior
   - Ticket médio + itens vendidos + quantidade de pedidos
   - Evolução temporal + atingimento de meta
   - Mapa de vendas por estado

2. **Canais & CNPJ**
   - Canal líder
   - CNPJ com maior faturamento
   - Evolução mensal por canal
   - Ranking detalhado dos 27 canais

3. **Geografia & Tempo**
   - Região líder
   - Estado líder
   - Dia da semana com mais vendas
   - Mapa interativo do Brasil

**Filtros Disponíveis:** Período, CNPJ, Canais, Situação

---

### **Dashboard Financeiro**

**Abas:**
1. **Contas a Pagar - Overview**
   - ⚠️ **Vence HOJE**
   - Vence em 7 dias
   - Vence em 30 dias
   - Fluxo de caixa projetado (60 dias, por dia)
   - Fluxo de caixa projetado (próximos 3 meses)
   - Top 10 fornecedores + categorias
   - Valores a pagar por categoria

2. **Fornecedores & Categorias**
   - Concentração de fornecedores (Pareto)
   - Detalhamento por título (vencimento, status)
   - Ticket médio por fornecedor

3. **Produção & Despacho**
   - Lead time médio (Tempo que leva do Pedido → DANFE)
   - Funil de produção (Total → Atendido → Em Produção)
   - Gargalos identificados (NFe sem DANFE)
   - Pedidos impressos vs não impressos (controle de produção)

**Filtros Disponíveis:** Período, CNPJ, Categoria

---

## 📸 Resultados

### Dashboard Comercial
![Dashboard Comercial 1](https://github.com/user-attachments/assets/aaf0ec58-20b3-46a6-9bcd-0aefea17f57a)
![Dashboard Comercial 2](https://github.com/user-attachments/assets/238f63bb-6874-4e07-9fbb-2c3d447c7b42)
![Dashboard Comercial 3](https://github.com/user-attachments/assets/2fe0114a-be11-4e43-b947-80e6ae350b30)

### Dashboard Financeiro  
![Dashboard Financeiro 1](https://github.com/user-attachments/assets/b4325dfe-92df-4738-8388-ca3b3a0e81b3)
![Dashboard Financeiro 2](https://github.com/user-attachments/assets/44550491-80a8-4df4-9fd3-ca7f18100cc0)
![Dashboard Financeiro 3](https://github.com/user-attachments/assets/bc5215d0-eb82-4354-996c-8932cbf60777)

**Observações**
- *⁠TODOS OS DADOS APRESENTADOS ACIMA SÃO FICTÍCIOS E AS IDENTIDADES DOS CLIENTES FORAM PRESERVADAS*
- ⁠A atualização do **Power BI** é automatizada via **Power Automate**, sincronizada com os horários de execução dos pipelines no EC2.

---

## ✅ Status de Implementação

### **🎉 Concluído (100%)**

#### **✅ Pipeline ETL**
- [x] 14 extratores (API Bling → Schema RAW)
- [x] 12 transformadores (RAW → DW)
- [x] Modelagem dimensional (15 tabelas)
- [x] Otimização INCREMENTAL (6x mais rápido)
- [x] Sistema de logs
- [x] Tratamento de erros robusto

#### **✅ Automação AWS**
- [x] EC2 Windows t3.small configurada
- [x] Lambda StartEC2-Projeto
- [x] Lambda StopEC2-Projeto
- [x] EventBridge (22 schedules)
- [x] Windows Task Scheduler (11 tasks)
- [x] AWS SNS (notificações por email)
- [x] IAM Roles e permissões
- [x] Economia de 81.5% em custos

#### **✅ Visualização**
- [x] Dashboard Comercial
- [x] Dashboard Financeiro
- [x] Filtros interativos

---

### **🚀 Próximas Melhorias**

#### **Otimizações**
- [ ] Analisar viabilidade de substituir o PG por Data Lake. Isso ajudaria a reduzir o custo de armazenamento (RDS) que hoje é o custo mais caro do projeto (~ $ 20/mês)

#### **Expansão**
- [ ] Nova página no Dashboard de produtos
- [ ] Nova página no Dashboard de contas a pagar
- [ ] Nova página no Dashboard de metas, por empresa e por canais de vendas

---

## 📄 Licença

Este projeto é proprietário e de uso restrito.

---

## 🧑🏻‍💻 Autor

**William Gesner**  
📧 william.gesner@outlook.com · 🔗 [LinkedIn](https://www.linkedin.com/in/william-gesner/) · 🔗 [GitHub](https://github.com/Williamgesner)

---

**Desenvolvido com ❤️ para otimizar decisões baseadas em dados**