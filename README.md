# Pipeline ETL Multi-Empresa do setor de e-commerce | Bling → PostgreSQL → Power BI

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-316192.svg)](https://www.postgresql.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-DirectQuery-F2C811.svg)](https://powerbi.microsoft.com/)
[![AWS](https://img.shields.io/badge/AWS-RDS-FF9900.svg)](https://aws.amazon.com/rds/)

> **Pipeline ETL** para consolidação de dados de 6 CNPJs do ERP Bling em um Data Warehouse único, com dashboards gerenciais em Power BI.

---

## 📋 Índice

- [O Problema](#-o-problema)
- [A Solução](#-a-solução)
- [Arquitetura](#-arquitetura)
- [Otimização INCREMENTAL](#-otimização-incremental)
- [Stack Tecnológica](#-stack-tecnológica)
- [Complexidade e Escala](#-complexibilidade-e-Escala)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Como Executar](#-como-executar)
- [Dashboards](#-dashboards)
- [Próximos Passos](#-próximos-passos)

---

## ❌ O Problema

**Cliente:** Empresa de e-commerce multi-CNPJ (setor de bicicletas)

**Dores identificadas:**
- 🏢 **6 CNPJs diferentes** no ERP Bling (dados isolados por empresa)
- 📊 **Impossível ter visão consolidada** de vendas e finanças
- ⏰ **Relatórios manuais** demorados e propensos a erro, pois precisava ser feito tudo manual 
- 📉 **Falta de KPIs em tempo real** para tomada de decisão
- 🔄 Necessidade de **atualização frequente** (mínimo 3x/dia)

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

### **4. Visualiza** 📊
- Dashboards **Power BI** (DirectQuery)
- 2 painéis: **Comercial** e **Financeiro**
- Atualização **a cada 2 horas** durante dias úteis

**Resultado:**
- ✅ Visão consolidada de 6 empresas em **um único dashboard**
- ✅ Decisões baseadas em dados **atualizados e confiáveis**
- ✅ Economia de **~10 horas/semana** em consolidação manual

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                         FLUXO DE DADOS                          │
└─────────────────────────────────────────────────────────────────┘

   API BLING (6 Empresas)
   ├─ Vendas
   ├─ Produtos
   ├─ Contatos
   ├─ NFe
   ├─ Contas a Pagar/Receber
   └─ Dados Cadastrais
         │
         │ (OAuth 2.0)
         ▼
   ┌─────────────────────┐
   │  PYTHON PIPELINES   │
   │  ─────────────────  │
   │  • extract/         │ ← 14 extratores
   │  • transform/       │ ← 12 transformadores
   │  • Pandas + SQL     │
   └─────────────────────┘
         │
         │ (SQLAlchemy)
         ▼
   ┌─────────────────────┐
   │  POSTGRESQL (AWS)   │
   │  ─────────────────  │
   │  Schema RAW         │ ← 11 tabelas (dados brutos)
   │  Schema PROCESSED   │ ← 15 tabelas (DW)
   │  • 10 Dimensões     │
   │  •  5 Fatos         │
   └─────────────────────┘
         │
         │ (DirectQuery)
         ▼
   ┌────────────────────────┐
   │        POWER BI        │
   │    ─────────────────   │
   │  Dashboard Comercial   │
   │  Dashboard Financeiro  │
   └────────────────────────┘
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

---

## ⚡ Otimização INCREMENTAL

### **Problema Inicial:**
- Pipeline executava extração **COMPLETA** (desde o início) a cada run
- ⏱️ Tempo: **2h00 -2h30 horas** por execução
- ❌ **Impossível** atender requisito de 3x/dia

### **Solução Implementada:**

Criação de **2 modos de extração** + filtro para extrair dados a partir de 2024 (a pedido do cliente):

| Modo | Janela Temporal | Limpa Órfãos? | Frequência | Duração |
|------|-----------------|---------------|------------|---------|
| **INCREMENTAL** | 7 dias (cadastros)<br>120 dias (financeiro) | ❌ Não | A cada 2 horas (seg-sex) | ~30-40 min |
| **FULL** | Desde 2024-01-01 | ✅ Sim | Domingo 02:00 | ~2h30 horas |

**Estratégia de Cobertura:**
```
Segunda a Sexta: INCREMENTAL captura 99% das alterações recentes
Sábado:          FULL captura 100% + limpa dados órfãos + corrige retroativos

Delay máximo para casos extremos: 6 dias (aceitável para dados históricos)
```

**Proteções Implementadas:**
- 🛡️ **Sem limpeza no INCREMENTAL**: Protege contra perda de dados se API falhar
- 🔐 **Renovação de tokens**: Validação antes de cada empresa
- 🚨 **Abort em caso de erro**: Preserva dados anteriores

**Resultado:**
- ✅ De **2-2h30 horas** para **30-40 minutos** (modo INCREMENTAL)
- ✅ **5x mais rápido** que o pipeline original
- ✅ Requisito de 3x/dia **superado** e totalmente possível

---

## 🛠️ Stack Tecnológica

### **Backend / ETL**
- **Python 3.8+** - Linguagem principal
- **Pandas 2.3.2** - Manipulação de dados
- **SQLAlchemy 2.0.41** - ORM e conexão com PostgreSQL
- **Requests + OAuth 2.0** - Comunicação com API Bling

### **Banco de Dados**
- **PostgreSQL** - Data Warehouse
- **AWS RDS** - Hospedagem cloud (us-east-1)

### **Visualização**
- **Power BI** - Dashboards interativos
- **DirectQuery** - Conexão em tempo real com PostgreSQL

### **Infraestrutura**
- **AWS RDS** - Banco de dados gerenciado
- *(Planejado)* AWS Lambda/EC2 + EventBridge - Orquestração automatizada

---

## 📈 Complexidade e Escala

- **Volume de dados**: ~1.8GB (crescente)
- **Registros processados**: ~1.340.133 + (26 tabelas raw + processed)
- **Tempo de execução**: 30-40 min (incremental) | 2h30 (full)
- **Frequência**: mínimo 3x/dia (seg-sex)
- **Uptime**: 99%+ (monitoramento via logs)

## 📁 Estrutura do Projeto

```
PROJETO-DIAS-BIKE/

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
│   ├── endpoints                         # main de cada endpoint individual (extrator e transformador) 
│   ├── pipeline_complete_FULL/           # Pipelines modo FULL (6 arquivos)
│   └── pipeline_complete_INCREMENTAL/    # Pipelines modo INCREMENTAL (6 arquivos)
│
├── data_business/                        # Dados de negócio
│   ├── empresas.csv                      # Cadastro de empresas
│   └── metas_empresas.csv                # KPIs/Metas
│
├── logs/                                 # Logs de execução
├── analysis/                             # Análises exploratórias e testes de API, Scripts e Endpoints
│
├── main_pipeline_complete_FULL.py        # Orquestrador principal (FULL)
├── main_pipeline_complete_INCREMENTAL.py # Orquestrador principal (INCREMENTAL)
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

### **Instalação**

```bash
# 1. Clone o repositório
git clone https://github.com/seu-usuario/projeto-dias-bike.git
cd projeto-dias-bike

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
postgres_database=database-diasbike
postgres_username=dias_bike
postgres_password=sua_senha_segura
postgres_port=5432
```

### **Executar Pipeline**

**Modo INCREMENTAL** (Recomendado para uso diário):
```bash
python main_pipeline_complete_INCREMENTAL.py
```

**Modo FULL** (Executar semanalmente):
```bash
python main_pipeline_complete_FULL.py
```

### **Logs e Monitoramento**

```bash
# Logs são salvos automaticamente em logs/
tail -f logs/pipeline_$(date +%Y%m%d).log
```

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

   Todas as páginas possuem filtros para que possam ser feitas consultas personalizadas por: PERÍODO, CNPJ, CANAIS e SITUAÇÃO

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

   Todas páginas com filtros para que possam serem feitas consultas personalizadas por: PERÍODO, CNPJ, CATEGORIA

## 📸 Resultados

### Dashboard Comercial
![Image](https://github.com/user-attachments/assets/368c678c-fe56-4f46-b7f8-464cdaf01879)
![Image](https://github.com/user-attachments/assets/b12fd6e6-bab5-412c-89c5-ce95bcdd393f)
![Image](https://github.com/user-attachments/assets/7533841e-374b-4541-9b8c-3a4e082d8f9f)

### Dashboard Financeiro  
![Image](https://github.com/user-attachments/assets/be23ee3e-e350-4a0f-9382-c25e49e5e64a)
![Image](https://github.com/user-attachments/assets/593fb7ef-9111-4a7c-bb05-e1dd657d99b2)
![Image](https://github.com/user-attachments/assets/0ff2e333-3133-4a6b-b55c-5ecd8dca85d1)

   * Obs.: TODOS OS DADOS ACIMA SÃO FICTÍCIOS, APENAS PARA ILUSTRAÇÃO.

---

## 🗓️ Próximos Passos

### **Deploy em Produção (AWS)**
- [ ] Deploy em AWS Lambda (serverless)
- [ ] Agendamento automático (EventBridge)
- [ ] Monitoramento

---

## 📄 Licença

Este projeto é proprietário e de uso restrito.

---

## 👤 Autor

**William Gesner**  
📧 william.gesner@outlook.com · 🔗 [LinkedIn](https://www.linkedin.com/in/william-gesner/) · 🔗 [GitHub](https://github.com/Williamgesner)

---

**Desenvolvido com ❤️ para otimizar decisões baseadas em dados**