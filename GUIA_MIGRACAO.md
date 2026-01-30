# 🚀 GUIA RÁPIDO DE MIGRAÇÃO

## ⚡ IMPLEMENTAÇÃO EM 5 PASSOS

### PASSO 1: Testar Modo FULL (Validação)
```bash
# Executar pipeline otimizado em modo FULL
python main_pipeline_01_optimized.py --full

# Verificar logs
tail -f logs/empresa_01_*.log

# Validar contagens no banco
# Devem ser IDÊNTICAS ao pipeline original
```

**Tempo esperado:** 2h30 (igual ao original)

**Validações:**
- ✅ Mesma quantidade de registros
- ✅ Limpeza de órfãos funcionando
- ✅ Logs detalhados gerados

---

### PASSO 2: Testar Modo INCREMENTAL
```bash
# Aguardar 1 hora (para ter alterações)
# Executar modo incremental
python main_pipeline_01_optimized.py --incremental

# Verificar logs
tail -f logs/empresa_01_*.log
```

**Tempo esperado:** 15-30 minutos

**Validações:**
- ✅ Apenas registros novos/alterados processados
- ✅ Limpeza de órfãos DESABILITADA
- ✅ Dados históricos preservados

---

### PASSO 3: Testar Modo Automático
```bash
# Executar sem argumentos (decide automaticamente)
python main_pipeline_01_optimized.py

# Verificar qual modo foi usado
cat .extraction_state_empresa_01.json
```

**Comportamento esperado:**
- Se última FULL < 7 dias → INCREMENTAL
- Se última FULL ≥ 7 dias → FULL

---

### PASSO 4: Substituir no Agendamento

#### Se usar Cron:
```bash
# Editar crontab
crontab -e

# ANTES (4x/dia, 10h total):
# 0 */6 * * * cd /path/to/project && python main_pipeline_01.py

# DEPOIS (4x/dia, 1h40 total):
0 0 * * 0 cd /path/to/project && python main_pipeline_01_optimized.py --full
0 6,12,18 * * * cd /path/to/project && python main_pipeline_01_optimized.py --incremental
```

#### Se usar Airflow:
```python
# dag_pipeline_empresa_01.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG para modo FULL (1x/semana)
dag_full = DAG(
    'pipeline_empresa_01_full',
    default_args=default_args,
    schedule_interval='0 0 * * 0',  # Domingo 00:00
    catchup=False
)

task_full = BashOperator(
    task_id='extract_transform_full',
    bash_command='cd /path/to/project && python main_pipeline_01_optimized.py --full',
    dag=dag_full
)

# DAG para modo INCREMENTAL (4x/dia)
dag_incremental = DAG(
    'pipeline_empresa_01_incremental',
    default_args=default_args,
    schedule_interval='0 6,12,18,23 * * *',  # 6h, 12h, 18h, 23h
    catchup=False
)

task_incremental = BashOperator(
    task_id='extract_transform_incremental',
    bash_command='cd /path/to/project && python main_pipeline_01_optimized.py --incremental',
    dag=dag_incremental
)
```

---

### PASSO 5: Monitorar Primeira Semana

#### Checklist Diário:
- [ ] Verificar tempo de execução (incremental ~20min)
- [ ] Validar contagens no Power BI
- [ ] Conferir arquivo de estado
- [ ] Revisar logs de erro

#### Checklist Semanal:
- [ ] Verificar execução FULL (domingo)
- [ ] Validar limpeza de órfãos
- [ ] Comparar contagens com API
- [ ] Documentar tempos reais

---

## 🔍 VALIDAÇÕES CRÍTICAS

### 1. Validar Contagens (Modo FULL)
```sql
-- Executar ANTES e DEPOIS do pipeline otimizado
-- Contagens devem ser IDÊNTICAS

-- Contatos
SELECT empresa_id, COUNT(*) 
FROM raw.contatos_raw 
WHERE empresa_id = 1
GROUP BY empresa_id;

-- Produtos
SELECT empresa_id, COUNT(*) 
FROM raw.produtos_raw 
WHERE empresa_id = 1
GROUP BY empresa_id;

-- Vendas
SELECT empresa_id, COUNT(*) 
FROM raw.vendas_raw 
WHERE empresa_id = 1
GROUP BY empresa_id;
```

### 2. Validar Limpeza de Órfãos
```sql
-- Verificar se órfãos foram removidos (apenas em modo FULL)
-- Executar ANTES do pipeline
SELECT COUNT(*) as total_antes FROM raw.contatos_raw WHERE empresa_id = 1;

-- Executar pipeline FULL
-- python main_pipeline_01_optimized.py --full

-- Executar DEPOIS do pipeline
SELECT COUNT(*) as total_depois FROM raw.contatos_raw WHERE empresa_id = 1;

-- Se total_depois < total_antes → Órfãos foram removidos ✅
-- Verificar logs para ver quais IDs foram removidos
```

### 3. Validar Modo INCREMENTAL
```sql
-- Verificar que dados históricos foram preservados
-- Executar ANTES do incremental
SELECT MIN(data_ingestao), MAX(data_ingestao), COUNT(*)
FROM raw.contatos_raw 
WHERE empresa_id = 1;

-- Executar pipeline INCREMENTAL
-- python main_pipeline_01_optimized.py --incremental

-- Executar DEPOIS do incremental
SELECT MIN(data_ingestao), MAX(data_ingestao), COUNT(*)
FROM raw.contatos_raw 
WHERE empresa_id = 1;

-- MIN(data_ingestao) deve ser IGUAL (dados antigos preservados) ✅
-- MAX(data_ingestao) deve ser ATUAL (novos dados adicionados) ✅
-- COUNT(*) deve ser >= anterior (apenas INSERT/UPDATE) ✅
```

---

## 🛡️ PLANO DE ROLLBACK

### Se algo der errado:

#### Opção 1: Voltar ao Pipeline Original
```bash
# Usar pipeline original (sem otimizações)
python main_pipeline_01.py

# Deletar arquivo de estado
rm .extraction_state_empresa_01.json
```

#### Opção 2: Forçar Modo FULL
```bash
# Executar FULL para resetar tudo
python main_pipeline_01_optimized.py --full

# Verificar integridade
python -c "
from config.database import Session
from sqlalchemy import text

session = Session()
result = session.execute(text('SELECT COUNT(*) FROM raw.contatos_raw WHERE empresa_id = 1'))
print(f'Total contatos: {result.scalar()}')
session.close()
"
```

#### Opção 3: Restaurar Backup
```bash
# Se tiver backup do banco
pg_restore -d seu_banco backup_antes_otimizacao.dump
```

---

## 📊 MÉTRICAS DE SUCESSO

### Semana 1 (Validação)
- [ ] Modo FULL executado com sucesso
- [ ] Modo INCREMENTAL executado 4x/dia
- [ ] Tempo total/dia < 2h
- [ ] Zero perda de dados
- [ ] Power BI atualizado corretamente

### Semana 2-4 (Estabilização)
- [ ] Modo automático funcionando
- [ ] Tempo médio incremental < 30min
- [ ] Modo FULL semanal executando
- [ ] Limpeza de órfãos funcionando
- [ ] Equipe confortável com novo sistema

### Mês 1+ (Produção)
- [ ] Sistema rodando 100% automático
- [ ] Economia de 80%+ confirmada
- [ ] Zero incidentes de perda de dados
- [ ] Documentação atualizada
- [ ] Outras empresas migradas

---

## 🎯 PRÓXIMAS EMPRESAS

### Empresa 2
```bash
# Copiar pipeline otimizado
cp main_pipeline_01_optimized.py main_pipeline_02_optimized.py

# Editar EMPRESA_ID
sed -i 's/EMPRESA_ID = 1/EMPRESA_ID = 2/g' main_pipeline_02_optimized.py

# Testar
python main_pipeline_02_optimized.py --full
python main_pipeline_02_optimized.py --incremental
```

### Empresas 3-6
Repetir processo acima para cada empresa.

### Pipeline Completo (Todas as Empresas)
```bash
# Criar versão otimizada do pipeline completo
# main_pipeline_complete_optimized.py

# Executar todas as empresas em modo coordenado:
# - Empresa 1: FULL (se necessário)
# - Empresas 2-6: INCREMENTAL
# - Rodízio de FULL entre empresas
```

---

## 📞 SUPORTE E DÚVIDAS

### Logs
```bash
# Ver logs em tempo real
tail -f logs/empresa_01_*.log

# Buscar erros
grep -i "erro\|error\|falha" logs/empresa_01_*.log

# Ver últimas execuções
ls -lht logs/ | head -10
```

### Estado do Sistema
```bash
# Ver arquivo de estado
cat .extraction_state_empresa_01.json | python -m json.tool

# Ver próxima execução FULL
python -c "
from config.extraction_mode import ExtractionModeManager
manager = ExtractionModeManager(1)
status = manager.get_status_report()
print(f'Próximo FULL: {status[\"next_full_recommended\"]}')
print(f'Modo recomendado: {status[\"mode_recommended\"]}')
"
```

### Validação Rápida
```bash
# Script de validação
python -c "
from config.database import Session
from sqlalchemy import text

session = Session()

# Contagens por empresa
for empresa_id in [1, 2, 3, 4, 5, 6]:
    contatos = session.execute(text(f'SELECT COUNT(*) FROM raw.contatos_raw WHERE empresa_id = {empresa_id}')).scalar()
    produtos = session.execute(text(f'SELECT COUNT(*) FROM raw.produtos_raw WHERE empresa_id = {empresa_id}')).scalar()
    vendas = session.execute(text(f'SELECT COUNT(*) FROM raw.vendas_raw WHERE empresa_id = {empresa_id}')).scalar()
    
    print(f'Empresa {empresa_id}:')
    print(f'  Contatos: {contatos}')
    print(f'  Produtos: {produtos}')
    print(f'  Vendas: {vendas}')
    print()

session.close()
"
```

---

## ✅ CHECKLIST FINAL

### Antes de Ir para Produção
- [ ] Testado modo FULL (sucesso)
- [ ] Testado modo INCREMENTAL (sucesso)
- [ ] Testado modo automático (sucesso)
- [ ] Validadas contagens (idênticas)
- [ ] Validada limpeza de órfãos (funciona)
- [ ] Validada preservação de dados (100%)
- [ ] Documentação lida e compreendida
- [ ] Equipe treinada
- [ ] Plano de rollback definido
- [ ] Monitoramento configurado
- [ ] Backup realizado

### Após 1 Semana em Produção
- [ ] Tempo médio incremental < 30min
- [ ] Modo FULL executado com sucesso
- [ ] Zero perda de dados
- [ ] Power BI atualizado corretamente
- [ ] Equipe satisfeita
- [ ] Métricas documentadas
- [ ] Próximas empresas planejadas

---

## 🎉 SUCESSO!

Se todos os checkpoints acima foram atingidos:

**PARABÉNS! Você reduziu o tempo de processamento em 83%!**

- ✅ De 10h/dia para 1h40/dia
- ✅ 4 atualizações/dia agora é viável
- ✅ 100% de segurança mantida
- ✅ Zero perda de dados
- ✅ Sistema escalável para outras empresas

**Próximos passos:**
1. Migrar outras empresas
2. Otimizar pipeline completo
3. Implementar monitoramento avançado
4. Documentar lições aprendidas
