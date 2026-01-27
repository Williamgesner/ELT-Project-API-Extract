# 📊 Data Business - Tabelas de Referência

Esta pasta contém **tabelas de negócio** que alimentam o Data Warehouse.

## 📋 Arquivos

### `empresas.csv`
- **Descrição:** Cadastro de empresas do projeto
- **Destino:** `processed.dim_empresas`
- **Atualização:** Quando adicionar nova empresa
- **Estrutura:**
```
  empresa_id,cnpj,razao_social
  1,12.345.678/0001-90,Empresa Exemplo LTDA
```

### `metas_empresas.xlsx`
- **Descrição:** Metas mensais de faturamento por empresa
- **Destino:** `processed.metas_mensais`
- **Atualização:** Início de cada mês (ou quando cliente alterar)
- **Estrutura:**
```
  empresa_id | ano  | mes | data_referencia | meta_faturamento
  1          | 2026 | 1   |   2026- 01-01   |363310
```

## 🚀 Como usar

Para subir/atualizar essas tabelas no PostgreSQL:
```bash
python main_empresas.py
python main_target.py

```

## 📝 Adicionar novas tabelas

1. Crie o arquivo (CSV ou Excel) nesta pasta
2. Adicione a lógica em `/main_`
3. Documente aqui