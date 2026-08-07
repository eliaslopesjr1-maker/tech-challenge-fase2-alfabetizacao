# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão Bronze - Indicador Criança Alfabetizada
# MAGIC
# MAGIC Este notebook baixa os dados originais da plataforma **Base dos Dados** e salva
# MAGIC uma cópia "crua" (sem nenhum tratamento) no Databricks. Essa é a camada **Bronze**
# MAGIC da Arquitetura Medalhão: aqui os dados ficam exatamente como vieram da fonte,
# MAGIC servindo como histórico e ponto de partida para as próximas camadas (Silver e Gold).

# COMMAND ----------

# MAGIC %pip install basedosdados

# COMMAND ----------

import basedosdados as bd
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração
# MAGIC
# MAGIC A Base dos Dados guarda os dados no Google BigQuery. Para consultar, é preciso
# MAGIC informar um projeto do Google Cloud (gratuito) que será "cobrado" pela consulta
# MAGIC (o Google dá 1 TB de consulta grátis por mês, o que é mais do que suficiente aqui).
# MAGIC
# MAGIC Substitua o valor abaixo pelo ID do seu projeto no Google Cloud.

# COMMAND ----------

dbutils.widgets.text("billing_project_id", "", "ID do projeto Google Cloud")
billing_project_id = dbutils.widgets.get("billing_project_id")

CATALOGO_BRONZE = "bronze"
SCHEMA_BRONZE = "alfabetizacao"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabelas a ingerir
# MAGIC
# MAGIC Cada item da lista abaixo representa uma tabela original da Base dos Dados:
# MAGIC - `dataset_id` / `table_id`: identificam a tabela no BigQuery
# MAGIC - `nome_bronze`: nome que a tabela vai ter aqui no Databricks

# COMMAND ----------

tabelas_para_ingerir = [
    {"dataset_id": "br_inep_avaliacao_alfabetizacao", "table_id": "uf", "nome_bronze": "uf"},
    {"dataset_id": "br_inep_avaliacao_alfabetizacao", "table_id": "municipio", "nome_bronze": "municipio"},
    {"dataset_id": "br_inep_avaliacao_alfabetizacao", "table_id": "meta_alfabetizacao_brasil", "nome_bronze": "meta_alfabetizacao_brasil"},
    {"dataset_id": "br_inep_avaliacao_alfabetizacao", "table_id": "meta_alfabetizacao_uf", "nome_bronze": "meta_alfabetizacao_uf"},
    {"dataset_id": "br_inep_avaliacao_alfabetizacao", "table_id": "meta_alfabetizacao_municipio", "nome_bronze": "meta_alfabetizacao_municipio"},
    {"dataset_id": "br_inep_avaliacao_alfabetizacao", "table_id": "alunos", "nome_bronze": "alunos"},
    {"dataset_id": "br_bd_diretorios_brasil", "table_id": "uf", "nome_bronze": "diretorio_uf"},
    {"dataset_id": "br_bd_diretorios_brasil", "table_id": "municipio", "nome_bronze": "diretorio_municipio"},
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download e gravação na camada Bronze
# MAGIC
# MAGIC Para cada tabela: baixa os dados da Base dos Dados, adiciona duas colunas de
# MAGIC controle (`_ingerido_em` e `_tabela_origem`) e grava como tabela Delta.
# MAGIC
# MAGIC As colunas de controle ajudam a saber depois de onde e quando cada linha veio -
# MAGIC útil tanto para auditoria quanto para o monitoramento da pipeline.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO_BRONZE}.{SCHEMA_BRONZE}")

for tabela in tabelas_para_ingerir:
    print(f"Baixando {tabela['dataset_id']}.{tabela['table_id']}...")

    df_pandas = bd.read_table(
        dataset_id=tabela["dataset_id"],
        table_id=tabela["table_id"],
        billing_project_id=billing_project_id,
    )

    df_spark = (
        spark.createDataFrame(df_pandas)
        .withColumn("_ingerido_em", current_timestamp())
        .withColumn("_tabela_origem", lit(f"{tabela['dataset_id']}.{tabela['table_id']}"))
    )

    nome_completo = f"{CATALOGO_BRONZE}.{SCHEMA_BRONZE}.{tabela['nome_bronze']}"
    df_spark.write.format("delta").mode("overwrite").saveAsTable(nome_completo)

    print(f"OK: {nome_completo} ({df_spark.count()} linhas)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximos passos
# MAGIC
# MAGIC Com os dados brutos salvos na camada Bronze, o próximo notebook (camada Silver)
# MAGIC vai limpar, padronizar os nomes de colunas (ex: `sigla_uf` vs `sigla`) e juntar
# MAGIC as tabelas relacionadas.
