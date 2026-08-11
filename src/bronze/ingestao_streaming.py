# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão Streaming - novas medições de desempenho
# MAGIC
# MAGIC Este notebook implementa a parte de **streaming** da pipeline híbrida pedida no
# MAGIC desafio. Ele usa o **Auto Loader** do Databricks (`cloudFiles`), que fica
# MAGIC monitorando uma pasta e processa automaticamente qualquer arquivo novo que
# MAGIC aparecer nela - sem precisar reler os arquivos que já foram processados antes.
# MAGIC
# MAGIC Isso é bem diferente do notebook de ingestão Bronze (batch): lá, toda vez que
# MAGIC roda, ele baixa a tabela inteira de novo. Aqui, cada execução só processa o que
# MAGIC é novidade desde a última vez.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

dbutils.widgets.text("caminho_landing", "/Volumes/bronze/alfabetizacao/landing_streaming", "Pasta de pouso dos eventos")
dbutils.widgets.text("caminho_checkpoint", "/Volumes/bronze/alfabetizacao/checkpoints/streaming_alunos", "Pasta de checkpoint")

caminho_landing = dbutils.widgets.get("caminho_landing")
caminho_checkpoint = dbutils.widgets.get("caminho_checkpoint")

TABELA_DESTINO = "bronze.alfabetizacao.alunos_streaming"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Por que definir o schema na mão
# MAGIC
# MAGIC O Auto Loader consegue descobrir o formato dos dados sozinho (schema inference),
# MAGIC mas isso é mais lento e pode causar surpresa se um arquivo vier com um campo
# MAGIC faltando ou de tipo diferente. Como já sabemos o formato exato dos eventos
# MAGIC (definido no notebook `simulador_eventos_streaming.py`), é mais seguro declarar
# MAGIC o schema aqui.

# COMMAND ----------

schema_eventos = StructType([
    StructField("id_aluno", StringType(), nullable=False),
    StructField("id_municipio", StringType(), nullable=False),
    StructField("ano", IntegerType(), nullable=False),
    StructField("rede", StringType(), nullable=True),
    StructField("proficiencia", DoubleType(), nullable=True),
    StructField("peso_aluno", DoubleType(), nullable=True),
    StructField("timestamp_evento", StringType(), nullable=False),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## O que é o checkpoint
# MAGIC
# MAGIC O `checkpoint` é uma pasta onde o Spark guarda "até onde eu já processei".
# MAGIC Graças a ele, se o notebook parar no meio (erro, cluster desligado) e for
# MAGIC executado de novo, ele continua de onde parou, em vez de reprocessar tudo ou
# MAGIC perder eventos.

# COMMAND ----------

df_streaming = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(schema_eventos)
    .load(caminho_landing)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravação contínua na camada Bronze
# MAGIC
# MAGIC `trigger(availableNow=True)` faz o streaming processar tudo que já chegou até
# MAGIC agora e depois parar sozinho - ideal para rodar este notebook periodicamente
# MAGIC (ex: como um Job agendado a cada poucos minutos). Para deixar rodando de forma
# MAGIC contínua e ao vivo, bastaria trocar por `trigger(processingTime="10 seconds")`.

# COMMAND ----------

query = (
    df_streaming
    .writeStream
    .format("delta")
    .option("checkpointLocation", caminho_checkpoint)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(TABELA_DESTINO)
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoramento básico
# MAGIC
# MAGIC O Spark guarda um histórico de cada "lote" processado pelo streaming - quantas
# MAGIC linhas entraram, quanto tempo levou, etc. Isso é a base do monitoramento da
# MAGIC pipeline pedido no desafio: dá pra saber se a ingestão está funcionando e o
# MAGIC quão rápido os dados estão chegando.

# COMMAND ----------

for lote in query.recentProgress:
    print(
        f"Lote em {lote['timestamp']}: "
        f"{lote['numInputRows']} linha(s) processada(s) "
        f"em {lote['durationMs'].get('triggerExecution', 0)} ms"
    )

total_linhas_streaming = spark.table(TABELA_DESTINO).count()
print(f"Total acumulado na tabela {TABELA_DESTINO}: {total_linhas_streaming} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximos passos
# MAGIC
# MAGIC Os dados que chegam por aqui (`alunos_streaming`) têm exatamente o mesmo
# MAGIC formato da tabela `alunos` (vinda do batch). Na camada Silver, as duas podem
# MAGIC ser unidas (`unionByName`) antes das validações, para que a camada Gold sempre
# MAGIC reflita tanto o histórico quanto as medições mais recentes.
