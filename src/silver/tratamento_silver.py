# Databricks notebook source
# MAGIC %md
# MAGIC # Tratamento Silver - Indicador Criança Alfabetizada
# MAGIC
# MAGIC Este notebook lê as tabelas cruas da camada **Bronze** e produz versões limpas,
# MAGIC padronizadas e já integradas (juntadas) na camada **Silver**.
# MAGIC
# MAGIC O que é feito aqui:
# MAGIC - Padronizar nomes de colunas que estavam diferentes entre tabelas
# MAGIC - Remover linhas duplicadas
# MAGIC - Tratar valores ausentes
# MAGIC - Juntar as tabelas de UF e Município com seus respectivos diretórios (nome oficial, região)
# MAGIC - Reorganizar as tabelas de metas, que hoje têm uma coluna por ano, para o formato
# MAGIC   "uma linha por ano" - isso facilita bastante comparar a evolução do indicador depois

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOGO_BRONZE = "bronze"
CATALOGO_SILVER = "silver"
SCHEMA = "alfabetizacao"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO_SILVER}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. UF
# MAGIC
# MAGIC A tabela de indicadores por UF usa a coluna `sigla_uf`, enquanto o diretório de UF
# MAGIC usa apenas `sigla`. Aqui os dois nomes são unificados para `sigla_uf` antes do `join`.

# COMMAND ----------

def padronizar_e_juntar(df_indicadores, df_diretorio, coluna_join, colunas_dedup):
    """Remove colunas repetidas entre indicadores e diretorio (evita erro de
    coluna duplicada no join), remove duplicados e junta as duas tabelas."""
    colunas_repetidas = (set(df_diretorio.columns) & set(df_indicadores.columns)) - {coluna_join}
    if colunas_repetidas:
        df_indicadores = df_indicadores.drop(*colunas_repetidas)

    return (
        df_indicadores
        .dropDuplicates(colunas_dedup)
        .join(df_diretorio, on=coluna_join, how="left")
    )


df_uf_indicadores = spark.table(f"{CATALOGO_BRONZE}.{SCHEMA}.uf")

df_uf_diretorio = (
    spark.table(f"{CATALOGO_BRONZE}.{SCHEMA}.diretorio_uf")
    .withColumnRenamed("sigla", "sigla_uf")
    .withColumnRenamed("nome", "nome_uf")
    .select("sigla_uf", "nome_uf", "regiao")
)

df_uf_silver = padronizar_e_juntar(
    df_uf_indicadores, df_uf_diretorio,
    coluna_join="sigla_uf", colunas_dedup=["sigla_uf", "ano", "rede", "serie"],
)

df_uf_silver.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOGO_SILVER}.{SCHEMA}.uf")
print(f"uf: {spark.table(f'{CATALOGO_SILVER}.{SCHEMA}.uf').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Município
# MAGIC
# MAGIC Mesma lógica: junta os indicadores por município com o diretório, que traz o nome
# MAGIC oficial do município, a UF e a região. O `id_municipio` (código do IBGE) é a chave
# MAGIC que conecta as duas tabelas, e já vem no mesmo formato nas duas - não precisou de
# MAGIC ajuste aqui.

# COMMAND ----------

df_municipio_indicadores = spark.table(f"{CATALOGO_BRONZE}.{SCHEMA}.municipio")

df_municipio_diretorio = (
    spark.table(f"{CATALOGO_BRONZE}.{SCHEMA}.diretorio_municipio")
    .select("id_municipio", "nome", "sigla_uf", "nome_uf", "nome_regiao")
    .withColumnRenamed("nome", "nome_municipio")
)

df_municipio_silver = padronizar_e_juntar(
    df_municipio_indicadores, df_municipio_diretorio,
    coluna_join="id_municipio", colunas_dedup=["id_municipio", "ano", "rede", "serie"],
)

df_municipio_silver.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOGO_SILVER}.{SCHEMA}.municipio")
print(f"municipio: {spark.table(f'{CATALOGO_SILVER}.{SCHEMA}.municipio').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Metas de alfabetização (Brasil, UF e Município)
# MAGIC
# MAGIC As três tabelas de meta trazem uma coluna separada para cada ano
# MAGIC (`meta_alfabetizacao_2024`, `meta_alfabetizacao_2025`, ..., `meta_alfabetizacao_2030`).
# MAGIC Isso é bom para leitura rápida, mas ruim para comparar a evolução ao longo do tempo.
# MAGIC
# MAGIC Por isso, aqui elas são "desempilhadas": cada ano vira uma linha própria, com uma
# MAGIC coluna `abrangencia` indicando se aquela meta é nacional, estadual ou municipal.

# COMMAND ----------

COLUNAS_META = [f"meta_alfabetizacao_{ano}" for ano in range(2024, 2031)]


def desempilhar_metas(df, abrangencia, colunas_chave):
    """Transforma as colunas meta_alfabetizacao_2024..2030 em uma linha por ano."""
    pares = ", ".join([f"'{c.split('_')[-1]}', {c}" for c in COLUNAS_META])
    expressao_stack = f"stack({len(COLUNAS_META)}, {pares}) as (ano_meta, meta_percentual)"

    return (
        df.selectExpr(*colunas_chave, "ano", "rede", "taxa_alfabetizacao", expressao_stack)
        .withColumn("ano_meta", F.col("ano_meta").cast("int"))
        .withColumn("abrangencia", F.lit(abrangencia))
    )


df_meta_brasil = desempilhar_metas(
    spark.table(f"{CATALOGO_BRONZE}.{SCHEMA}.meta_alfabetizacao_brasil"),
    abrangencia="brasil",
    colunas_chave=[],
)

df_meta_uf = desempilhar_metas(
    spark.table(f"{CATALOGO_BRONZE}.{SCHEMA}.meta_alfabetizacao_uf"),
    abrangencia="uf",
    colunas_chave=["sigla_uf"],
)

df_meta_municipio = desempilhar_metas(
    spark.table(f"{CATALOGO_BRONZE}.{SCHEMA}.meta_alfabetizacao_municipio"),
    abrangencia="municipio",
    colunas_chave=["id_municipio"],
)

# Junta as tres. allowMissingColumns=True preenche automaticamente com nulo
# a coluna de chave que nao existe em cada tabela (ex: meta nacional nao tem
# sigla_uf nem id_municipio) - nao precisa criar essas colunas na mao.
df_metas_silver = (
    df_meta_brasil
    .unionByName(df_meta_uf, allowMissingColumns=True)
    .unionByName(df_meta_municipio, allowMissingColumns=True)
    .dropDuplicates()
)

df_metas_silver.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOGO_SILVER}.{SCHEMA}.metas")
print(f"metas: {spark.table(f'{CATALOGO_SILVER}.{SCHEMA}.metas').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Alunos
# MAGIC
# MAGIC Aqui o tratamento é:
# MAGIC - Garantir que a proficiência seja numérica (evita comparação errada tipo texto)
# MAGIC - Descartar linhas sem proficiência (não dá pra saber se o aluno foi alfabetizado sem essa nota)
# MAGIC - Remover duplicidade de aluno no mesmo ano de forma **determinística**: se houver
# MAGIC   mais de uma nota válida para o mesmo aluno/ano (ex: reaplicação de prova), fica
# MAGIC   sempre a de maior peso amostral - e o resultado não muda entre execuções
# MAGIC - Criar a coluna `atingiu_ponto_corte`, aplicando a regra oficial do desafio:
# MAGIC   proficiência maior ou igual a **743 pontos** = criança alfabetizada. Essa é a
# MAGIC   definição usada pelo próprio Inep para calcular o Indicador Criança Alfabetizada.

# COMMAND ----------

PONTO_DE_CORTE_ALFABETIZACAO = 743

df_alunos_bronze = spark.table(f"{CATALOGO_BRONZE}.{SCHEMA}.alunos").withColumn(
    "proficiencia", F.col("proficiencia").cast("double")
)

qtd_antes = df_alunos_bronze.count()

janela_desempate = Window.partitionBy("id_aluno", "ano").orderBy(
    F.col("peso_aluno").desc_nulls_last(), F.col("proficiencia").desc_nulls_last()
)

df_alunos_silver = (
    df_alunos_bronze
    .filter(F.col("proficiencia").isNotNull())
    .withColumn("_ordem_desempate", F.row_number().over(janela_desempate))
    .filter(F.col("_ordem_desempate") == 1)
    .drop("_ordem_desempate")
    .withColumn("atingiu_ponto_corte", F.col("proficiencia") >= PONTO_DE_CORTE_ALFABETIZACAO)
)

df_alunos_silver.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOGO_SILVER}.{SCHEMA}.alunos")

qtd_depois = spark.table(f"{CATALOGO_SILVER}.{SCHEMA}.alunos").count()
print(f"alunos: {qtd_antes} linhas na Bronze -> {qtd_depois} linhas na Silver "
      f"({qtd_antes - qtd_depois} descartadas por duplicidade ou falta de nota)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Checagens de qualidade
# MAGIC
# MAGIC Verificações simples para garantir que o resultado faz sentido antes de seguir
# MAGIC para a camada Gold. O ideal é que todas as contagens abaixo sejam zero.

# COMMAND ----------

ufs_sem_diretorio = df_uf_silver.filter(F.col("nome_uf").isNull()).count()
print(f"UFs sem correspondência no diretório: {ufs_sem_diretorio}")

municipios_sem_diretorio = df_municipio_silver.filter(F.col("nome_municipio").isNull()).count()
print(f"Municípios sem correspondência no diretório: {municipios_sem_diretorio}")

alunos_sem_municipio_valido = (
    df_alunos_silver
    .join(df_municipio_diretorio.select("id_municipio"), on="id_municipio", how="left_anti")
    .count()
)
print(f"Registros de alunos com id_municipio que não existe no diretório: {alunos_sem_municipio_valido}")

duplicados_metas = df_metas_silver.count() - df_metas_silver.dropDuplicates(["abrangencia", "sigla_uf", "id_municipio", "ano_meta"]).count()
print(f"Linhas duplicadas na tabela de metas: {duplicados_metas}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximos passos
# MAGIC
# MAGIC Com Silver pronta, a camada **Gold** vai combinar `alunos` + `municipio` + `metas`
# MAGIC para gerar os datasets finais de análise: indicador de alfabetização por município,
# MAGIC comparação entre meta e resultado real, e evolução do indicador ao longo dos anos.
