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

df_uf_indicadores = spark.table(f"{CATALOGO_BRONZE}.{SCHEMA}.uf")

df_uf_diretorio = (
    spark.table(f"{CATALOGO_BRONZE}.{SCHEMA}.diretorio_uf")
    .withColumnRenamed("sigla", "sigla_uf")
    .withColumnRenamed("nome", "nome_uf")
    .select("sigla_uf", "nome_uf", "regiao")
)

df_uf_silver = (
    df_uf_indicadores
    .dropDuplicates(["sigla_uf", "ano", "rede", "serie"])
    .join(df_uf_diretorio, on="sigla_uf", how="left")
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

# Evita erro de coluna duplicada caso a tabela de indicadores tambem tenha
# sigla_uf/nome_uf (o join usa so id_municipio como chave, entao colunas
# repetidas nos dois lados quebrariam o saveAsTable la na frente)
colunas_repetidas = (set(df_municipio_diretorio.columns) & set(df_municipio_indicadores.columns)) - {"id_municipio"}
if colunas_repetidas:
    df_municipio_indicadores = df_municipio_indicadores.drop(*colunas_repetidas)

df_municipio_silver = (
    df_municipio_indicadores
    .dropDuplicates(["id_municipio", "ano", "rede", "serie"])
    .join(df_municipio_diretorio, on="id_municipio", how="left")
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
# MAGIC - Descartar linhas sem proficiência (não dá pra saber se o aluno foi alfabetizado sem essa nota)
# MAGIC - Remover duplicidade de aluno no mesmo ano (nessa ordem: primeiro tira quem não
# MAGIC   tem nota, depois remove duplicado - assim, se um aluno tiver duas linhas e só uma
# MAGIC   com nota, é a linha com nota que sobra, não a vazia por sorte)
# MAGIC - Criar a coluna `atingiu_ponto_corte`, aplicando a regra oficial do desafio:
# MAGIC   proficiência maior ou igual a **743 pontos** = criança alfabetizada. Essa é a
# MAGIC   definição usada pelo próprio Inep para calcular o Indicador Criança Alfabetizada.

# COMMAND ----------

PONTO_DE_CORTE_ALFABETIZACAO = 743

df_alunos_bronze = spark.table(f"{CATALOGO_BRONZE}.{SCHEMA}.alunos")

qtd_antes = df_alunos_bronze.count()

df_alunos_silver = (
    df_alunos_bronze
    .filter(F.col("proficiencia").isNotNull())
    .dropDuplicates(["id_aluno", "ano"])
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
