# Databricks notebook source
# MAGIC %md
# MAGIC # Simulador de eventos - novas medições de desempenho
# MAGIC
# MAGIC Os dados da Base dos Dados são de uma pesquisa que já aconteceu (histórico), então
# MAGIC não existe um sistema real enviando dados novos o tempo todo. Para atender o
# MAGIC requisito de **ingestão em tempo quase real** do desafio, este notebook simula
# MAGIC uma fonte externa: a cada execução, ele gera um pequeno arquivo com "novas
# MAGIC medições de desempenho" de alunos (como se uma escola tivesse acabado de aplicar
# MAGIC uma prova) e deixa esse arquivo numa pasta de pouso (**landing zone**).
# MAGIC
# MAGIC O próximo notebook (`ingestao_streaming.py`) fica de olho nessa pasta e processa
# MAGIC cada arquivo novo automaticamente - é essa combinação (gerador de eventos +
# MAGIC leitor contínuo) que demonstra o funcionamento de uma ingestão via streaming.

# COMMAND ----------

import json
import random
import time
import uuid
from datetime import datetime, timezone

# COMMAND ----------

dbutils.widgets.text("caminho_landing", "/Volumes/bronze/alfabetizacao/landing_streaming", "Pasta de pouso dos eventos")
dbutils.widgets.text("quantidade_eventos", "5", "Quantos arquivos gerar nesta execução")
dbutils.widgets.text("intervalo_segundos", "5", "Espera entre um evento e outro (segundos)")

caminho_landing = dbutils.widgets.get("caminho_landing")
quantidade_eventos = int(dbutils.widgets.get("quantidade_eventos"))
intervalo_segundos = int(dbutils.widgets.get("intervalo_segundos"))

dbutils.fs.mkdirs(caminho_landing)

# COMMAND ----------

# MAGIC %md
# MAGIC ## De onde vêm os municípios simulados
# MAGIC
# MAGIC Para os eventos parecerem realistas, os códigos de município usados são
# MAGIC sorteados a partir da tabela de município já carregada na Bronze (não são
# MAGIC inventados do zero).

# COMMAND ----------

municipios_disponiveis = [
    linha["id_municipio"]
    for linha in spark.table("bronze.alfabetizacao.municipio").select("id_municipio").distinct().limit(200).collect()
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geração dos eventos
# MAGIC
# MAGIC Cada evento representa a proficiência de um aluno em uma nova avaliação, no
# MAGIC mesmo formato da tabela `alunos` original, mais um campo `timestamp_evento`
# MAGIC (quando o evento "aconteceu"). Um arquivo JSON pequeno é escrito por vez,
# MAGIC com uma pausa entre eles - simulando eventos chegando aos poucos, e não tudo
# MAGIC de uma vez como acontece na ingestão batch.

# COMMAND ----------

def gerar_evento():
    return {
        "id_aluno": str(uuid.uuid4()),
        "id_municipio": random.choice(municipios_disponiveis),
        "ano": 2024,
        "rede": random.choice(["municipal", "estadual", "privada"]),
        "proficiencia": round(random.uniform(400, 950), 1),
        "peso_aluno": round(random.uniform(0.5, 2.0), 2),
        "timestamp_evento": datetime.now(timezone.utc).isoformat(),
    }


for i in range(quantidade_eventos):
    evento = gerar_evento()
    nome_arquivo = f"{caminho_landing}/evento_{evento['timestamp_evento']}_{i}.json"
    dbutils.fs.put(nome_arquivo, json.dumps(evento), overwrite=True)
    print(f"Evento {i + 1}/{quantidade_eventos} gravado: {nome_arquivo}")

    if i < quantidade_eventos - 1:
        time.sleep(intervalo_segundos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Observação sobre uso real
# MAGIC
# MAGIC Em um cenário de produção, este notebook não rodaria manualmente: ele seria
# MAGIC substituído por um sistema real publicando eventos (uma API do INEP, uma fila
# MAGIC de mensagens etc.), ou este mesmo notebook rodaria como um **Job agendado** no
# MAGIC Databricks, chamado periodicamente. Aqui ele serve para demonstrar o formato
# MAGIC dos eventos e permitir testar o notebook de ingestão em streaming.
