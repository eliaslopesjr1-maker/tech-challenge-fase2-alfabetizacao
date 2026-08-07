# Decisões técnicas do projeto

Este arquivo registra as principais decisões tomadas durante o desenvolvimento, com o motivo de cada uma. Serve como referência para a documentação final e para a apresentação em vídeo.

## Nuvem e ferramentas

**Decisão**: usar Databricks rodando sobre Azure.

**Por quê**: o Databricks já oferece de forma nativa os três pilares que o desafio pede - armazenamento em formato Delta (bom para a Arquitetura Medalhão), processamento em lote e em streaming na mesma plataforma (Spark Structured Streaming), e um catálogo de dados (Unity Catalog) para organizar as camadas Bronze, Silver e Gold com controle de acesso.

## Fonte de dados

**Decisão**: os dados vêm da plataforma Base dos Dados, que disponibiliza as tabelas do INEP no Google BigQuery.

**Tabelas usadas** (dataset `br_inep_avaliacao_alfabetizacao` no BigQuery):
- `uf`, `municipio`, `meta_alfabetizacao_brasil`, `meta_alfabetizacao_uf`, `meta_alfabetizacao_municipio`, `alunos`

**Tabelas complementares** (dataset `br_bd_diretorios_brasil`), usadas para enriquecer os dados com nome oficial de município/UF:
- `uf`, `municipio`

**Como é acessado**: pacote Python `basedosdados`, que consulta o BigQuery. As tabelas pequenas (metas, município, UF) também podem ser baixadas direto pelo site, sem custo. A tabela `alunos`, por ser grande (quase 4 milhões de linhas), exige um projeto gratuito no Google Cloud para ser consultada via BigQuery.

**Ponto de atenção identificado**: a coluna que representa a sigla do estado tem nomes diferentes entre as tabelas (`sigla_uf` nas tabelas de alfabetização, `sigla` no diretório de UF). Essa diferença precisa ser corrigida na camada Silver antes de juntar as tabelas.

## Estrutura do repositório

**Decisão**: seguir a Arquitetura Medalhão (Bronze / Silver / Gold), com uma pasta em `src/` para cada camada, mais uma pasta `quality/` separada para os scripts de validação de dados.

**Por quê**: separar por camada deixa claro em que estágio de tratamento cada parte do código atua, e facilita explicar a arquitetura tanto no README quanto no vídeo.

## Fluxo de trabalho no Git

**Decisão**: nenhum código de pipeline vai direto para a `main`. O fluxo é: criar uma branch por funcionalidade → desenvolver e commitar ali → abrir um Pull Request explicando o que foi feito → mesclar na `main`.

**Por quê**: é exatamente o que o desafio pede como evidência de boas práticas de Git, e também é como equipes reais trabalham - a `main` fica sempre com uma versão que funciona.

**Já executado**: a ingestão da camada Bronze (notebook `src/bronze/ingestao_bronze.py`) seguiu esse fluxo completo - branch `feature/ingestao-bronze` → commit → push → Pull Request #1 → merge na `main`.

## Ingestão da camada Bronze

**Decisão**: um único notebook baixa todas as tabelas da Base dos Dados e grava cada uma como uma tabela Delta separada, sem nenhuma transformação, adicionando duas colunas de controle: `_ingerido_em` (data/hora da ingestão) e `_tabela_origem` (de qual tabela original aquele dado veio).

**Por quê**: manter os dados brutos sem alteração é o princípio da camada Bronze - se algo der errado mais na frente, sempre dá para voltar aos dados originais. As colunas de controle ajudam a rastrear e também servem de base para o monitoramento da pipeline (ex: saber quando cada tabela foi atualizada pela última vez).

## Decisões ainda em aberto

- Como simular a parte de **streaming** (o desafio pede ingestão híbrida batch + streaming)
- Regras específicas de limpeza e validação da camada **Silver**
- Formato final dos datasets da camada **Gold**
- Estratégia de monitoramento e como isso vai ser demonstrado
- Como a otimização de custos (FinOps) será aplicada e documentada
