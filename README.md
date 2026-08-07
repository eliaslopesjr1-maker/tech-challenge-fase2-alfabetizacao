# Tech Challenge Fase 2 - Pipeline de Dados sobre Alfabetização no Brasil

## 1. Contexto do problema

A alfabetização das crianças até o final do 2º ano do ensino fundamental é uma meta nacional do governo brasileiro (Compromisso Nacional Criança Alfabetizada), com objetivo de que todas as crianças estejam alfabetizadas até 2030.

Para medir isso, o INEP criou o **Indicador Criança Alfabetizada**, baseado em uma nota de corte de 743 pontos na escala do Saeb. Esse indicador mostra o percentual de crianças que atingiram o nível esperado de leitura e escrita.

O problema é que esse indicador sozinho não conta toda a história. Para entender por que algumas regiões têm resultado melhor que outras, é preciso cruzar esse indicador com outras informações, como dados de município, metas por estado e dados de desempenho dos alunos.

## 2. O desafio

Este projeto simula o trabalho de um time de engenharia de dados que precisa juntar várias fontes de dados públicas sobre alfabetização, organizá-las e deixá-las prontas para análise, seguindo boas práticas de mercado.

## 3. Arquitetura da solução

O projeto segue a **Arquitetura Medalhão**, um padrão comum em engenharia de dados que organiza a informação em três camadas, cada uma mais "limpa" que a anterior:

- **Bronze**: dados exatamente como vieram da fonte, sem alteração. Serve como cópia de segurança e histórico.
- **Silver**: dados limpos, com nomes e tipos padronizados, sem duplicidade e já cruzados entre as tabelas.
- **Gold**: dados finais, prontos para gráficos, dashboards e modelos de machine learning.

### Origem dos dados

Os dados vêm da plataforma [Base dos Dados](https://basedosdados.org/), que disponibiliza os dados do INEP no Google BigQuery, dentro do conjunto `br_inep_avaliacao_alfabetizacao`.

| Tabela do desafio | Nome técnico | Tamanho aproximado |
|---|---|---|
| UF | `uf` | 145 linhas |
| Meta Alfabetização Brasil | `meta_alfabetizacao_brasil` | 3 linhas |
| Meta Alfabetização por UF | `meta_alfabetizacao_uf` | 81 linhas |
| Meta Alfabetização por Município | `meta_alfabetizacao_municipio` | ~10.700 linhas |
| Município | `municipio` | ~24.000 linhas |
| Dados de alunos | `alunos` | ~3,9 milhões de linhas |

Além dessas, o conjunto `br_bd_diretorios_brasil` fornece tabelas de referência (`uf` e `municipio`) com nome oficial, região e outras informações de cadastro, usadas para enriquecer os dados na camada Silver.

**Como os dados são obtidos:**
- As tabelas menores (metas, município, UF) podem ser baixadas diretamente pelo site da Base dos Dados.
- A tabela `alunos`, por ser grande, é acessada de forma gratuita via Google BigQuery (é necessário um projeto gratuito no Google Cloud) ou pelo pacote Python `basedosdados`.

**Atenção ao integrar as tabelas**: a coluna que identifica o estado tem nomes diferentes dependendo da tabela (`sigla_uf` nas tabelas de alfabetização, `sigla` no diretório de UF). Essa padronização de nomes é feita na camada Silver, antes de juntar as tabelas.

### Ingestão híbrida (batch + streaming)

- **Batch**: usado para dados que mudam pouco, como metas e dados de município. É carregado em blocos, de tempos em tempos.
- **Streaming**: usado para simular atualizações que chegam aos poucos, como novas medições de desempenho ou atualização de metas.

### Diagrama da pipeline

> TODO: adicionar o diagrama em `docs/architecture/`

### Fluxo de dados

> TODO: descrever o caminho do dado, passo a passo, desde a fonte até a camada Gold

## 4. Tecnologias utilizadas

| Ferramenta | Uso no projeto | Por que foi escolhida |
|---|---|---|
| Databricks | Processamento e armazenamento das camadas Bronze/Silver/Gold | > TODO |
| Azure | Nuvem onde o ambiente Databricks está hospedado | > TODO |
| Delta Lake | Formato de armazenamento das tabelas | > TODO |

> TODO: completar a tabela conforme as ferramentas forem definidas

## 5. Decisões arquiteturais

> TODO: explicar as escolhas feitas e as alternativas descartadas, por exemplo:
> - Por que batch para uma fonte e streaming para outra
> - Por que usar data lake em vez de data warehouse (ou o contrário)
> - Trade-off entre custo e performance

## 6. Qualidade de dados

A pipeline inclui verificações automáticas para garantir a confiabilidade dos dados, como:

- Checagem de linhas duplicadas
- Checagem de valores ausentes (nulos)
- Validação de chaves entre tabelas (ex: código do município existe na tabela de municípios)
- Consistência entre as camadas

Os scripts de validação ficam na pasta [`quality/`](quality/).

## 7. Monitoramento e controle de custos (FinOps)

> TODO: explicar como o pipeline é monitorado (falhas, volume processado, alertas) e quais decisões foram tomadas para reduzir custo (ex: formato Parquet/Delta, particionamento de tabelas, desligamento automático de cluster)

## 8. Aplicação em Inteligência Artificial

A camada Gold, com os dados já limpos e organizados, pode ser usada como base para:

- Modelos que tentam prever o risco de um município não atingir a meta de alfabetização
- Análises que mostram desigualdades educacionais entre regiões
- Apoio a decisões de política pública baseadas em dados reais

## 9. Estrutura do repositório

```
├── src/
│   ├── bronze/     # ingestão dos dados brutos
│   ├── silver/     # limpeza, padronização e integração das tabelas
│   └── gold/       # dados finais para análise
├── quality/        # scripts de validação de qualidade dos dados
├── docs/
│   └── architecture/  # diagrama e documentação da arquitetura
└── notebooks/      # notebooks de exploração dos dados
```

## 10. Como o Git foi usado neste projeto

- A branch `main` contém sempre a versão estável do projeto
- Novas funcionalidades são feitas em branches separadas (ex: `feature/ingestao-bronze`)
- Mudanças são integradas à `main` por Pull Request, com descrição do que foi feito
