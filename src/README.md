# 🔎 BusinessRadar

### Engenharia de dados aplicada ao cenário empresarial brasileiro

O **BusinessRadar** é um projeto de Engenharia de Dados que transforma bases públicas sobre empresas brasileiras em um **modelo analítico estruturado**, utilizando **PySpark, Databricks, Delta Lake e arquitetura Medallion (Bronze → Silver → Gold)**.

O projeto integra informações cadastrais de empresas e estabelecimentos, atividades econômicas (CNAEs), enquadramento no Simples Nacional/MEI e referências geográficas. O objetivo é construir uma base confiável para análises sobre a distribuição e as características do ambiente empresarial brasileiro.

**Escopo atual:** pipeline batch implementado no Databricks, com tabelas Delta no Unity Catalog. Orquestração automática, dashboards e CI/CD estão no roteiro de evolução — não fazem parte desta versão.

---

## 🛠️ Tecnologias

- **Python e PySpark:** leitura, validação, transformação e integração de dados.
- **Delta Lake:** persistência das camadas Bronze, Silver e Gold.
- **Unity Catalog:** organização e acesso às tabelas no catálogo `workspace.businessradar`.
- **Git e GitHub:** versionamento do código-fonte.

## 🏗️ Arquitetura

O fluxo segue a arquitetura **Bronze → Silver → Gold**, desde a ingestão de dados públicos até a disponibilização de tabelas analíticas.

### 🥉 Bronze — Ingestão

Leitura dos arquivos de origem com controle de **schema, delimitadores, codificação e rastreabilidade** (`arquivo_origem`). As fontes incluem:

- Dados públicos de **empresas e estabelecimentos** da Receita Federal;
- Classificação Nacional de Atividades Econômicas (**CNAEs**);
- Opção pelo **Simples Nacional e MEI**;
- Correspondência municipal **TOM–IBGE**;
- Base de **população estimada por município**.

### 🥈 Silver — Qualidade e integração

Transformações reutilizáveis em PySpark para:

- Corrigir problemas de leitura em CSVs, incluindo campos com aspas e barras invertidas;
- Padronizar textos e tratar marcadores de ausência de informação;
- Converter datas e valores monetários/populacionais para tipos adequados;
- Deduplicar empresas utilizando **Window Functions**;
- Construir identificadores como **CNPJ completo** e **código IBGE**;
- Integrar e normalizar códigos municipais **TOM–IBGE**, com tratamento documentado de exceções.

### 🥇 Gold — Modelo analítico

| Tabela | Granularidade | Conteúdo principal |
|---|---|---|
| `fato_estabelecimentos` | 1 linha por CNPJ completo | Localização, CNAE, datas, situação cadastral e indicadores de atividade/matriz |
| `dim_empresas` | 1 linha por CNPJ básico | Dados da empresa, capital social, porte e informações de Simples/MEI |
| `dim_cnaes` | 1 linha por código CNAE | Código e descrição da atividade econômica |
| `dim_localizacao` | 1 linha por código municipal da Receita | Município, UF e correspondência com código IBGE |

**Relacionamentos principais:**

- `fato_estabelecimentos.cnpj_basico` → `dim_empresas.cnpj_basico`
- `fato_estabelecimentos.cnae_fiscal_principal` → `dim_cnaes.codigo_cnae`
- `fato_estabelecimentos.municipio` → `dim_localizacao.codigo_municipio`
- `dim_localizacao.codigo_ibge` → `silver_populacao.codigo_ibge` (para análises demográficas futuras)

A população **não é duplicada na fato**: sua fonte permanece separada e pode ser utilizada quando houver necessidade de indicadores per capita.

## 📊 Escala e validações

Contagens verificadas durante o desenvolvimento desta versão:

| Conjunto | Registros |
|---|---:|
| Fato de estabelecimentos | **73.809.379** |
| Dimensão de empresas | **70.517.437** |
| Base do Simples Nacional/MEI | **50.806.102** |
| Dimensão CNAEs | **1.359** |
| Dimensão de localização | **5.572** |

As validações realizadas incluíram **unicidade das chaves**, preservação da granularidade após relacionamentos, consistência de contagens, tipos das colunas e tratamento de valores especiais.

Os totais refletem o conjunto de dados utilizado nesta execução, não uma contagem atualizada em tempo real.

## 📁 Estrutura do repositório

`src/bronze/` — Notebooks de ingestão dos arquivos.

`src/silver/` — Transformações e padronizações em PySpark.

`src/gold/` — Fato e dimensões analíticas.

Os notebooks de exploração das fontes são utilizados no desenvolvimento, mas **não fazem parte do código publicado** neste repositório.

## ▶️ Como reproduzir

1. Prepare um workspace **Databricks** com acesso ao Unity Catalog e ao schema `workspace.businessradar` (ou adapte os caminhos no código).
2. Disponibilize os arquivos de origem no Volume configurado para a ingestão e revise os caminhos usados nos notebooks.
3. Execute os notebooks em `src/bronze/` para criar as tabelas de origem.
4. Execute as transformações em `src/silver/`, respeitando as dependências entre as fontes.
5. Execute os scripts em `src/gold/` para materializar a fato e as dimensões.
6. Confira as contagens e as chaves das tabelas geradas no Unity Catalog.

**Nota:** os dados brutos e as tabelas Delta **não estão armazenados no GitHub**. O repositório contém o código da solução; a reprodução depende das fontes de dados e de um ambiente Databricks configurado.

## 🧩 Desafios técnicos abordados

- Tratamento de arquivos CSV de grande volume, com particularidades de delimitador, `quote`, `escape` e `ISO-8859-1`;
- Processamento distribuído de dezenas de milhões de registros;
- Conversões de schema, deduplicação e preservação de identificadores com zeros à esquerda;
- Definição de **granularidade** e controle de cardinalidade em relacionamentos;
- Resolução de divergências entre códigos municipais da Receita e do IBGE;
- Persistência e organização de camadas analíticas em Delta Lake.

## 🚀 Próximas evoluções

- Orquestração dos pipelines com **Apache Airflow**;
- Consultas analíticas, indicadores agregados e visualizações para consumo dos dados.

---

**Projeto desenvolvido como prática de Engenharia de Dados, com foco em processamento distribuído, qualidade de dados e modelagem analítica.**