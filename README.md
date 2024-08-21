# Análise de Dados COVID-19 no Databricks

## Visão Geral

Este projeto utiliza o Azure Databricks para processar e analisar dados relacionados ao COVID-19. A análise é realizada em um ambiente de Big Data, aproveitando a escalabilidade e as capacidades de processamento do Databricks e do Azure Blob Storage.

## Estrutura do Projeto

O projeto é dividido em três camadas principais de dados, cada uma representando um estágio no pipeline de dados:

1. **Camada Bronze:**
   - Contém dados brutos e não processados provenientes de fontes externas. Os dados são lidos diretamente de arquivos CSV armazenados no Azure Blob Storage.

2. **Camada Silver:**
   - Dados da camada Bronze são limpos e transformados. Nesta camada, os dados são filtrados para remover registros incompletos e convertidos para o formato Delta para otimizar o desempenho nas operações subsequentes.

3. **Camada Gold:**
   - Dados da camada Silver são enriquecidos e refinados. Nesta camada, são realizadas transformações adicionais, como conversão de tipos de dados e adição de colunas derivadas. Os dados são então armazenados em formato Delta, particionados por ano e mês, para análise agregada e relatórios.

## Visualização de Dados

Após o processamento e armazenamento dos dados nas camadas Gold e agregada, os dados foram visualizados usando o Power BI. O Power BI foi utilizado para criar dashboards e relatórios interativos que permitem a análise visual dos dados COVID-19, oferecendo insights valiosos sobre a evolução da pandemia.

## Objetivos

- **Montagem de Containers:** Montar diferentes containers do Azure Blob Storage para acessar dados em três camadas distintas.
- **Criação de Banco de Dados:** Configurar um banco de dados no Spark SQL para armazenar e gerenciar os dados.
- **Processamento e Transformação de Dados:** Limpar, transformar e armazenar dados em formatos otimizados para análise.
- **Criação de Tabelas Agregadas:** Gerar tabelas agregadas para facilitar a análise e visualização dos dados.
- **Visualização de Dados:** Criar e analisar dashboards interativos no Power BI com os dados processados.

## Dataset

Os dados utilizados neste projeto são provenientes de um conjunto disponível no Kaggle, que pode ser acessado através do seguinte link:
[COVID-19 Open Datasets for Brazil](https://www.kaggle.com/datasets/cprete/covid19-open-datasets-for-brazil?resource=download)

## Requisitos

- **Azure Databricks**: Ambiente para execução do projeto.
- **Azure Blob Storage**: Armazenamento para os dados brutos e processados.
- **Power BI**: Ferramenta de visualização para análise interativa dos dados.
- **Credenciais do Azure**: Chaves de acesso para o Azure Blob Storage.

---

Para mais informações, sinta-se à vontade para entrar em contato.

