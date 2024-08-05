# Databricks notebook source
import os

account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')

dbutils.fs.mount(
    source = f"wasbs://bronze@{account_name}.blob.core.windows.net/",
    mount_point = f"/mnt/bronze",
    extra_configs = {f"fs.azure.account.key.{account_name}.blob.core.windows.net": account_key}
)

# COMMAND ----------

#silver

import os

account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')

dbutils.fs.mount(
    source = f"wasbs://silver@{account_name}.blob.core.windows.net/",
    mount_point = f"/mnt/silver",
    extra_configs = {f"fs.azure.account.key.{account_name}.blob.core.windows.net": account_key}
)

# COMMAND ----------

#gold

import os

account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')

dbutils.fs.mount(
    source = f"wasbs://gold@{account_name}.blob.core.windows.net/",
    mount_point = f"/mnt/gold",
    extra_configs = {f"fs.azure.account.key.{account_name}.blob.core.windows.net": account_key}
)

# COMMAND ----------

#criar database

spark.sql("CREATE DATABASE IF NOT EXISTS covid")

# COMMAND ----------

#ler dados da camada bronze

df_covid_br_bronze = spark.read.format('csv').options(header='true', infer_schema='true', delimiter=';').load('dbfs:/mnt/bronze/SRAG_01-06.csv')

# COMMAND ----------

#limpeza dados vazios

df_covid_br_silver = df_covid_br_bronze.filter(df_covid_br_bronze.ID_REGIONA.isNotNull())

# COMMAND ----------

#transforma arquivos em parquet

df_covid_br_silver.write.format('delta').mode('overwrite').save('/mnt/silver/br_covid')

# COMMAND ----------

#camada gold
from pyspark.sql.functions import concat, to_date, year, month

# COMMAND ----------

df_covid_br_silver = spark.read.format('delta').load('/mnt/silver/br_covid')

# COMMAND ----------

#converte as datas do tipo string para date

df_covid_br_silver = df_covid_br_silver.withColumn('DT_NOTIFIC', to_date(df_covid_br_silver['DT_NOTIFIC']))
df_covid_br_silver = df_covid_br_silver.withColumn('DT_SIN_PRI', to_date(df_covid_br_silver['DT_SIN_PRI']))
df_covid_br_silver = df_covid_br_silver.withColumn('DT_NASC', to_date(df_covid_br_silver['DT_NASC']))

# COMMAND ----------

#adicionar coluna data da carga
from pyspark.sql.functions import current_date

df_covid_br_gold = df_covid_br_silver.withColumn('DT_CARGA', current_date())

# COMMAND ----------

#renomeando colunas

df_covid_br_gold = df_covid_br_gold.withColumnRenamed("DT_NOTIFIC", "DT_NOTIFICACAO").withColumnRenamed("DT_SIN_PRI", "DT_PRIMEIROS_SINTOMAS").withColumnRenamed("DT_NASC", "DT_NASCIMENTO")

# COMMAND ----------

#adicionando colunas YEAR e MONTH

df_covid_br_gold = df_covid_br_gold.withColumn('YEAR', year(df_covid_br_gold['DT_NOTIFICACAO']))
df_covid_br_gold = df_covid_br_gold.withColumn('MONTH', month(df_covid_br_gold['DT_NOTIFICACAO']))

# COMMAND ----------

df_covid_br_gold.write.format('delta').mode('overwrite').option("mergeSchema", "true").partitionBy('YEAR', 'MONTH').save('/mnt/gold/br_covid')

# COMMAND ----------

#criar tabela agregada

from pyspark.sql.functions import count

selected_columns = ["year", "month", "DT_NOTIFICACAO", "SG_UF_NOT", "ID_MUNICIP", "DT_CARGA"]
df_select_columns_gold = df_covid_br_gold.select(selected_columns)
grouped_df = df_select_columns_gold.groupBy(selected_columns).agg(count("*").alias("count"))

# COMMAND ----------

display(grouped_df)

# COMMAND ----------

grouped_df.write.format('delta').mode('overwrite').partitionBy('YEAR', 'MONTH').save('/mnt/gold/br_covid_gold_agg')
