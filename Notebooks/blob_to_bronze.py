# Databricks notebook source
df = spark.readStream.format("cloudfiles")\
                .option("cloudFiles.format","csv")\
                .option("cloudFiles.schemaLocation", "/Volumes/retail_q/volumes/blob_source/schema/")\
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")\
                .load("/Volumes/retail_q/volumes/blob_source/transactions_source/")

# COMMAND ----------

df.writeStream.format("delta")\
            .option("checkpointLocation", "/Volumes/retail_q/volumes/blob_source/checkpoint/")\
            .trigger(availableNow=True)\
            .table("retail_q.blob_bronze.transactions")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_q.blob_bronze.transactions

# COMMAND ----------

