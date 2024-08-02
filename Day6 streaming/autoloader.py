# Databricks notebook source

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/janaki/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/janaki/autoloader/")
.trigger(once=True)
.table("bronze.autoloader")
)
 

# COMMAND ----------


(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","_____")
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/____/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/_____/autoloader")
.option("mergeSchema",True)
.table("_________")
)
 


# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/hexawaredatabricks/raw/schemalocation/janaki/
# MAGIC

# COMMAND ----------


