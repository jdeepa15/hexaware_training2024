# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------


(
spark
.readStream
.schema(users_schema)
.csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/",header=True)
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/janaki/stream")
.trigger(once=True)
.table("datamaster.bronze.stream")
)
 

# COMMAND ----------


users_schema=StructType([StructField("Id", IntegerType()),
                        StructField("Name", StringType()),
                        StructField("Gender", StringType()),
                        StructField("Salary", IntegerType()),
                        StructField("Country", StringType()),
                        StructField("Date", StringType())
                        ])
 

# COMMAND ----------



from pyspark.sql.types import *
 
 

# COMMAND ----------


(spark.readStream.schema(users_schema).csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/",header=True)
 .writeStream.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/mith/stream").trigger(once=True).table("bronze.stream"))
 

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC create schema if not exists datamaster;
# MAGIC use bronze
# MAGIC  

# COMMAND ----------


