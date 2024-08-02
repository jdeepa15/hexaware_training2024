# Databricks notebook source

df=spark.read.json("dbfs:/mnt/hexawaredatabricks/raw/input_files/adobe_sample.json")
 

# COMMAND ----------

df=spark.read.json(
"dbfs:/mnt/hexawaredatabricks/raw/input_files/adobe_sample.json"
,multiLine=
True
)

# COMMAND ----------


df1=df.withColumn("batters",explode("batters.batter"))\
.withColumn("batters_id",col("batters.id"))\
.withColumn("batters_type",col("batters.type"))\
.drop("batters")\
.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop("topping")
 

# COMMAND ----------

from pyspark.sql.types import*

# COMMAND ----------

# MAGIC %run "/Workspace/Users/jdeepa15@gmail.com/day5/Includes/"

# COMMAND ----------

df1.createOrReplaceTempView("adobe")

# COMMAND ----------

# MAGIC %sql select * from adobe;

# COMMAND ----------

spark.sql("select * from adobe").display()

# COMMAND ----------

df1.where("batters_type='Chocolate'and topping_id=5001").display()

# COMMAND ----------

df1.sort(col("topping_id").desc(),"batters_type").display()

# COMMAND ----------


df1.where("batters_type='Chocolate'").explain()
 
