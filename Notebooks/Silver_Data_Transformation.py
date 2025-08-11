# Databricks notebook source
df=spark.read.format("delta").option("header",True).option("inferSchema",True).load("abfss://bronze@storageadlssudipnetflix.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df=df.fillna({"duration_minutes":0,"duration_seasons":1})

# COMMAND ----------

display(df)

# COMMAND ----------

df=df.withColumn("duration_minutes",col("duration_minutes").cast(IntegerType())).withColumn("duration_seasons",col("duration_seasons").cast(IntegerType()))

# COMMAND ----------

display(df)

# COMMAND ----------

df=df.withColumn("ShortTitle",split(col("title"),":")[0])

# COMMAND ----------

df=df.withColumn("rating",split(col('rating'),"-")[0])

# COMMAND ----------

display(df)

# COMMAND ----------

df=df.withColumn("type_flag",when(col("type")=="Movie",1).when(col("type")=="TV Show",0).otherwise(2))
display(df)

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df=df.withColumn("duration_ranking",dense_rank().over(Window.orderBy(col("duration_minutes").desc())))

# COMMAND ----------

display(df)

# COMMAND ----------

df_vis=df.groupBy("type").agg(count("*").alias("Total_count"))
display(df_vis)

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("path","abfss://silver@storageadlssudipnetflix.dfs.core.windows.net/netflix_titles").save()