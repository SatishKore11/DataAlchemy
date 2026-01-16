#!/usr/bin/env python
# coding: utf-8

# In[1]:


# script to create date for dimension staging 


# In[2]:


# Import required libraries
import sys
from lib.spark_session import get_spark_session
from lib.utils import date_data, get_string_cols, get_rundate
from lib.job_control import insert_log, get_max_timestamp
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, expr, to_date, lit, split, to_timestamp, coalesce
from datetime import datetime
from delta import DeltaTable


# In[3]:


# JOB Parameters
rundate = get_rundate()
schema_name = "edw_stg"
table_name = "dim_customer_stg"
table_full_name = f"{schema_name}.{table_name}"
landing_table_full_name = "edw_ld.dim_customer_ld"
print("SPARK_APP: JOB triggered for rundate - " + rundate)


# In[4]:


# generate sparksession
spark: SparkSession = get_spark_session(f"Staging load - {table_full_name}")
print("SPARK_APP: Spark UI - " + spark.sparkContext.uiWebUrl)


# In[5]:


# Spark Configs
spark.conf.set("spark.sql.shuffle.partitions", 8)
spark.conf.set("spark.sql.parquet.mergeSchema", True)


# In[6]:


# Get the max_timestamp for data load in staging
max_timestamp = get_max_timestamp(spark, schema_name, table_name)
print("SPARK_APP: Max timestamp for staging data load - " + max_timestamp)


# In[7]:


# read data from landing based on max timestamp
df_ld = spark \
    .read \
    .table(landing_table_full_name) \
    .where(f"insert_dt > to_timestamp('{max_timestamp}')")

print("SPARK_APP: Landing Data Count - " + str(df_ld.count()))
print("SPARK_APP: Printing Landing Schema --")
df_ld.printSchema()


# In[8]:


# dedupe the data based on NK
df_dedupe = df_ld.withColumn("_rnk", expr(f"row_number() over(partition by customer_id order by insert_dt desc)")) \
    .where("_rnk = 1").drop("_rnk")
print("SPARK_APP: Landing Data Count after de-dupe - " + str(df_dedupe.count()))


# In[14]:


# format the landing data 
df_stg = df_dedupe \
    .withColumn("first_name", split("name", " ")[0]) \
    .withColumn("last_name", split("name", " ")[1]) \
    .withColumn("date_of_birth", to_date("date_of_birth", "dd-MM-yyyy")) \
    .withColumn("plan_type", coalesce("plan_type", lit("NA"))) \
    .withColumn("effective_start_dt", current_timestamp()) \
    .withColumn("effective_end_dt", to_timestamp(lit("9999-12-31 00:00:00.000000"))) \
    .withColumn("active_flg", lit(1)) \
    .withColumn("insert_flg", current_timestamp()) \
    .withColumn("update_dt", current_timestamp()) 

print("SPARK_APP: Staging Data Count - " + str(df_stg.count()))
print("SPARK_APP: Printing Staging Schema -- ")
df_stg.printSchema()


# In[15]:


# write the data to staging table in overwrite mode for truncate
df_stg.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_full_name)

print("SPARK_APP: Data written to staging table")


# In[16]:


# Add job details in JOB CONTROL
insert_log(spark, schema_name, table_name, datetime.now(), rundate)
print("SPARK_APP: Update JOB Control Log")


# In[17]:


spark.sql(f"select * from edw.job_control where table_name = '{table_name}' order by insert_dt desc limit 1").show(truncate=False)


# In[18]:


# Get the logs from delta table version
dt = DeltaTable.forName(spark, table_full_name)
dt.history().limit(1).select("version","operationMetrics.executionTimeMs", 
                                 "operationMetrics.numTargetRowsInserted",
                                "operationMetrics.numTargetRowsUpdated",
                                "operationMetrics.numOutputRows").show(1, False)


# In[19]:


# Generate Symlink manifest for Athena Access
dt.generate("symlink_format_manifest")
print("SPARK_APP: Symlink Manifest file generated")


# In[20]:


spark.sql("select * from edw_stg.dim_customer_stg").show()


# In[21]:


spark.stop()


# In[ ]:




