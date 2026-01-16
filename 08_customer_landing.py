#!/usr/bin/env python
# coding: utf-8

# In[1]:


# script to create dtae for customer dimension landing


# In[2]:


# import required libraries
import sys
from lib.spark_session import get_spark_session
from lib.utils import date_data, get_string_cols, get_rundate
from lib.job_control import insert_log, get_max_timestamp
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime
from delta import DeltaTable
from lib.aws_s3 import archive_landing_object


# In[3]:


# job parameters
rundate = get_rundate()
schema_name = "edw_ld"
table_name = "dim_customer_ld"
table_full_name = f"{schema_name}.{table_name}"
landing_file_name = f"customer_{rundate}.csv"
landing_file_path = f"s3a://datasatish/dw-with-pyspark/landing/customer/{landing_file_name}"


# In[4]:


# generate spark session
spark: SparkSession = get_spark_session(f"Landing load - {table_full_name}")
print("SPARK_APP: Spark UI - " + spark.sparkContext.uiWebUrl)


# In[5]:


# spark configs
spark.conf.set("spark.sql.shiffle.partitions", 8)
spark.conf.set("spark.sql.parquet.mergeSchema", True)


# In[6]:


# Create raw dataframe and get col info
df_raw = spark \
    .read \
    .format("csv") \
    .option("header", True) \
    .load(landing_file_path)

print("SPARK_APP: Printing Raw Schema --")
df_raw.printSchema()

# get landing count
print("SPARK_APP: Landing data count - " + str(df_raw.count()))


# In[7]:


# cast all columns to string
df_casted = df_raw.selectExpr(get_string_cols(spark, df_raw))
print("SPARK_APP: Casted all columns to string")

# add audit columns
df_ld = df_casted.withColumn("insert_dt", current_timestamp()) \
    .withColumn("rundate", lit(rundate))
print("SPARK_APP: Added AUDIT Column")

# get final layer count 
print("SPARK_APP: Final layer data count - " + str(df_ld.count()))
print("SPARK_APP: Printing Landing Schema --")
df_ld.printSchema()


# In[8]:


# write the date to landing layer checking if table exists
if get_max_timestamp(spark, schema_name, table_name) != "1900-01-01 00:00:00.000000":
    df_ld.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(table_full_name)
else:
    df_ld.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_full_name)

print("SPARK_APP: Data written to landing layer")


# In[9]:


# archive the landing file 
if archive_landing_object(landing_file_name, "customer"):
    print(f"SPARK_APP: Landing file {landing_file_name} archived")
else:
    print(f"SPARK_APP: Landing file {landing_file_name} not archived. Please Archive the file manually")


# In[16]:


# add job details in JOB control
insert_log(spark, schema_name, table_name, datetime.now(), rundate)
print("SPARK_APP: Update JOB Control Log")


# In[11]:


spark.sql(f"select * from edw.job_control where table_name = '{table_name}' order by insert_dt desc limit 1").show(truncate=False)


# In[12]:


# get the logs from delta table version
dt = DeltaTable.forName(spark, table_full_name)
dt.history().limit(1).select("version", "operationMetrics.executinTimesMs",
                             "operationMetrics.numTargetRowsInserted",
                             "operationMetrics.numTargetRowsUpdated",
                             "operationMetrics.numOutputRows").show(1, False)


# In[13]:


# generate symlink manifest for athena access
dt.generate("symlink_format_manifest")
print("SPARK_APP: Symlink Manifest file generated")


# In[14]:


spark.sql("select * from edw_ld.dim_customer_ld").show()


# In[15]:


spark.stop()


# In[ ]:




