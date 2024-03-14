# Databricks notebook source

from datetime import datetime, timedelta
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import col,concat, lit, expr, trim, unix_timestamp, upper
import pyspark.sql.functions as func
from pyspark.sql import SQLContext

# COMMAND ----------

spark.sql("create table IF NOT EXISTS data_quality_tenant_2_DB.audit_table (ISSUE_NAME string, TRADE_ID string, SECURITY_ID string, CLIENT_ID string) using delta location 's3://data-quality-tenant-2/data_quality_tenant_2_DB/audit_table'");

# COMMAND ----------

#Import multiple audit queries from CSV file, execute them and store in audit_table
path = "s3://source-dataset-tenant-2/DQ/Query.csv"
df_query = sqlContext.read.format("csv").option("header", "true").load(path)
df_query = df_query.toPandas ()
query_list = df_query['Query'].tolist()
for item in query_list[:len(query_list)]:
    query = str(item)
    sqlContext.sql(query)

# COMMAND ----------

spark.sql("select count(DISTINCT ISSUE_NAME) from data_quality_tenant_2_DB.audit_table");

# COMMAND ----------

spark.sql("select * from data_quality_tenant_2_DB.audit_table");
