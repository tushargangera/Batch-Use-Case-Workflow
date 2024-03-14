# Databricks notebook source

spark.sql("SHOW DATABASES");

# COMMAND ----------

spark.sql("create database if not exists bronze_lake_tenant_2_DB location 's3a://bronze-lake-tenant-2/bronze_lake_tenant_2_DB'");

# COMMAND ----------

spark.sql("create database if not exists silver_lake_tenant_2_DB location 's3a://silver-lake-tenant-2/silver_lake_tenant_2_DB'");

# COMMAND ----------

spark.sql("create database if not exists gold_lake_tenant_2_DB location 's3a://gold-lake-tenant-2/gold_lake_tenant_2_DB'");

# COMMAND ----------

spark.sql("create database if not exists data_quality_tenant_2_DB location 's3a://data-quality-tenant-2/data_quality_tenant_2_DB'");

# COMMAND ----------

# MAGIC %scala
# MAGIC //Reading the table data into a dataframe
# MAGIC val dfs = sqlContext.read.table("trial_db_1.glue")
# MAGIC 
# MAGIC //Writing the dataframe as a delta table and storing it in bronze lake
# MAGIC dfs.write.format("delta").mode("overwrite").saveAsTable("bronze_lake_tenant_2_DB.b_securities")

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM bronze_lake_tenant_2_DB.b_securities WHERE security_id = 'security_ID';
# MAGIC 
# MAGIC select * from bronze_lake_tenant_2_DB.b_securities;
