# Databricks notebook source

# MAGIC %scala
# MAGIC //Reading bronze lake data into a dataframe
# MAGIC val dfs_2 = sqlContext.read.table("bronze_lake_tenant_2_DB.b_trades")
# MAGIC val dfs_3 = sqlContext.read.table("bronze_lake_tenant_2_DB.b_securities")
# MAGIC val dfs_4 = sqlContext.read.table("bronze_lake_tenant_2_DB.b_clients")
# MAGIC 
# MAGIC //Performing transformations on the dataframe
# MAGIC val s_trade_details_df=dfs_2.join(dfs_3,dfs_2("security_id") === dfs_3("security_id"),"inner").join(dfs_4,dfs_2("client_id") === dfs_4("client_id"),"inner").drop(dfs_2("security_id")).drop(dfs_2("client_id"))
# MAGIC 
# MAGIC //Writing the dataframe as a delta table and storing it in silver lake
# MAGIC s_trade_details_df.write.format("delta").mode("overwrite").saveAsTable("silver_lake_tenant_2_DB.S_TRADE_DETAILS")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_lake_tenant_2_DB.S_TRADE_DETAILS;
