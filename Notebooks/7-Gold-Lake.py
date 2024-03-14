# Databricks notebook source

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC val sTradeDetailsDf = spark.read.table("silver_lake_tenant_2_DB.S_TRADE_DETAILS")
# MAGIC  
# MAGIC //Loading trade_details table to gold_zone as it is.
# MAGIC sTradeDetailsDf.write.format("Delta").mode("overwrite").saveAsTable("gold_lake_tenant_2_DB.G_TRADE_DETAILS")
# MAGIC  
# MAGIC //Grouping the trade details table on sec_level
# MAGIC val gMeasuresAtSecLevelDF =sTradeDetailsDf.groupBy("SECURITY_ID","SECURITY_NM","SEC_COUNTRY")
# MAGIC .agg(sum("SHARES_QTY").as("Total_share_qty"),sum("AMOUNT").as("Total_Amount"))
# MAGIC  
# MAGIC gMeasuresAtSecLevelDF.write.format("Delta").mode("overwrite").saveAsTable("gold_lake_tenant_2_DB.G_MEASURES_AT_SEC_LEVEL")
# MAGIC  
# MAGIC //Grouping the trade details table on client level
# MAGIC val gMeasuresAtClientLevelDF =sTradeDetailsDf.groupBy("CLIENT_ID","CLIENT_NM","CLIENT_REGION")
# MAGIC .agg(sum("SHARES_QTY").as("Total_shares_qty"),sum("AMOUNT").as("Total_Amount"))
# MAGIC  
# MAGIC gMeasuresAtClientLevelDF.write.format("Delta").mode("overwrite").saveAsTable("gold_lake_tenant_2_DB.G_MEASURES_AT_CLIENT_LEVEL")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lake_tenant_2_DB.G_TRADE_DETAILS

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lake_tenant_2_DB.G_MEASURES_AT_SEC_LEVEL

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_lake_tenant_2_DB.G_MEASURES_AT_CLIENT_LEVEL
