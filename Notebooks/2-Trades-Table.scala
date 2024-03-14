// Databricks notebook source

import com.databricks.sql.CloudFilesAWSResourceManager
val manager_trades = CloudFilesAWSResourceManager
    .newManager
    .option("cloudFiles.region", "us-east-2")
    .option("path", "s3://source-dataset-tenant-2/autoloader/GDL_EC/Trades") // required only for setUpNotificationServices
    .create()

// List notification services created by Auto Loader
val T_SNS = manager_trades.listNotificationServices()

val Trades_SNS = T_SNS.filter("path like '%Trades'")

display(Trades_SNS)

// Set up an SQS queue and a topic subscribed to the path provided in the manager. Available in Databricks Runtime 7.4 and above.
if(Trades_SNS.isEmpty){
  manager_trades.setUpNotificationServices("Trades")
}

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger

val upload_path_t = "s3://source-dataset-tenant-2/autoloader/GDL_EC/Trades/"
val checkpoint_path_t = "s3://bronze-lake-tenant-2/autoloader/_checkpoints/trades_checkpoint"

// Set up the stream to begin reading incoming files from the
// upload_path location.
val df_t = spark.readStream.format("cloudFiles") 
  .option("cloudFiles.format", "csv") 
  .option("header", "true") 
  .option("cloudFiles.queueUrl", "https://sqs.us-east-2.amazonaws.com/576259462660/databricks-auto-ingest-Trades") 
  .option("cloudFiles.useNotifications","true") 
  .option("cloudFiles.inferColumnTypes","true")
  .option("cloudFiles.rescuedDataColumn", "Trades_rescued_data")
  .option("cloudFiles.schemaLocation", checkpoint_path_t) 
  .load(upload_path_t) 
  .writeStream.format("delta") 
  .queryName("Trades-delta") 
  .trigger(Trigger.Once) 
  .option("checkpointLocation", checkpoint_path_t) 
  .table("bronze_lake_tenant_2_DB.b_trades")

