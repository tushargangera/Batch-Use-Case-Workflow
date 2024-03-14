// Databricks notebook source

import com.databricks.sql.CloudFilesAWSResourceManager
val manager_clients = CloudFilesAWSResourceManager
    .newManager
    .option("cloudFiles.region", "us-east-2")
    .option("path", "s3://source-dataset-tenant-2/autoloader/GDL_EC/Clients") // required only for setUpNotificationServices
    .create()

val C_SNS = manager_clients.listNotificationServices()

val Clients_SNS = C_SNS.filter("path like '%Clients'")

display(Clients_SNS)

if(Clients_SNS.isEmpty){
  manager_clients.setUpNotificationServices("Clients")
}

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger

val upload_path_c = "s3://source-dataset-tenant-2/autoloader/GDL_EC/Clients/"
val checkpoint_path_c = "s3://bronze-lake-tenant-2/autoloader/_checkpoints/clients_checkpoint"

//Set up the stream to begin reading incoming files from the
//upload_path location.
val df_c = spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("header", "true")
      .option("cloudFiles.queueUrl", "https://sqs.us-east-2.amazonaws.com/576259462660/databricks-auto-ingest-Clients")
      .option("cloudFiles.useNotifications","true")
      .option("cloudFiles.inferColumnTypes","true")
      .option("cloudFiles.rescuedDataColumn", "Clients_rescued_data")
      .option("cloudFiles.schemaLocation", checkpoint_path_c)
      .load(upload_path_c)
      .writeStream.format("delta")
      .queryName("Clients-delta")
      .trigger(Trigger.Once)
      .option("checkpointLocation", checkpoint_path_c)
      .table("bronze_lake_tenant_2_DB.b_clients")
