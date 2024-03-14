terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
      version = "0.5.7"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

resource "databricks_instance_profile" "instance_profile" {
  instance_profile_arn = var.databricks_instance_profile
}

resource "databricks_notebook" "Securities" {
  source = "Notebooks/1-Securities-Table.py"
  path   = "/Shared/Batch-use-case/1-Securities-Table"
  language = "PYTHON"
}

resource "databricks_notebook" "Trades" {
  source = "Notebooks/2-Trades-Table.scala"
  path   = "/Shared/Batch-use-case/2-Trades-Table"
  language = "SCALA"
}

resource "databricks_notebook" "Clients" {
  source = "Notebooks/3-Clients-Table.scala"
  path   = "/Shared/Batch-use-case/3-Clients-Table"
  language = "SCALA"
}

resource "databricks_notebook" "Deequ" {
  source = "Notebooks/4-Deequ.py"
  path   = "/Shared/Batch-use-case/4-Deequ"
  language = "PYTHON"
}

resource "databricks_notebook" "Audit-Framework" {
  source = "Notebooks/5-Audit-Framework.py"
  path   = "/Shared/Batch-use-case/5-Audit-Framework"
  language = "PYTHON"
}

resource "databricks_notebook" "Silver-Lake" {
  source = "Notebooks/6-Silver-Lake.py"
  path   = "/Shared/Batch-use-case/6-Silver-Lake"
  language = "PYTHON"
}

resource "databricks_notebook" "Gold-Lake" {
  source = "Notebooks/7-Gold-Lake.py"
  path   = "/Shared/Batch-use-case/7-Gold-Lake"
  language = "PYTHON"
}

resource "databricks_cluster" "shared" {
  cluster_name = "Trades_Processing"
  num_workers = 0
  spark_version = "9.1.x-scala2.12"
  node_type_id = "m5d.large"
  autotermination_minutes = 60
  library {
    maven {
      coordinates = "com.amazon.deequ:deequ:2.0.0-spark-3.1"
    }
  }
  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode",
    "spark.databricks.hive.metastore.glueCatalog.enabled"  = true,
    "spark.master"  = "local[*]"
  }
  aws_attributes {
    first_on_demand = 1
    availability = "SPOT"
    zone_id = "us-east-1b"
    instance_profile_arn = "arn:aws:iam::576259462660:instance-profile/data-ingestion"
    spot_bid_price_percent = 100
  }
  custom_tags = {
  ResourceClass = "SingleNode"
  }
}

resource "databricks_job" "Consolidated_Trades_Processing" {
  name = "Consolidated_Trades_Processing"

  # job_cluster {
  #   job_cluster_key = "job-cluster"
  #   new_cluster {
  #     num_workers = 0
  #     spark_version = "9.1.x-scala2.12"
  #     node_type_id = "m5d.large"
  #     spark_conf = {
  #       "spark.databricks.cluster.profile" = "singleNode",
  #       "spark.databricks.hive.metastore.glueCatalog.enabled"  = true,
  #       "spark.master"  = "local[*]"
  #     }
  #     aws_attributes {
  #       first_on_demand = 1
  #       availability = "SPOT"
  #       zone_id = "us-east-1b"
  #       instance_profile_arn = "arn:aws:iam::576259462660:instance-profile/data-ingestion"
  #       spot_bid_price_percent = 100
  #     }
  #     custom_tags = {
  #       ResourceClass = "SingleNode"
  #     }
  #   }
  # }

  task {
    task_key = "Securities"

    //job_cluster_key = "job-cluster"
    existing_cluster_id = databricks_cluster.shared.id

    notebook_task {
      notebook_path = databricks_notebook.Securities.path
    }
  }

  task {
    task_key = "Trades"

    //job_cluster_key = "job-cluster"
    existing_cluster_id = databricks_cluster.shared.id

    notebook_task {
      notebook_path = databricks_notebook.Trades.path
    }
  }

  task {
    task_key = "Clients"

    //job_cluster_key = "job-cluster"
    existing_cluster_id = databricks_cluster.shared.id

    notebook_task {
      notebook_path = databricks_notebook.Clients.path
    }
  }

  task {
    task_key = "Deequ"
    //this task will only run after task a
    depends_on {
      task_key= "Securities"
    }

    depends_on {
      task_key= "Trades"
    }

    depends_on {
      task_key= "Clients"
    }

    //job_cluster_key = "job-cluster"
    existing_cluster_id = databricks_cluster.shared.id

    # library {
    #   maven {
    #     coordinates = "com.amazon.deequ:deequ:2.0.0-spark-3.1"
    #   }
    # }

    notebook_task {
      notebook_path = databricks_notebook.Deequ.path
    }
  }

  task {
    task_key = "Audit-Framework"

    depends_on {
      task_key= "Securities"
    }

    depends_on {
      task_key= "Trades"
    }

    depends_on {
      task_key= "Clients"
    }

    //job_cluster_key = "job-cluster"
    existing_cluster_id = databricks_cluster.shared.id

    notebook_task {
      notebook_path = databricks_notebook.Audit-Framework.path
    }
  }

  task {
    task_key = "Silver-Lake"

    depends_on {
      task_key= "Deequ"
    }

    depends_on {
      task_key= "Audit-Framework"
    }

    //job_cluster_key = "job-cluster"
    existing_cluster_id = databricks_cluster.shared.id

    notebook_task {
      notebook_path = databricks_notebook.Silver-Lake.path
    }
  }

  task {
    task_key = "Gold-Lake"

    depends_on {
      task_key = "Silver-Lake"
    }

    //job_cluster_key = "job-cluster"
    existing_cluster_id = databricks_cluster.shared.id

    notebook_task {
      notebook_path = databricks_notebook.Gold-Lake.path
    }
  }

  email_notifications {
    on_start = ["tushar.gangera@oracle.com"]
    on_success = ["tushar.gangera@oracle.com"]
    on_failure = ["tushar.gangera@oracle.com"]
    no_alert_for_skipped_runs = false
  }

  schedule {
    quartz_cron_expression = "20 15 16 * * ?"
    timezone_id = "Asia/Kolkata"
    pause_status = "UNPAUSED"
  }

  max_concurrent_runs = 3
}

output "batch-use-case-workflow" {
  value = databricks_job.Consolidated_Trades_Processing.id
}