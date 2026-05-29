# ── EMR Cluster ────────────────────────────────
resource "aws_emr_cluster" "main" {
  name          = "fraud-detection-spark"
  release_label = "emr-6.15.0"
  applications  = ["Spark"]
  service_role  = "EMR_DefaultRole"

  ec2_attributes {
    subnet_id                         = var.subnet_id
    emr_managed_master_security_group = var.master_sg_id
    emr_managed_slave_security_group  = var.slave_sg_id
    instance_profile                  = "EMR_EC2_DefaultRole"
    key_name                          = "vockey"
  }

  master_instance_group {
    instance_type = "m5.xlarge"
  }

  core_instance_group {
    instance_type  = "m5.xlarge"
    instance_count = 2
  }

  log_uri = "s3://${var.bucket_name}/logs/emr/"

  step {
    name              = "spark-streaming"
    action_on_failure = "CONTINUE"

    hadoop_jar_step {
      jar = "command-runner.jar"
      args = [
        "bash", "-c",
        "pip3 install 'scikit-learn==1.3.2' 'numpy<2.0' pandas pyarrow && spark-submit --deploy-mode cluster --master yarn --conf spark.yarn.submit.waitAppCompletion=false --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 s3://${var.bucket_name}/scripts/spark_stream.py"
      ]
    }
  }

  configurations_json = jsonencode([
    {
      Classification = "spark-env"
      Configurations = [
        {
          Classification = "export"
          Properties = {
            KAFKA_BOOTSTRAP_SERVERS = var.msk_bootstrap
            S3_BUCKET               = var.bucket_name
          }
        }
      ]
    }
  ])

  keep_job_flow_alive_when_no_steps = true
  termination_protection            = false

  tags = {
    Project = "fraud-detection"
  }
}

output "cluster_id" {
  value = aws_emr_cluster.main.id
}

output "master_dns" {
  value = aws_emr_cluster.main.master_public_dns
}