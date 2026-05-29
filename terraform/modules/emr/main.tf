resource "aws_emr_cluster" "main" {
  name          = "fraud-detection-spark"
  release_label = "emr-6.15.0"
  applications  = ["Spark"]
  service_role  = "EMR_DefaultRole"

  ec2_attributes {
    subnet_id        = var.subnet_id
    instance_profile = "EMR_EC2_DefaultRole"
    key_name         = "vockey"
  }

  master_instance_group {
    instance_type = "m5.xlarge"
  }

  core_instance_group {
    instance_type  = "m5.xlarge"
    instance_count = 2
  }

  log_uri = "s3://${var.bucket_name}/logs/emr/"

  bootstrap_action {
    name = "install-python-deps"
    path = "s3://elasticmapreduce/bootstrap-actions/run-if"
    args = [
      "instance.isMaster=false",
      "sudo pip3 install scikit-learn==1.0.2 pandas pyarrow numpy"
    ]
  }

  step {
    name              = "install-deps-and-stream"
    action_on_failure = "CONTINUE"

    hadoop_jar_step {
      jar  = "command-runner.jar"
      args = [
        "bash", "-c",
        "pip3 install 'scikit-learn==1.0.2' 'numpy<2.0' pandas pyarrow && spark-submit --deploy-mode cluster --master yarn --conf spark.yarn.submit.waitAppCompletion=false --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3 --conf spark.executorEnv.PYSPARK_PYTHON=python3 --conf spark.yarn.appMasterEnv.KAFKA_BOOTSTRAP_SERVERS=${var.kafka_private_ip}:9092 --conf spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS=${var.kafka_private_ip}:9092 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 s3://${var.bucket_name}/scripts/spark_stream.py"
      ]
    }
  }

  configurations_json = jsonencode([  
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.yarn.appMasterEnv.KAFKA_BOOTSTRAP_SERVERS" = "${var.kafka_private_ip}:9092"
        "spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS"       = "${var.kafka_private_ip}:9092"
        "spark.yarn.appMasterEnv.S3_BUCKET"               = var.bucket_name
        "spark.executorEnv.S3_BUCKET"                     = var.bucket_name
      }
    },
    {
      Classification = "yarn-site"
      Properties = {
        "yarn.log-aggregation-enable"       = "true"
        "yarn.log-aggregation.retain-seconds" = "86400"
      }
    }
  ])

  keep_job_flow_alive_when_no_steps = true
  termination_protection            = false

  tags = {
    Project = "fraud-detection"
  }
}

data "aws_security_group" "emr_master" {
  filter {
    name   = "group-name"
    values = ["ElasticMapReduce-master"]
  }
}

resource "aws_security_group_rule" "emr_master_ssh" {
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = data.aws_security_group.emr_master.id

  lifecycle {
    ignore_changes = all
  }
}

output "cluster_id" {
  value = aws_emr_cluster.main.id
}

output "master_dns" {
  value = aws_emr_cluster.main.master_public_dns
}