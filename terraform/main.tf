terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# ─────────────────────────────────────────────
# S3 — almacenamiento compartido
# ─────────────────────────────────────────────
module "s3" {
  source      = "./modules/s3"
  bucket_name = var.bucket_name
}

# ─────────────────────────────────────────────
# EC2 — FastAPI + Kafka + cron
# ─────────────────────────────────────────────
module "ec2" {
  source      = "./modules/ec2"
  vpc_id      = var.vpc_id
  subnet_id   = var.subnet_id
  bucket_name = var.bucket_name
  depends_on  = [module.s3]
}

# ─────────────────────────────────────────────
# EMR — Spark Streaming
# ─────────────────────────────────────────────
module "emr" {
  source           = "./modules/emr"
  bucket_name      = var.bucket_name
  subnet_id        = var.subnet_id
  kafka_private_ip = module.ec2.private_ip
  depends_on       = [module.s3, module.ec2]
}
