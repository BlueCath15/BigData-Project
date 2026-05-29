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
# S3
# ─────────────────────────────────────────────
module "s3" {
  source      = "./modules/s3"
  bucket_name = var.bucket_name
}

# ─────────────────────────────────────────────
# MSK (Kafka)
# ─────────────────────────────────────────────
module "msk" {
  source     = "./modules/msk"
  vpc_id     = var.vpc_id
  subnet_ids = var.subnet_ids
  sg_id      = var.default_sg_id
}

# ─────────────────────────────────────────────
# EMR (Spark)
# ─────────────────────────────────────────────
module "emr" {
  source        = "./modules/emr"
  bucket_name   = var.bucket_name
  subnet_id     = var.subnet_ids[0]
  master_sg_id  = var.emr_master_sg_id
  slave_sg_id   = var.emr_slave_sg_id
  msk_bootstrap = module.msk.bootstrap_brokers
  depends_on    = [module.s3, module.msk]
}

# ─────────────────────────────────────────────
# ECS Fargate (FastAPI)
# ─────────────────────────────────────────────
module "ecs" {
  source       = "./modules/ecs"
  vpc_id       = var.vpc_id
  subnet_ids   = var.subnet_ids
  sg_id        = var.default_sg_id
  bucket_name  = var.bucket_name
  msk_bootstrap = module.msk.bootstrap_brokers
  depends_on   = [module.msk]
}