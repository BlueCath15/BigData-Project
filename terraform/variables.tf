variable "region" {
  default = "us-east-1"
}

variable "bucket_name" {
  description = "Nombre del bucket S3 — debe ser único globalmente"
  default     = "ssdlc-fraud-detection-992382522951"  # account_id al final para unicidad
}

variable "vpc_id" {
  default = "vpc-0eb2af7e99adf1018"
}

variable "subnet_ids" {
  default = [
    "subnet-0c89518331094bb6a",  # us-east-1a
    "subnet-0e4d537ba8a2129bb",  # us-east-1b
    "subnet-0fa056e3794fe61d0",  # us-east-1c
  ]
}

variable "default_sg_id" {
  default = "sg-031b615b7c53eb08d"
}

variable "emr_master_sg_id" {
  default = "sg-010a4acaa721cad89"
}

variable "emr_slave_sg_id" {
  default = "sg-06bf7246e4e0c9e17"
}
