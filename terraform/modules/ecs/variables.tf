variable "vpc_id" {}
variable "subnet_ids" { type = list(string) }
variable "sg_id" {}
variable "bucket_name" {}
variable "msk_bootstrap" {}
