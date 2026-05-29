output "bucket_name" {
  value = module.s3.bucket_name
}

output "msk_bootstrap_brokers" {
  value = module.msk.bootstrap_brokers
}

output "emr_cluster_id" {
  value = module.emr.cluster_id
}

output "emr_master_dns" {
  value = module.emr.master_dns
}

output "fastapi_url" {
  value = module.ecs.service_url
}
