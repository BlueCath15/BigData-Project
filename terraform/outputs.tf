output "ec2_public_ip" {
  value       = module.ec2.public_ip
  description = "IP publica del EC2 — FastAPI en http://<IP>:8000"
}

output "ec2_private_ip" {
  value       = module.ec2.private_ip
  description = "IP privada del EC2 — Kafka en <IP>:9092"
}

output "emr_cluster_id" {
  value = module.emr.cluster_id
}

output "emr_master_dns" {
  value = module.emr.master_dns
}

output "bucket_name" {
  value = module.s3.bucket_name
}
