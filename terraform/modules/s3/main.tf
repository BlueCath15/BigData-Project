resource "aws_s3_bucket" "main" {
  bucket        = var.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "main" {
  bucket = aws_s3_bucket.main.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_object" "folders" {
  for_each = toset(["models/", "training-data/", "seed/", "scripts/", "logs/", "checkpoints/"])
  bucket   = aws_s3_bucket.main.id
  key      = each.value
}

resource "aws_s3_object" "spark_stream" {
  bucket = aws_s3_bucket.main.id
  key    = "scripts/spark_stream.py"
  source = "${path.root}/../app/streaming/spark_stream.py"
  etag   = filemd5("${path.root}/../app/streaming/spark_stream.py")
}

resource "aws_s3_object" "retrain" {
  bucket = aws_s3_bucket.main.id
  key    = "scripts/retrain.py"
  source = "${path.root}/../app/retrain.py"
  etag   = filemd5("${path.root}/../app/retrain.py")
}

resource "aws_s3_object" "generate_seed" {
  bucket = aws_s3_bucket.main.id
  key    = "scripts/generate_seed.py"
  source = "${path.root}/../generate_seed.py"
  etag   = filemd5("${path.root}/../generate_seed.py")
}

output "bucket_name" {
  value = aws_s3_bucket.main.id
}

resource "aws_s3_object" "app_code" {
  for_each = fileset("${path.root}/../app", "**/*.py")
  bucket   = aws_s3_bucket.main.id
  key      = "app/${each.value}"
  source   = "${path.root}/../app/${each.value}"
  etag     = filemd5("${path.root}/../app/${each.value}")
}

resource "aws_s3_object" "fraud_model" {
  for_each = fileset("${path.root}/../app/fraud_rf_model", "**")
  bucket   = aws_s3_bucket.main.id
  key      = "models/fraud_rf_model/${each.value}"
  source   = "${path.root}/../app/fraud_rf_model/${each.value}"
  etag     = filemd5("${path.root}/../app/fraud_rf_model/${each.value}")
}

resource "aws_s3_object" "seed_data" {
  bucket = aws_s3_bucket.main.id
  key    = "seed/seed_data.parquet"
  source = "${path.root}/../app/seed_data.parquet"
  etag   = filemd5("${path.root}/../app/seed_data.parquet")
}