resource "aws_s3_bucket" "main" {
  bucket        = var.bucket_name
  force_destroy = true  # permite destruir aunque tenga archivos
}

resource "aws_s3_bucket_versioning" "main" {
  bucket = aws_s3_bucket.main.id
  versioning_configuration {
    status = "Enabled"
  }
}

# ── Estructura de carpetas ──────────────────────
resource "aws_s3_object" "models_folder" {
  bucket = aws_s3_bucket.main.id
  key    = "models/"
}

resource "aws_s3_object" "training_data_folder" {
  bucket = aws_s3_bucket.main.id
  key    = "training-data/"
}

resource "aws_s3_object" "seed_folder" {
  bucket = aws_s3_bucket.main.id
  key    = "seed/"
}

resource "aws_s3_object" "scripts_folder" {
  bucket = aws_s3_bucket.main.id
  key    = "scripts/"
}

resource "aws_s3_object" "logs_folder" {
  bucket = aws_s3_bucket.main.id
  key    = "logs/"
}

# ── Subir scripts de Spark ──────────────────────
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

output "bucket_arn" {
  value = aws_s3_bucket.main.arn
}
