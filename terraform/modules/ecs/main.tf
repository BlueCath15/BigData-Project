# ── ECR — repositorio de la imagen Docker ──────
resource "aws_ecr_repository" "fastapi" {
  name         = "fraud-detection-fastapi"
  force_delete = true
}

# ── ECS Cluster ────────────────────────────────
resource "aws_ecs_cluster" "main" {
  name = "fraud-detection-cluster"
}

# ── Task Definition ────────────────────────────
resource "aws_ecs_task_definition" "fastapi" {
  family                   = "fraud-detection-fastapi"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = "arn:aws:iam::992382522951:role/LabRole"
  task_role_arn            = "arn:aws:iam::992382522951:role/LabRole"

  container_definitions = jsonencode([{
    name  = "fastapi"
    image = "${aws_ecr_repository.fastapi.repository_url}:latest"
    portMappings = [{
      containerPort = 8000
      protocol      = "tcp"
    }]
    environment = [
      {
        name  = "KAFKA_BOOTSTRAP_SERVERS"
        value = var.msk_bootstrap
      }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/ecs/fraud-detection-fastapi"
        "awslogs-region"        = "us-east-1"
        "awslogs-stream-prefix" = "ecs"
      }
    }
  }])
}

# ── CloudWatch Log Group ───────────────────────
resource "aws_cloudwatch_log_group" "fastapi" {
  name              = "/ecs/fraud-detection-fastapi"
  retention_in_days = 7
}

# ── Security Group para Fargate ────────────────
resource "aws_security_group" "fargate" {
  name   = "fraud-fargate-sg"
  vpc_id = var.vpc_id

  ingress {
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ── ECS Service ────────────────────────────────
resource "aws_ecs_service" "fastapi" {
  name            = "fraud-detection-fastapi"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.fastapi.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = [aws_security_group.fargate.id]
    assign_public_ip = true
  }
}

output "ecr_repository_url" {
  value = aws_ecr_repository.fastapi.repository_url
}

output "service_url" {
  value = "Ver IP pública en ECS → Tasks → ENI"
}