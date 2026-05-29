# ── Security Group para Kafka EC2 ──────────────
resource "aws_security_group" "kafka" {
  name   = "fraud-kafka-sg"
  vpc_id = var.vpc_id

  ingress {
    description = "Kafka"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
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

# ── EC2 con Kafka instalado via user_data ──────
resource "aws_instance" "kafka" {
  ami                         = "ami-02b9a589195146a8f"
  instance_type               = "t3.medium"
  subnet_id                   = var.subnet_ids[0]
  vpc_security_group_ids      = [aws_security_group.kafka.id]
  key_name                    = "vockey"              # ← key pair de AWS Academy
  associate_public_ip_address = true

  user_data = <<-EOF
    #!/bin/bash
    yum update -y
    yum install -y java-11-amazon-corretto wget

    # Descargar Kafka
    cd /opt
    wget -q https://downloads.apache.org/kafka/3.5.1/kafka_2.12-3.5.1.tgz
    tar -xzf kafka_2.12-3.5.1.tgz
    mv kafka_2.12-3.5.1 kafka
    rm kafka_2.12-3.5.1.tgz

    export KAFKA_HOME=/opt/kafka
    export PATH=$PATH:$KAFKA_HOME/bin

    # Configurar KRaft
    CLUSTER_ID=$(kafka-storage.sh random-uuid)
    kafka-storage.sh format -t $CLUSTER_ID \
      -c $KAFKA_HOME/config/kraft/server.properties

    # Obtener IP privada y configurar listeners
    PRIVATE_IP=$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)
    sed -i "s|#advertised.listeners=.*|advertised.listeners=PLAINTEXT://$PRIVATE_IP:9092|" \
      $KAFKA_HOME/config/kraft/server.properties
    sed -i "s|listeners=PLAINTEXT.*|listeners=PLAINTEXT://:9092,CONTROLLER://:9093|" \
      $KAFKA_HOME/config/kraft/server.properties

    # Servicio systemd
    cat > /etc/systemd/system/kafka.service << 'SERVICE'
[Unit]
Description=Apache Kafka
After=network.target

[Service]
Type=simple
User=root
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target
SERVICE

    systemctl daemon-reload
    systemctl enable kafka
    systemctl start kafka

    # Esperar y crear topic
    sleep 15
    kafka-topics.sh --create \
      --topic transactions \
      --bootstrap-server localhost:9092 \
      --partitions 1 \
      --replication-factor 1 \
      --if-not-exists
  EOF

  tags = {
    Name    = "fraud-kafka"
    Project = "fraud-detection"
  }
}

output "bootstrap_brokers" {
  value = "${aws_instance.kafka.private_ip}:9092"
}

output "kafka_public_ip" {
  value = aws_instance.kafka.public_ip
}
