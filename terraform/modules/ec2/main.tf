# ── Security Group ─────────────────────────────
resource "aws_security_group" "main" {
  name   = "fraud-ec2-sg"
  vpc_id = var.vpc_id

  ingress {
    description = "FastAPI"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

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

# ── EC2 ────────────────────────────────────────
resource "aws_instance" "main" {
  ami                         = "ami-02b9a589195146a8f"
  instance_type               = "t3.medium"
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.main.id]
  key_name                    = "vockey"
  associate_public_ip_address = true
  iam_instance_profile        = "LabInstanceProfile"

  user_data = <<-USERDATA
#!/bin/bash
set +e
exec > /var/log/userdata.log 2>&1

# ── Dependencias del sistema ──────
yum update -y
yum install -y java-11-amazon-corretto wget python3 python3-pip

# ── Dependencias Python ───────────
pip3 install fastapi uvicorn kafka-python sqlalchemy pyarrow pandas python-multipart

# ── Descargar código desde S3 ─────
mkdir -p /app
aws s3 cp s3://${var.bucket_name}/app/ /app/ --recursive

# ── Instalar Kafka ────────────────
cd /opt
wget -q https://archive.apache.org/dist/kafka/3.5.1/kafka_2.12-3.5.1.tgz
tar -xzf kafka_2.12-3.5.1.tgz
mv kafka_2.12-3.5.1 kafka
rm -f kafka_2.12-3.5.1.tgz

KAFKA_HOME=/opt/kafka

# Obtener IP privada
PRIVATE_IP=$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)

CLUSTER_ID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
$KAFKA_HOME/bin/kafka-storage.sh format -t $CLUSTER_ID \
  -c $KAFKA_HOME/config/kraft/server.properties

# Configurar listeners
sed -i "s|listeners=PLAINTEXT.*|listeners=PLAINTEXT://:9092,CONTROLLER://:9093|" \
  $KAFKA_HOME/config/kraft/server.properties

# Eliminar advertised.listeners existente y escribir con IP real
sed -i '/^advertised\.listeners/d' $KAFKA_HOME/config/kraft/server.properties
echo "advertised.listeners=PLAINTEXT://$PRIVATE_IP:9092" >> \
  $KAFKA_HOME/config/kraft/server.properties

# ── Servicio Kafka ────────────────
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
RestartSec=5

[Install]
WantedBy=multi-user.target
SERVICE

systemctl daemon-reload
systemctl enable kafka
systemctl start kafka

# Esperar que Kafka esté realmente listo
echo "Esperando que Kafka esté disponible..."
for i in $(seq 1 12); do
  if $KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 &>/dev/null; then
    echo "Kafka listo."
    break
  fi
  echo "Intento $i/12, esperando 5s..."
  sleep 5
done

# ── Crear topic ───────────────────
$KAFKA_HOME/bin/kafka-topics.sh --create \
  --topic transactions \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

# ── Servicio FastAPI ──────────────
cat > /etc/systemd/system/fastapi.service << 'SERVICE'
[Unit]
Description=FastAPI Fraud Detection
After=network.target kafka.service

[Service]
Type=simple
User=root
WorkingDirectory=/
Environment=KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ExecStart=/usr/bin/python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
SERVICE

systemctl daemon-reload
systemctl enable fastapi
systemctl start fastapi

# ── Cron reentrenamiento 2am ──────
echo "0 2 * * * root /usr/bin/aws s3 cp s3://${var.bucket_name}/scripts/retrain.py /tmp/retrain.py && /usr/bin/spark-submit --master yarn /tmp/retrain.py >> /var/log/retrain.log 2>&1" > /etc/cron.d/retrain
chmod 0644 /etc/cron.d/retrain

echo "[userdata] Completado exitosamente"
USERDATA

  tags = {
    Name    = "fraud-detection-server"
    Project = "fraud-detection"
  }
}

output "public_ip" {
  value = aws_instance.main.public_ip
}

output "private_ip" {
  value = aws_instance.main.private_ip
}