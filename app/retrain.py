"""
retrain.py — Reentrenamiento periódico del modelo de detección de fraude.

Modos de ejecución:
  - Primera vez (spark-initial-train): sin datos reales → usa seed_data.parquet
  - Noche (retrain-cron):              lee training_data/ acumulado por el streaming

Flujo:
  1. Detectar si hay datos reales o usar semilla
  2. Preparar features (igual que streaming.py)
  3. Entrenar RandomForestClassifier
  4. Validar F1 mínimo antes de reemplazar
  5. Backup del modelo anterior + guardar el nuevo
"""

import os
import shutil
import logging
from datetime import datetime

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, when, count
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────

MODEL_PATH = "s3://ssdlc-fraud-detection-992382522951/models/fraud_rf_model"
BACKUP_DIR = "s3://ssdlc-fraud-detection-992382522951/backups/"
DATA_PATH  = "s3://ssdlc-fraud-detection-992382522951/training-data/"
SEED_PATH  = "s3://ssdlc-fraud-detection-992382522951/seed/seed_data.parquet"
LOG_PATH   = "s3://ssdlc-fraud-detection-992382522951/logs/retrain.log"

MIN_ROWS      = 1000     # mínimo de filas para entrenar con datos reales
F1_THRESHOLD  = 0.70     # F1 mínimo para reemplazar el modelo en producción
TRAIN_RATIO   = 0.8
RANDOM_SEED   = 42

RF_NUM_TREES  = 100
RF_MAX_DEPTH  = 10


# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────

os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger("retrain")


# ─────────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("RetrainFraudModel")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# ─────────────────────────────────────────────
# 1. Cargar datos
# ─────────────────────────────────────────────

# Columnas base que existen en ambos DataFrames
BASE_COLS = ["user_id", "amount", "transaction_type", "currency",
             "destination_id", "ip_address", "created_at", "label"]

def load_data():
    has_real_data = (
        os.path.exists(DATA_PATH)
        and len([f for f in os.listdir(DATA_PATH) if not f.startswith(".")]) > 0
    )

    if has_real_data:
        df = spark.read.parquet(DATA_PATH).select(BASE_COLS)
        total = df.count()
        log.info(f"Datos reales encontrados: {total} filas")

        if total < MIN_ROWS:
            log.warning(
                f"Solo {total} filas reales (mínimo {MIN_ROWS}). "
                "Combinando con seed para enriquecer el entrenamiento..."
            )
            seed_df = spark.read.parquet(SEED_PATH).select(BASE_COLS)
            df = df.unionByName(seed_df)
            log.info(f"Total tras combinar con seed: {df.count()} filas")
    else:
        log.info("Sin datos reales — usando seed_data.parquet para entrenamiento inicial")
        df = spark.read.parquet(SEED_PATH).select(BASE_COLS)
        log.info(f"Seed cargado: {df.count()} filas")

    return df


# ─────────────────────────────────────────────
# 2. Feature engineering
# (idéntico al bloque df_ml en streaming.py)
# ─────────────────────────────────────────────

def build_features(df):
    """
    Genera las mismas columnas que calcula streaming.py:
      - hour          → hora de la transacción
      - dest_external → 1.0 si destino es externo, 0.0 si no

    El DataFrame de entrada debe tener:
      amount, transaction_type, destination_id, user_id, created_at, label
    """
    return (
        df
        .withColumn("hour", hour(col("created_at")).cast("float"))
        .withColumn(
            "dest_external",
            when(
                col("destination_id").isNotNull()
                & (col("destination_id") != col("user_id")),
                1.0
            ).otherwise(0.0)
        )
    )


# ─────────────────────────────────────────────
# 3. Validar balance de clases
# ─────────────────────────────────────────────

def log_class_distribution(df):
    log.info("Distribución de clases:")
    df.groupBy("label").agg(count("*").alias("n")).show()


# ─────────────────────────────────────────────
# 4. Construir pipeline ML
# ─────────────────────────────────────────────

def build_pipeline():
    """
    Pipeline con los mismos stages que espera streaming.py:
      StringIndexer → VectorAssembler → RandomForestClassifier
    """
    indexer = StringIndexer(
        inputCol="transaction_type",
        outputCol="tx_type_idx",
        handleInvalid="keep",       # evita crash con tipos nuevos en producción
    )

    assembler = VectorAssembler(
        inputCols=["amount", "tx_type_idx", "dest_external", "hour"],
        outputCol="features",
        handleInvalid="skip",
    )

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",           # 0=approved  1=flagged  2=blocked
        numTrees=RF_NUM_TREES,
        maxDepth=RF_MAX_DEPTH,
        probabilityCol="probability",
        seed=RANDOM_SEED,
    )

    return Pipeline(stages=[indexer, assembler, rf])


# ─────────────────────────────────────────────
# 5. Evaluar modelo
# ─────────────────────────────────────────────

def evaluate(model, val_df):
    predictions = model.transform(val_df)

    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
    )

    f1        = evaluator.setMetricName("f1").evaluate(predictions)
    accuracy  = evaluator.setMetricName("accuracy").evaluate(predictions)
    precision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)
    recall    = evaluator.setMetricName("weightedRecall").evaluate(predictions)

    log.info(f"  F1        : {f1:.4f}")
    log.info(f"  Accuracy  : {accuracy:.4f}")
    log.info(f"  Precision : {precision:.4f}")
    log.info(f"  Recall    : {recall:.4f}")

    return f1


# ─────────────────────────────────────────────
# 6. Reemplazar modelo con backup
# ─────────────────────────────────────────────

def replace_model(new_model):
    import subprocess
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    backup_path = f"{BACKUP_DIR}/fraud_rf_model_{timestamp}"

    # Backup
    try:
        new_model.save(backup_path)
        log.info(f"Backup guardado en: {backup_path}")
    except Exception as e:
        log.warning(f"No se pudo guardar backup: {e}")

    # Borrar modelo anterior en S3
    subprocess.run(
        ["aws", "s3", "rm", MODEL_PATH, "--recursive"],
        capture_output=True
    )

    # Guardar nuevo modelo
    new_model.save(MODEL_PATH)
    log.info(f"Nuevo modelo guardado en: {MODEL_PATH}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    log.info("=" * 55)
    log.info("Iniciando reentrenamiento del modelo de fraude")
    log.info("=" * 55)

    # 1. Datos
    df = load_data()
    df = build_features(df)
    log_class_distribution(df)

    # 2. Split train/val
    train_df, val_df = df.randomSplit(
        [TRAIN_RATIO, 1 - TRAIN_RATIO],
        seed=RANDOM_SEED
    )
    log.info(f"Train: {train_df.count()} filas | Val: {val_df.count()} filas")

    # 3. Entrenar
    log.info("Entrenando pipeline...")
    pipeline  = build_pipeline()
    new_model = pipeline.fit(train_df)
    log.info("Entrenamiento completado")

    # 4. Evaluar
    log.info("Evaluando en validación...")
    f1 = evaluate(new_model, val_df)

    if f1 < F1_THRESHOLD:
        log.warning(
            f"F1 {f1:.4f} por debajo del umbral {F1_THRESHOLD}. "
            "Modelo NO reemplazado — se mantiene el anterior."
        )
        spark.stop()
        return

    # 5. Reemplazar
    replace_model(new_model)

    log.info("Reentrenamiento finalizado correctamente")
    log.info("=" * 55)

    spark.stop()


if __name__ == "__main__":
    main()