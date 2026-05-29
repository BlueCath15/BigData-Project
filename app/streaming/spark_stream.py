from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count,
    when, lit, expr,
    hour as spark_hour
)
from pyspark.ml.functions import vector_to_array
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)
from pyspark.ml import PipelineModel
import os
import sys

import sys
sys.path.insert(0, "/app") 

# ── Kafka bootstrap ────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# ─────────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("TransactionStreaming")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ─────────────────────────────────────────────
# Cargar modelo Spark ML
# ─────────────────────────────────────────────

fraud_pipeline = PipelineModel.load(
    "s3://fraud-detection-992382522951/models/fraud_rf_model"
) 
# ─────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────

schema = StructType([
    StructField("user_id",           StringType()),
    StructField("amount",            DoubleType()),
    StructField("transaction_type",  StringType()),
    StructField("currency",          StringType()),
    StructField("destination_id",    StringType()),
    StructField("ip_address",        StringType()),
    StructField("created_at",        TimestampType())
])

# ─────────────────────────────────────────────
# Read Kafka Stream
# ─────────────────────────────────────────────

df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)   # ← cambiar
    .option("subscribe", "transactions")
    .option("startingOffsets", "latest")
    .load()
)

# ─────────────────────────────────────────────
# Parse JSON
# ─────────────────────────────────────────────

df = (
    df_raw
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# ─────────────────────────────────────────────
# Simple rules (MAP)
# ─────────────────────────────────────────────

df_rules = (
    df
    .withColumn("high_amount",
        when(col("amount") > 10000, 40)
        .when(col("amount") > 5000, 20)
        .otherwise(0))
    .withColumn("round_sus",
        when(
            (col("amount").isin(999, 4999, 9999, 49999, 99999)) |
            ((col("amount") % 1000) >= 990), 15)
        .otherwise(0))
    .withColumn("struct_sus",
        when(
            (col("amount") >= 10000) & (col("amount") % 1000 == 0), 15)
        .otherwise(0))
    .withColumn("external_transfer",
        when(
            (col("transaction_type").isin("TRANSFER", "WITHDRAWAL")) &
            (col("destination_id").isNotNull()) &
            (col("destination_id") != col("user_id")), 10)
        .otherwise(0))
    .withColumn("map_score",
        expr("high_amount + round_sus + struct_sus + external_transfer"))
)

# ─────────────────────────────────────────────
# ML inference (MAP)
# ─────────────────────────────────────────────

# Después — vector_to_array como función Python
df_ml = (
    df_rules
    .withColumn("hour", spark_hour(col("created_at")))
    .withColumn(
        "dest_external",
        when(
            col("destination_id").isNotNull() &
            (col("destination_id") != col("user_id")), 1.0
        ).otherwise(0.0)
    )
    .transform(lambda df: fraud_pipeline.transform(df))
    .withColumn("prob_array", vector_to_array(col("probability")))
    .withColumn(
        "ml_score",
        expr("least(prob_array[1] * 50 + prob_array[2] * 100, 100)")
    )
)

# ─────────────────────────────────────────────
# Complex rules (REDUCE)
# ─────────────────────────────────────────────

df_final = (
    df_ml
    .groupBy(
        window(col("created_at"), "1 hour", "5 minutes"),
        col("user_id")
    )
    .agg(
        count("*").alias("tx_count"),
        expr("sum(map_score)").alias("map_score_sum"),
        expr("avg(ml_score)").alias("ml_score_avg")
    )
    .withColumn("score_frequency",
        when(col("tx_count") >= 5, 30)
        .when(col("tx_count") >= 3, 15)
        .otherwise(0))
    .withColumn("rules_score",
        expr("least(map_score_sum + score_frequency, 100)"))
    .withColumn("total_score",
        expr("least(rules_score * 0.5 + ml_score_avg * 0.5, 100)"))
    .withColumn("risk_level",
        when(col("total_score") >= 76, "CRITICAL")
        .when(col("total_score") >= 51, "HIGH")
        .when(col("total_score") >= 26, "MEDIUM")
        .otherwise("LOW"))
)

# ─────────────────────────────────────────────
# Sink 1 — consola
# ─────────────────────────────────────────────

query_console = (
    df_final.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", False)
    .start()
)

# ─────────────────────────────────────────────
# Sink 2 — parquet para reentrenamiento
# ─────────────────────────────────────────────

query_sink = (
    df_ml
    .withColumn("label",
        when(col("ml_score") >= 70, 2.0)
        .when(col("ml_score") >= 40, 1.0)
        .otherwise(0.0)
    )
    .select(
        "user_id", "amount", "transaction_type",
        "currency", "destination_id", "ip_address",
        "created_at", "label", "hour"
    )
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "s3://fraud-detection-992382522951/training-data/")
    .option("checkpointLocation", "s3://fraud-detection-992382522951/checkpoints/sink/")
    .partitionBy("hour")
    .start()
)

# ─────────────────────────────────────────────
# Await
# ─────────────────────────────────────────────

query_console.awaitTermination()