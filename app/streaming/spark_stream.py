from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, to_timestamp, when, lit, expr, abs, pandas_udf, hour as spark_hour
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)

import pandas as pd
import numpy as np
import io
import torch
import torch.nn as nn
import joblib

import sys
sys.path.insert(0, "/app")


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
# Modelo ML — broadcast a workers                                    # ← NUEVO
# ─────────────────────────────────────────────

model_bytes = open("/app/app/model.pt", "rb").read()                # ← NUEVO
scaler      = joblib.load("/app/app/scaler.pkl")                    # ← NUEVO

bc_model_bytes = spark.sparkContext.broadcast(model_bytes)          # ← NUEVO
bc_scaler      = spark.sparkContext.broadcast(scaler)               # ← NUEVO

# ─────────────────────────────────────────────
# UDF de inferencia ML (MAP por partition)                           # ← NUEVO
# ─────────────────────────────────────────────

TX_TYPE_MAP = {"TRANSFER": 0, "PAYMENT": 1, "WITHDRAWAL": 2, "DEPOSIT": 3}  # ← NUEVO

@pandas_udf(DoubleType())                                           # ← NUEVO
def ml_score_udf(                                                   # ← NUEVO
    amount:         pd.Series,                                      # ← NUEVO
    tx_type:        pd.Series,                                      # ← NUEVO
    user_id:        pd.Series,                                      # ← NUEVO
    destination_id: pd.Series,                                      # ← NUEVO
    hour:           pd.Series,                                      # ← NUEVO
) -> pd.Series:           
    import sys                          
    sys.path.insert(0, "/app")          
    from app.ml_service import FraudModel                                      
    # Reconstruir modelo desde broadcast en cada worker             # ← NUEVO
    buf   = io.BytesIO(bc_model_bytes.value)                        # ← NUEVO
    model = FraudModel()                                            # ← NUEVO
    model.load_state_dict(                                          # ← NUEVO
        torch.load(buf, map_location="cpu", weights_only=True)      # ← NUEVO
    )                                                               # ← NUEVO
    model.eval()                                                    # ← NUEVO
    scaler = bc_scaler.value                                        # ← NUEVO

    tx_int   = tx_type.map(TX_TYPE_MAP).fillna(1).astype("float32") # ← NUEVO
    dest_ext = (                                                    # ← NUEVO
        destination_id.notna() & (destination_id != user_id)       # ← NUEVO
    ).astype("float32")                                             # ← NUEVO

    # Matriz (N, 5) — todas las transacciones del batch juntas      # ← NUEVO
    X = np.stack([                                                  # ← NUEVO
        amount.values.astype("float32"),                            # ← NUEVO
        tx_int.values,                                              # ← NUEVO
        dest_ext.values,                                            # ← NUEVO
        hour.values.astype("float32"),                              # ← NUEVO
        np.zeros(len(amount), dtype="float32"),  # recent_tx_count se cubre en REDUCE
    ], axis=1)                                                      # ← NUEVO

    X_scaled = scaler.transform(X).astype("float32")               # ← NUEVO

    with torch.no_grad():                                           # ← NUEVO
        probs = torch.softmax(                                      # ← NUEVO
            model(torch.tensor(X_scaled)), dim=1                    # ← NUEVO
        ).numpy()                                                   # ← NUEVO

    # 0=approved→0pts  1=flagged→50pts  2=blocked→100pts            # ← NUEVO
    scores = np.clip(probs[:, 1] * 50 + probs[:, 2] * 100, 0, 100) # ← NUEVO
    return pd.Series(scores.round(2))                               # ← NUEVO


# ─────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────

schema = StructType([
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("transaction_type", StringType()),
    StructField("currency", StringType()),
    StructField("destination_id", StringType()),
    StructField("ip_address", StringType()),
    StructField("created_at", TimestampType())
])

# ─────────────────────────────────────────────
# Read Kafka Stream
# ─────────────────────────────────────────────

df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "latest")
    .load()
)

# ─────────────────────────────────────────────
# Convert bytes → string
# ─────────────────────────────────────────────

df_string = df_raw.selectExpr(
    "CAST(value AS STRING)"
)

# ─────────────────────────────────────────────
# Parse JSON
# ─────────────────────────────────────────────

df = (
    df_string
    .select(
        from_json(
            col("value"),
            schema
        ).alias("data")
    )
    .select("data.*")
)

# ─────────────────────────────────────────────
# Data Processing (simple rules) (MAP)
# ─────────────────────────────────────────────

df_rules = (
    df
    .withColumn(
        "high_amount",
        when(col("amount") > 10000, 40)
        .when(col("amount") > 5000, 20)
        .otherwise(0)
    )
    .withColumn(
        "round_sus",
        when(
            (col("amount").isin(999, 4999, 9999, 49999, 99999)) |
            ((col("amount") % 1000) >= 990),
            15
        ).otherwise(0)
    )
    .withColumn(
        "struct_sus",
        when(
            (col("amount") >= 10000 ) & (col("amount") % 1000 == 0), 15
        ).otherwise(0)
    )
    .withColumn(
        "external_transfer",
        when(
            (col("transaction_type").isin("TRANSFER", "WITHDRAWAL")) &
            (col("destination_id").isNotNull()) &
            (col("destination_id") != col("user_id")),
            10
        ).otherwise(0)
    )
    .withColumn(
        "map_score",
        expr("high_amount + round_sus + struct_sus + external_transfer")
    )
)

# ─────────────────────────────────────────────
# Data Processing (ML inference) (MAP)                               # ← NUEVO
# ─────────────────────────────────────────────

df_ml = (                                                           # ← NUEVO
    df_rules                                                        # ← NUEVO
    .withColumn("hour", spark_hour(col("created_at")))              # ← NUEVO
    .withColumn(                                                    # ← NUEVO
        "ml_score",                                                 # ← NUEVO
        ml_score_udf(                                               # ← NUEVO
            col("amount"),                                          # ← NUEVO
            col("transaction_type"),                                # ← NUEVO
            col("user_id"),                                         # ← NUEVO
            col("destination_id"),                                  # ← NUEVO
            col("hour"),                                            # ← NUEVO
        )                                                           # ← NUEVO
    )                                                               # ← NUEVO
)      

# ─────────────────────────────────────────────
# Data Processing (complex rules) (REDUCE)
# ─────────────────────────────────────────────
df_final = (
    df_ml                                                           # ← NUEVO (era df_rules)
    .groupBy(
        window(col("created_at"), "1 hour", "5 minutes"),
        col("user_id")
    )
    .agg(
        count("*").alias("tx_count"),
        expr("sum(map_score)").alias("map_score_sum"),
        expr("avg(ml_score)").alias("ml_score_avg")                 # ← NUEVO
    )
    .withColumn(
        "score_frequency",
        when(col("tx_count") >= 5, 30)
        .when(col("tx_count") >= 3, 15)
        .otherwise(0)
    )
    .withColumn(                                                    # ← NUEVO (era total_score directo)
        "rules_score",                                              # ← NUEVO
        expr("least(map_score_sum + score_frequency, 100)")         # ← NUEVO
    )                                                               # ← NUEVO
    .withColumn(
        "total_score",
        expr("least(rules_score * 0.5 + ml_score_avg * 0.5, 100)") # ← NUEVO (era solo map_score_sum)
    )
    .withColumn(
        "risk_level",
        when(col("total_score") >= 76, "CRITICAL")
        .when(col("total_score") >= 51, "HIGH")
        .when(col("total_score") >= 26, "MEDIUM")
        .otherwise("LOW")
    )
)


# ─────────────────────────────────────────────
# Output to console
# ─────────────────────────────────────────────

query = (
    df_final.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", False)
    .start()
)

query.awaitTermination()