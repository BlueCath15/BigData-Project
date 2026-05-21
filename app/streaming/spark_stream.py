from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, to_timestamp, when, lit, expr, abs
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)

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
# Data Processing (complex rules) (REDUCE)
# ─────────────────────────────────────────────
df_final = (
    df_rules
    .groupBy(
        window(col("created_at"), "1 hour", "5 minutes"),
        col("user_id")
    )
    .agg(
        count("*").alias("tx_count"),
        expr("sum(map_score)").alias("map_score_sum")
    )
    .withColumn(
        "score_frequency",
        when(col("tx_count") >= 5, 30)
        .when(col("tx_count") >= 3, 15)
        .otherwise(0)
    )
    .withColumn(
        "total_score",
        expr("least(map_score_sum + score_frequency, 100)")
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