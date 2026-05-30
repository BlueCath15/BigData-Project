from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, when, col
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorAssembler

spark = SparkSession.builder.appName("TrainFraudModel").getOrCreate()

# training_df debe tener: amount, transaction_type, destination_id, user_id, created_at, label
training_df = spark.read.parquet("/app/app/training_data.parquet")

# ── Las mismas features que calcula el streaming ──
training_df = (
    training_df
    .withColumn("hour", hour(col("created_at")))
    .withColumn(
        "dest_external",
        when(
            col("destination_id").isNotNull() & (col("destination_id") != col("user_id")),
            1.0
        ).otherwise(0.0)
    )
)

# ── Pipeline idéntico al que espera el streaming ──
indexer = StringIndexer(
    inputCol="transaction_type",
    outputCol="tx_type_idx",
    handleInvalid="keep"        # ← importante: evita crash con tipos nuevos en prod
)

assembler = VectorAssembler(
    inputCols=["amount", "tx_type_idx", "dest_external", "hour"],
    outputCol="features"
)

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",           # 0=approved, 1=flagged, 2=blocked
    numTrees=100,
    maxDepth=10,
    probabilityCol="probability"
)

pipeline = Pipeline(stages=[indexer, assembler, rf])
model = pipeline.fit(training_df)
model.save("/app/app/fraud_rf_model")
