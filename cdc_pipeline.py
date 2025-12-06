from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType
from delta.tables import DeltaTable
import os

# 1. Initialize Spark (Using the compatible 3.5 setup)
spark = SparkSession.builder \
    .appName("LocalCDCPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

BASE_PATH = os.path.abspath("data_lake")
BRONZE_PATH = f"{BASE_PATH}/bronze/transactions"
SILVER_PATH = f"{BASE_PATH}/silver/transactions"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints/transactions"

# 2. Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "cdc.public.transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# --- THE FIX STARTS HERE ---

# 3. Define the Debezium JSON Schema Manually
# Debezium wraps data in a 'payload' object.
# We define the structure of your Postgres table here.
debezium_schema = StructType([
    StructField("payload", StructType([
        StructField("after", StructType([
            StructField("transaction_id", IntegerType()),
            StructField("customer_id", IntegerType()),
            StructField("amount", DoubleType()),
            StructField("status", StringType()),
            # Debezium sends timestamps as Long (microseconds) or String depending on config
            # We use LongType here to be safe, or StringType if you see parsing errors
            StructField("created_at", LongType()),
            StructField("updated_at", LongType())
        ])),
        StructField("op", StringType()),  # Operation: c, u, d
        StructField("ts_ms", LongType())  # Timestamp of the event
    ]))
])

# 4. Parse the JSON
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), debezium_schema).alias("data")
).select(
    col("data.payload.after.*"),  # Flatten the 'after' struct
    col("data.payload.op").alias("operation"),
    col("data.payload.ts_ms").alias("timestamp")
)


# --- THE FIX ENDS HERE ---

# 5. Processing Logic (Micro-Batch)
def upsert_to_silver(micro_batch_df, batch_id):
    # Skip empty batches to avoid errors
    if micro_batch_df.count() == 0:
        return

    # A. Write to Bronze (Raw History)
    micro_batch_df.write \
        .format("delta") \
        .mode("append") \
        .save(BRONZE_PATH)

    # B. Merge into Silver (Upsert)
    # Deduplicate within the micro-batch first
    deduped_batch = micro_batch_df.dropDuplicates(["transaction_id"])

    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        deduped_batch.write.format("delta").save(SILVER_PATH)
    else:
        delta_table = DeltaTable.forPath(spark, SILVER_PATH)
        delta_table.alias("target").merge(
            deduped_batch.alias("source"),
            "target.transaction_id = source.transaction_id"
        ).whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()


# 6. Start the Stream
print("Starting Stream... (Press Ctrl+C to stop)")
query = parsed_df.writeStream \
    .foreachBatch(upsert_to_silver) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()

query.awaitTermination()