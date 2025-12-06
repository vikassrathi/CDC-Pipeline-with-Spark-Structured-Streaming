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

print("--- SILVER TABLE (Latest State) ---")
spark.read.format("delta").load("data_lake/silver/transactions").show()