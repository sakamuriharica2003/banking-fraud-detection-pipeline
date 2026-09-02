from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.hadoop.fs.s3a.endpoint.region", "ap-southeast-2") \
    .getOrCreate()

schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("card_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("merchant_category", StringType()) \
    .add("location", StringType()) \
    .add("timestamp", TimestampType())

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load()

parsed = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Simple fraud rule: flag any transaction over $3000
flagged = parsed.filter(col("amount") > 3000)

# Write ALL transactions to S3 (raw zone)
raw_query = parsed.writeStream \
    .format("parquet") \
    .option("path", "s3a://haricafrauddetection/raw/") \
    .option("checkpointLocation", "/tmp/checkpoints/raw") \
    .outputMode("append") \
    .start()

# Write FLAGGED transactions to S3 (flagged zone)
flagged_query = flagged.writeStream \
    .format("parquet") \
    .option("path", "s3a://haricafrauddetection/flagged/") \
    .option("checkpointLocation", "/tmp/checkpoints/flagged") \
    .outputMode("append") \
    .start()

raw_query.awaitTermination()