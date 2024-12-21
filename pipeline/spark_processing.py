import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, LongType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Spark Session
try:
    spark = SparkSession.builder.appName("CustomerChurnAnalysis").getOrCreate()
except Exception as e:
    print(f"Error creating Spark session: {e}")
    # Handle exception (e.g., retry or exit)

# Define Kafka source
kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
raw_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_servers).option("subscribe", "app_activity").load()

# Define Schema
schema = StructType().add("customer_id", StringType()).add("event", StringType()).add("timestamp", LongType())

# Parse JSON messages
parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Transform data (e.g., aggregation)
agg_df = parsed_df.groupBy("customer_id").count()

# Write processed data to console with logging
query = agg_df.writeStream.outputMode("append").foreachBatch(lambda batch_df, _: logging.info(batch_df.collect())).start()

query.awaitTermination()