import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, LongType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Spark Session
try:
    spark = SparkSession.builder \
    .appName("CustomerChurnAnalysis") \
    .config("spark.jars", "../ICDMAI-Hack/dependencies/spark-sql-kafka-0-10_2.12-3.4.0.jar,../ICDMAI-Hack/dependencies/spark-token-provider-kafka-0-10_2.12-3.4.0.jar") \
    .getOrCreate()
    logging.info("Spark session created successfully.")
except Exception as e:
    logging.error(f"Error creating Spark session: {e}")
    raise

# Define Kafka source
kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
try:
    raw_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", "app_activity") \
        .load()
    logging.info("Kafka source initialized successfully.")
except Exception as e:
    logging.error(f"Error initializing Kafka source: {e}")
    raise

# Define Schema
schema = StructType() \
    .add("customer_id", StringType()) \
    .add("event", StringType()) \
    .add("timestamp", LongType())

# Parse JSON messages
try:
    parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    logging.info("JSON parsing and schema application completed.")
except Exception as e:
    logging.error(f"Error parsing JSON messages: {e}")
    raise

# Transform data (e.g., aggregation)
try:
    agg_df = parsed_df.groupBy("customer_id").count()
    logging.info("Data transformation completed.")
except Exception as e:
    logging.error(f"Error during data transformation: {e}")
    raise

# Write processed data to console with logging
try:
    query = agg_df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda batch_df, _: logging.info(batch_df.collect())) \
        .start()
    logging.info("Streaming query started successfully.")
    query.awaitTermination()
except Exception as e:
    logging.error(f"Error in streaming query: {e}")
    raise
