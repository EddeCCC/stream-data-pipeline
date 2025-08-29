import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_date, date_format
from pyspark.sql.types import StructType, StringType, BooleanType, LongType, StructField
from util.logger import logging as log

KAFKA_BROKER = "localhost:29092"
TOPIC = "wikimedia.recentchange"
OUTPUT_PATH = "../data/parquet/wikimedia/"
CHECKPOINT_PATH = "../data/parquet/_checkpoints/"

spark = (SparkSession.builder
         .appName("kafka-stream")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") # Has to match pyspark version!
         .getOrCreate())

# Filter for interesting fields
# Also see load/example-event.json
schema = StructType([
    StructField("id", LongType()),
    StructField("type", StringType()),
    StructField("title", StringType()),
    StructField("title_url", StringType()),
    StructField("comment", StringType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("timestamp", LongType()),
    StructField("meta", StructType([
        StructField("domain", StringType())
    ]))
])

def consume_messages():
    """Consume Kafka message and convert to dataframe"""
    df = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", KAFKA_BROKER)
              .option("subscribe", TOPIC)
              .option("startingOffsets", "latest")
              .load())

    df_json = (df.selectExpr("CAST(value AS STRING) as json_str")       # parse to JSON string
               .select(from_json("json_str", schema).alias("data")) # enforce schema
               .select("data.*"))                                       # flatten data (data.id -> id)
    return df_json

def pre_transform(df):
    """Filter data and run pre-transformations"""
    df_filtered = (df.filter(col("meta.domain") != "canary") # remove artificial events
                   .drop("meta")
                   .filter(col("id").isNotNull())
                   )
    return (df_filtered.withColumn("date", to_date(from_unixtime(col("timestamp"))))              # convert long to date
            .withColumn("time", date_format(from_unixtime(col("timestamp")), "HH:mm:ss"))) # convert long to time

def build_query(df):
    """Build query to load data into Parquet"""
    return (df.writeStream
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("date")
        .trigger(processingTime="1 minute")
        .start())

def build_debug_query(df):
    """Build query to load data into console for debugging"""
    return (df.writeStream
             .format("console")
             .outputMode("append")
             .option("truncate", False)
             .start())

if __name__ == "__main__":
    while True:
        log.info("Starting consumer...")
        try:
            df = consume_messages()
            transformed_df = pre_transform(df)
            query = build_query(transformed_df)
            query.awaitTermination()
        except Exception as e:
            log.error(f"Consumer interrupted: {e}")
            time.sleep(5)
