from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

class ClickstreamAnalyzer:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("ClickstreamAnalysis") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.0") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Define schema for clickstream data
        self.schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("page_url", StringType(), True),
            StructField("referrer", StringType(), True),
            StructField("user_agent", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("country", StringType(), True),
            StructField("city", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("duration_seconds", IntegerType(), True),
            StructField("is_bounce", BooleanType(), True),
            StructField("conversion_value", DoubleType(), True)
        ])

    def read_kafka_stream(self):
        """Read streaming data from Kafka"""
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "clickstream") \
            .option("startingOffsets", "latest") \
            .load()

    def process_clickstream(self):
        """Process and analyze clickstream data"""
        # Read from Kafka
        df = self.read_kafka_stream()
        
        # Parse JSON data
        clickstream_df = df.select(
            from_json(col("value").cast("string"), self.schema).alias("data")
        ).select("data.*")
        
        # Add processing timestamp and convert timestamp
        processed_df = clickstream_df.withColumn(
            "processing_time", current_timestamp()
        ).withColumn(
            "event_timestamp", to_timestamp(col("timestamp"))
        )
        
        return processed_df

    def analyze_page_views(self, df):
        """Analyze page views in real-time"""
        page_views = df.groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("page_url")
        ).count().orderBy(desc("count"))
        
        query = page_views.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query

    def analyze_device_distribution(self, df):
        """Analyze device type distribution"""
        device_stats = df.groupBy(
            window(col("event_timestamp"), "2 minutes"),
            col("device_type")
        ).agg(
            count("*").alias("total_clicks"),
            avg("duration_seconds").alias("avg_duration"),
            sum("conversion_value").alias("total_conversion")
        )
        
        query = device_stats.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime="60 seconds") \
            .start()
        
        return query

    def analyze_bounce_rate(self, df):
        """Calculate bounce rate by referrer"""
        bounce_analysis = df.groupBy(
            window(col("event_timestamp"), "5 minutes"),
            col("referrer")
        ).agg(
            count("*").alias("total_visits"),
            sum(when(col("is_bounce"), 1).otherwise(0)).alias("bounces"),
            (sum(when(col("is_bounce"), 1).otherwise(0)) * 100.0 / count("*")).alias("bounce_rate")
        ).orderBy(desc("bounce_rate"))
        
        query = bounce_analysis.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime="120 seconds") \
            .start()
        
        return query

    def start_analysis(self):
        """Start all analysis streams"""
        print("Starting clickstream analysis...")
        
        # Process clickstream
        df = self.process_clickstream()
        
        # Start different analysis streams
        page_views_query = self.analyze_page_views(df)
        device_query = self.analyze_device_distribution(df)
        bounce_query = self.analyze_bounce_rate(df)
        
        try:
            # Wait for all streams
            page_views_query.awaitTermination()
            device_query.awaitTermination()
            bounce_query.awaitTermination()
        except KeyboardInterrupt:
            print("Stopping analysis...")
        finally:
            self.spark.stop()

if __name__ == "__main__":
    analyzer = ClickstreamAnalyzer()
    analyzer.start_analysis()