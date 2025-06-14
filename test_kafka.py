# test_kafka.py - Simple test to verify Kafka connectivity
import json
import time
import threading
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

def test_producer():
    """Test basic Kafka producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=5000
        )
        
        # Send test message
        test_message = {
            'timestamp': time.time(),
            'user_id': 'test_user',
            'page_url': '/test'
        }
        
        future = producer.send('clickstream', value=test_message)
        result = future.get(timeout=10)  # Wait up to 10 seconds
        print(f"Test message sent successfully! Partition: {result.partition}, Offset: {result.offset}")
        producer.close()
        return True
        
    except Exception as e:
        print(f"Producer failed: {e}")
        return False

def test_consumer():
    """Test basic Kafka consumer with timeout"""
    try:
        consumer = KafkaConsumer(
            'clickstream',
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',  # Changed to earliest to read existing messages
            consumer_timeout_ms=10000  # Timeout after 10 seconds
        )
        
        print("Checking for messages (10 second timeout)...")
        message_count = 0
        
        for message in consumer:
            print(f"Received: {message.value}")
            message_count += 1
            if message_count >= 5:  # Stop after 5 messages
                break
        
        if message_count == 0:
            print("No messages found in topic")
        
        consumer.close()
        return message_count > 0
        
    except Exception as e:
        print(f"Consumer failed: {e}")
        return False

def check_kafka_connection():
    """Check if Kafka is running"""
    try:
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
        topics = consumer.topics()
        print(f"Connected to Kafka. Available topics: {list(topics)}")
        consumer.close()
        return True
    except Exception as e:
        print(f"Cannot connect to Kafka: {e}")
        print("Make sure Kafka is running: brew services start kafka")
        return False

def test_with_producer_consumer():
    """Test by producing and consuming simultaneously"""
    def produce_messages():
        time.sleep(2)  # Wait a bit before producing
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        for i in range(3):
            message = {
                'id': i,
                'timestamp': time.time(),
                'message': f'Test message {i}'
            }
            producer.send('clickstream', value=message)
            print(f"Sent message {i}")
            time.sleep(1)
        
        producer.close()
    
    # Start producer in background
    producer_thread = threading.Thread(target=produce_messages)
    producer_thread.start()
    
    # Start consumer
    try:
        consumer = KafkaConsumer(
            'clickstream',
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            consumer_timeout_ms=15000
        )
        
        print("Consumer waiting for messages...")
        for message in consumer:
            print(f"Consumer received: {message.value}")
        
        consumer.close()
        
    except Exception as e:
        print(f"Consumer error: {e}")
    
    producer_thread.join()

if __name__ == "__main__":
    print("=== Testing Kafka Connectivity ===")
    
    # Step 1: Check basic connection
    if not check_kafka_connection():
        exit(1)
    
    print("\n=== Testing Producer ===")
    producer_success = test_producer()
    
    print("\n=== Testing Consumer ===")
    consumer_success = test_consumer()
    
    if not consumer_success:
        print("\n=== Testing with simultaneous producer/consumer ===")
        test_with_producer_consumer()
    
    print("\n=== Test Complete ===")
    if producer_success:
        print("✓ Producer working")
    if consumer_success:
        print("✓ Consumer working")
    
    if producer_success and consumer_success:
        print("✓ Kafka is working correctly!")
    else:
        print("❌ Some issues detected")

# simple_spark_test.py - Test Spark with Kafka
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def test_spark_kafka():
    """Test Spark with Kafka connectivity"""
    
    # Create Spark session with Kafka package
    spark = SparkSession.builder \
        .appName("KafkaTest") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Try to read from Kafka
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "clickstream") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Simple processing - just show the raw data
        query = df.select(
            col("key").cast("string"),
            col("value").cast("string"),
            col("timestamp")
        ).writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .start()
        
        print("Spark-Kafka connection successful! Waiting for data...")
        query.awaitTermination(timeout=30)  # Wait 30 seconds
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    test_spark_kafka()

# fixed_consumer.py - Updated consumer with better error handling
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

class FixedClickstreamAnalyzer:
    def __init__(self):
        # Set JAVA_HOME if needed
        java_home = "/opt/homebrew/opt/openjdk@11"
        if os.path.exists(java_home):
            os.environ["JAVA_HOME"] = java_home
        
        self.spark = SparkSession.builder \
            .appName("ClickstreamAnalysis") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Define schema for clickstream data
        self.schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("page_url", StringType(), True),
            StructField("referrer", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("duration_seconds", IntegerType(), True),
            StructField("conversion_value", DoubleType(), True)
        ])

    def test_kafka_connection(self):
        """Test basic Kafka connectivity"""
        try:
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "clickstream") \
                .option("startingOffsets", "latest") \
                .load()
            
            # Just show raw messages
            query = df.select(
                col("value").cast("string")
            ).writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", False) \
                .trigger(processingTime="10 seconds") \
                .start()
            
            print("Connected to Kafka! Showing raw messages...")
            query.awaitTermination()
            
        except Exception as e:
            print(f"Kafka connection failed: {e}")
            print("Make sure Kafka is running and the topic exists")
        finally:
            self.spark.stop()

    def analyze_simple(self):
        """Simple analysis with error handling"""
        try:
            # Read from Kafka
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "clickstream") \
                .option("startingOffsets", "latest") \
                .load()
            
            # Parse JSON data
            clickstream_df = df.select(
                from_json(col("value").cast("string"), self.schema).alias("data")
            ).select("data.*").filter(col("user_id").isNotNull())
            
            # Simple page view count
            page_views = clickstream_df.groupBy("page_url").count()
            
            query = page_views.writeStream \
                .outputMode("complete") \
                .format("console") \
                .option("truncate", False) \
                .trigger(processingTime="15 seconds") \
                .start()
            
            print("Starting simple analysis...")
            query.awaitTermination()
            
        except Exception as e:
            print(f"Analysis failed: {e}")
        finally:
            self.spark.stop()

if __name__ == "__main__":
    analyzer = FixedClickstreamAnalyzer()
    # First test connection
    print("Testing Kafka connection...")
    analyzer.test_kafka_connection()