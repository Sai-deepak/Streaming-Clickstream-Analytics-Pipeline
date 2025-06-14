from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import pandas as pd

class BatchClickstreamAnalyzer:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("BatchClickstreamAnalysis") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")

    def load_historical_data(self, path="clickstream_data.json"):
        """Load historical clickstream data"""
        return self.spark.read.json(path)

    def user_journey_analysis(self, df):
        """Analyze user journey patterns"""
        print("=== User Journey Analysis ===")
        
        # Most common page sequences
        page_sequences = df.groupBy("session_id").agg(
            collect_list("page_url").alias("page_sequence"),
            count("*").alias("page_count"),
            sum("duration_seconds").alias("total_duration")
        ).filter(col("page_count") > 1)
        
        # Common entry and exit pages
        entry_pages = df.groupBy("session_id").agg(
            first("page_url").alias("entry_page")
        ).groupBy("entry_page").count().orderBy(desc("count"))
        
        exit_pages = df.groupBy("session_id").agg(
            last("page_url").alias("exit_page")
        ).groupBy("exit_page").count().orderBy(desc("count"))
        
        print("Top Entry Pages:")
        entry_pages.show(10)
        
        print("Top Exit Pages:")
        exit_pages.show(10)
        
        return page_sequences, entry_pages, exit_pages

    def conversion_analysis(self, df):
        """Analyze conversion patterns"""
        print("=== Conversion Analysis ===")
        
        # Conversion by referrer
        conversion_by_referrer = df.groupBy("referrer").agg(
            count("*").alias("total_visits"),
            sum("conversion_value").alias("total_revenue"),
            avg("conversion_value").alias("avg_conversion"),
            sum(when(col("conversion_value") > 0, 1).otherwise(0)).alias("conversions")
        ).withColumn(
            "conversion_rate", 
            col("conversions") * 100.0 / col("total_visits")
        ).orderBy(desc("conversion_rate"))
        
        print("Conversion by Referrer:")
        conversion_by_referrer.show()
        
        # Conversion by device type
        conversion_by_device = df.groupBy("device_type").agg(
            count("*").alias("total_visits"),
            sum("conversion_value").alias("total_revenue"),
            avg("conversion_value").alias("avg_conversion")
        ).orderBy(desc("total_revenue"))
        
        print("Conversion by Device:")
        conversion_by_device.show()
        
        return conversion_by_referrer, conversion_by_device

    def time_based_analysis(self, df):
        """Analyze patterns by time"""
        print("=== Time-based Analysis ===")
        
        # Add time components
        df_with_time = df.withColumn(
            "hour", hour(to_timestamp(col("timestamp")))
        ).withColumn(
            "day_of_week", dayofweek(to_timestamp(col("timestamp")))
        )
        
        # Traffic by hour
        hourly_traffic = df_with_time.groupBy("hour").agg(
            count("*").alias("total_clicks"),
            avg("duration_seconds").alias("avg_duration")
        ).orderBy("hour")
        
        print("Hourly Traffic Pattern:")
        hourly_traffic.show(24)
        
        # Traffic by day of week
        daily_traffic = df_with_time.groupBy("day_of_week").agg(
            count("*").alias("total_clicks"),
            avg("duration_seconds").alias("avg_duration")
        ).orderBy("day_of_week")
        
        print("Daily Traffic Pattern:")
        daily_traffic.show()
        
        return hourly_traffic, daily_traffic

    def create_visualizations(self, hourly_traffic, conversion_by_device):
        """Create visualizations using matplotlib"""
        print("Creating visualizations...")
        
        # Convert to Pandas for plotting
        hourly_pd = hourly_traffic.toPandas()
        device_pd = conversion_by_device.toPandas()
        
        # Create plots
        fig, axes = plt.subplots(2, 1, figsize=(12, 8))
        
        # Hourly traffic plot
        axes[0].plot(hourly_pd['hour'], hourly_pd['total_clicks'], marker='o')
        axes[0].set_title('Hourly Traffic Pattern')
        axes[0].set_xlabel('Hour of Day')
        axes[0].set_ylabel('Total Clicks')
        axes[0].grid(True)
        
        # Device revenue plot
        axes[1].bar(device_pd['device_type'], device_pd['total_revenue'])
        axes[1].set_title('Revenue by Device Type')
        axes[1].set_xlabel('Device Type')
        axes[1].set_ylabel('Total Revenue')
        
        plt.tight_layout()
        plt.savefig('clickstream_analysis.png')
        plt.show()

    def run_batch_analysis(self):
        """Run complete batch analysis"""
        print("Starting batch clickstream analysis...")
        
        # For demo, we'll create some sample data
        # In practice, you'd load from your data lake/warehouse
        sample_data = self.create_sample_data()
        
        # Run analyses
        page_sequences, entry_pages, exit_pages = self.user_journey_analysis(sample_data)
        conversion_by_referrer, conversion_by_device = self.conversion_analysis(sample_data)
        hourly_traffic, daily_traffic = self.time_based_analysis(sample_data)
        
        # Create visualizations
        self.create_visualizations(hourly_traffic, conversion_by_device)
        
        print("Batch analysis complete!")

    def create_sample_data(self):
        """Create sample data for demonstration"""
        from datetime import datetime, timedelta
        import random
        
        # Generate sample data
        data = []
        for i in range(1000):
            data.append({
                'timestamp': (datetime.now() - timedelta(hours=random.randint(0, 168))).isoformat(),
                'user_id': f'user_{random.randint(1, 100)}',
                'session_id': f'session_{random.randint(1, 200)}',
                'page_url': random.choice(['/home', '/products', '/cart', '/checkout', '/profile']),
                'referrer': random.choice(['google.com', 'facebook.com', 'direct']),
                'device_type': random.choice(['desktop', 'mobile', 'tablet']),
                'duration_seconds': random.randint(10, 300),
                'conversion_value': random.uniform(0, 100) if random.random() < 0.15 else 0
            })
        
        return self.spark.createDataFrame(data)

if __name__ == "__main__":
    analyzer = BatchClickstreamAnalyzer()
    analyzer.run_batch_analysis()
