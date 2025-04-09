from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def apply_aggregations(df):
    """
    Apply aggregations to create analytics from streaming data
    
    Args:
        df: Transformed DataFrame
        
    Returns:
        Aggregated DataFrame with analytics
    """
    # Create a watermark to handle late data
    df_with_watermark = df.withWatermark("timestamp", "1 minute")
    
    # Split by data source for specific aggregations
    sensor_df = df_with_watermark.filter(col("topic") == "sensor-data")
    social_df = df_with_watermark.filter(col("topic") == "social-data")
    market_df = df_with_watermark.filter(col("topic") == "market-data")
    
    # Apply specific aggregations based on data type
    sensor_aggs = aggregate_sensor_data(sensor_df)
    social_aggs = aggregate_social_data(social_df)
    market_aggs = aggregate_market_data(market_df)
    
    # Union all aggregations
    return sensor_aggs.unionByName(
        social_aggs, allowMissingColumns=True
    ).unionByName(
        market_aggs, allowMissingColumns=True
    )

def aggregate_sensor_data(df):
    """Aggregate sensor data"""
    if df.isEmpty():
        return df.select("timestamp", "sensor_type", "location", "value", "is_anomaly")
    
    # Tumbling window aggregations by sensor type and location
    return df.groupBy(
        window(col("timestamp"), "1 minute"),
        col("sensor_type"),
        col("location")
    ).agg(
        avg("value").alias("avg_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value"),
        stddev("value").alias("stddev_value"),
        sum(when(col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
        count("*").alias("reading_count")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sensor_type"),
        col("location"),
        col("avg_value"),
        col("min_value"),
        col("max_value"),
        col("stddev_value"),
        col("anomaly_count"),
        col("reading_count"),
        (col("anomaly_count") / col("reading_count")).alias("anomaly_rate")
    )

def aggregate_social_data(df):
    """Aggregate social media data"""
    if df.isEmpty():
        return df.select("timestamp", "topic", "sentiment", "engagement_count", "is_anomaly")
    
    # Tumbling window aggregations by topic and sentiment
    return df.groupBy(
        window(col("timestamp"), "1 minute"),
        col("topic"),
        col("sentiment")
    ).agg(
        avg("engagement_count").alias("avg_engagement"),
        max("engagement_count").alias("max_engagement"),
        count("*").alias("post_count"),
        sum(when(col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("topic"),
        col("sentiment"),
        col("avg_engagement"),
        col("max_engagement"),
        col("post_count"),
        col("anomaly_count"),
        (col("anomaly_count") / col("post_count")).alias("anomaly_rate")
    )

def aggregate_market_data(df):
    """Aggregate market data"""
    if df.isEmpty():
        return df.select("timestamp", "symbol", "sector", "price", "volume", "is_anomaly")
    
    # Tumbling window aggregations by sector
    return df.groupBy(
        window(col("timestamp"), "1 minute"),
        col("sector")
    ).agg(
        avg("price").alias("avg_price"),
        avg("price_change_percent").alias("avg_price_change_pct"),
        sum("volume").alias("total_volume"),
        count("*").alias("update_count"),
        sum(when(col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sector"),
        col("avg_price"),
        col("avg_price_change_pct"),
        col("total_volume"),
        col("update_count"),
        col("anomaly_count"),
        (col("anomaly_count") / col("update_count")).alias("anomaly_rate")
    )