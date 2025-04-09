from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

def apply_transformations(df):
    """
    Apply transformations to the data stream based on source type
    
    Args:
        df: Input DataFrame from Kafka
        
    Returns:
        Transformed DataFrame
    """
    # Split the stream by source type
    sensor_df = df.filter("topic = 'sensor-data'")
    social_df = df.filter("topic = 'social-data'")
    market_df = df.filter("topic = 'market-data'")
    
    # Apply source-specific transformations
    transformed_sensor_df = transform_sensor_data(sensor_df)
    transformed_social_df = transform_social_data(social_df)
    transformed_market_df = transform_market_data(market_df)
    
    # Union the transformed DataFrames
    return transformed_sensor_df.unionByName(
        transformed_social_df, allowMissingColumns=True
    ).unionByName(
        transformed_market_df, allowMissingColumns=True
    )

def transform_sensor_data(df):
    """Transform sensor data"""
    if df.isEmpty():
        return df
        
    # Add processing timestamp
    df = df.withColumn("processing_time", current_timestamp())
    
    # Calculate time since the event
    df = df.withColumn(
        "processing_delay_ms", 
        (col("processing_time").cast("long") - col("timestamp").cast("long")) * 1000
    )
    
    # Categorize readings
    df = df.withColumn(
        "reading_category",
        when(col("sensor_type") == "temperature", 
             when(col("value") < 0, "freezing")
             .when(col("value") < 15, "cold")
             .when(col("value") < 25, "moderate")
             .when(col("value") < 35, "warm")
             .otherwise("hot"))
        .when(col("sensor_type") == "humidity",
             when(col("value") < 30, "dry")
             .when(col("value") < 60, "comfortable")
             .otherwise("humid"))
        .otherwise("normal")
    )
    
    return df

def transform_social_data(df):
    """Transform social media data"""
    if df.isEmpty():
        return df
        
    # Add processing timestamp
    df = df.withColumn("processing_time", current_timestamp())
    
    # Calculate time since the event
    df = df.withColumn(
        "processing_delay_ms", 
        (col("processing_time").cast("long") - col("timestamp").cast("long")) * 1000
    )
    
    # Calculate content length
    df = df.withColumn("content_length", length(col("content")))
    
    # Categorize engagement
    df = df.withColumn(
        "engagement_level",
        when(col("engagement_count") < 10, "low")
        .when(col("engagement_count") < 100, "medium")
        .when(col("engagement_count") < 1000, "high")
        .otherwise("viral")
    )
    
    return df

def transform_market_data(df):
    """Transform market data"""
    if df.isEmpty():
        return df
        
    # Add processing timestamp
    df = df.withColumn("processing_time", current_timestamp())
    
    # Calculate time since the event
    df = df.withColumn(
        "processing_delay_ms", 
        (col("processing_time").cast("long") - col("timestamp").cast("long")) * 1000
    )
    
    # Categorize price movement
    df = df.withColumn(
        "price_movement",
        when(col("price_change_percent") < -5.0, "sharp_drop")
        .when(col("price_change_percent") < -1.0, "drop")
        .when(col("price_change_percent") < 1.0, "stable")
        .when(col("price_change_percent") < 5.0, "rise")
        .otherwise("sharp_rise")
    )
    
    # Calculate trading activity level
    df = df.withColumn(
        "trading_activity",
        when(col("volume") < 1000, "low")
        .when(col("volume") < 10000, "medium")
        .otherwise("high")
    )
    
    return df