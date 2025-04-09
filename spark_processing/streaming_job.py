import os
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

# Import local modules
from config import SPARK_CONFIG, KAFKA_SPARK_CONFIG, MONGODB_CONFIG
from transformations import apply_transformations
from aggregations import apply_aggregations

# Configure logging
logging.basicConfig(
    level=getattr(logging, SPARK_CONFIG['log_level']),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StreamingJob:
    """
    Main Spark Streaming job for processing data from Kafka
    """
    
    def __init__(self):
        """Initialize Spark session and configurations"""
        # Create Spark session
        self.spark = SparkSession.builder \
            .appName(SPARK_CONFIG['app_name']) \
            .master(SPARK_CONFIG['master']) \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.mongodb.input.uri", MONGODB_CONFIG['connection_string']) \
            .config("spark.mongodb.output.uri", MONGODB_CONFIG['connection_string']) \
            .getOrCreate()
        
        # Set log level
        self.spark.sparkContext.setLogLevel(SPARK_CONFIG['log_level'])
        
        # Create checkpoint directory if it doesn't exist
        os.makedirs(SPARK_CONFIG['checkpoint_dir'], exist_ok=True)
        
        logger.info("Spark Streaming job initialized")
        
    def read_from_kafka(self):
        """Read data from Kafka topics"""
        # Define schema for incoming messages
        schema = StructType([
            StructField("source", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("is_anomaly", BooleanType(), True),
            # Message-specific fields will be handled in transformations
            StructField("value", StringType(), True)
        ])
        
        # Read from Kafka as a streaming DataFrame
        kafka_df = self.spark.readStream \
            .format("kafka") \
            .options(**KAFKA_SPARK_CONFIG) \
            .load()
        
        # Parse the value column as JSON
        parsed_df = kafka_df \
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "timestamp") \
            .withColumn("parsed_value", from_json(col("value"), schema)) \
            .select("topic", "timestamp", "parsed_value.*")
        
        logger.info("Created streaming DataFrame from Kafka")
        return parsed_df
    
    def process_data(self, df):
        """Process the data stream with transformations and aggregations"""
        # Apply transformations based on data source
        transformed_df = apply_transformations(df)
        
        # Apply aggregations for analytics
        aggregated_df = apply_aggregations(transformed_df)
        
        logger.info("Applied transformations and aggregations to data stream")
        return transformed_df, aggregated_df
    
    def write_to_mongodb(self, transformed_df, collection_name):
        """Write transformed data to MongoDB"""
        # Write the streaming DataFrame to MongoDB
        mongo_query = transformed_df.writeStream \
            .outputMode("append") \
            .foreachBatch(lambda batch_df, batch_id: 
                batch_df.write \
                    .format("mongo") \
                    .mode("append") \
                    .option("database", MONGODB_CONFIG['database']) \
                    .option("collection", f"{MONGODB_CONFIG['collection_prefix']}{collection_name}") \
                    .save()
            ) \
            .option("checkpointLocation", f"{SPARK_CONFIG['checkpoint_dir']}/mongodb_{collection_name}") \
            .start()
        
        logger.info(f"Started MongoDB write stream to collection {collection_name}")
        return mongo_query
    
    def write_to_kafka(self, df, topic):
        """Write data back to Kafka for further processing or alerts"""
        # Convert DataFrame to JSON string and write to Kafka
        kafka_query = df \
            .selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_SPARK_CONFIG['bootstrap.servers']) \
            .option("topic", topic) \
            .option("checkpointLocation", f"{SPARK_CONFIG['checkpoint_dir']}/kafka_{topic}") \
            .start()
        
        logger.info(f"Started Kafka write stream to topic {topic}")
        return kafka_query
    
    def run(self):
        """Run the streaming job"""
        try:
            # Read from Kafka
            input_df = self.read_from_kafka()
            
            # Process the data
            transformed_df, aggregated_df = self.process_data(input_df)
            
            # Write transformed data to MongoDB
            mongo_query = self.write_to_mongodb(transformed_df, "processed_data")
            
            # Write aggregated data to MongoDB
            agg_mongo_query = self.write_to_mongodb(aggregated_df, "aggregated_data")
            
            # Write anomalies to Kafka for alerting
            anomalies_df = transformed_df.filter("is_anomaly = true")
            kafka_query = self.write_to_kafka(anomalies_df, "detected-anomalies")
            
            # Await termination for the streaming queries
            mongo_query.awaitTermination()
            agg_mongo_query.awaitTermination()
            kafka_query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in streaming job: {str(e)}")
            raise
        finally:
            self.spark.stop()
            logger.info("Spark Streaming job stopped")

if __name__ == "__main__":
    # Run the streaming job
    streaming_job = StreamingJob()
    streaming_job.run()