import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Spark configuration
SPARK_CONFIG = {
    'app_name': 'StreamFlux',
    'master': 'local[*]',
    'batch_duration': 5,  # in seconds
    'checkpoint_dir': './checkpoints',
    'log_level': os.getenv('LOG_LEVEL', 'INFO')
}

# Kafka configuration for Spark
KAFKA_SPARK_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'subscribe': 'sensor-data,social-data,market-data',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false',
    'maxOffsetsPerTrigger': 10000
}

# MongoDB configuration
MONGODB_CONFIG = {
    'connection_string': os.getenv('MONGODB_CONNECTION_STRING', 'mongodb://localhost:27017'),
    'database': os.getenv('MONGODB_DATABASE', 'streamflux'),
    'collection_prefix': 'streamflux_'
}