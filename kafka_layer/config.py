import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'client.id': os.getenv('KAFKA_CLIENT_ID', 'streamflux-client'),
    'auto.offset.reset': os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
}

# Define Kafka topics
TOPICS = {
    'SENSOR_DATA': 'sensor-data',
    'SOCIAL_DATA': 'social-data',
    'MARKET_DATA': 'market-data',
    'PROCESSED_DATA': 'processed-data',
    'ANOMALIES': 'detected-anomalies',
    'ERRORS': 'processing-errors'
}