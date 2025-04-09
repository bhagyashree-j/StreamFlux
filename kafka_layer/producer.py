import json
import logging
import time
from kafka import KafkaProducer as BaseKafkaProducer
from .config import KAFKA_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaProducer:
    """Kafka producer for sending messages to Kafka topics"""
    
    def __init__(self, bootstrap_servers=None):
        """Initialize the Kafka producer"""
        servers = bootstrap_servers or KAFKA_CONFIG['bootstrap.servers']
        
        try:
            # Create a producer with compatible parameters
            self.producer = BaseKafkaProducer(
                bootstrap_servers=servers,
                # Use custom serializers instead of the keyword args
                api_version=(1, 0, 0)  # Use an older, more compatible API version
            )
            logger.info(f"Kafka producer connected to {servers}")
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {str(e)}")
            self.producer = None
            raise
    
    def send(self, topic, value, key=None, partition=None, headers=None):
        """Send a message to a Kafka topic"""
        try:
            if self.producer is None:
                logger.error("Producer is not initialized")
                return None
                
            # Manually serialize the data
            value_bytes = json.dumps(value).encode('utf-8')
            key_bytes = key.encode('utf-8') if isinstance(key, str) else json.dumps(key).encode('utf-8') if key else None
            
            future = self.producer.send(
                topic=topic,
                value=value_bytes,
                key=key_bytes,
                partition=partition,
                headers=headers
            )
            return future
        except Exception as e:
            logger.error(f"Error sending message to topic {topic}: {str(e)}")
            raise
    
    def flush(self):
        """Flush all pending messages"""
        if self.producer:
            self.producer.flush()
    
    def close(self):
        """Close the producer connection"""
        if self.producer:
            self.producer.close()