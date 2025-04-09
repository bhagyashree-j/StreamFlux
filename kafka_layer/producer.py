import json
import logging
from kafka import KafkaProducer
from .config import KAFKA_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaProducer:
    """Kafka producer for sending messages to Kafka topics"""
    
    def __init__(self, bootstrap_servers=None):
        """Initialize the Kafka producer"""
        servers = bootstrap_servers or KAFKA_CONFIG['bootstrap.servers']
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else json.dumps(k).encode('utf-8')
            )
            logger.info(f"Kafka producer connected to {servers}")
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {str(e)}")
            raise
    
    def send(self, topic, value, key=None, partition=None, headers=None):
        """Send a message to a Kafka topic"""
        try:
            future = self.producer.send(
                topic=topic,
                value=value,
                key=key,
                partition=partition,
                headers=headers
            )
            # Wait for message to be sent (optional, can be removed for higher throughput)
            # future.get(timeout=10)
            return future
        except Exception as e:
            logger.error(f"Error sending message to topic {topic}: {str(e)}")
            raise
    
    def flush(self):
        """Flush all pending messages"""
        self.producer.flush()
    
    def close(self):
        """Close the producer connection"""
        self.producer.close()