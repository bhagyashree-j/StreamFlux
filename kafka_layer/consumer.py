import json
import logging
from kafka.consumer import KafkaConsumer
from .config import KAFKA_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaConsumer:
    """Kafka consumer for consuming messages from Kafka topics"""
    
    def __init__(self, topics, group_id, bootstrap_servers=None, auto_offset_reset='earliest'):
        """Initialize the Kafka consumer"""
        servers = bootstrap_servers or KAFKA_CONFIG['bootstrap.servers']
        
        try:
            self.consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=servers,
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None
            )
            logger.info(f"Kafka consumer connected to {servers}, topics: {topics}")
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {str(e)}")
            raise
    
    def consume(self, timeout_ms=1000):
        """Consume messages from subscribed topics"""
        try:
            return self.consumer.poll(timeout_ms=timeout_ms)
        except Exception as e:
            logger.error(f"Error polling messages: {str(e)}")
            raise
    
    def consume_batch(self, batch_size=100, timeout_ms=1000):
        """Consume a batch of messages"""
        messages = []
        poll_result = self.consume(timeout_ms)
        
        for topic_partition, partition_messages in poll_result.items():
            for message in partition_messages:
                messages.append({
                    'topic': topic_partition.topic,
                    'partition': topic_partition.partition,
                    'offset': message.offset,
                    'key': message.key,
                    'value': message.value,
                    'timestamp': message.timestamp
                })
                
                if len(messages) >= batch_size:
                    break
        
        return messages
    
    def subscribe(self, topics):
        """Subscribe to additional topics"""
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to additional topics: {topics}")
    
    def close(self):
        """Close the consumer connection"""
        self.consumer.close()