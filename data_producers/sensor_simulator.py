import json
import random
import time
from datetime import datetime
import uuid
import sys
import os

# Add root directory to path to import kafka_layer module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_layer.producer import KafkaProducer

class SensorSimulator:
    """Simulates IoT sensor data streams"""
    
    def __init__(self, num_sensors=5, kafka_topic="sensor-data"):
        self.num_sensors = num_sensors
        self.kafka_topic = kafka_topic
        self.producer = KafkaProducer()
        self.sensor_types = ["temperature", "humidity", "pressure", "light", "motion"]
        self.locations = ["building-a", "building-b", "building-c", "warehouse", "outdoor"]
        self.sensors = self._initialize_sensors()
        
    def _initialize_sensors(self):
        """Create a set of sensor configurations"""
        sensors = []
        for i in range(self.num_sensors):
            sensor_type = random.choice(self.sensor_types)
            location = random.choice(self.locations)
            
            # Define normal ranges based on sensor type
            if sensor_type == "temperature":
                normal_range = (15.0, 30.0)
                unit = "°C"
            elif sensor_type == "humidity":
                normal_range = (30.0, 70.0)
                unit = "%"
            elif sensor_type == "pressure":
                normal_range = (980.0, 1020.0)
                unit = "hPa"
            elif sensor_type == "light":
                normal_range = (0.0, 1000.0)
                unit = "lux"
            elif sensor_type == "motion":
                normal_range = (0.0, 1.0)
                unit = "boolean"
            
            sensor_id = f"{sensor_type}-{location}-{uuid.uuid4().hex[:8]}"
            
            sensors.append({
                "sensor_id": sensor_id,
                "sensor_type": sensor_type,
                "location": location,
                "normal_range": normal_range,
                "unit": unit,
                "anomaly_probability": 0.05  # 5% chance of generating anomalous data
            })
        
        return sensors
    
    def generate_sensor_reading(self, sensor):
        """Generate a single sensor reading"""
        timestamp = datetime.now().isoformat()
        anomaly = random.random() < sensor["anomaly_probability"]
        
        if anomaly:
            # Generate an anomalous value outside the normal range
            lower, upper = sensor["normal_range"]
            if random.choice([True, False]):
                # Go below range
                value = lower - random.uniform(lower * 0.1, lower * 0.5)
            else:
                # Go above range
                value = upper + random.uniform(upper * 0.1, upper * 0.5)
        else:
            # Generate a normal value within range
            lower, upper = sensor["normal_range"]
            value = random.uniform(lower, upper)
            
        # For motion sensors, convert to binary value
        if sensor["sensor_type"] == "motion":
            value = 1 if value > 0.5 else 0
        
        return {
            "sensor_id": sensor["sensor_id"],
            "sensor_type": sensor["sensor_type"],
            "location": sensor["location"],
            "timestamp": timestamp,
            "value": round(value, 2),
            "unit": sensor["unit"],
            "is_anomaly": anomaly,
            "source": "sensor_simulator"
        }
    
    def run(self, interval=1.0, duration=None):
        """
        Run the simulator to generate and send sensor data.
        
        Args:
            interval: Time between readings in seconds
            duration: How long to run in seconds, None for indefinite
        """
        start_time = time.time()
        count = 0
        
        try:
            print(f"Starting sensor simulator with {self.num_sensors} sensors...")
            while duration is None or time.time() - start_time < duration:
                for sensor in self.sensors:
                    reading = self.generate_sensor_reading(sensor)
                    
                    # Send to Kafka
                    self.producer.send(self.kafka_topic, reading)
                    
                    # Print sample data occasionally
                    if count % 50 == 0:
                        print(f"Sample sensor data: {json.dumps(reading, indent=2)}")
                    
                    count += 1
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("Sensor simulator stopped by user")
        finally:
            self.producer.close()
            print(f"Sensor simulator finished after sending {count} readings")