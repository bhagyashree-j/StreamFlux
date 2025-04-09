import json
import random
import time
from datetime import datetime
import uuid
import sys
import os
import argparse
import logging
import pymongo
import threading
from bson import ObjectId
from pymongo import MongoClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Custom JSON encoder to handle datetime objects
# Custom JSON encoder to handle datetime objects and MongoDB ObjectId
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, ObjectId):
            return str(obj)  # Convert ObjectId to string
        return super().default(obj)

def json_dumps(data, indent=None):
    """Helper function to dump JSON with datetime handling"""
    return json.dumps(data, indent=indent, cls=MongoJSONEncoder)

class MongoDBConnector:
    """Simplified MongoDB connector"""
    
    def __init__(self, connection_string="mongodb://localhost:27017", database="streamflux"):
        self.connection_string = connection_string
        self.database_name = database
        self.client = None
        self.db = None
        
        # Connect to MongoDB
        self._connect()
    
    def _connect(self):
        """Establish connection to MongoDB"""
        try:
            self.client = MongoClient(self.connection_string)
            
            # Test the connection
            self.client.admin.command('ping')
            
            self.db = self.client[self.database_name]
            logger.info(f"Connected to MongoDB database: {self.database_name}")
            return True
            
        except Exception as e:
            logger.error(f"MongoDB connection failed: {str(e)}")
            self.client = None
            self.db = None
            return False
    
    def insert_one(self, collection_name, document):
        """Insert a single document into a collection"""
        if not self.client:
            logger.error("MongoDB connection not established")
            return None
            
        try:
            # Add timestamp if not present
            if 'created_at' not in document:
                document['created_at'] = datetime.now()
                
            collection = self.db[collection_name]
            result = collection.insert_one(document)
            return str(result.inserted_id)
            
        except Exception as e:
            logger.error(f"Error inserting document into {collection_name}: {str(e)}")
            return None


class SensorSimulator:
    """Simulates IoT sensor data"""
    
    def __init__(self, num_sensors=5, mongodb_connector=None):
        self.num_sensors = num_sensors
        self.mongodb = mongodb_connector
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
        if not self.mongodb:
            logger.error("MongoDB connector not provided. Cannot run simulator.")
            return
            
        start_time = time.time()
        count = 0
        
        try:
            print(f"Starting sensor simulator with {self.num_sensors} sensors...")
            while duration is None or time.time() - start_time < duration:
                for sensor in self.sensors:
                    reading = self.generate_sensor_reading(sensor)
                    
                    # Send to MongoDB
                    self.mongodb.insert_one("sensor_data", reading)
                    
                    # Print sample data occasionally
                    if count % 50 == 0:
                        print(f"Sample sensor data: {json_dumps(reading, indent=2)}")
                    
                    count += 1
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("Sensor simulator stopped by user")
        finally:
            print(f"Sensor simulator finished after sending {count} readings")


class SocialFeedSimulator:
    """Simulates social media data"""
    
    def __init__(self, num_users=10, mongodb_connector=None):
        self.num_users = num_users
        self.mongodb = mongodb_connector
        self.users = self._initialize_users()
        
        # Load sample content for social posts
        self.topics = ["tech", "weather", "sports", "news", "entertainment"]
        self.sentiments = ["positive", "neutral", "negative"]
        
        # Sample words for each sentiment and topic
        self.word_bank = {
            "positive": ["great", "awesome", "excellent", "amazing", "good", "love", "happy", "perfect", "best", "wonderful"],
            "neutral": ["okay", "fine", "regular", "normal", "average", "typical", "standard", "common", "usual", "moderate"],
            "negative": ["bad", "terrible", "awful", "poor", "horrible", "hate", "dislike", "worst", "disappointing", "failure"]
        }
        
        self.topic_words = {
            "tech": ["computer", "software", "app", "technology", "digital", "innovation", "AI", "smartphone", "coding", "device"],
            "weather": ["sunny", "cloudy", "rainy", "storm", "forecast", "temperature", "humidity", "climate", "wind", "weather"],
            "sports": ["game", "team", "player", "score", "win", "lose", "championship", "tournament", "match", "athlete"],
            "news": ["report", "headline", "story", "event", "update", "politics", "economy", "announcement", "breaking", "media"],
            "entertainment": ["movie", "music", "celebrity", "show", "stream", "concert", "performance", "actor", "film", "song"]
        }
        
    def _initialize_users(self):
        """Create a set of simulated social media users"""
        users = []
        for i in range(self.num_users):
            user_id = f"user-{uuid.uuid4().hex[:8]}"
            users.append({
                "user_id": user_id,
                "post_frequency": random.uniform(0.1, 1.0),  # Posts per second on average
                "preferred_topics": random.sample(["tech", "weather", "sports", "news", "entertainment"], 
                                                 k=random.randint(1, 3)),
                "sentiment_bias": random.choice(["positive", "neutral", "negative"]),
                "anomaly_probability": 0.03  # 3% chance of generating anomalous data
            })
        return users
    
    def _generate_post_content(self, user):
        """Generate simulated post content based on user preferences"""
        topic = random.choice(user["preferred_topics"])
        
        # Bias sentiment based on user's typical sentiment
        sentiment_weights = {
            "positive": {"positive": 0.7, "neutral": 0.2, "negative": 0.1},
            "neutral": {"positive": 0.3, "neutral": 0.4, "negative": 0.3},
            "negative": {"positive": 0.1, "neutral": 0.2, "negative": 0.7}
        }
        
        weights = sentiment_weights[user["sentiment_bias"]]
        sentiment = random.choices(
            ["positive", "neutral", "negative"],
            [weights["positive"], weights["neutral"], weights["negative"]]
        )[0]
        
        # Generate a simple post by combining words from the topic and sentiment
        topic_word_count = random.randint(3, 8)
        sentiment_word_count = random.randint(2, 5)
        
        topic_words = random.sample(self.topic_words[topic], min(topic_word_count, len(self.topic_words[topic])))
        sentiment_words = random.sample(self.word_bank[sentiment], min(sentiment_word_count, len(self.word_bank[sentiment])))
        
        all_words = topic_words + sentiment_words
        random.shuffle(all_words)
        
        # Add some common words to make it more readable
        common_words = ["the", "is", "are", "was", "a", "an", "this", "that", "and", "or", "but", "to", "with", "for"]
        for i in range(len(all_words) - 1, 0, -1):
            if random.random() < 0.3:  # 30% chance to insert a common word
                all_words.insert(i, random.choice(common_words))
        
        post = " ".join(all_words)
        # Capitalize first letter and add end punctuation
        post = post[0].upper() + post[1:] + random.choice([".", "!", "?"])
        
        return {
            "content": post,
            "topic": topic,
            "sentiment": sentiment
        }
    
    def generate_social_post(self, user):
        """Generate a single social media post"""
        timestamp = datetime.now().isoformat()
        anomaly = random.random() < user["anomaly_probability"]
        
        # Generate post content
        post_data = self._generate_post_content(user)
        
        # For anomalies, switch the sentiment to its opposite
        if anomaly:
            if post_data["sentiment"] == "positive":
                post_data["sentiment"] = "negative"
            elif post_data["sentiment"] == "negative":
                post_data["sentiment"] = "positive"
            
            # Also make the content more extreme
            extreme_words = {
                "positive": ["ecstatic", "phenomenal", "extraordinary", "revolutionary", "mind-blowing"],
                "negative": ["catastrophic", "disastrous", "horrific", "atrocious", "despicable"]
            }
            
            if post_data["sentiment"] in extreme_words:
                # Add extreme words to the content
                words = post_data["content"].split()
                for _ in range(min(2, len(words))):
                    i = random.randint(0, len(words) - 1)
                    words[i] = random.choice(extreme_words[post_data["sentiment"]])
                post_data["content"] = " ".join(words)
        
        return {
            "user_id": user["user_id"],
            "timestamp": timestamp,
            "content": post_data["content"],
            "topic": post_data["topic"],
            "sentiment": post_data["sentiment"],
            "engagement_count": random.randint(0, 1000),
            "is_anomaly": anomaly,
            "source": "social_feed_simulator"
        }
    
    def run(self, interval=0.1, duration=None):
        """
        Run the simulator to generate and send social media data.
        
        Args:
            interval: Minimum time between posts in seconds
            duration: How long to run in seconds, None for indefinite
        """
        if not self.mongodb:
            logger.error("MongoDB connector not provided. Cannot run simulator.")
            return
            
        start_time = time.time()
        count = 0
        
        try:
            print(f"Starting social feed simulator with {self.num_users} users...")
            while duration is None or time.time() - start_time < duration:
                for user in self.users:
                    # Only generate a post based on the user's posting frequency
                    if random.random() < user["post_frequency"] * interval:
                        post = self.generate_social_post(user)
                        
                        # Send to MongoDB
                        self.mongodb.insert_one("social_data", post)
                        
                        # Print sample data occasionally
                        if count % 50 == 0:
                            print(f"Sample social post: {json_dumps(post, indent=2)}")
                        
                        count += 1
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("Social feed simulator stopped by user")
        finally:
            print(f"Social feed simulator finished after sending {count} posts")


class MarketDataSimulator:
    """Simulates financial market data"""
    
    def __init__(self, num_symbols=20, mongodb_connector=None):
        self.num_symbols = num_symbols
        self.mongodb = mongodb_connector
        self.start_time = datetime.now()
        self.last_market_update = {}
        self.market_symbols = self._initialize_symbols()
        
    def _initialize_symbols(self):
        """Create a set of market symbols with initial prices"""
        symbols = []
        
        # Stock symbol components for generating random symbols
        prefixes = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", 
                   "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"]
        
        # Sectors for categorization
        sectors = ["Technology", "Healthcare", "Finance", "Energy", "Consumer", 
                  "Industrial", "Materials", "Real Estate", "Utilities", "Communication"]
        
        # Create random symbols
        for i in range(self.num_symbols):
            # Generate a 1-4 letter symbol
            length = random.randint(1, 4)
            symbol = "".join(random.choices(prefixes, k=length))
            
            # Assign random initial price based on sector
            sector = random.choice(sectors)
            
            if sector == "Technology":
                base_price = random.uniform(50.0, 500.0)
                volatility = random.uniform(0.005, 0.02)
            elif sector == "Healthcare":
                base_price = random.uniform(30.0, 300.0)
                volatility = random.uniform(0.003, 0.015)
            elif sector == "Finance":
                base_price = random.uniform(20.0, 200.0)
                volatility = random.uniform(0.004, 0.01)
            else:
                base_price = random.uniform(10.0, 100.0)
                volatility = random.uniform(0.002, 0.01)
            
            symbols.append({
                "symbol": symbol,
                "company_name": f"{symbol} Corporation",
                "sector": sector,
                "base_price": base_price,
                "current_price": base_price,
                "volatility": volatility,
                "trend": random.uniform(-0.001, 0.001),  # Daily trend
                "anomaly_probability": 0.02  # 2% chance of generating anomalous data
            })
            
            # Initialize last market update
            self.last_market_update[symbol] = {
                "price": base_price,
                "volume": 0,
                "timestamp": self.start_time
            }
            
        return symbols
    
    def _simulate_price_movement(self, symbol_data):
        """Simulate realistic price movement based on random walk with drift"""
        # Get current data
        current_price = symbol_data["current_price"]
        volatility = symbol_data["volatility"]
        trend = symbol_data["trend"]
        
        # Calculate time factor (for cyclical patterns)
        seconds_since_start = (datetime.now() - self.start_time).total_seconds()
        time_factor = (
            0.0001 * 0.0001 * (seconds_since_start / 3600) +  # Hourly cycle
            0.0002 * 0.0001 * (seconds_since_start / 86400)   # Daily cycle
        )
        
        # Calculate price change with random walk, trend, and time factor
        price_change_percent = (
            random.normalvariate(0, 1) * volatility +  # Random component
            trend +                                     # Trend component
            time_factor                                 # Time-based cyclical component
        )
        
        # Apply price change
        new_price = current_price * (1 + price_change_percent)
        
        # Ensure price doesn't go negative or too small
        new_price = max(new_price, 0.01)
        
        return new_price
    
    def generate_market_update(self, symbol_data):
        """Generate a single market data update"""
        timestamp = datetime.now().isoformat()
        symbol = symbol_data["symbol"]
        anomaly = random.random() < symbol_data["anomaly_probability"]
        
        # Get the last update for this symbol
        last_update = self.last_market_update[symbol]
        
        if anomaly:
            # Generate an anomalous price movement (a sudden spike or drop)
            if random.choice([True, False]):
                # Spike up
                new_price = last_update["price"] * (1 + random.uniform(0.05, 0.15))
            else:
                # Drop down
                new_price = last_update["price"] * (1 - random.uniform(0.05, 0.15))
        else:
            # Normal price movement
            new_price = self._simulate_price_movement(symbol_data)
        
        # Update the current price in the symbol data
        symbol_data["current_price"] = new_price
        
        # Generate trading volume based on price change magnitude
        price_change_pct = abs(new_price - last_update["price"]) / last_update["price"]
        base_volume = random.randint(1000, 10000)
        volume = int(base_volume * (1 + 10 * price_change_pct))
        
        # Update last market update
        self.last_market_update[symbol] = {
            "price": new_price,
            "volume": volume,
            "timestamp": datetime.now()
        }
        
        return {
            "symbol": symbol,
            "company_name": symbol_data["company_name"],
            "sector": symbol_data["sector"],
            "price": round(new_price, 2),
            "price_change": round(new_price - last_update["price"], 2),
            "price_change_percent": round(100 * (new_price - last_update["price"]) / last_update["price"], 2),
            "volume": volume,
            "timestamp": timestamp,
            "is_anomaly": anomaly,
            "source": "market_data_simulator"
        }
    
    def run(self, interval=0.5, duration=None):
        """
        Run the simulator to generate and send market data.
        
        Args:
            interval: Time between market updates in seconds
            duration: How long to run in seconds, None for indefinite
        """
        if not self.mongodb:
            logger.error("MongoDB connector not provided. Cannot run simulator.")
            return
            
        start_time = time.time()
        count = 0
        
        try:
            print(f"Starting market data simulator with {self.num_symbols} symbols...")
            while duration is None or time.time() - start_time < duration:
                # Each interval, update a subset of the symbols
                update_count = random.randint(1, min(5, self.num_symbols))
                symbols_to_update = random.sample(self.market_symbols, update_count)
                
                for symbol_data in symbols_to_update:
                    update = self.generate_market_update(symbol_data)
                    
                    # Send to MongoDB
                    self.mongodb.insert_one("market_data", update)
                    
                    # Print sample data occasionally
                    if count % 50 == 0:
                        print(f"Sample market update: {json_dumps(update, indent=2)}")
                    
                    count += 1
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("Market data simulator stopped by user")
        finally:
            print(f"Market data simulator finished after sending {count} updates")


class SimplifiedSimulatorManager:
    """Manages multiple data simulators without Kafka dependency"""
    
    def __init__(self):
        self.simulators = []
        self.running = False
        self.threads = []
        # Initialize MongoDB connector
        self.mongodb = MongoDBConnector()
    
    def add_simulator(self, simulator, interval, duration=None):
        """Add a simulator to be managed"""
        self.simulators.append({
            "simulator": simulator,
            "interval": interval,
            "duration": duration
        })
    
    def start_simulator(self, simulator_config):
        """Start a single simulator in its own thread"""
        simulator = simulator_config["simulator"]
        interval = simulator_config["interval"]
        duration = simulator_config["duration"]
        
        print(f"Starting {simulator.__class__.__name__}")
        simulator.run(interval=interval, duration=duration)
    
    def start_all(self):
        """Start all simulators in separate threads"""
        self.running = True
        
        for simulator_config in self.simulators:
            thread = threading.Thread(
                target=self.start_simulator,
                args=(simulator_config,)
            )
            thread.daemon = True
            thread.start()
            self.threads.append(thread)
    
    def stop_all(self):
        """Signal all simulators to stop"""
        print("Stopping all simulators...")
        self.running = False
        
        # Wait for all threads to finish
        for thread in self.threads:
            thread.join(timeout=2)
            
        print("All simulators stopped")


def signal_handler(sig, frame):
    """Handle interrupt signals"""
    print("Signal received, stopping simulators...")
    if 'manager' in globals():
        manager.stop_all()
    sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run simplified data simulators for StreamFlux")
    
    parser.add_argument("--sensors", type=int, default=5,
                        help="Number of IoT sensors to simulate")
    parser.add_argument("--users", type=int, default=10,
                        help="Number of social media users to simulate")
    parser.add_argument("--symbols", type=int, default=20,
                        help="Number of market symbols to simulate")
    parser.add_argument("--duration", type=int, default=None,
                        help="Duration to run simulators in seconds (default: indefinite)")
    
    args = parser.parse_args()
    
    # Create MongoDB connector
    mongodb = MongoDBConnector()
    
    # Create simulator instances
    sensor_simulator = SensorSimulator(num_sensors=args.sensors, mongodb_connector=mongodb)
    social_simulator = SocialFeedSimulator(num_users=args.users, mongodb_connector=mongodb)
    market_simulator = MarketDataSimulator(num_symbols=args.symbols, mongodb_connector=mongodb)
    
    # Create and configure the manager
    manager = SimplifiedSimulatorManager()
    manager.add_simulator(sensor_simulator, interval=1.0, duration=args.duration)
    manager.add_simulator(social_simulator, interval=0.1, duration=args.duration)
    manager.add_simulator(market_simulator, interval=0.5, duration=args.duration)
    
    # Register signal handler for graceful shutdown
    import signal
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start all simulators
    print("Starting StreamFlux data simulators...")
    manager.start_all()
    
    try:
        # Keep the main thread alive
        while manager.running:
            time.sleep(1)
    except KeyboardInterrupt:
        manager.stop_all()