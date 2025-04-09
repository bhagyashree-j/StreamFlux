# data_producers/market_data_simulator.py
import json
import random
import math
import time
from datetime import datetime, timedelta
import uuid
import sys
import os

# Add root directory to path to import kafka_layer module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_layer.producer import KafkaProducer

class MarketDataSimulator:
    """Simulates financial market data streams"""
    
    def __init__(self, num_symbols=20, kafka_topic="market-data"):
        self.num_symbols = num_symbols
        self.kafka_topic = kafka_topic
        self.producer = KafkaProducer()
        self.market_symbols = self._initialize_symbols()
        self.start_time = datetime.now()
        self.last_market_update = {}
        
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
            0.0001 * math.sin(seconds_since_start / 3600) +  # Hourly cycle
            0.0002 * math.sin(seconds_since_start / 86400)   # Daily cycle
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
                    
                    # Send to Kafka
                    self.producer.send(self.kafka_topic, update)
                    
                    # Print sample data occasionally
                    if count % 50 == 0:
                        print(f"Sample market update: {json.dumps(update, indent=2)}")
                    
                    count += 1
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("Market data simulator stopped by user")
        finally:
            self.producer.close()
            print(f"Market data simulator finished after sending {count} updates")