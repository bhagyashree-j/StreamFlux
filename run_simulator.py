import sys
import os

# Add the current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the simulator
from data_producers.sensor_simulator import SensorSimulator
from data_producers.social_feed_simulator import SocialFeedSimulator
from data_producers.market_data_simulator import MarketDataSimulator

# Create simulator instances
sensor_simulator = SensorSimulator(num_sensors=3)
social_simulator = SocialFeedSimulator(num_users=5)
market_simulator = MarketDataSimulator(num_symbols=10)

# Run one simulator as a test
print("Starting sensor simulator...")
sensor_simulator.run(duration=30)  # Run for 30 seconds