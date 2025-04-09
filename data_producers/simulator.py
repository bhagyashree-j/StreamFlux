# data_producers/simulator_manager.py
import threading
import argparse
import time
import signal
import sys
from sensor_simulator import SensorSimulator
from social_feed_simulator import SocialFeedSimulator
from market_data_simulator import MarketDataSimulator

class SimulatorManager:
    """Manages multiple data simulators"""
    
    def __init__(self):
        self.simulators = []
        self.running = False
        self.threads = []
    
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run data simulators for StreamFlux")
    
    parser.add_argument("--sensors", type=int, default=5,
                        help="Number of IoT sensors to simulate")
    parser.add_argument("--users", type=int, default=10,
                        help="Number of social media users to simulate")
    parser.add_argument("--symbols", type=int, default=20,
                        help="Number of market symbols to simulate")
    parser.add_argument("--duration", type=int, default=None,
                        help="Duration to run simulators in seconds (default: indefinite)")
    
    args = parser.parse_args()
    
    # Create simulator instances
    sensor_simulator = SensorSimulator(num_sensors=args.sensors)
    social_simulator = SocialFeedSimulator(num_users=args.users)
    market_simulator = MarketDataSimulator(num_symbols=args.symbols)
    
    # Create and configure the manager
    manager = SimulatorManager()
    manager.add_simulator(sensor_simulator, interval=1.0, duration=args.duration)
    manager.add_simulator(social_simulator, interval=0.1, duration=args.duration)
    manager.add_simulator(market_simulator, interval=0.5, duration=args.duration)
    
    def signal_handler(sig, frame):
        print("Signal received, stopping simulators...")
        manager.stop_all()
        sys.exit(0)

    # Register signal handler for graceful shutdown
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