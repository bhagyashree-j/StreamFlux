import numpy as np
import pandas as pd
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StatisticalDetector:
    """
    Anomaly detection using statistical methods such as Z-score
    and moving averages
    """
    
    def __init__(self, window_size=100, z_threshold=3.0):
        """
        Initialize the statistical detector
        
        Args:
            window_size: Size of the sliding window for calculations
            z_threshold: Z-score threshold for anomaly detection
        """
        self.window_size = window_size
        self.z_threshold = z_threshold
        self.data_windows = {}  # Store historical data by key
        
    def detect_anomalies(self, data_batch, key_field, value_field):
        """
        Detect anomalies in a batch of data using z-score method
        
        Args:
            data_batch: List of dictionaries containing data points
            key_field: Field name to group data by (e.g., 'sensor_id')
            value_field: Field name of the value to analyze (e.g., 'temperature')
            
        Returns:
            List of data points with added anomaly score and detection
        """
        result = []
        
        # Group data by key
        grouped_data = {}
        for item in data_batch:
            key = item.get(key_field)
            if not key:
                continue
                
            if key not in grouped_data:
                grouped_data[key] = []
            grouped_data[key].append(item)
            
        # Process each group
        for key, items in grouped_data.items():
            # Initialize data window if not exists
            if key not in self.data_windows:
                self.data_windows[key] = []
            
            # Get existing window
            window = self.data_windows[key]
            
            # Process each item
            for item in items:
                value = item.get(value_field)
                
                # Skip if value is not a number
                if not isinstance(value, (int, float)):
                    item["anomaly_score"] = 0
                    item["is_anomaly"] = False
                    result.append(item)
                    continue
                
                # Add to window
                window.append(value)
                
                # Keep window at specified size
                if len(window) > self.window_size:
                    window = window[-self.window_size:]
                
                # Calculate z-score if we have enough data
                if len(window) >= 10:  # Need minimum data for meaningful stats
                    mean = np.mean(window)
                    std = np.std(window)
                    
                    # Avoid division by zero
                    if std == 0:
                        z_score = 0
                    else:
                        z_score = abs((value - mean) / std)
                    
                    # Flag as anomaly if z-score exceeds threshold
                    is_anomaly = z_score > self.z_threshold
                    
                    item["anomaly_score"] = z_score
                    item["is_anomaly"] = is_anomaly
                else:
                    # Not enough data yet
                    item["anomaly_score"] = 0
                    item["is_anomaly"] = False
                
                result.append(item)
            
            # Update window
            self.data_windows[key] = window
            
        return result

    def detect_anomalies_moving_average(self, data_batch, key_field, value_field, ma_window=5):
        """
        Detect anomalies using moving average method
        
        Args:
            data_batch: List of dictionaries containing data points
            key_field: Field name to group data by
            value_field: Field name of the value to analyze
            ma_window: Window size for moving average
            
        Returns:
            List of data points with added anomaly detection
        """
        # Implementation similar to z-score but using moving average
        # This is a simplified implementation
        result = []
        
        # Group data by key
        grouped_data = {}
        for item in data_batch:
            key = item.get(key_field)
            if not key:
                continue
                
            if key not in grouped_data:
                grouped_data[key] = []
            grouped_data[key].append(item)
            
        # Process each group
        for key, items in grouped_data.items():
            # Initialize data window if not exists
            if key not in self.data_windows:
                self.data_windows[key] = []
            
            # Get existing window
            window = self.data_windows[key]
            
            # Process each item
            for item in items:
                value = item.get(value_field)
                
                # Skip if value is not a number
                if not isinstance(value, (int, float)):
                    item["anomaly_score"] = 0
                    item["is_anomaly"] = False
                    result.append(item)
                    continue
                
                # Add to window
                window.append(value)
                
                # Keep window at specified size
                if len(window) > self.window_size:
                    window = window[-self.window_size:]
                
                # Calculate moving average if we have enough data
                if len(window) >= ma_window:
                    moving_avg = np.mean(window[-ma_window:])
                    
                    # Deviation from moving average
                    deviation = abs(value - moving_avg)
                    
                    # Calculate threshold based on historical volatility
                    if len(window) >= 2*ma_window:
                        # Use standard deviation of moving averages as threshold
                        ma_values = [np.mean(window[i:i+ma_window]) for i in range(len(window)-ma_window)]
                        threshold = 3 * np.std(ma_values)
                    else:
                        # Use a simpler threshold until we have more data
                        threshold = 0.2 * moving_avg
                    
                    is_anomaly = deviation > threshold
                    
                    item["anomaly_score"] = deviation / moving_avg
                    item["is_anomaly"] = is_anomaly
                else:
                    # Not enough data yet
                    item["anomaly_score"] = 0
                    item["is_anomaly"] = False
                
                result.append(item)
            
            # Update window
            self.data_windows[key] = window
            
        return result