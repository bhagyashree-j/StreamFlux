from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
import os
import sys
import json
from datetime import datetime, timedelta
import pymongo
from bson import ObjectId 
from pymongo import MongoClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Update the class
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, ObjectId):
            return str(obj)  # Convert ObjectId to string
        return super().default(obj)

# Initialize Flask app
app = Flask(__name__)
CORS(app) 

# Initialize MongoDB connector
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
    
    def ensure_connection(self):
        """Ensure connection is established"""
        if self.client is None:
            return self._connect()
        return True
    
    def find_one(self, collection_name, query=None):
        """Find a single document in a collection"""
        if not self.ensure_connection():
            return None
            
        try:
            collection = self.db[collection_name]
            result = collection.find_one(query or {})
            return result
            
        except Exception as e:
            logger.error(f"Error finding document in {collection_name}: {str(e)}")
            return None
    
    def find_many(self, collection_name, query=None, limit=None, sort=None):
        """Find multiple documents in a collection"""
        if not self.ensure_connection():
            return []
            
        try:
            collection = self.db[collection_name]
            cursor = collection.find(query or {})
            
            if sort:
                cursor = cursor.sort(sort)
                
            if limit:
                cursor = cursor.limit(limit)
                
            return list(cursor)
            
        except Exception as e:
            logger.error(f"Error finding documents in {collection_name}: {str(e)}")
            return []
    
    def aggregate(self, collection_name, pipeline):
        """Perform an aggregation pipeline on a collection"""
        if not self.ensure_connection():
            return []
            
        try:
            collection = self.db[collection_name]
            return list(collection.aggregate(pipeline))
            
        except Exception as e:
            logger.error(f"Error performing aggregation on {collection_name}: {str(e)}")
            return []

mongodb = MongoDBConnector()

# Set JSON encoder for Flask
app.json_encoder = MongoJSONEncoder

@app.route('/anomalies', methods=['GET'])
def get_anomalies():
    """Get detected anomalies"""
    try:
        # Time range filter (default: last 24 hours)
        hours = request.args.get('hours', 24, type=int)
        since = datetime.now() - timedelta(hours=hours)
        
        # Source filter
        source = request.args.get('source')
        
        # Limit
        limit = request.args.get('limit', 100, type=int)
        
        anomalies = []
        
        # Collect anomalies from all collections or specific source
        collections = []
        if source:
            collections = [source]
        else:
            collections = ['sensor_data', 'social_data', 'market_data']
            
        for collection in collections:
            # Build query
            query = {'is_anomaly': True, 'timestamp': {'$gte': since.isoformat()}}
                
            # Get anomalies
            collection_anomalies = mongodb.find_many(
                collection, 
                query=query,
                limit=limit,
                sort=[('timestamp', -1)]
            )
            
            anomalies.extend(collection_anomalies)
        
        # Sort all anomalies by timestamp (most recent first)
        anomalies.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        
        # Limit total results
        if len(anomalies) > limit:
            anomalies = anomalies[:limit]
        
        return jsonify({
            'count': len(anomalies),
            'anomalies': anomalies
        })
        
    except Exception as e:
        logger.error(f"Error fetching anomalies: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/data/<collection>', methods=['GET'])
def get_data(collection):
    """Get data from a collection"""
    try:
        # Query parameters
        hours = request.args.get('hours', 24, type=int)
        limit = request.args.get('limit', 100, type=int)
        
        # Time range filter
        since = datetime.now() - timedelta(hours=hours)
        
        # Get data
        data = mongodb.find_many(
            collection,
            query={'timestamp': {'$gte': since.isoformat()}},
            limit=limit,
            sort=[('timestamp', -1)]
        )
        
        return jsonify({
            'collection': collection,
            'count': len(data),
            'data': data
        })
        
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint"""
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'service': 'StreamFlux API (Simplified)'
    })

@app.route('/metrics', methods=['GET'])
def get_metrics():
    """Get system metrics"""
    try:
        # Time range filter (default: last 24 hours)
        hours = request.args.get('hours', 24, type=int)
        since = datetime.now() - timedelta(hours=hours)
        
        # Get data counts for each source
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'time_range': f"Last {hours} hours",
            'data_counts': []
        }
        
        for collection in ['sensor_data', 'social_data', 'market_data']:
            # Count total documents
            total_count = len(mongodb.find_many(collection, 
                                                query={'timestamp': {'$gte': since.isoformat()}}))
            
            # Count anomalies
            anomaly_count = len(mongodb.find_many(collection, 
                                                  query={'timestamp': {'$gte': since.isoformat()},
                                                         'is_anomaly': True}))
            
            metrics['data_counts'].append({
                'source': collection,
                'count': total_count,
                'anomalies': anomaly_count
            })
        
        return jsonify(metrics)
        
    except Exception as e:
        logger.error(f"Error fetching metrics: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Run the Flask application
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
