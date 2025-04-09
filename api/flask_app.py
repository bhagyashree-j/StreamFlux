from flask import Flask, request, jsonify
import logging
import os
import sys
import json
from datetime import datetime, timedelta

# Add parent directory to path to import local modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import from other modules
from storage.mongodb_connector import MongoDBConnector
from anomaly_detection.alert_system import AlertSystem

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Initialize connectors
mongodb = MongoDBConnector()
alert_system = AlertSystem()

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint"""
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'service': 'StreamFlux API'
    })

@app.route('/metrics', methods=['GET'])
def get_metrics():
    """Get system metrics"""
    try:
        # Time range filter (default: last 24 hours)
        hours = request.args.get('hours', 24, type=int)
        since = datetime.now() - timedelta(hours=hours)
        
        # Get processed data counts
        pipeline = [
            {'$match': {'timestamp': {'$gte': since.isoformat()}}},
            {'$group': {
                '_id': '$topic',
                'count': {'$sum': 1},
                'anomalies': {'$sum': {'$cond': ['$is_anomaly', 1, 0]}}
            }}
        ]
        
        data_counts = mongodb.aggregate('streamflux_processed_data', pipeline)
        
        # Get processing statistics
        pipeline = [
            {'$match': {'timestamp': {'$gte': since.isoformat()}}},
            {'$group': {
                '_id': None,
                'avg_delay': {'$avg': '$processing_delay_ms'},
                'max_delay': {'$max': '$processing_delay_ms'},
                'min_delay': {'$min': '$processing_delay_ms'}
            }}
        ]
        
        processing_stats = mongodb.aggregate('streamflux_processed_data', pipeline)
        
        # Format response
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'time_range': f"Last {hours} hours",
            'data_counts': list(data_counts),
            'processing_stats': list(processing_stats)[0] if processing_stats else {}
        }
        
        return jsonify(metrics)
        
    except Exception as e:
        logger.error(f"Error fetching metrics: {str(e)}")
        return jsonify({'error': str(e)}), 500

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
        
        # Build query
        query = {'is_anomaly': True, 'timestamp': {'$gte': since.isoformat()}}
        if source:
            query['source'] = source
            
        # Get anomalies
        anomalies = mongodb.find_many(
            'streamflux_processed_data', 
            query=query,
            limit=limit,
            sort=[('timestamp', -1)]
        )
        
        return jsonify({
            'count': len(anomalies),
            'anomalies': anomalies
        })
        
    except Exception as e:
        logger.error(f"Error fetching anomalies: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/alerts', methods=['POST'])
def create_alert():
    """Manually create an alert"""
    try:
        data = request.json
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
            
        # Required fields
        required_fields = ['message', 'source', 'severity']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f"Missing required field: {field}"}), 400
                
        # Format alert data
        alert_data = {
            'message': data['message'],
            'source': data['source'],
            'timestamp': datetime.now().isoformat(),
            'severity': float(data['severity']),
            'manual': True
        }
        
        # Send the alert
        success = alert_system.send_alert(alert_data, alert_data['severity'])
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Alert sent successfully'
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to send alert'
            }), 500
            
    except Exception as e:
        logger.error(f"Error creating alert: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/data/<collection>', methods=['GET'])
def get_data(collection):
    """Get data from a collection"""
    try:
        # Ensure collection name has prefix
        if not collection.startswith('streamflux_'):
            collection = f"streamflux_{collection}"
            
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

if __name__ == '__main__':
    # Run the Flask application
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)