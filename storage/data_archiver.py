import logging
import os
import json
import gzip
import io
from datetime import datetime, timedelta
import threading
import time
from dotenv import load_dotenv

# Import local modules
from mongodb_connector import MongoDBConnector
from s3_connector import S3Connector

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataArchiver:
    """
    Archives data from MongoDB to S3 for long-term storage
    and implements data retention policies
    """
    
    def __init__(self):
        """Initialize the data archiver"""
        self.mongodb = MongoDBConnector()
        self.s3 = S3Connector()
        
        # Configuration
        self.archive_interval_hours = int(os.getenv('ARCHIVE_INTERVAL_HOURS', '24'))
        self.retention_days = {
            'processed_data': int(os.getenv('PROCESSED_DATA_RETENTION_DAYS', '30')),
            'aggregated_data': int(os.getenv('AGGREGATED_DATA_RETENTION_DAYS', '90')),
            'anomalies': int(os.getenv('ANOMALIES_RETENTION_DAYS', '365'))
        }
        
        self.archiving_active = False
        self.archive_thread = None
    
    def start_scheduled_archiving(self):
        """Start scheduled archiving in a background thread"""
        if self.archiving_active:
            logger.warning("Archiving already active")
            return
            
        self.archiving_active = True
        self.archive_thread = threading.Thread(target=self._archiving_loop)
        self.archive_thread.daemon = True
        self.archive_thread.start()
        
        logger.info(f"Scheduled archiving started with interval: {self.archive_interval_hours} hours")
    
    def stop_scheduled_archiving(self):
        """Stop scheduled archiving"""
        self.archiving_active = False
        if self.archive_thread:
            self.archive_thread.join(timeout=60)
            logger.info("Scheduled archiving stopped")
    
    def _archiving_loop(self):
        """Background loop for scheduled archiving"""
        while self.archiving_active:
            try:
                logger.info("Running scheduled data archiving")
                self.archive_old_data()
                self.enforce_retention_policy()
            except Exception as e:
                logger.error(f"Error in archiving loop: {str(e)}")
                
            # Sleep until next archiving interval
            sleep_seconds = self.archive_interval_hours * 3600
            for _ in range(sleep_seconds):
                if not self.archiving_active:
                    break
                time.sleep(1)
    
    def archive_old_data(self, days_old=7):
        """
        Archive data older than specified days
        
        Args:
            days_old: Archive data older than this many days
        """
        cutoff_date = datetime.now() - timedelta(days=days_old)
        logger.info(f"Archiving data older than {cutoff_date.isoformat()}")
        
        # Collections to archive
        collections = [
            'streamflux_processed_data',
            'streamflux_aggregated_data'
        ]
        
        for collection_name in collections:
            # Extract base name without prefix
            base_name = collection_name.replace('streamflux_', '')
            
            # Find data older than cutoff date
            query = {'created_at': {'$lt': cutoff_date}}
            old_data = self.mongodb.find_many(collection_name, query)
            
            if not old_data:
                logger.info(f"No old data found in {collection_name}")
                continue
                
            logger.info(f"Found {len(old_data)} records to archive from {collection_name}")
            
            # Create archive file
            archive_date = datetime.now().strftime("%Y%m%d%H%M%S")
            archive_key = f"archives/{base_name}/{archive_date}.json.gz"
            
            # Serialize and compress the data
            json_data = json.dumps(old_data, default=self._json_serial)
            compressed_data = gzip.compress(json_data.encode('utf-8'))
            
            # Upload to S3
            metadata = {
                'collection': collection_name,
                'record_count': str(len(old_data)),
                'archive_date': archive_date,
                'cutoff_date': cutoff_date.isoformat()
            }
            
            success = self.s3.upload_data(
                compressed_data,
                archive_key,
                content_type='application/gzip',
                metadata=metadata
            )
            
            if success:
                # Delete archived data from MongoDB
                result = self.mongodb.delete_many(collection_name, query)
                logger.info(f"Archived and deleted {result} records from {collection_name}")
            else:
                logger.error(f"Failed to archive data from {collection_name}")
    
    def enforce_retention_policy(self):
        """
        Enforce data retention policy by deleting old data
        """
        logger.info("Enforcing data retention policy")
        
        for base_name, days in self.retention_days.items():
            collection_name = f"streamflux_{base_name}"
            cutoff_date = datetime.now() - timedelta(days=days)
            
            # Delete data older than retention period
            query = {'created_at': {'$lt': cutoff_date}}
            result = self.mongodb.delete_many(collection_name, query)
            
            logger.info(f"Deleted {result} records from {collection_name} due to retention policy")
    
    def restore_from_archive(self, archive_key, collection_name=None):
        """
        Restore data from an S3 archive
        
        Args:
            archive_key: S3 object key of the archive
            collection_name: Optional target collection name
            
        Returns:
            Number of records restored
        """
        logger.info(f"Restoring data from archive: {archive_key}")
        
        # Download the archive
        compressed_data = self.s3.download_data(archive_key)
        if not compressed_data:
            logger.error(f"Failed to download archive: {archive_key}")
            return 0
            
        # Decompress the data
        try:
            json_data = gzip.decompress(compressed_data).decode('utf-8')
            archived_data = json.loads(json_data)
        except Exception as e:
            logger.error(f"Error decompressing archive: {str(e)}")
            return 0
            
        # Determine target collection
        if not collection_name:
            # Extract from archive key
            parts = archive_key.split('/')
            if len(parts) >= 3:
                base_name = parts[1]
                collection_name = f"streamflux_{base_name}"
            else:
                logger.error("Could not determine target collection from archive key")
                return 0
                
        # Insert restored data to MongoDB
        result = self.mongodb.insert_many(collection_name, archived_data)
        
        if result:
            logger.info(f"Restored {len(result)} records to {collection_name}")
            return len(result)
        else:
            logger.error(f"Failed to restore data to {collection_name}")
            return 0
    
    def _json_serial(self, obj):
        """Helper function to serialize datetime objects in JSON"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")