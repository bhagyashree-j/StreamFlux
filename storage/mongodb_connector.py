import logging
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import os
from dotenv import load_dotenv
import json
from datetime import datetime

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoDBConnector:
    """Connector for MongoDB operations"""
    
    def __init__(self, connection_string=None, database=None):
        """
        Initialize MongoDB connector
        
        Args:
            connection_string: MongoDB connection string
            database: MongoDB database name
        """
        self.connection_string = connection_string or os.getenv('MONGODB_CONNECTION_STRING', 'mongodb://localhost:27017')
        self.database_name = database or os.getenv('MONGODB_DATABASE', 'streamflux')
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
            
        except ConnectionFailure as e:
            logger.error(f"MongoDB connection failed: {str(e)}")
            self.client = None
            self.db = None
            return False
            
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {str(e)}")
            self.client = None
    def ensure_connection(self):
        """Ensure connection is established"""
        if self.client is None:
            return self._connect()
        return True
    
    def insert_one(self, collection_name, document):
        """
        Insert a single document into a collection
        
        Args:
            collection_name: Name of the collection
            document: Document to insert
            
        Returns:
            Inserted document ID if successful, None otherwise
        """
        if not self.ensure_connection():
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
    
    def insert_many(self, collection_name, documents):
        """
        Insert multiple documents into a collection
        
        Args:
            collection_name: Name of the collection
            documents: List of documents to insert
            
        Returns:
            List of inserted document IDs if successful, None otherwise
        """
        if not self.ensure_connection():
            return None
            
        if not documents:
            return []
            
        try:
            # Add timestamp to each document if not present
            for doc in documents:
                if 'created_at' not in doc:
                    doc['created_at'] = datetime.now()
                    
            collection = self.db[collection_name]
            result = collection.insert_many(documents)
            return [str(id) for id in result.inserted_ids]
            
        except Exception as e:
            logger.error(f"Error inserting documents into {collection_name}: {str(e)}")
            return None
    
    def find_one(self, collection_name, query=None):
        """
        Find a single document in a collection
        
        Args:
            collection_name: Name of the collection
            query: Query dictionary
            
        Returns:
            Document if found, None otherwise
        """
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
        """
        Find multiple documents in a collection
        
        Args:
            collection_name: Name of the collection
            query: Query dictionary
            limit: Maximum number of documents to return
            sort: Sort criteria
            
        Returns:
            List of documents if found, empty list otherwise
        """
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
    
    def create_index(self, collection_name, keys, **kwargs):
        """
        Create an index on a collection
        
        Args:
            collection_name: Name of the collection
            keys: Index keys
            **kwargs: Additional index options
            
        Returns:
            Index name if successful, None otherwise
        """
        if not self.ensure_connection():
            return None
            
        try:
            collection = self.db[collection_name]
            result = collection.create_index(keys, **kwargs)
            logger.info(f"Created index {result} on collection {collection_name}")
            return result
            
        except Exception as e:
            logger.error(f"Error creating index on {collection_name}: {str(e)}")
            return None
    
    def update_one(self, collection_name, query, update, upsert=False):
        """
        Update a single document in a collection
        
        Args:
            collection_name: Name of the collection
            query: Query to find document to update
            update: Update operations
            upsert: Whether to insert if document doesn't exist
            
        Returns:
            Number of documents modified
        """
        if not self.ensure_connection():
            return 0
            
        try:
            # Add update timestamp
            if '$set' in update:
                update['$set']['updated_at'] = datetime.now()
            else:
                update['$set'] = {'updated_at': datetime.now()}
                
            collection = self.db[collection_name]
            result = collection.update_one(query, update, upsert=upsert)
            return result.modified_count
            
        except Exception as e:
            logger.error(f"Error updating document in {collection_name}: {str(e)}")
            return 0
    
    def delete_one(self, collection_name, query):
        """
        Delete a single document from a collection
        
        Args:
            collection_name: Name of the collection
            query: Query to find document to delete
            
        Returns:
            Number of documents deleted
        """
        if not self.ensure_connection():
            return 0
            
        try:
            collection = self.db[collection_name]
            result = collection.delete_one(query)
            return result.deleted_count
            
        except Exception as e:
            logger.error(f"Error deleting document from {collection_name}: {str(e)}")
            return 0
    
    def aggregate(self, collection_name, pipeline):
        """
        Perform an aggregation pipeline on a collection
        
        Args:
            collection_name: Name of the collection
            pipeline: Aggregation pipeline
            
        Returns:
            Aggregation results
        """
        if not self.ensure_connection():
            return []
            
        try:
            collection = self.db[collection_name]
            return list(collection.aggregate(pipeline))
            
        except Exception as e:
            logger.error(f"Error performing aggregation on {collection_name}: {str(e)}")
            return []
    
    def close(self):
        """Close the MongoDB connection"""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
            logger.info("MongoDB connection closed")