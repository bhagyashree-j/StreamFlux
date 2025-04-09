import logging
import boto3
import os
from dotenv import load_dotenv
import json
from datetime import datetime
from botocore.exceptions import ClientError
import io

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3Connector:
    """Connector for AWS S3 storage operations"""
    
    def __init__(self, bucket_name=None, region=None):
        """
        Initialize S3 connector
        
        Args:
            bucket_name: S3 bucket name
            region: AWS region
        """
        self.bucket_name = bucket_name or os.getenv('AWS_S3_BUCKET', 'streamflux-data')
        self.region = region or os.getenv('AWS_REGION', 'us-east-1')
        
        # Initialize S3 client
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize the S3 client"""
        try:
            self.s3_client = boto3.client('s3', region_name=self.region)
            logger.info(f"Initialized S3 client for bucket: {self.bucket_name}")
            
            # Ensure bucket exists
            self.ensure_bucket_exists()
            
        except Exception as e:
            logger.error(f"Error initializing S3 client: {str(e)}")
            self.s3_client = None
    
    def ensure_bucket_exists(self):
        """Ensure the configured bucket exists, create if it doesn't"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"S3 bucket {self.bucket_name} exists")
            return True
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            
            if error_code == '404':
                # Bucket doesn't exist, create it
                logger.info(f"S3 bucket {self.bucket_name} doesn't exist, creating...")
                try:
                    self.s3_client.create_bucket(
                        Bucket=self.bucket_name,
                        CreateBucketConfiguration={
                            'LocationConstraint': self.region
                        }
                    )
                    logger.info(f"Created S3 bucket: {self.bucket_name}")
                    return True
                except Exception as create_error:
                    logger.error(f"Error creating S3 bucket: {str(create_error)}")
                    return False
            else:
                logger.error(f"Error checking S3 bucket: {str(e)}")
                return False
    
    def upload_file(self, file_path, object_key=None, metadata=None):
        """
        Upload a file to S3
        
        Args:
            file_path: Path to the local file
            object_key: S3 object key (path in the bucket)
            metadata: Optional metadata dictionary
            
        Returns:
            True if successful, False otherwise
        """
        if not self.s3_client:
            return False
            
        # If object_key not provided, use the file name
        if object_key is None:
            object_key = os.path.basename(file_path)
            
        try:
            # Upload file with metadata if provided
            extra_args = {}
            if metadata:
                extra_args['Metadata'] = metadata
                
            self.s3_client.upload_file(
                file_path, 
                self.bucket_name, 
                object_key,
                ExtraArgs=extra_args
            )
            
            logger.info(f"Uploaded file {file_path} to s3://{self.bucket_name}/{object_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading file to S3: {str(e)}")
            return False
    
    def upload_data(self, data, object_key, content_type=None, metadata=None):
        """
        Upload data to S3 directly
        
        Args:
            data: Data to upload (bytes, string, or file-like object)
            object_key: S3 object key (path in the bucket)
            content_type: Content type of the data
            metadata: Optional metadata dictionary
            
        Returns:
            True if successful, False otherwise
        """
        if not self.s3_client:
            return False
            
        try:
            # Convert string to bytes if necessary
            if isinstance(data, str):
                data = data.encode('utf-8')
                
            # Prepare arguments
            extra_args = {}
            if content_type:
                extra_args['ContentType'] = content_type
            if metadata:
                extra_args['Metadata'] = metadata
                
            # Upload data
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=object_key,
                Body=data,
                **extra_args
            )
            
            logger.info(f"Uploaded data to s3://{self.bucket_name}/{object_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading data to S3: {str(e)}")
            return False
    
    def download_file(self, object_key, file_path):
        """
        Download a file from S3
        
        Args:
            object_key: S3 object key to download
            file_path: Local path to save the file
            
        Returns:
            True if successful, False otherwise
        """
        if not self.s3_client:
            return False
            
        try:
            self.s3_client.download_file(
                self.bucket_name, 
                object_key, 
                file_path
            )
            
            logger.info(f"Downloaded s3://{self.bucket_name}/{object_key} to {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading file from S3: {str(e)}")
            return False
    
    def download_data(self, object_key):
        """
        Download data from S3 directly
        
        Args:
            object_key: S3 object key to download
            
        Returns:
            Data as bytes if successful, None otherwise
        """
        if not self.s3_client:
            return None
            
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, 
                Key=object_key
            )
            
            data = response['Body'].read()
            logger.info(f"Downloaded data from s3://{self.bucket_name}/{object_key}")
            return data
            
        except Exception as e:
            logger.error(f"Error downloading data from S3: {str(e)}")
            return None
    
    def list_objects(self, prefix=None, max_keys=1000):
        """
        List objects in the bucket
        
        Args:
            prefix: Optional prefix to filter objects
            max_keys: Maximum number of keys to return
            
        Returns:
            List of object information dictionaries
        """
        if not self.s3_client:
            return []
            
        try:
            params = {'Bucket': self.bucket_name, 'MaxKeys': max_keys}
            if prefix:
                params['Prefix'] = prefix
                
            response = self.s3_client.list_objects_v2(**params)
            
            if 'Contents' not in response:
                return []
                
            # Extract relevant information
            objects = []
            for obj in response['Contents']:
                objects.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat(),
                    'etag': obj['ETag'].strip('"')
                })
                
            return objects
            
        except Exception as e:
            logger.error(f"Error listing objects in S3: {str(e)}")
            return []
    
    def delete_object(self, object_key):
        """
        Delete an object from the bucket
        
        Args:
            object_key: S3 object key to delete
            
        Returns:
            True if successful, False otherwise
        """
        if not self.s3_client:
            return False
            
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name, 
                Key=object_key
            )
            
            logger.info(f"Deleted object s3://{self.bucket_name}/{object_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting object from S3: {str(e)}")
            return False