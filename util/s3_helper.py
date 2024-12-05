import hashlib
import mimetypes
import boto3
import aioboto3
from botocore.exceptions import ClientError
from typing import Dict, Any, Optional, BinaryIO, Union, List
from io import BytesIO
import logging
import sys
from tqdm import tqdm
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import settings

logger = logging.getLogger(__name__)

class S3Helper:
    """
    Enhanced S3/MinIO helper with comprehensive content management capabilities.
    
    This class provides both synchronous and asynchronous operations for:
    - Content upload/download
    - Metadata management
    - Efficient transfers
    - Content verification
    - Temporary URL generation
    """
    
    def __init__(self, **kwargs):
        """
        Initialize S3 helper with provided configuration.
        
        Args:
            **kwargs: Override default settings from config
        """
        self.endpoint_url = kwargs.get('AWS_ENDPOINT_URL', settings.AWS_ENDPOINT_URL)
        self.access_key = kwargs.get('AWS_ACCESS_KEY_ID', settings.AWS_ACCESS_KEY_ID)
        self.secret_key = kwargs.get('AWS_SECRET_ACCESS_KEY', settings.AWS_SECRET_ACCESS_KEY)
        self.bucket_name = kwargs.get('AWS_BUCKET_NAME', settings.AWS_BUCKET_NAME)
        
        # Initialize clients
        self.client = self._create_client()
    
    def _create_client(self):
        """Create and configure S3 client."""
        return boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )
    
    def verify_bucket_exists(self):
        """Create bucket if it doesn't exist."""
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError:
            self.client.create_bucket(Bucket=self.bucket_name)
            return False
    
    def list_objects(
        self,
        prefix: Optional[str] = None,
        recursive: bool = False
    ) -> List[Dict[str, Any]]:
        """Synchronously list objects in bucket."""
        paginator = self.client.get_paginator('list_objects_v2')
        
        params = {'Bucket': self.bucket_name}
        if prefix:
            params['Prefix'] = prefix
        if not recursive:
            params['Delimiter'] = '/'
        
        objects = []
        for page in paginator.paginate(**params):
            if 'Contents' in page:
                objects.extend(page['Contents'])
        
        return objects

    def upload_file(
        self,
        file_obj: Union[BinaryIO, BytesIO],
        path: str,
        metadata: Optional[Dict[str, str]] = None,
        content_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """Synchronously upload file to MinIO with metadata."""
        # Prepare upload parameters
        params = {}
        if metadata:
            params['Metadata'] = metadata
        
        if content_type:
            params['ContentType'] = content_type
        else:
            content_type = mimetypes.guess_type(path)[0]
            if content_type:
                params['ContentType'] = content_type
        
        # Calculate checksum
        file_obj.seek(0)
        checksum = hashlib.sha256(file_obj.read()).hexdigest()
        file_obj.seek(0)
        
        # Upload file
        self.client.upload_fileobj(file_obj,
                                   self.bucket_name,
                                   path,
                                   ExtraArgs=params)
        
        return {
            'path': path,
            'checksum': checksum,
            'content_type': params.get('ContentType'),
            'metadata': metadata
        }
    def swap_version_folders(
    self,
    table_name: str
    ) -> None:
        """
        Swap folder names between dev and prod versions based on table name.
        
        If table_name is dev_{timestamp}, renames:
        - Current dev_{timestamp} → prod_{timestamp}
        - Current prod_{timestamp} → dev_{timestamp}
        
        Args:
            table_name: Name of the metadata table (dev_{timestamp} or prod_{timestamp})
        """
        try:
            # Extract timestamp from table name
            if not table_name.startswith(('dev_', 'prod_')):
                logger.error(f"Invalid table name format: {table_name}")
                return
                
            timestamp = table_name.split('_', 1)[1]
            
            # Only proceed if this is a dev table
            if table_name.startswith('dev_'):
                dev_prefix = f"dev_{timestamp}"
                prod_prefix = f"prod_{timestamp}"
                
                # Check if folders exist
                objects = self.list_objects()
                prefixes = {obj['Key'].split('/')[0] for obj in objects}
                
                if dev_prefix not in prefixes:
                    logger.error(f"Source folder {dev_prefix} not found")
                    return
                    
                # If prod folder exists, we need to swap
                if prod_prefix in prefixes:
                    # Create temporary prefix for the swap
                    temp_prefix = f"temp_{timestamp}"
                    
                    # Rename prod → temp
                    self._rename_folder(prod_prefix, temp_prefix)
                    
                    # Rename dev → prod
                    self._rename_folder(dev_prefix, prod_prefix)
                    
                    # Rename temp → dev
                    self._rename_folder(temp_prefix, dev_prefix)
                    
                else:
                    # Simply rename dev → prod
                    self._rename_folder(dev_prefix, prod_prefix)
                
                logger.info(f"Successfully swapped version folders for {timestamp}")
                
        except Exception as e:
            logger.error(f"Error swapping version folders: {str(e)}")
            raise

    def _rename_folder(
        self,
        old_prefix: str,
        new_prefix: str,
        batch_size: int = 1000
    ) -> None:
        """
        Rename a folder by copying all objects to new prefix and deleting old ones.
        
        Args:
            old_prefix: Current folder name
            new_prefix: New folder name
            batch_size: Number of objects to process in each batch
        """
        try:
            # List all objects under old prefix
            objects = []
            for obj in self.list_objects(prefix=old_prefix):
                objects.append(obj)
                
                # Process batch when size reached
                if len(objects) >= batch_size:
                    self._rename_batch(objects, old_prefix, new_prefix)
                    objects = []
            
            # Process remaining objects
            if objects:
                self._rename_batch(objects, old_prefix, new_prefix)
                
            logger.info(f"Successfully renamed folder {old_prefix} to {new_prefix}")
            
        except Exception as e:
            logger.error(f"Error renaming folder {old_prefix}: {str(e)}")
            raise

    def _rename_batch(
        self,
        objects: List[Dict],
        old_prefix: str,
        new_prefix: str
    ) -> None:
        """
        Rename a batch of objects from old prefix to new prefix.
        
        Args:
            objects: List of object metadata dictionaries
            old_prefix: Current prefix to replace
            new_prefix: New prefix to use
        """
        try:
            for obj in objects:
                old_key = obj['Key']
                new_key = old_key.replace(old_prefix, new_prefix, 1)
                
                # Copy object to new location
                self.client.copy_object(
                    Bucket=self.bucket_name,
                    CopySource={'Bucket': self.bucket_name, 'Key': old_key},
                    Key=new_key
                )
                
                # Delete old object
                self.client.delete_object(
                    Bucket=self.bucket_name,
                    Key=old_key
                )
                
        except Exception as e:
            logger.error(f"Error in rename batch operation: {str(e)}")
            raise
        