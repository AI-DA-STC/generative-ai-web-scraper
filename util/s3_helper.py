import hashlib
import mimetypes
import boto3
import aioboto3
from botocore.exceptions import ClientError
from typing import Dict, Any, Optional, BinaryIO, Union, List
from io import BytesIO
import logging
import sys
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
        
        # Ensure bucket exists
        self._ensure_bucket_exists()
    
    def _create_client(self):
        """Create and configure S3 client."""
        return boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )
    
    def _ensure_bucket_exists(self):
        """Create bucket if it doesn't exist."""
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
        except ClientError:
            self.client.create_bucket(Bucket=self.bucket_name)
    
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
    
    def download_file(
        self,
        path: str
    ) -> BytesIO:
        """Synchronously download file from MinIO."""
        buffer = BytesIO()
        self.client.download_fileobj(
            Bucket=self.bucket_name,
            Key=path,
            Fileobj=buffer
        )
        buffer.seek(0)
        return buffer
    
    def generate_presigned_url(
        self,
        path: str,
        expiration: int = 3600,
        response_headers: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Generate temporary presigned URL for content access.
        
        Args:
            path: Storage path in bucket
            expiration: URL validity in seconds
            response_headers: Optional response headers
            
        Returns:
            Presigned URL string
        """
        params = {
            'Bucket': self.bucket_name,
            'Key': path,
        }
        
        if response_headers:
            params['ResponseHeaders'] = response_headers
        
        return self.client.generate_presigned_url(
            'get_object',
            Params=params,
            ExpiresIn=expiration
        )
    
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
    
    def delete_objects(
        self,
        paths: List[str]
    ) -> Dict[str, List[str]]:
        """Synchronously delete multiple objects."""
        objects = [{'Key': path} for path in paths]
        
        response = self.client.delete_objects(
            Bucket=self.bucket_name,
            Delete={'Objects': objects}
        )
        
        deleted = [obj['Key'] for obj in response.get('Deleted', [])]
        errors = [obj['Key'] for obj in response.get('Errors', [])]
        
        return {
            'deleted': deleted,
            'errors': errors
        }
    
    def get_object_metadata(
        self,
        path: str
    ) -> Dict[str, Any]:
        """Synchronously get object metadata."""
        response = self.client.head_object(
            Bucket=self.bucket_name,
            Key=path
        )
        
        return {
            'size': response['ContentLength'],
            'content_type': response.get('ContentType'),
            'last_modified': response['LastModified'],
            'metadata': response.get('Metadata', {})
        }
