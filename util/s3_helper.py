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
    
    def get_content_path(
        self,
        timestamp: str,
        content_type: str,
        page_id: str,
        filename: str,
        is_prod: bool = False
    ) -> str:
        """
        Generate path for content storage following versioning structure.
        
        Creates paths in the format:
        {timestamp}/{content_type}/{page_id}/{filename} or
        {timestamp}_prod/{content_type}/{page_id}/{filename}
        """
        base_prefix = f"{timestamp}_prod" if is_prod else timestamp
        return f"{base_prefix}/{content_type}/{page_id}/{filename}"
    
    def create_version_folders(
        self,
        timestamp: str,
        is_prod: bool = False
    ) -> None:
        """
        Create folder structure for a new version.
        Creates html, images, pdfs, and tables folders under the timestamp directory.
        """
        prefix = f"{timestamp}_prod" if is_prod else timestamp
        folders = ['html', 'images', 'pdfs', 'tables']
        
        for folder in folders:
            path = f"{prefix}/{folder}/"
            try:
                self.client.put_object(
                    Bucket=self.bucket_name,
                    Key=path
                )
                logger.info(f"Created folder: {path}")
            except Exception as e:
                logger.error(f"Error creating folder {path}: {str(e)}")
                raise
    
    def copy_to_prod(
        self,
        timestamp: str,
        batch_size: int = 100
    ) -> None:
        """
        Copy versioned content to production.
        Copies all content from timestamp version to {timestamp}_prod folders
        while maintaining the same structure.
        """
        try:
            source_prefix = timestamp
            dest_prefix = f"prod_{timestamp}"
            
            # List all objects
            objects = self.list_objects(prefix=source_prefix)
            
            # Process in batches
            for i in tqdm(range(0, len(objects), batch_size),desc='copying files to production'):
                batch = objects[i:i + batch_size]
                for obj in batch:
                    source_key = obj['Key']
                    dest_key = source_key.replace(source_prefix, dest_prefix)
                    
                    # Copy object
                    self.client.copy_object(
                        Bucket=self.bucket_name,
                        CopySource={'Bucket': self.bucket_name, 'Key': source_key},
                        Key=dest_key
                    )
                
            logger.info(f"Successfully copied {len(objects)} objects to production")
            
        except Exception as e:
            logger.error(f"Error copying to production: {str(e)}")
            raise
    
    def cleanup_versions(
    self,
    keep_versions: int = 5
) -> None:
        """
        Remove old versions from MinIO storage while maintaining history.
        
        Handles two types of paths:
        1. Production paths (starting with 'prod_'): Keeps only the latest version
        2. Version paths: Keeps the specified number of recent versions
        
        Args:
            keep_versions: Number of version folders to retain
        """
        try:
            # List all objects and extract unique base paths
            all_objects = self.list_objects(recursive=False)
            
            # Get unique base paths by extracting first part of path
            prod_paths = set()
            version_paths = set()
            
            for obj in all_objects:
                path = obj['Key'].split('/')[0]  # Get first part of path (timestamp or prod_timestamp)
                if path.startswith('prod_'):
                    prod_paths.add(path)
                else:
                    version_paths.add(path)
            
            # Handle production paths - keep only latest
            if prod_paths:
                sorted_prod = sorted(list(prod_paths), reverse=True)
                # Delete all but the latest prod folder
                for old_prod in sorted_prod[1:]:
                    self.delete_prefix(old_prod)
                    logger.info(f"Removed old production path: {old_prod}")
            
            # Handle version paths - keep last n versions
            sorted_versions = sorted(list(version_paths), reverse=True)
            if len(sorted_versions) > keep_versions:
                old_versions = sorted_versions[keep_versions:]
                for old_version in old_versions:
                    self.delete_prefix(old_version)
                    logger.info(f"Removed old version path: {old_version}")
            
            logger.info(f"Cleanup completed. Retained latest prod path and {keep_versions} version paths")
            
        except Exception as e:
            logger.error(f"Error cleaning up versions: {str(e)}")
            raise
    
    def delete_prefix(
        self,
        prefix: str,
        batch_size: int = 1000
    ) -> None:
        """
        Delete all objects under a prefix.
        Handles large deletions by processing in batches.
        """
        try:
            objects = []
            for obj in self.list_objects(prefix=prefix):
                objects.append({'Key': obj['Key']})
                
                # Process batch when size reached
                if len(objects) >= batch_size:
                    self._delete_batch(objects)
                    objects = []
            
            # Process remaining objects
            if objects:
                self._delete_batch(objects)
                
            logger.info(f"Successfully deleted prefix: {prefix}")
            
        except Exception as e:
            logger.error(f"Error deleting prefix {prefix}: {str(e)}")
            raise
    
    def _delete_batch(self, objects: List[Dict[str, str]]) -> None:
        """
        Delete a batch of objects atomically.
        Internal helper for batch deletion operations.
        """
        try:
            response = self.client.delete_objects(
                Bucket=self.bucket_name,
                Delete={'Objects': objects}
            )
            
            # Check for errors
            if 'Errors' in response:
                logger.error(f"Errors during batch deletion: {response['Errors']}")
                
        except Exception as e:
            logger.error(f"Error in batch deletion: {str(e)}")
            raise
    
    def verify_version_structure(
        self,
        timestamp: str,
        is_prod: bool = False
    ) -> bool:
        """
        Verify that the version folder structure is complete.
        Checks for existence of all required folders in the version.
        """
        prefix = f"{timestamp}_prod" if is_prod else timestamp
        required_folders = [
            f"{prefix}/html/",
            f"{prefix}/images/",
            f"{prefix}/pdfs/",
            f"{prefix}/tables/"
        ]
        
        try:
            existing_objects = self.list_objects(prefix=prefix, recursive=False)
            existing_prefixes = [obj['Key'] for obj in existing_objects]
            
            return all(folder in existing_prefixes for folder in required_folders)
            
        except Exception as e:
            logger.error(f"Error verifying version structure: {str(e)}")
            return False

