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
        
        # Initialize client
        self.client = self._create_client()
    
    def _create_client(self):
        """Create and configure S3 client."""
        return boto3.client(
            's3',
            endpoint_url=settings.AWS_ENDPOINT_URL,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY
        )
    
    def verify_bucket_exists(self):
        """Create bucket if it doesn't exist."""
        try:
            self.client.head_bucket(Bucket=settings.AWS_BUCKET_NAME)
            return True
        except ClientError:
            self.client.create_bucket(Bucket=settings.AWS_BUCKET_NAME)
            return False

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
                                   settings.AWS_BUCKET_NAME,
                                   path,
                                   ExtraArgs=params)
        
        return {
            'path': path,
            'checksum': checksum,
            'content_type': params.get('ContentType'),
            'metadata': metadata
        }
    