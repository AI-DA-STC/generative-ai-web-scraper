import hashlib
import requests
from io import BytesIO
from typing import Dict, Any
import sys
from urllib.parse import quote
import pyprojroot
root = pyprojroot.find_root(pyprojroot.has_dir("config"))
sys.path.append(str(root))

from app.db.session import get_db
from app.schemas.scraper import ScrapedMetadata
from util.s3_helper import S3Helper
from config import logger

class ContentPipeline:
    """Pipeline for processing and storing scraped content."""
    
    def __init__(self):
        """Initialize pipeline with storage client."""
        self.session = None
        self.table_name = None
        self.base_path = None
        
    def open_spider(self, spider):
        """Create requests session when spider opens."""
        self.session = requests.Session()
        self.db = get_db()
        self.table_name = spider.table_name
        self.ScrapedPage = spider.model
        self.base_path = f"{self.table_name}"
    
    def close_spider(self, spider):
        """Clean up session when spider closes."""
        if self.session:
            self.session.close()
    
    def process_item(self, item: Dict[str, Any], spider) -> Dict[str, Any]:
        """Process a scraped item by storing content and metadata."""
        try:
            
            if item['type'] == 'URL':
                # Process HTML content
                item['checksum'] = hashlib.sha256(item['raw_html'].encode()).hexdigest()
                safe_url = quote(item['element_id'], safe='')
                path = f"{self.base_path}/{safe_url}_{item['checksum']}.html"
                self._store_html_content(item['raw_html'], path)
                item['content'] = path
                item.pop('raw_html', None)
                
            elif item['type'] == 'PDF':
                # Process PDF content
                response = self.session.get(item['element_id'])
                if response.status_code == 200:
                    content = response.content #raw binary of pdf
                    item['checksum'] = hashlib.sha256(content).hexdigest()
                    safe_url = quote(item['element_id'], safe='')
                    path = f"{self.base_path}/{safe_url}_{item['checksum']}.pdf"
                    
                    content_stream = BytesIO(content)
                    S3Helper().upload_file(
                        file_obj=content_stream,
                        path=path,
                        content_type='application/pdf'
                    )
                    item['content'] = path
                item.pop('raw_pdf', None)
                
            elif item['type'] == 'Image':
                # Process image content 
                response = self.session.get(item['element_id'])
                if response.status_code == 200:
                    content = response.content #raw binary of image
                    item['checksum'] = hashlib.sha256(content).hexdigest()
                    safe_url = quote(item['element_id'], safe='')
                    extension = self._get_extension(item['element_id'])
                    path = f"{self.base_path}/{safe_url}_{item['checksum']}{extension}"
                    
                    content_stream = BytesIO(content)
                    S3Helper().upload_file(
                        file_obj=content_stream,
                        path=path,
                        content_type=response.headers.get('content-type', 'image/jpeg')
                    )
                    item['content'] = path
                item.pop('raw_image', None)
            
            # Validate using schema
            metadata = ScrapedMetadata(**item)
            
            # Store in database
            db_page = self.ScrapedPage(
                element_id=metadata.element_id,
                type=metadata.type,
                content=metadata.content,
                checksum=metadata.checksum,
                parent_id=metadata.parent_id
            )
            
            self.db.add(db_page)
            self.db.commit()
            
            return item
            
        except Exception as e:
            logger.error(f"Error processing item {item.get('element_id', 'unknown')}: {str(e)}")
            raise
        
        finally:
            self.db.close()
    
    def _store_html_content(self, html_content: str, path: str) -> None:
        """Store HTML content in MinIO."""
        content_stream = BytesIO(html_content.encode('utf-8'))
        S3Helper().upload_file(
            file_obj=content_stream,
            path=path,
            content_type='text/html'
        )
    
    def _get_extension(self, url: str) -> str:
        """Extract file extension from URL."""
        if '.' in url:
            return '.' + url.split('.')[-1].lower()
        return '.jpeg'