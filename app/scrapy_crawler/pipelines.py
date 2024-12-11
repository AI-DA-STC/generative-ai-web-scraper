import hashlib
import requests
from io import BytesIO
import tempfile
from typing import Dict, Any
import sys
import os
from urllib.parse import quote
import pyprojroot
root = pyprojroot.find_root(pyprojroot.has_dir("config"))
sys.path.append(str(root))

from app.models.scraper import Scrapy
from app.services.scrapy_job_service import create_scrapy
from util.s3_helper import S3Helper
from util.minerU_helper import pdf_parse_main
from util.jina_extractor import JinaExtractor
from config import logger

class ContentPipeline:
    """Pipeline for processing and storing scraped content."""
    
    def __init__(self):
        """Initialize pipeline with storage client."""
        self.session = None
        
    def open_spider(self, spider):
        """Create requests session when spider opens."""
        self.session = requests.Session()
        self.job_id = spider.job_id
        self.db = spider.db
    
    def close_spider(self, spider):
        """Clean up session when spider closes."""
        if self.session:
            self.session.close()
    
    def process_item(self, item: Dict[str, Any], spider) -> Dict[str, Any]:
        """Process a scraped item by storing content and metadata."""
        try:
            
            if item['type'] == 'URL':
                # Process and save raw HTML content
                item['checksum'] = hashlib.sha256(item['raw_html'].encode()).hexdigest()
                raw_path = f"{self.job_id}/raw/{item['element_id']}.html"
                html_content = item['raw_html']
                content_stream = BytesIO(html_content.encode('utf-8'))
                S3Helper().upload_file(
                    file_obj=content_stream,
                    path=raw_path,
                    content_type='text/html'
                )
                item['raw_content_path'] = raw_path

                #process html to markdown and save markdown content
                markdown_content = JinaExtractor().jina_reader_html2md(item['URL'])
                processed_path = f"{self.job_id}/processed/{item['element_id']}.md"
                content_stream = BytesIO(markdown_content.encode('utf-8'))
                S3Helper().upload_file(
                    file_obj=content_stream,
                    path=processed_path,
                    content_type='text/html'
                )
                item['processed_content_path'] = processed_path
                
                item.pop('raw_html',None)

            elif item['type'] == 'PDF':
                # Process PDF content
                response = self.session.get(item['URL'])
                if response.status_code == 200:
                    content = response.content
                    item['checksum'] = hashlib.sha256(content).hexdigest()
                    
                    # Store raw PDF in 'raw' folder
                    raw_path = f"{self.job_id}/raw/{item['element_id']}.pdf"
                    content_stream = BytesIO(content)
                    S3Helper().upload_file(
                        file_obj=content_stream,
                        path=raw_path,
                        content_type='application/pdf'
                    )
                    
                    # Temporarily save PDF for minerU processing
                    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                        tmp_file.write(content)
                        tmp_path = tmp_file.name
                    
                    try:
                        # Convert PDF to markdown using minerU
                        markdown_content = pdf_parse_main(
                            pdf_path=tmp_path,
                            parse_method='ocr'
                        )
                        
                        # Store processed markdown in 'processed' folder
                        processed_path = f"{self.job_id}/processed/{item['element_id']}.md"
                        markdown_stream = BytesIO(markdown_content.encode('utf-8'))
                        S3Helper().upload_file(
                            file_obj=markdown_stream,
                            path=processed_path,
                            content_type='text/markdown'
                        )
                        
                        # Update item with both paths
                        item['raw_content_path'] = raw_path
                        item['processed_content_path'] = processed_path
                        
                    finally:
                        # Clean up temporary file
                        if os.path.exists(tmp_path):
                            os.unlink(tmp_path)
                
                item.pop('raw_pdf', None)
                
            elif item['type'] == 'Image':
                # Process image content 
                response = self.session.get(item['URL'])
                if response.status_code == 200:
                    content = response.content #raw binary of image
                    item['checksum'] = hashlib.sha256(content).hexdigest()
                    extension = self._get_extension(item['element_id'])
                    path = f"{self.job_id}/raw/{item['element_id']}{extension}"
                    
                    content_stream = BytesIO(content)
                    S3Helper().upload_file(
                        file_obj=content_stream,
                        path=path,
                        content_type=response.headers.get('content-type', 'image/jpeg')
                    )
                    item['raw_content_path'] = path
                    item['processed_content_path'] = '' #TO DO
                item.pop('raw_img', None)
            
            metadata_obj = Scrapy(**item)
            create_scrapy(self.db,metadata_obj)
            
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