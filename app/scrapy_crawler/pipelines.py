import hashlib
import requests
from io import BytesIO
from typing import Dict, Any, List
import fitz  # PyMuPDF

from datetime import datetime
import sys
import pyprojroot
root = pyprojroot.find_root(pyprojroot.has_dir("config"))
sys.path.append(str(root))

from app.db.session import SessionLocal
from app.db.base import get_model_for_timestamp
from app.schemas.scraper import ScrapedMetadata, EmbeddedPDF, EmbeddedImage, EmbeddedTable
from util.s3_helper import S3Helper
from config import settings, logger

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
            db = SessionLocal()
            
            # Store raw HTML
            html_path = f"{self.base_path}/html/{item['id']}_content.html"
            self._store_html_content(item['raw_html'], html_path)
            item['html_content'] = html_path
            
            # Process and validate embedded content
            item['embedded_pdfs'] = self._process_pdfs(item)
            item['embedded_images'] = self._process_images(item)
            item['tables'] = self._process_tables(item)
            
            # Validate complete metadata using schema
            metadata = ScrapedMetadata(**item)
            
            # Store in database
            db_page = self.ScrapedPage(
                id=metadata.id,
                url=metadata.url,
                title=metadata.title,
                meta_description=metadata.meta_description,
                language=metadata.language,
                last_scraped_timestamp=metadata.last_scraped_timestamp,
                last_updated=metadata.last_updated,
                crawl_depth=metadata.crawl_depth,
                html_content=metadata.html_content,
                html_checksum=metadata.html_checksum,
                word_count=metadata.word_count,
                pdf_count=metadata.pdf_count,
                image_count=metadata.image_count,
                table_count=metadata.table_count,
                link_count=metadata.link_count,
                tables=[table.model_dump() for table in metadata.tables],
                embedded_pdfs=[pdf.model_dump() for pdf in metadata.embedded_pdfs],
                embedded_images=[img.model_dump() for img in metadata.embedded_images]
            )
            
            db.add(db_page)
            db.commit()
            
            # Clean up raw content
            item.pop('raw_html', None)
            
            return item
            
        except Exception as e:
            logger.error(f"Error processing item {item.get('url', 'unknown')}: {str(e)}")
            raise
        
        finally:
            db.close()
    
    def _store_html_content(self, html_content: str, path: str) -> None:
        """Store HTML content in MinIO."""
        content_stream = BytesIO(html_content.encode('utf-8'))
        S3Helper().upload_file(
        file_obj=content_stream,
        path=path,
        content_type='text/html'
        )
    
    def _process_pdfs(self, item: Dict[str, Any]) -> List[Dict]:
        """Download and process PDF files."""
        processed_pdfs = []
        for pdf in item['embedded_pdfs']:
            try:
                response = self.session.get(pdf['url'])
                if response.status_code == 200:
                    content = response.content
                    
                    # Process PDF metadata
                    pdf_doc = fitz.open(stream=content, filetype="pdf")
                    pdf['page_count'] = len(pdf_doc)
                    pdf['pdf_size'] = len(content)
                    
                    # Store in MinIO
                    path = f"{self.base_path}/pdfs/{item['id']}/{pdf['id']}.pdf"
                    content_stream = BytesIO(content)
                    S3Helper().upload_file(
                        file_obj=content_stream,
                        path=path,
                        content_type='application/pdf'
                    )
                    pdf['pdf_content'] = path
                    
                    validated_pdf = EmbeddedPDF(**pdf)
                    processed_pdfs.append(validated_pdf.model_dump())
                    
            except Exception as e:
                logger.error(f"Error processing PDF {pdf['url']}: {str(e)}")
        
        return processed_pdfs
    
    def _process_images(self, item: Dict[str, Any]) -> List[Dict]:
        """Download and process images."""
        processed_images = []
        for img in item['embedded_images']:
            try:
                response = self.session.get(img['url'])
                if response.status_code == 200:
                    content = response.content
                    
                    img['checksum'] = hashlib.sha256(content).hexdigest()
                    img['size'] = len(content)
                    
                    path = f"{self.base_path}/images/{item['id']}/{img['id']}{self._get_extension(img['url'])}"
                    content_stream = BytesIO(content)
                    S3Helper().upload_file(
                        file_obj=content_stream,
                        path=path,
                        content_type=response.headers.get('content-type', 'image/jpeg')
                    )
                    img['image_content'] = path
                    
                    validated_image = EmbeddedImage(**img)
                    processed_images.append(validated_image.model_dump())
                    
            except Exception as e:
                logger.error(f"Error processing image {img['url']}: {str(e)}")
        
        return processed_images
    
    def _process_tables(self, item: Dict[str, Any]) -> List[Dict]:
        """Store HTML tables."""
        processed_tables = []
        for table in item['tables']:
            try:
                path = f"{self.base_path}/tables/{item['id']}/{table['id']}.html"
                content_stream = BytesIO(table['content'].encode('utf-8'))
                S3Helper().upload_file(
                    file_obj=content_stream,
                    path=path,
                    content_type='text/html'
                )
                table['content'] = path
                
                # Validate using schema
                validated_table = EmbeddedTable(**table)
                processed_tables.append(validated_table.model_dump())
                
            except Exception as e:
                logger.error(f"Error processing table {table['id']}: {str(e)}")
                
        return processed_tables
    
    def _get_extension(self, url: str) -> str:
        """Extract file extension from URL."""
        if '.' in url:
            return '.' + url.split('.')[-1].lower()
        return '.jpg'
    
    async def close_spider(self, spider):
        """Cleanup when spider closes."""
        await self.session.close()