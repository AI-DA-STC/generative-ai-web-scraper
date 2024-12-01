import sys
import pyprojroot
root = pyprojroot.find_root(pyprojroot.has_dir("config"))
sys.path.append(str(root))

from app.db.base import Base

from sqlalchemy import Column, String, Integer, DateTime, JSON
from sqlalchemy.sql import func


class ScrapedPage(Base):
    """
    SQLAlchemy model for storing scraped webpage data.
    Stores page content and metadata from crawler results.
    """
    __tablename__ = "scraped_pages"

    id = Column(
        String, 
        primary_key=True,
        comment="Unique identifier combining checksum and timestamp"
    )
    url = Column(
        String, 
        index=True, 
        nullable=False,
        comment="Source URL of the scraped page"
    )
    title = Column(
        String, 
        nullable=True,
        comment="Page title"
    )
    meta_description = Column(
        String, 
        nullable=True,
        comment="Meta description from page"
    )
    language = Column(
        String, 
        nullable=False,
        comment="Page language"
    )
    last_scraped_timestamp = Column(
        DateTime, 
        nullable=False,
        server_default=func.now(),
        comment="When the page was scraped"
    )
    last_updated = Column(
        DateTime, 
        nullable=True,
        comment="Page's last update time if available"
    )
    crawl_depth = Column(
        Integer, 
        nullable=False,
        comment="Depth from seed URL"
    )
    
    # Content storage references
    html_content = Column(
        String, 
        nullable=False,
        comment="MinIO URL to raw HTML"
    )
    html_checksum = Column(
        String, 
        nullable=False,
        comment="SHA-256 hash of HTML content"
    )
    
    # Content statistics
    word_count = Column(
        Integer, 
        nullable=False, 
        default=0,
        comment="Number of words in content"
    )
    pdf_count = Column(
        Integer, 
        nullable=False, 
        default=0,
        comment="Number of embedded PDFs"
    )
    image_count = Column(
        Integer, 
        nullable=False, 
        default=0,
        comment="Number of embedded images"
    )
    table_count = Column(
        Integer, 
        nullable=False, 
        default=0,
        comment="Number of HTML tables"
    )
    link_count = Column(
        Integer, 
        nullable=False, 
        default=0,
        comment="Number of outgoing links"
    )
    
    # Nested content storage (as JSON)
    tables = Column(
        JSON, 
        nullable=False, 
        default=list,
        comment="List of table metadata and MinIO URLs"
    )
    embedded_pdfs = Column(
        JSON, 
        nullable=False, 
        default=list,
        comment="List of PDF metadata and MinIO URLs"
    )
    embedded_images = Column(
        JSON, 
        nullable=False, 
        default=list,
        comment="List of image metadata and MinIO URLs"
    )

    def to_dict(self):
        """Convert model instance to dictionary."""
        return {
            'id': self.id,
            'url': self.url,
            'title': self.title,
            'meta_description': self.meta_description,
            'language': self.language,
            'last_scraped_timestamp': self.last_scraped_timestamp,
            'last_updated': self.last_updated,
            'crawl_depth': self.crawl_depth,
            'html_content': self.html_content,
            'html_checksum': self.html_checksum,
            'word_count': self.word_count,
            'pdf_count': self.pdf_count,
            'image_count': self.image_count,
            'table_count': self.table_count,
            'link_count': self.link_count,
            'tables': self.tables,
            'embedded_pdfs': self.embedded_pdfs,
            'embedded_images': self.embedded_images
        }