import sys
import pyprojroot
root = pyprojroot.find_root(pyprojroot.has_dir("config"))
sys.path.append(str(root))

from sqlalchemy import Column, String
from app.db.base import Base

from sqlalchemy import Column, String

def create_scrapedpage_model(table_name: str):
    """
    Creates a dynamic SQLAlchemy model for storing scraped content metadata.
    
    Args:
        table_name: Name for the database table
        
    Returns:
        SQLAlchemy model class configured for the specified table name
    """
    class ScrapedPage(Base):
        """
        SQLAlchemy model for storing scraped content metadata.
        Handles HTML pages, PDFs, and images with parent-child relationships.
        """
        __tablename__ = table_name
        
        element_id = Column(
            String,
            primary_key=True,
            comment="URL of HTML/PDF/Image content"
        )
        
        type = Column(
            String,
            nullable=False,
            index=True,
            comment="Content type (URL/PDF/Image)"
        )
        
        content = Column(
            String,
            nullable=False,
            comment="MinIO URL to raw content"
        )
        
        checksum = Column(
            String,
            nullable=False,
            comment="SHA-256 hash of content"
        )
        
        parent_id = Column(
            String,
            nullable=True,
            comment="Parent HTML URL (element_id) for PDFs/Images"
        )

        def to_dict(self):
            """Convert model instance to dictionary."""
            return {
                'element_id': self.element_id,
                'type': self.type,
                'content': self.content,
                'checksum': self.checksum,
                'parent_id': self.parent_id
            }
            
    return ScrapedPage