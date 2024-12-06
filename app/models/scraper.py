import sys
import pyprojroot
root = pyprojroot.find_root(pyprojroot.has_dir("config"))
sys.path.append(str(root))

from sqlalchemy import Column, String, UniqueConstraint
from app.db.base import Base

class Scrapy(Base):
    """
    SQLAlchemy model for storing scraped content metadata.
    Handles HTML pages, PDFs, and images with parent-child relationships.
    """
    __tablename__ = "scraped_metadata"
    
    element_id = Column(
        String,
        primary_key=True,
        comment="checksum_URL + job_id"
    )

    URL = Column(
        String,
        nullable=False,
        comment="URL of HTML/PDF/Image"
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


    