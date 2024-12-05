import sys
import pyprojroot
root = pyprojroot.find_root(pyprojroot.has_dir("config"))
sys.path.append(str(root))

from sqlalchemy import Column, String
from app.db.base import Base

from sqlalchemy import Column, String, ARRAY

class ScrapedChanges(Base):
    """Model for storing changes between versions."""

    __tablename__ = "scraped_metadata_changes"
    
    version_id = Column(
        String,
        primary_key=True)  # Will be prod_{timestamp}
    
    deleted = Column(
        ARRAY(String),
        nullable=False,
        default=list)
    
    added = Column(
        ARRAY(String),
        nullable=False,
        default=list)
    
    modified = Column(
        ARRAY(String),
        nullable=False,
        default=list)
    def to_dict(self):
        """Convert model instance to dictionary."""
        return {
            'version_id': self.version_id,
            'deleted': self.deleted,
            'added': self.added,
            'modified': self.modified
        }
            