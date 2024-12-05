from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

from typing import Type

import sys
import pyprojroot
root = pyprojroot.find_root(pyprojroot.has_dir("config"))
sys.path.append(str(root))

from config import logger

def get_model(tablename: str) -> Type:
    """
    Get or create a SQLAlchemy model class for a given tablename.
    
    Args:
        tablename: table naming
        
    Returns:
        SQLAlchemy model class configured for the tablename
    """
    from app.models.scraper import create_scrapedpage_model
    
    # Create model with timestamp-based table name
    model = create_scrapedpage_model(tablename)
        
    return model
