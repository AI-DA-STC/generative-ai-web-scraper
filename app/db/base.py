from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

import sys
import pyprojroot
root = pyprojroot.find_root(pyprojroot.has_dir("config"))
sys.path.append(str(root))

def import_all_models() -> None:
    """
    Imports all SQLAlchemy models to ensure they're registered with Base.
    
    This function is crucial for SQLAlchemy's metadata management and should be called
    when initializing the application to ensure all models are properly registered
    before creating database tables.
    
    Note: Import statements are inside the function to avoid circular imports, as models
    themselves import Base from this module.
    """
    # Import models here to avoid circular imports
    from app.models.scraper import ScrapedPage  # Model for scraped content
