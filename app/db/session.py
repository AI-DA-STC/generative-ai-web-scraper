from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

import sys
import pyprojroot
root = pyprojroot.find_root(pyprojroot.has_dir("config"))
sys.path.append(str(root))

from app.db.base import get_model_for_timestamp
from config import settings,logger

# Create database engine with connection pooling
engine = create_engine(
    settings.DATABASE_URL,
)

# Create sessionmaker with the configured engine
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False
)

def get_db() -> Session:
    """
    Get a database session.
    
    Returns:
        Session: SQLAlchemy session object
    """
    db = SessionLocal()
    try:
        return db
    except Exception:
        db.close()
        raise