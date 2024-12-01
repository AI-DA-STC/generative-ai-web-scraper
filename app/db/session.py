from sqlalchemy import create_engine, inspect
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker, Session

import sys
import pyprojroot
root = pyprojroot.find_root(pyprojroot.has_dir("config"))
sys.path.append(str(root))

from app.db.base import Base,import_all_models
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

def init_db() -> None:
    """
    Initialize the database system by creating the database if it doesn't exist
    and ensuring all tables are properly created.
    
    This function performs several important steps:
    1. Checks if the database exists, creates it if it doesn't
    2. Creates a connection to the database
    3. Imports all models to register them with SQLAlchemy
    4. Creates all tables that don't already exist
    
    The function uses exception handling to ensure database operations are safe
    and provides detailed logging for troubleshooting.
    """
    try:
        # Check database existence
        if not database_exists(settings.DATABASE_URL):
            logger.info(f"Database does not exist. Creating database...")
            create_database(settings.DATABASE_URL)
            logger.info(f"Database created successfully")
        # Import models
        import_all_models()
        logger.info("created model schemas")
        
        # Create tables
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        logger.info("created tables from model schemas")
        
        # Get all models that inherit from Base
        models = Base.metadata.tables.keys()
        
        # Create only missing tables
        for table_name in models:
            if table_name not in existing_tables:
                logger.info(f"Creating table: {table_name}")
                Base.metadata.tables[table_name].create(bind=engine)
            else:
                logger.info(f"Table already exists: {table_name}")
        
        logger.info("Database initialization completed successfully")
        
    except Exception as e:
        logger.error(f"Error during database initialization: {str(e)}")
        raise