from sqlalchemy import create_engine, inspect
from sqlalchemy_utils import database_exists, create_database
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

def init_db(timestamp: str) -> None:
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

        # Check for existing tables with 'prod_' prefix
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        logger.info(f"Found ({len(existing_tables)}) tables :\n{existing_tables}")
        has_prod_tables = any(table.startswith('prod_') for table in existing_tables)
        
        # Create new versioned table if a production table is already present
        table_name = f"prod_{timestamp}" if not has_prod_tables else timestamp

        model = get_model_for_timestamp(table_name)
        logger.info(f"created model schemas for tablename {model.__tablename__}")
        
        # Create table if it doesn't exist
        if not inspector.has_table(model.__tablename__):
            model.__table__.create(bind=engine)
            logger.info(f"Created new table: {model.__tablename__}")
            if not has_prod_tables:
                logger.info("This is the first production table in the database")
        else:
            logger.info(f"Table already exists: {model.__tablename__}")
        
        return table_name,model
        
    except Exception as e:
        logger.error(f"Error during database initialization: {str(e)}")
        raise