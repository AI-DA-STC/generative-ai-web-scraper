from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_utils import database_exists, create_database
from typing import Dict, List, Tuple, Optional

from app.db.session import get_db, engine
from app.db.base import get_model
from config import settings, logger
from app.models import scraper, scraped_changes

class SQLHelper:
    """
    Helper class for managing SQL operations in the web scraper system.
    Handles table versioning, comparisons, and updates while maintaining 
    data integrity and versioning history.
    """
    
    def __init__(self):
        self.engine = engine
        self.settings = settings
        self.db = next(get_db())
        
    def init_db(self,timestamp: str):
        """
        Initialize the database system by creating the database if it doesn't exist
        and ensuring all tables are properly created.
        
        This function performs several important steps:
        1. Checks if the database exists, creates it if it doesn't
        2. Checks if prod table already exists, creates it if it doesn't
        3. Checks if dev table already exists, creates it if it doesn't
        4. Imports all models to register them with SQLAlchemy
        
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
            table_name = f"prod_{timestamp}" if not has_prod_tables else f"dev_{timestamp}"

            #Create model for desired table name
            model = get_model(table_name)
            
            # Create table if it doesn't exist
            if not inspector.has_table(model.__tablename__):
                model.__table__.create(bind=engine)
                logger.info(f"Created new table: {model.__tablename__}")
                if not has_prod_tables:
                    logger.info("This is the first production table in the database")
            else:
                logger.info(f"Table already exists: {model.__tablename__}")
            
            return table_name, model
            
        except Exception as e:
            logger.error(f"Error during database initialization: {str(e)}")
            raise


    def cleanup_tables(self,prod_table, keep_versions: int = 5) -> None:
        """
        Clean up database tables by renaming current prod table and removing old versions.
        
        Args:
            keep_versions: Number of most recent versions to keep
        """
        try:
            with self.db:
                inspector = inspect(engine)
                all_tables = inspector.get_table_names()

                # Rename prod table by removing 'prod_' prefix
                new_table_name = prod_table.replace('prod_', '', 1)
                self.db.execute(text(f"ALTER TABLE {prod_table} RENAME TO {new_table_name}"))
                logger.info(f"Renamed {prod_table} to {new_table_name}")
                
                # Get all dev tables (excluding prod prefix and changes table)
                dev_tables = [t for t in all_tables if t.startswith('dev_')]
                
                # Sort tables by timestamp (newest first)
                dev_tables.sort(reverse=True)
                
                # Keep N most recent dev tables, remove the rest
                if len(dev_tables) > keep_versions:
                    tables_to_remove = dev_tables[keep_versions:]
                    for table in tables_to_remove:
                        self.db.execute(text(f"DROP TABLE IF EXISTS {table}"))
                        logger.info(f"Dropped old dev table: {table}")
                
                    self.db.commit()
                    logger.info(f"Cleanup Complete. Kept {min(keep_versions, len(dev_tables))} most recent dev tables") 
                else:
                    logger.info(f"Cleanup Skipped. Kept all {len(dev_tables)} dev tables")  
                    
        except SQLAlchemyError as e:
            logger.error(f"Error during table cleanup: {str(e)}")
            self.db.rollback()
            raise

    def _get_table_info(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Get prod and dev table names if they exist.
        Returns tuple of (prod_table, dev_table) or (None, None).
        """
        inspector = inspect(engine)
        all_tables = inspector.get_table_names()
        
        prod_tables = [t for t in all_tables if t.startswith('prod_')]
        dev_tables = [t for t in all_tables if t.startswith('dev_')]
        
        if not dev_tables:
            return None, None
            
        prod_table = prod_tables[0]  # Get most recent prod table
        dev_table = dev_tables[0] if dev_tables else None
        
        return prod_table, dev_table

    def convert_dev_to_prod_table(self,dev_table) -> None:
        """Update production table with new data."""
        try:
                
            with self.db:
                # Extract timestamp from dev table
                timestamp = dev_table.split('_')[1]  
                
                # Create new prod table
                new_prod_table = f"prod_{timestamp}"
                
                # Copy data from dev to new prod
                self.db.execute(
                    text(f"""
                        CREATE TABLE {new_prod_table} 
                        AS TABLE {dev_table}
                    """)
                )
                
                self.db.commit()
                logger.info(f"Successfully created new production table: {new_prod_table}")
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to update production table: {str(e)}")
            self.db.rollback()
            raise

    def update_tables(self) -> Dict[str, List[str]]:
        """Compare changes between prod and dev tables."""
        try:
            prod_table, dev_table = self._get_table_info()
            
            if not dev_table:
                logger.info("No dev table found. Skipping comparison.")
                return {
                    'deleted': [],
                    'added': [],
                    'modified': []
                }
            
            with self.db:
                # Get models
                DevModel = scraper.create_scrapedpage_model(dev_table)
                ProdModel = scraper.create_scrapedpage_model(prod_table)
                
                # Fetch all records
                dev_pages = {page.element_id: page for page in self.db.query(DevModel).all()}
                prod_pages = {page.element_id: page for page in self.db.query(ProdModel).all()}
                
                # Track changes
                changes = {
                    'deleted': [],
                    'added': [],
                    'modified': []
                }
                
                # Find added and modified
                for element_id, dev_page in dev_pages.items():
                    if element_id not in prod_pages:
                        changes['added'].append(element_id)
                    elif dev_page.checksum != prod_pages[element_id].checksum:
                        changes['modified'].append(element_id)
                
                # Find deleted
                for element_id in prod_pages:
                    if element_id not in dev_pages:
                        changes['deleted'].append(element_id)
                
                # Store changes to separate table
                ChangesModel = scraped_changes.ScrapedChanges
                
                db_changes = ChangesModel(
                    deleted=changes['deleted'],
                    added=changes['added'],
                    modified=changes['modified']
                )
                
                self.db.add(db_changes)
                self.db.commit()

                #Convert dev table to prod table
                self.convert_dev_to_prod_table(dev_table)
                self.cleanup_tables(prod_table)
                
                return changes
                
        except SQLAlchemyError as e:
            logger.error(f"Error comparing tables: {str(e)}")
            raise


