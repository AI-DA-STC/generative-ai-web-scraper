from sqlalchemy import inspect, text
from typing import Dict, List

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.db.session import SessionLocal, engine
from app.models.scraper import create_scrapedpage_model
from config import settings,logger

class SQLHelper:
    """
    Helper class for managing SQL operations in the web scraper system.
    Handles table versioning, comparisons, and updates while maintaining 
    data integrity and versioning history.
    """
    
    def __init__(self):
        self.engine = engine
        self.settings = settings
        
    def verify_connection(self) -> bool:
        """
        Verify database connection is active and working.
        Returns True if connection is successful, raises exception otherwise.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise

    def create_version_table(self, timestamp: str) -> str:
        """
        Creates a new versioned table for storing scraped data.
        Naming convention: scraped_pages_{timestamp}
        
        Used for storing new scraping results before they are approved for production.
        """
        try:
            table_name = f"scraped_pages_{timestamp}"
            model = create_scrapedpage_model(table_name)
            
            # Create new table with timestamp
            if not inspect(self.engine).has_table(table_name):
                model.__table__.create(self.engine)
                logger.info(f"Created versioned table: {table_name}")
            
            return table_name
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to create version table: {str(e)}")
            raise

    def create_prod_table(self, timestamp: str) -> str:
        """
        Creates or updates the production table with timestamp suffix.
        Naming convention: scraped_pages_{timestamp}_prod
        
        Production table contains the approved and active dataset.
        """
        try:
            prod_table = f"prod_{timestamp}"
            model = create_scrapedpage_model(prod_table)
            
            # Create prod table if it doesn't exist
            if not inspect(self.engine).has_table(prod_table):
                model.__table__.create(self.engine)
                logger.info(f"Created production table: {prod_table}")
            
            return prod_table
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to create production table: {str(e)}")
            raise

    def compare_table_changes(
        self, 
        timestamp: str
    ) -> Dict[str, List[Dict]]:
        """
        Compares newly scraped data with production data to identify changes.
        
        Performs detailed comparison of content and metadata to identify:
        - New pages added
        - Existing pages modified
        - Pages removed
        Returns a dictionary with categorized changes.
        """
        try:
            # Get list of all tables
            inspector = inspect(self.engine)
            all_tables = inspector.get_table_names()
            
            # Find production table if it exists
            prod_table = [table for table in all_tables if table.startswith('prod_')][0]
            
            if not prod_table:
                logger.info("No production table found. Skipping comparison.")
                return {
                    'added': [],
                    'modified': [],
                    'deleted': []
                }
            
            # Use the existing production table for comparison
            version_table = f"{timestamp}"
            
            with SessionLocal() as db:
                # Get models for both tables
                VersionModel = create_scrapedpage_model(version_table)
                ProdModel = create_scrapedpage_model(prod_table)
                
                # Fetch all pages from both tables
                new_pages = {page.url: page for page in db.query(VersionModel).all()}
                prod_pages = {page.url: page for page in db.query(ProdModel).all()}
                
                # Initialize change tracking
                changes = {
                    'added': [],
                    'modified': [],
                    'deleted': []
                }
                
                # Find added and modified pages
                for url, new_page in new_pages.items():
                    if url not in prod_pages:
                        # New page
                        changes['added'].append(new_page.to_dict())
                    else:
                        # Check for modifications
                        prod_page = prod_pages[url]
                        if self._detect_content_changes(new_page, prod_page):
                            changes['modified'].append({
                                'url': url,
                                'changes': self._get_change_details(new_page, prod_page)
                            })
                
                # Find deleted pages
                for url in prod_pages:
                    if url not in new_pages:
                        changes['deleted'].append({'url': url})
                
                return changes
                
        except SQLAlchemyError as e:
            logger.error(f"Error comparing tables: {str(e)}")
            raise

    def update_prod_table(self, timestamp: str) -> None:
        """
        Updates production table with new data after approval.
        
        Process:
        1. Creates backup of current production data
        2. Updates production table with new version
        3. Maintains data integrity with transaction handling
        """
        try:
            version_table = f"{timestamp}"

            # Get list of all tables
            inspector = inspect(self.engine)
            all_tables = inspector.get_table_names()
            
            # Find production table if it exists
            prod_table = [table for table in all_tables if table.startswith('prod_')][0]
            
            with SessionLocal() as db:
                
                # Create new prod table
                self.create_prod_table(timestamp)
                
                # Copy approved data to production
                db.execute(
                    text(f"""
                        INSERT INTO {prod_table} 
                        SELECT * FROM {version_table}
                    """)
                )
                
                db.commit()
                logger.info(f"Successfully updated production table")
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to update production table: {str(e)}")
            db.rollback()
            raise

    def cleanup_old_versions(self, keep_versions: int = 5) -> None:
        """
        Cleans up old database tables while maintaining history.
        
        Handles two types of tables:
        1. Production tables (prefix 'prod_'): Keeps only the latest version
        2. Version tables: Keeps the specified number of recent versions
        
        Args:
            keep_versions: Number of version tables to retain
        """
        try:
            inspector = inspect(self.engine)
            all_tables = inspector.get_table_names()
            
            # Handle production tables - keep only latest
            prod_tables = [t for t in all_tables if t.startswith('prod_')]
            if prod_tables:
                # Sort by timestamp in table name (newest first)
                prod_tables.sort(reverse=True)
                # Remove all but the latest prod table
                for table in prod_tables[1:]:
                    self.engine.execute(text(f"DROP TABLE IF EXISTS {table}"))
                    logger.info(f"Removed old production table: {table}")
            
            # Handle version tables
            version_tables = [
                t for t in all_tables 
                if not t.startswith('prod_')
            ]
            
            # Sort by timestamp (newest first)
            version_tables.sort(reverse=True)
            
            # Remove excess version tables
            if len(version_tables) > keep_versions:
                tables_to_remove = version_tables[keep_versions:]
                for table in tables_to_remove:
                    self.engine.execute(text(f"DROP TABLE IF EXISTS {table}"))
                    logger.info(f"Removed old version table: {table}")
                    
            logger.info(f"Cleanup completed. Retained latest prod table : {prod_tables[0]}")
            
        except SQLAlchemyError as e:
            logger.error(f"Error cleaning up old versions: {str(e)}")
            raise

    def _detect_content_changes(self, new_page: object, prod_page: object) -> bool:
        """
        Checks for meaningful changes between page versions.
        
        Compares:
        - HTML content checksum
        - Resource counts (PDFs, images, tables)
        - Content length and structure
        """
        return any([
            new_page.html_checksum != prod_page.html_checksum,
            new_page.pdf_count != prod_page.pdf_count,
            new_page.image_count != prod_page.image_count,
            new_page.table_count != prod_page.table_count,
            abs(new_page.word_count - prod_page.word_count) > self.settings.MIN_CONTENT_CHANGE_THRESHOLD
        ])

    def _create_backup(self, db: Session, source: str, backup: str) -> None:
        """
        Creates a backup copy of a table before making changes.
        
        Ensures data safety by maintaining a backup copy before
        any significant changes to production data.
        """
        try:
            # Create backup table with same structure
            db.execute(text(f"CREATE TABLE {backup} (LIKE {source} INCLUDING ALL)"))
            
            # Copy data
            db.execute(text(f"INSERT INTO {backup} SELECT * FROM {source}"))
            db.commit()
            
            logger.info(f"Created backup table: {backup}")
            
        except SQLAlchemyError as e:
            logger.error(f"Backup creation failed: {str(e)}")
            db.rollback()
            raise

    def _get_change_details(self, new_page: object, prod_page: object) -> Dict:
        """
        Generates detailed change report between page versions.
        
        Identifies specific changes in:
        - Metadata (title, description)
        - Content statistics
        - Embedded resources
        """
        changes = {}
        
        # Check basic attributes
        for attr in ['title', 'meta_description', 'word_count', 'pdf_count', 
                    'image_count', 'table_count', 'link_count']:
            new_val = getattr(new_page, attr)
            prod_val = getattr(prod_page, attr)
            if new_val != prod_val:
                changes[attr] = {
                    'old': prod_val,
                    'new': new_val,
                    'change_pct': self._calculate_change_pct(prod_val, new_val)
                }
        
        # Compare embedded resources
        for resource_type in ['embedded_pdfs', 'embedded_images', 'tables']:
            self._compare_resources(
                changes,
                getattr(new_page, resource_type),
                getattr(prod_page, resource_type),
                resource_type
            )
        
        return changes

    def _table_exists(self, table_name: str) -> bool:
        """
        Checks if a table exists in the database.
        
        Used for verification before table operations to prevent errors.
        """
        return inspect(self.engine).has_table(table_name)

    @staticmethod
    def _calculate_change_pct(old: int, new: int) -> float:
        """
        Calculates percentage change between values.
        
        Used for quantifying the magnitude of changes in content metrics.
        """
        if old == 0:
            return 100.0 if new > 0 else 0.0
        return ((new - old) / old) * 100