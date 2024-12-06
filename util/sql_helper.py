from datetime import datetime

from app.db.session import get_db
from app.models.scraper import Scrapy
from app.db.base import import_models
from config import logger,settings
from util.s3_helper import S3Helper


class SQLHelper: 

    def init_db_session(self):
        try:
            #create job_id
            job_id = datetime.now().strftime('%Y%m%d_%H%M%S')

            #verify if minIO bucket exists
            if not S3Helper().verify_bucket_exists():
                raise Exception("Failed to verify MinIO storage")
            
            #initialise db session
            db = get_db()
            
            #import all models 
            import_models()

            return db,job_id
            
        except Exception as e:
            logger.error(f"Error during database initialization: {str(e)}")
            raise
    
    def create_table(self):

        from sqlalchemy import create_engine, inspect
        
        engine = create_engine(settings.DATABASE_URL)
        inspector = inspect(engine)

        if not inspector.has_table(Scrapy.__tablename__):
            Scrapy.__table__.create(bind=engine)
            logger.info(f"Created new table: {Scrapy.__tablename__}")
        else:
            logger.info(f"Skipping. Table already exists: {Scrapy.__tablename__}")

