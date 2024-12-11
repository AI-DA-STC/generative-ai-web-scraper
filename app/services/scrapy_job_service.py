from sqlalchemy.orm import Session

from app.models.scraper import Scrapy
from app.schemas.scraper import ScrapyCreate


def create_scrapy(db: Session, scrapy_data: ScrapyCreate) -> None :
    # Create a new Scrapy object
    db_scrapy_data = Scrapy(
        element_id=scrapy_data.element_id,
        URL=scrapy_data.URL,
        type=scrapy_data.type,
        raw_content_path=scrapy_data.raw_content_path,
        processed_content_path=scrapy_data.processed_content_path,
        checksum=scrapy_data.checksum,
        parent_id=scrapy_data.parent_id,
    )
    # Add the new event to the database
    db.add(db_scrapy_data)
    # Commit the changes
    db.commit()
    # Refresh the event
    db.refresh(db_scrapy_data)