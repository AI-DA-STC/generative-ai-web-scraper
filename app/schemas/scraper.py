from pydantic import BaseModel, Field, ConfigDict
from typing import Optional

class ScrapyCreate(BaseModel):
    """
    Schema for scrapy job metadata storage
    """
    model_config = ConfigDict(from_attributes=True)

    element_id: str = Field(...,description="checksum of URL of HTML/PDF/Image + job id of scraping job")
    URL: str = Field(..., description="URL of HTML/PDF/Image")
    type: str = Field(..., description="Type of content, URL/PDF/Image")
    raw_content_path: str = Field(..., description="URL to minIO containing raw content")
    processed_content_path: str = Field(..., description="URL to minIO containing processed content")
    checksum: str = Field(..., description="SHA256 checksum of content")
    parent_id: Optional[str] = Field(None, description="element_id if entry is PDF/Image, None if HTML")

