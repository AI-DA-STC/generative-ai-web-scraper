from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List


class ScrapedMetadata(BaseModel):
    """
    Schema for scraped webpage metadata.
    Used by the content pipeline for validating and processing crawled page data
    before storage in database.
    """
    model_config = ConfigDict(from_attributes=True)

    element_id: str = Field(...,description="URL of HTML/PDF/Image")
    type: str = Field(..., description="Type of content, URL/PDF/Image")
    content: str = Field(..., description="URL to minIO containing raw content")
    checksum: str = Field(..., description="SHA256 checksum of content")
    parent_id: Optional[str] = Field(None, description="element_id if entry is PDF/Image, None if HTML")

class TableChanges(BaseModel):
    """Schema for tracking changes between versions."""
    model_config = ConfigDict(from_attributes=True)
    version_id: str = Field(..., description="Version ID of the production tables")
    deleted: List[str] = Field(default_factory=list, description="List of deleted element_ids")
    added: List[str] = Field(default_factory=list, description="List of added element_ids")
    modified: List[str] = Field(default_factory=list, description="List of modified element_ids")