from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional
from datetime import datetime

class EmbeddedPDF(BaseModel):
    """
    Schema for embedded PDF files found during crawling.
    Used by the content pipeline for processing and validating PDF metadata.
    """
    id: str = Field(..., description="Unique identifier for the PDF")
    url: str = Field(..., description="Source URL of the PDF")
    pdf_content: str = Field(..., description="MinIO URL to PDF content")
    pdf_title: str = Field(..., description="PDF title from metadata or filename")
    page_count: int = Field(..., description="Number of pages")

class EmbeddedImage(BaseModel):
    """
    Schema for embedded images found during crawling.
    Used by the content pipeline for processing and validating image metadata.
    """
    id: str = Field(..., description="Unique identifier for the image")
    url: str = Field(..., description="Source URL of the image")
    image_content: str = Field(..., description="MinIO URL to image content")
    figure_caption: Optional[str] = Field(None, description="Caption if available")
    checksum: str = Field(..., description="SHA-256 hash of image content")

class EmbeddedTable(BaseModel):
    """
    Schema for HTML tables found during crawling.
    Used by the content pipeline for processing and validating table metadata.
    """
    id: str = Field(..., description="Unique identifier for the table")
    content: str = Field(..., description="MinIO URL to HTML content")

class ScrapedMetadata(BaseModel):
    """
    Schema for scraped webpage metadata.
    Used by the content pipeline for validating and processing crawled page data
    before storage in database.
    """
    model_config = ConfigDict(from_attributes=True)

    id: str = Field(..., description="Unique identifier")
    url: str = Field(..., description="Web page URL")
    title: str = Field(..., description="Page title")
    meta_description: Optional[str] = Field(None, description="Meta description")
    language: str = Field(..., description="Page language")
    last_scraped_timestamp: datetime = Field(..., description="Scraping timestamp")
    last_updated: Optional[datetime] = Field(None, description="Last update timestamp")
    crawl_depth: int = Field(..., description="Depth from seed URL")
    
    html_content: str = Field(..., description="MinIO URL to HTML content")
    html_checksum: str = Field(..., description="SHA-256 hash of HTML")
    word_count: int = Field(..., description="Number of words")
    pdf_count: int = Field(..., description="Number of PDFs")
    image_count: int = Field(..., description="Number of images")
    table_count: int = Field(..., description="Number of tables")
    link_count: int = Field(..., description="Number of links")
    
    tables: List[EmbeddedTable] = Field(default_factory=list)
    embedded_pdfs: List[EmbeddedPDF] = Field(default_factory=list)
    embedded_images: List[EmbeddedImage] = Field(default_factory=list)