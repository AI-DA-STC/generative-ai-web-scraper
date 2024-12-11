from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.http import Request
import hashlib
from typing import Dict, Any, List
from urllib.parse import urljoin

class WebCrawlerSpider(CrawlSpider):
    """Spider for crawling websites and extracting content."""
    name = 'web_crawler'
    
    def __init__(
        self,
        job_id: str,
        db,
        start_urls: List[str],
        max_depth: int = 2,
        follow_links: bool = True,
        *args,
        **kwargs
    ):
        """Initialize the spider with crawling parameters."""
        self.start_urls = start_urls
        self.max_depth = max_depth
        self.follow_links = follow_links
        self.job_id = job_id
        self.db = db

        # Define crawling rules
        rules = []
        if follow_links:
            rules.append(
                Rule(
                    LinkExtractor(),
                    callback='parse_page',
                    follow=True,
                    process_request='process_request'
                )
            )
        
        self.rules = rules
        super().__init__(*args, **kwargs)
    
    def start_requests(self):
        """Generate initial requests for start URLs."""
        for url in self.start_urls:
            yield Request(url, callback=self.parse_page, meta={'depth': 0})
    
    def process_request(self, request, spider):
        """Process and filter requests based on depth constraints."""
        depth = request.meta.get('depth', 0)
        if depth >= self.max_depth:
            return None
        request.meta['depth'] = depth + 1
        return request
    
    def parse_page(self, response):
        """Parse a webpage and extract content and metadata."""

        html_content = response.text
        html_URL_checksum = hashlib.sha256(response.url.encode()).hexdigest()
        
        # Create HTML page entry
        yield {
            'element_id': f"{html_URL_checksum}_{self.job_id}",
            'URL': response.url,
            'type': 'URL',
            'raw_content_path': '',  # Will be set by pipeline
            'processed_content_path':'', # Will be set by pipeline
            'checksum': '',
            'parent_id': None,
            'raw_html': html_content  # For pipeline processing
        }
        
        # Extract and yield PDFs
        for pdf in self._extract_pdfs(response):
            pdf_URL_checksum = hashlib.sha256(pdf['url'].encode()).hexdigest()
            yield {
                'element_id': f"{pdf_URL_checksum}_{self.job_id}",
                'URL': pdf['url'],
                'type': 'PDF',
                'raw_content_path': '',  # Will be set by pipeline
                'processed_content_path':'', # Will be set by pipeline
                'checksum': '',  # Will be set by pipeline
                'parent_id': response.url,
                'raw_pdf': pdf  # For pipeline processing
            }
        
        # Extract and yield images
        for img in self._extract_images(response):
            img_URL_checksum = hashlib.sha256(img['url'].encode()).hexdigest()
            yield {
                'element_id': f"{img_URL_checksum}_{self.job_id}",
                'URL': img['url'],
                'type': 'Image',
                'raw_content_path': '',  # Will be set by pipeline
                'processed_content_path':'', # Will be set by pipeline
                'checksum': '',  # Will be set by pipeline
                'parent_id': response.url,
                'raw_img': img  # For pipeline processing
            }
    
    def _extract_pdfs(self, response) -> List[Dict]:
        """Extract PDF links including those with query parameters.
        
        Handles PDF URLs in formats like:
        - direct.pdf
        - path/file.pdf
        - file.pdf?parameter=value
        - file.pdf#section
        """
        pdfs = []
        # Match href containing .pdf anywhere in the string
        pdf_links = response.css('a[href*=".pdf"]::attr(href)').getall()
        
        for pdf_link in pdf_links:
            # Verify this is actually a PDF link (not just containing .pdf somewhere random)
            if '.pdf' in pdf_link.lower().split('?')[0]:  # Check before any query parameters
                absolute_url = urljoin(response.url, pdf_link)
                pdfs.append({
                    'url': absolute_url
                })
        
        return pdfs
    
    def _extract_images(self, response) -> List[Dict]:
        """Extract images."""
        images = []
        for img in response.css('img'):
            src = img.css('::attr(src)').get()
            if src:
                absolute_url = urljoin(response.url, src)
                images.append({
                    'url': absolute_url
                })
        return images
        
    def crawl(self, settings: Dict[str, Any]) -> List[Dict]:
        """Run the crawler with given settings."""
        from scrapy.crawler import CrawlerProcess
        
        process = CrawlerProcess(settings)
        
        process.crawl(
            self.__class__,
            start_urls=self.start_urls,
            max_depth=self.max_depth,
            follow_links=self.follow_links,
            job_id = self.job_id,
            db = self.db
        )
        
        process.start()