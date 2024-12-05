from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.http import Request
import hashlib
from typing import Dict, Any, List
from urllib.parse import urljoin
from bs4 import BeautifulSoup

class WebCrawlerSpider(CrawlSpider):
    """Spider for crawling websites and extracting content."""
    name = 'web_crawler'
    
    def __init__(
        self,
        table_name: str,
        model: Any,
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
        self.table_name = table_name
        self.model = model

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
        soup = BeautifulSoup(response.text, 'html.parser')
        html_content = response.text
        
        # Calculate checksum of HTML content
        html_checksum = hashlib.sha256(html_content.encode()).hexdigest()
        
        # Create HTML page entry
        yield {
            'element_id': response.url,
            'type': 'URL',
            'content': '',  # Will be set by pipeline after S3 upload
            'checksum': html_checksum,
            'parent_id': None,
            'raw_html': html_content  # For pipeline processing
        }
        
        # Extract and yield PDFs
        for pdf in self._extract_pdfs(response):
            yield {
                'element_id': pdf['url'],
                'type': 'PDF',
                'content': '',  # Will be set by pipeline
                'checksum': '',  # Will be set by pipeline
                'parent_id': response.url,
                'raw_pdf': pdf  # For pipeline processing
            }
        
        # Extract and yield images
        for img in self._extract_images(response):
            yield {
                'element_id': img['url'],
                'type': 'Image',
                'content': '',  # Will be set by pipeline
                'checksum': '',  # Will be set by pipeline
                'parent_id': response.url,
                'raw_image': img  # For pipeline processing
            }
    
    def _extract_pdfs(self, response) -> List[Dict]:
        """Extract PDF links."""
        pdfs = []
        pdf_links = response.css('a[href$=".pdf"]::attr(href)').getall()
        
        for pdf_link in pdf_links:
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
            table_name=self.table_name,
            model=self.model
        )
        
        process.start()