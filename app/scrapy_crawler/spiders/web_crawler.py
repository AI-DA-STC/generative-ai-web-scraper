from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.http import Request
from datetime import datetime
import hashlib
from typing import Dict, Any, List
import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup

class WebCrawlerSpider(CrawlSpider):
    """Spider for crawling websites and extracting content."""
    name = 'web_crawler'
    
    def __init__(
        self,
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
        
        # Generate unique ID
        html_checksum = hashlib.sha256(html_content.encode()).hexdigest()
        timestamp = datetime.now().isoformat()
        page_id = f"{html_checksum}_{timestamp}"
        
        # Extract content
        tables = self._extract_tables(soup)
        pdfs = self._extract_pdfs(response)
        images = self._extract_images(response)
        
        # Count elements
        word_count = len(re.findall(r'\w+', soup.get_text()))
        link_count = len(soup.find_all('a', href=True))
        
        # Build metadata matching ScrapedMetadata schema
        metadata = {
            'id': page_id,
            'url': response.url,
            'title': soup.title.string if soup.title else '',
            'meta_description': self._get_meta_description(soup),
            'language': self._detect_language(soup),
            'last_scraped_timestamp': datetime.now(),
            'last_updated': self._get_last_updated(soup),
            'crawl_depth': response.meta.get('depth', 0),
            
            'html_content': '',  # Set by pipeline after S3 upload
            'html_checksum': html_checksum,
            'word_count': word_count,
            'pdf_count': len(pdfs),
            'image_count': len(images),
            'table_count': len(tables),
            'link_count': link_count,
            
            'tables': tables,
            'embedded_pdfs': pdfs,
            'embedded_images': images,
        }
        
        metadata['raw_html'] = html_content  # For pipeline processing
        
        yield metadata
    
    # Helper methods remain unchanged as they work with our schema
    def _extract_tables(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract HTML tables matching EmbeddedTable schema."""
        tables = []
        for idx, table in enumerate(soup.find_all('table')):
            tables.append({
                'id': f'table_{idx}',
                'content': str(table)
            })
        return tables
    
    def _extract_pdfs(self, response) -> List[Dict]:
        """Extract PDF links matching EmbeddedPDF schema."""
        pdfs = []
        pdf_links = response.css('a[href$=".pdf"]::attr(href)').getall()
        
        for idx, pdf_link in enumerate(pdf_links):
            absolute_url = urljoin(response.url, pdf_link)
            pdfs.append({
                'id': f'pdf_{idx}',
                'url': absolute_url,
                'pdf_content': '',
                'pdf_title': pdf_link.split('/')[-1],
                'pdf_size': 0,
                'page_count': 0
            })
        return pdfs
    
    def _extract_images(self, response) -> List[Dict]:
        """Extract images matching EmbeddedImage schema."""
        images = []
        for idx, img in enumerate(response.css('img')):
            src = img.css('::attr(src)').get()
            if src:
                absolute_url = urljoin(response.url, src)
                caption = img.xpath('./following-sibling::figcaption/text()').get()
                
                images.append({
                    'id': f'img_{idx}',
                    'url': absolute_url,
                    'image_content': '',
                    'figure_caption': caption,
                    'checksum': '',
                    'size': 0
                })
        return images
    
    def _get_meta_description(self, soup: BeautifulSoup) -> str:
        """Extract meta description."""
        meta = soup.find('meta', attrs={'name': 'description'})
        return meta.get('content', '') if meta else ''
    
    def _detect_language(self, soup: BeautifulSoup) -> str:
        """Detect page language."""
        html_tag = soup.find('html')
        return html_tag.get('lang', 'en') if html_tag else 'en'
    
    def _get_last_updated(self, soup: BeautifulSoup) -> datetime:
        """Extract last updated date."""
        meta = soup.find('meta', attrs={'name': 'last-modified'})
        if meta and meta.get('content'):
            try:
                return datetime.fromisoformat(meta.get('content'))
            except ValueError:
                pass
        return None
        
    def crawl(self, settings: Dict[str, Any]) -> List[Dict]:
        """
        Run the crawler with given settings.
        Called by main.py to start the crawling process.
        """
        from scrapy.crawler import CrawlerProcess
        
        # Use CrawlerProcess instead of CrawlerRunner for simplified handling
        process = CrawlerProcess(settings)
        
        # Add crawler
        process.crawl(
            self.__class__,
            start_urls=self.start_urls,
            max_depth=self.max_depth,
            follow_links=self.follow_links
        )
        
        # Run the process
        process.start()
        