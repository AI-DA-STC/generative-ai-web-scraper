from scrapy import signals
from typing import Any, Optional
from scrapy.http import Request, Response
from scrapy.spiders import Spider

class ScrapyCrawlerSpiderMiddleware:
    """Spider middleware for processing requests and responses."""

    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware

    def process_spider_input(self, response: Response, spider: Spider) -> None:
        """Process spider input before passing to callback."""
        return None

    def process_spider_output(self, response: Response, result: Any, spider: Spider):
        """Process spider output before passing to item pipeline."""
        for item in result:
            yield item

    def process_spider_exception(self, response: Response, exception: Exception, spider: Spider):
        """Handle exceptions raised during processing."""
        pass

    def process_start_requests(self, start_requests: Any, spider: Spider):
        """Process initial requests from the spider."""
        for request in start_requests:
            yield request

    def spider_opened(self, spider: Spider):
        spider.logger.info(f'Spider opened: {spider.name}')


class ScrapyCrawlerDownloaderMiddleware:
    """Downloader middleware for processing requests and downloads."""

    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware

    def process_request(self, request: Request, spider: Spider) -> Optional[Response]:
        """Process request before sending to downloader."""
        # Check depth and respect max_depth setting
        depth = request.meta.get('depth', 0)
        if hasattr(spider, 'max_depth') and depth > spider.max_depth:
            spider.logger.debug(f'Ignoring request to {request.url} - max depth reached')
            return None
        return None

    def process_response(self, request: Request, response: Response, spider: Spider) -> Response:
        """Process response before passing to spider."""
        return response

    def process_exception(self, request: Request, exception: Exception, spider: Spider):
        """Handle download exceptions."""
        spider.logger.error(f'Download error on {request.url}: {str(exception)}')
        pass

    def spider_opened(self, spider: Spider):
        spider.logger.info(f'Spider opened: {spider.name}')