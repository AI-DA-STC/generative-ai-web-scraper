from typing import Dict, Any

class ScrapyConfig:
    """Configuration class for Scrapy crawler settings."""
    
    @staticmethod
    def get_settings() -> Dict[str, Any]:
        """Get complete Scrapy settings dictionary."""
        return {
            # Basic crawler settings
            'BOT_NAME': 'eserv_bot',
            'SPIDER_MODULES': ['app.scrapy_crawler.spiders'],
            'NEWSPIDER_MODULE': 'app.scrapy_crawler.spiders',
            
            # Crawling behavior
            'ROBOTSTXT_OBEY': True,
            'CONCURRENT_REQUESTS': 16,
            'DOWNLOAD_DELAY': 1,
            'CONCURRENT_REQUESTS_PER_DOMAIN': 8,
            
            # Politeness settings
            'COOKIES_ENABLED': False,
            'TELNETCONSOLE_ENABLED': False,
            
            # Cache settings
            'HTTPCACHE_ENABLED': True,
            'HTTPCACHE_EXPIRATION_SECS': 86400,
            'HTTPCACHE_DIR': 'httpcache',
            'HTTPCACHE_IGNORE_HTTP_CODES': [503, 504, 505, 500, 400, 401, 402, 403, 404],
            
            # Retry configuration
            'RETRY_ENABLED': True,
            'RETRY_TIMES': 3,
            'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
            
            # Download timeouts
            'DOWNLOAD_TIMEOUT': 180,
            
            # Pipeline
            'ITEM_PIPELINES': {
                'app.scrapy_crawler.pipelines.ContentPipeline': 300,
            },
            
            # Middleware
            'DOWNLOADER_MIDDLEWARES': {
                'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
                'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware': 100,
                'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware': 100,
                'scrapy.spidermiddlewares.depth.DepthMiddleware': 100,
                'app.scrapy_crawler.middlewares.ScrapyCrawlerDownloaderMiddleware': 500,
            },
            
            'SPIDER_MIDDLEWARES': {
                'app.scrapy_crawler.middlewares.ScrapyCrawlerSpiderMiddleware': 543,
            },
            
            # User agent
            'USER_AGENT': 'Mozilla/5.0 (compatible; WebCrawler/1.0)',
            
            # Media pipeline settings
            'MEDIA_ALLOW_REDIRECTS': True,
            
            # Logging
            'LOG_LEVEL': 'INFO',
            'LOG_FORMAT': '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
        }