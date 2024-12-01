from pathlib import Path
from typing import Dict, Any
from functools import lru_cache

import sys
import pyprojroot
root = pyprojroot.find_root(pyprojroot.has_dir("config"))
sys.path.append(str(root))
from app.scrapy_crawler.crawler_settings import ScrapyConfig
from config import settings

class CrawlerConfigManager:
    """
    Manages configuration for Scrapy crawler.
    Ensures proper settings integration and directory structure.
    """
    
    def __init__(self):
        """Initialize the configuration manager."""
        self.base_path = Path(__file__).parent.parent
        self.scrapy_settings = ScrapyConfig.get_settings()

    
    def get_integrated_settings(self) -> Dict[str, Any]:
        """Get complete settings dictionary with all integrations."""
        integrated = dict(self.scrapy_settings)
        
        # Add storage settings
        integrated.update({
            'AWS_ACCESS_KEY_ID': settings.AWS_ACCESS_KEY_ID,
            'AWS_SECRET_ACCESS_KEY': settings.AWS_SECRET_ACCESS_KEY,
            'AWS_ENDPOINT_URL': settings.AWS_ENDPOINT_URL,
            'AWS_BUCKET_NAME': settings.AWS_BUCKET_NAME,
            
            # Paths
            'DATA_PATH': str(self.base_path / 'data'),
            'HTTPCACHE_DIR': str(self.base_path / 'data/httpcache'),
            'LOG_FILE': settings.LOG_FILE,
            
            # Database
            'DATABASE_URL': settings.DATABASE_URL,
            
            # Crawler settings from config
            'CONCURRENT_REQUESTS': settings.CONCURRENT_REQUESTS,
            'DOWNLOAD_DELAY': settings.DOWNLOAD_DELAY,
            'ROBOTSTXT_OBEY': settings.RESPECT_ROBOTS_TXT
        })
        
        return integrated

@lru_cache()
def get_crawler_config() -> CrawlerConfigManager:
    """Get or create crawler configuration manager instance."""
    return CrawlerConfigManager()