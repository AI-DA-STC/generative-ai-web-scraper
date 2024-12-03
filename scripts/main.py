import sys
from pathlib import Path

import pyprojroot
root = pyprojroot.find_root(pyprojroot.has_dir("config"))
sys.path.append(str(root))

import click
import logging
from datetime import datetime

from app.db.session import init_db
from app.scrapy_crawler.config_manager import get_crawler_config
from app.scrapy_crawler.spiders.web_crawler import WebCrawlerSpider
from util.s3_helper import S3Helper
from config import settings, logger

def create_required_directories():
    """
    Creates required application directories if they don't exist.
    This includes directories for logs, downloads, and cache.
    """
    directories = [
        "logs"
    ]
    
    for directory in directories:
        Path(settings.BASE / directory).mkdir(parents=True, exist_ok=True)

def verify_storage():
    """Verify MinIO connection and bucket existence."""
    try:
        S3Helper().client.head_bucket(Bucket=settings.AWS_BUCKET_NAME)
        logger.info("MinIO connection verified")
        return True
    except Exception as e:
        logger.error(f"Failed to connect to MinIO: {str(e)}")
        return False

def initialize_system():
    """Initialize all required system components."""
    try:
        # Initialize database
        logger.info("Initializing SQL database...")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        table_name,model = init_db(timestamp)
        
        # Verify MinIO connection
        logger.info("Verifying MinIO connection...")
        if not verify_storage():
            raise Exception("Failed to verify MinIO storage")
        
        # Create required directories
        create_required_directories()
        
        return table_name, model
        
    except Exception as e:
        logger.error(f"Initialization failed: {str(e)}")
        sys.exit(1)

@click.command()
@click.argument('urls', nargs=-1, required=True)
@click.option('--depth', '-d', default=6, help='Maximum crawl depth (default: 6)')
@click.option('--follow/--no-follow', default=True, help='Whether to follow links (default: True)')
def main(urls: tuple, depth: int, follow: bool, verbose: bool=True):
    """
    Web crawler script that processes URLs and stores content and metadata.
    
    Args:
        urls: One or more URLs to crawl
        depth: Maximum crawl depth
        follow: Whether to follow links found in pages
        verbose: Enable verbose logging
    """
    # Configure logging
    log_level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(log_level)
    
    # Initialize system
    logger.info("Starting crawler system...")
    table_name, model = initialize_system()

    try:
        # Get crawler configuration
        crawler_config = get_crawler_config()
        settings = crawler_config.get_integrated_settings()
        # Create spider instance
        spider = WebCrawlerSpider(
            table_name = table_name,
            model = model,
            start_urls=list(urls),
            max_depth=depth,
            follow_links=follow
        )
        
        # Run crawler
        logger.info(f"Starting crawl of {len(urls)} URLs with depth {depth}")
        spider.crawl(settings)
    
        
    except Exception as e:
        logger.error(f"Crawling failed: {str(e)}")
        sys.exit(1)
    
    logger.info("Crawling process completed successfully")

if __name__ == "__main__":
    main()