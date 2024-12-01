# GenAI Web scraping and KB Creation Tool

```tree
root
├── app                 <- Main application directory containing core functionality.
│   │
│   ├── db             <- Database configuration and session management
│   │   ├── __init__.py
│   │   ├── base.py          <- SQLAlchemy base class and model registry
│   │   └── session.py       <- Database connection and session handling
│   │
│   ├── models         <- SQLAlchemy models defining database schema
│   │   ├── __init__.py
│   │   ├── file.py          <- File storage and tracking models
│   │   └── scraped_page.py  <- Web page content and metadata models
│   │
│   ├── schemas        <- Pydantic models for data validation
│   │   ├── __init__.py
│   │   ├── file.py          <- File metadata schemas
│   │   └── crawler.py       <- Crawler data validation schemas
│   │
│   ├── scrapy_crawler <- Scrapy-based web crawler implementation
│   │   ├── __init__.py
│   │   ├── spiders
│   │   │   ├── __init__.py
│   │   │   └── web_crawler.py    <- Main crawler spider implementation
│   │   ├── config_manager.py     <- Crawler configuration management
│   │   ├── middlewares.py        <- Request/response processing middleware
│   │   ├── pipelines.py          <- Content processing pipeline
│   │   └── settings.py           <- Scrapy-specific settings
├── config             <- Configuration management
│   ├── __init__.py
│   ├── settings.py         <- Web scraper settings
│   └── log_config.py     <- log configuration
│
├── dependencies       <- Environment and dependency management
│   └── requirements.txt
│   └── ste-genai-web-scraping-kb.yaml
│
├── scripts           <- Utility and maintenance scripts
│   └── __init__.py
│   └── main.py       <- Main script to run the web scraper
│
├── util              <- Utility functions and helpers
│   ├── __init__.py
│   └── s3_helper.py  <- S3/MinIO storage helper functions
│
├── .env              <- Environment variables (ignored by git)
├── .gitignore        <- Files and directories to be ignored by git
└── README.md         <- Project documentation and setup instructions
```