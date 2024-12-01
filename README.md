## Project structure

project_root/
├── app/
│   ├── api/
│   │   ├── __init__.py
│   │   ├── file_api.py
│   │   └── crawler_api.py             # The crawler-api artifact
│   ├── db/
│   │   ├── __init__.py
│   │   ├── base.py                    # The database-base artifact
│   │   └── session.py                 # The database-session artifact
│   ├── models/
│   │   ├── __init__.py
│   │   ├── file.py
│   │   └── scraped_page.py            # The crawler-model artifact
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── file.py
│   │   └── crawler.py                 # The crawler-schemas artifact
│   ├── scrapy_crawler/
│   │   ├── __init__.py
│   │   ├── spiders/
│   │   │   ├── __init__.py
│   │   │   └── web_crawler.py         # The scrapy-spider artifact
│   │   ├── config_manager.py          # The crawler-config-manager artifact
│   │   ├── middlewares.py             # The scrapy-middleware artifact
│   │   ├── pipelines.py               # The scrapy-pipeline artifact
│   │   └── settings.py                # The scrapy-settings artifact
│   ├── services/
│   │   ├── __init__.py
│   │   ├── file.py
│   │   ├── crawler_service.py         # The crawler-service artifact
│   │   └── job_service.py             # The crawler-job-service artifact
│   └── main.py
├── config/
│   ├── __init__.py
│   ├── settings.py
│   └── agent_config.py
├── dependencies/
│   └── __init__.py
├── scripts/
│   └── __init__.py
└── util/
    ├── __init__.py
    └── s3_helper.py                   # The enhanced-s3-helper artifact