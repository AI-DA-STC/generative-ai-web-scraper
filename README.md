# GenAI Web scraping and KB Creation Tool

### Setup Python environment

For Linux : 
```bash
conda env create -f dependencies/ste-genai-web-scraping-kb.yaml
```

### Environment variables

Make sure to set the following environment variables in your .env.dev file:

```bash
DATABASE_URL=
POSTGRES_URL=

AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_ENDPOINT_URL=
AWS_BUCKET_NAME=
```

### Setting up 

Create an empty postgres table (Only if running for the 1st time)

```bash
python scripts/create_table.py
```

### Running scrapy job

```bash
python scripts/main.py
```

### Scraped metadata schema 

```bash
{
	"element_id": "string, {checksum of URL}_{job_id}",
	"URL":"string, URL of HTML/PDF/Image",
	"type": "string, URL/PDF/Image",
	"content": "string, URL to minIO containing raw content",
	"checksum": "string, SHA-256 hash of content",
	"parent_id": "string, element_id if entry is PDF/Image
}
```

### MinIO folder structure

```tree
bucket_name/
├── {job_id}/
│   │   ├──{element_id}.html
│   │   ├──{element_id}.jpeg
│   │   ├──{element_id}.pdf
│   │   └── ...
```