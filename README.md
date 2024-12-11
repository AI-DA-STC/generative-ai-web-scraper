# GenAI Web scraping and KB Creation Tool

### Setup Python environment

For Linux : 
```bash
conda env create -f dependencies/ste-genai-web-scraping-kb.yaml
```

### Install custom dependencies for MinerU (First time only)

```bash
sudo apt-get install -y libglib2.0-0 libsm6 libxrender1 libxext6
```
```bash
pip install -U magic-pdf[full] --extra-index-url https://wheels.myhloli.com
```
```bash
python -m pip install paddlepaddle-gpu==3.0.0b1 -i https://www.paddlepaddle.org.cn/packages/stable/cu118/
```

### Download models for MinerU (First time only)

```bash
python scripts/download_nodels_hf.py
```

### Enable CUDA Acceleration for minerU (First time only)


Modify the value of "device-mode" in the magic-pdf.json configuration file located in your home directory : 
```bash
{
  "device-mode": "cuda"
}
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

### Running scrapy job if you want to scrape just one page

```bash
python scripts/main.py https://www.skillsfuture.gov.sg --depth 1 --no-follow
```

### Running scrapy job if you want to scrape embedded links one level deeper

```bash
python scripts/main.py https://www.skillsfuture.gov.sg --depth 1 --follow
```

### Running scrapy job if you want to scrape embedded links six levels deeper

```bash
python scripts/main.py https://www.skillsfuture.gov.sg --depth 6 --follow
```

### Scraped metadata schema 

```bash
{
	"element_id": "string, {checksum of URL}_{job_id}",
	"URL":"string, URL of HTML/PDF/Image",
	"type": "string, URL/PDF/Image",
	"raw_content_path": "string, URL to minIO containing raw content",
	"processed_content_path": "string, URL to minIO containing processed content",
	"checksum": "string, SHA-256 hash of content",
	"parent_id": "string, element_id if entry is PDF/Image
}
```

### MinIO folder structure

```tree
bucket_name/
├── {job_id}/
├────── {raw}/
│   │   ├──{element_id}.html
│   │   ├──{element_id}.jpeg
│   │   ├──{element_id}.pdf
│   │   └── ...
├────── {processed}/
│   │   ├──{element_id}.md
│   │   ├──{element_id}.md
│   │   ├──{element_id}.md
│   │   └── ...
```

where job_id : timestamp