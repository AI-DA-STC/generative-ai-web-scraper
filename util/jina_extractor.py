from typing import List
import requests
from urllib.parse import quote

import sys
import pyprojroot
root = pyprojroot.find_root(pyprojroot.has_dir("config"))
sys.path.append(str(root))

from config import settings

class JinaExtractor:
    def __init__(self):
        self.url_prefix = 'https://r.jina.ai/'
        
    def jina_reader_html2md(self,url:str) -> str:
        headers = {
            'Authorization': f'Bearer jina_{settings.JINA_API_KEY}',
            'Content-Type': 'application/json',
            'X-Remove-Selector': 'footer, header',
            'X-Retain-Images': 'none'
        }
        data = {
            "url": url
        }

        response = requests.post(self.url_prefix, headers=headers, json=data)
        return response.text

    def jina_search(self, query:str, restricted_urls:List[str]=None) -> str:

        encoded_query = quote(query)
        url = f'https://s.jina.ai/{encoded_query}'
        headers = {
            'Authorization': 'Bearer jina_c2b564bd1ba748bf843f9ecd9c365a6fy-4IcRcEeAVWsLyMJa-PYTPECKJI',
            'X-Retain-Images': 'none',
            'X-Site': restricted_urls
        }

        response = requests.get(url, headers=headers)

        return response.text

