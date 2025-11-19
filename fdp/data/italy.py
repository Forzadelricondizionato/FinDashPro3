# fdp/data/italy.py
import requests_cache
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict
import re

class BorsaItalianaScraper:
    def __init__(self, cache_name: str = "italy_cache"):
        self.session = requests_cache.CachedSession(cache_name, expire_after=86400)
        self.base_url = "https://www.borsaitaliana.it"
    
    def scrape_mib40(self) -> List[Dict[str, str]]:
        url = f"{self.base_url}/indexes/daily/IT0005216807.html"
        response = self.session.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        
        tickers = []
        table = soup.find("table", class_="m-table -lg")
        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if len(cols) >= 2:
                symbol = re.sub(r'\.\w+$', '', cols[0].text.strip())
                tickers.append({
                    "symbol": symbol,
                    "region": "ita",
                    "type": "stock",
                    "full_name": cols[1].text.strip()
                })
        
        return tickers
    
    def scrape_all_tickers(self) -> pd.DataFrame:
        tickers = []
        for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            url = f"{self.base_url}/instruments/instrumentssearchresults.htm?searchType=2&text={letter}&lang=it"
            response = self.session.get(url)
            soup = BeautifulSoup(response.content, "html.parser")
            
            for link in soup.find_all("a", href=re.compile(r"/it/azioni/")):
                symbol = link.text.split(" ")[0]
                tickers.append({
                    "symbol": symbol,
                    "region": "ita",
                    "type": "stock"
                })
        
        return pd.DataFrame(tickers)
