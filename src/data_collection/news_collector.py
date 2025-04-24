"""
Bitcoin news data collection module using CryptoCompare API.
"""
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CryptoNewsCollector:
    BASE_URL = "https://min-api.cryptocompare.com/data"
    
    def __init__(self, api_key=None, keywords=None):
        self.session = requests.Session()
        self.api_key = api_key or os.environ.get('CRYPTOCOMPARE_API_KEY', '')
        if not self.api_key:
            logger.error("CryptoCompare API key is required. Set CRYPTOCOMPARE_API_KEY environment variable.")
            raise ValueError("Missing CryptoCompare API key")
        self.keywords = keywords or ["bitcoin", "btc", "crypto", "cryptocurrency", "blockchain"]
    
    def _make_request(self, endpoint, params=None):
        url = f"{self.BASE_URL}/{endpoint}"
        if params is None:
            params = {}
        params['api_key'] = self.api_key
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            time.sleep(0.5)  # Respect rate limits
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None
    
    def _contains_keywords(self, title, summary):
        text = (title + " " + summary).lower()
        return any(keyword.lower() in text for keyword in self.keywords)
    
    def get_historical_news(self, days=30, start_date=None):
        logger.info(f"Fetching news for {days} days starting from {start_date or 'now'}")
        
        end_date = datetime.now() if not start_date else start_date + timedelta(days=days)
        start_date = end_date - timedelta(days=days) if not start_date else start_date
        
        articles = []
        page = 1
        limit = 100
        
        while True:
            params = {
                'limit': limit,
                'page': page,
                'categories': 'BTC',
                'lTs': int(end_date.timestamp())
            }
            
            data = self._make_request("news", params)
            
            if not data or 'Data' not in data:
                logger.error("Failed to fetch news data")
                break
            
            news_items = data['Data']
            if not news_items:
                logger.info("No more news to fetch")
                break
            
            for item in news_items:
                pub_date = datetime.fromtimestamp(item['published_on']).replace(tzinfo=None)
                if pub_date < start_date:
                    logger.info("Reached news older than specified period")
                    return pd.DataFrame(articles)
                
                title = item.get('title', '')
                summary = item.get('body', '')
                if self._contains_keywords(title, summary):
                    article = {
                        'title': title,
                        'link': item.get('url', ''),
                        'summary': summary,
                        'published_date': pub_date,
                        'source': item.get('source', 'CryptoCompare')
                    }
                    articles.append(article)
            
            page += 1
            logger.info(f"Fetched {len(news_items)} news items, total so far: {len(articles)}")
        
        if not articles:
            logger.warning("No news articles retrieved")
            return pd.DataFrame()
        
        news_df = pd.DataFrame(articles)
        news_df['date'] = news_df['published_date'].dt.date
        logger.info(f"Retrieved {len(news_df)} news articles")
        return news_df

if __name__ == "__main__":
    collector = CryptoNewsCollector()
    news = collector.get_historical_news(days=2000)
    if not news.empty:
        print(f"Retrieved {len(news)} news articles")
        print(news[['title', 'source', 'published_date']].head())
        news.to_csv("/app/data/crypto_news.csv", index=False)
