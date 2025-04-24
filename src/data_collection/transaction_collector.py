"""
Bitcoin whale transaction data collection module using Blockchair API (API-free tier).
"""
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WhaleTransactionCollector:
    BASE_URL = "https://api.blockchair.com/bitcoin"
    
    def __init__(self):
        self.session = requests.Session()
    
    def _make_request(self, endpoint, params=None):
        url = f"{self.BASE_URL}/{endpoint}"
        if params is None:
            params = {}
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            time.sleep(2)  # Respect 1,000 calls/day (30/min)
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            if hasattr(e.response, 'status_code') and e.response.status_code == 429:
                logger.warning("Rate limit hit, waiting 60 seconds...")
                time.sleep(60)
                return self._make_request(endpoint, params)
            return None
    
    def get_whale_transactions(self, days=14, min_btc=100, start_date=None):
        logger.info(f"Fetching whale transactions for {days} days starting from {start_date or 'now'}")
        
        end_date = datetime.now() if not start_date else start_date + timedelta(days=days)
        start_date = end_date - timedelta(days=days) if not start_date else start_date
        min_satoshi = min_btc * 100_000_000
        
        transactions = []
        offset = 0
        limit = 100
        
        while True:
            params = {
                'limit': limit,
                'offset': offset,
                'q': f'output_total(>={min_satoshi})',
                's': 'time(desc)'
            }
            
            data = self._make_request("transactions", params)
            
            if not data or 'data' not in data:
                logger.error("Failed to fetch transaction data")
                break
            
            txs = data['data']
            if not txs:
                logger.info("No more transactions to fetch")
                break
            
            for tx in txs:
                tx_time = pd.to_datetime(tx['time'])
                if tx_time < start_date:
                    logger.info("Reached transactions older than specified period")
                    return pd.DataFrame(transactions)
                
                transactions.append({
                    'tx_hash': tx['hash'],
                    'timestamp': tx_time,
                    'value_btc': tx['output_total'] / 100_000_000,
                    'block_height': tx['block_id'],
                    'input_count': tx['input_count'],
                    'output_count': tx['output_count']
                })
            
            offset += limit
            logger.info(f"Fetched {len(txs)} transactions, total so far: {len(transactions)}")
            
            if len(txs) < limit:
                break
        
        if not transactions:
            logger.warning("No whale transactions found")
            return pd.DataFrame()
        
        df = pd.DataFrame(transactions)
        df['date'] = df['timestamp'].dt.date
        logger.info(f"Retrieved {len(df)} whale transactions")
        return df

if __name__ == "__main__":
    collector = WhaleTransactionCollector()
    whale_txs = collector.get_whale_transactions(days=14, min_btc=100)
    if not whale_txs.empty:
        print(f"Retrieved {len(whale_txs)} whale transactions")
        print(whale_txs.head())
        whale_txs.to_csv("/app/data/whale_transactions.csv", index=False)
