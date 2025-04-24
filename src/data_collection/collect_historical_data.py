"""
Script to collect historical data for price, transactions, and news over 2000 days.
This is a one-time operation before setting up the streaming pipeline.
"""
import os
import json
import logging
import time
from datetime import datetime, timedelta
from tqdm import tqdm
import pandas as pd

from src.data_collection.price_collector import BitcoinPriceCollector
from src.data_collection.transaction_collector import WhaleTransactionCollector
from src.data_collection.news_collector import CryptoNewsCollector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("data_collection.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class ProgressTracker:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.progress_file = os.path.join(data_dir, "collection_progress.json")
        self.progress = self._load_progress()
    
    def _load_progress(self):
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading progress file: {e}")
                return self._init_progress()
        return self._init_progress()
    
    def _init_progress(self):
        return {
            "status": "initializing",
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "steps": {
                "price_daily": {"status": "pending", "count": 0, "completed": False, "last_date": None},
                "price_hourly": {"status": "pending", "count": 0, "completed": False, "last_date": None},
                "whale_transactions": {"status": "pending", "count": 0, "completed": False, "last_block": None},
                "news": {"status": "pending", "count": 0, "completed": False, "last_date": None}
            },
            "overall_progress": 0
        }
    
    def update_step(self, step_name, status, count=None, completed=None, metadata=None):
        if step_name in self.progress["steps"]:
            if status:
                self.progress["steps"][step_name]["status"] = status
            if count is not None:
                self.progress["steps"][step_name]["count"] = count
            if completed is not None:
                self.progress["steps"][step_name]["completed"] = completed
            if metadata:
                for key, value in metadata.items():
                    self.progress["steps"][step_name][key] = value
            completed_steps = sum(1 for step in self.progress["steps"].values() if step["completed"])
            self.progress["overall_progress"] = (completed_steps / len(self.progress["steps"])) * 100
            self._save_progress()
    
    def complete(self):
        self.progress["status"] = "completed"
        self.progress["end_time"] = datetime.now().isoformat()
        self.progress["overall_progress"] = 100
        self._save_progress()
    
    def _save_progress(self):
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving progress file: {e}")

def main():
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data")
    os.makedirs(data_dir, exist_ok=True)
    tracker = ProgressTracker(data_dir)
    
    start_time = datetime.now()
    logger.info(f"Starting historical data collection at {start_time}")
    
    try:
        # 1. Collect Bitcoin price data
        logger.info("Collecting Bitcoin price data...")
        price_collector = BitcoinPriceCollector()
        
        logger.info("Collecting daily price data for past 2000 days...")
        daily_data = price_collector.get_extended_historical_data(
            start_date=(datetime.now() - timedelta(days=2000)).strftime('%Y-%m-%d'),
            end_date=datetime.now().strftime('%Y-%m-%d'),
            interval='daily'
        )
        
        if not daily_data.empty:
            daily_data_path = os.path.join(data_dir, "bitcoin_daily_prices.csv")
            daily_data.to_csv(daily_data_path, index=False)
            logger.info(f"Saved {len(daily_data)} daily price records to {daily_data_path}")
            tracker.update_step("price_daily", "completed", len(daily_data), True, {"last_date": daily_data['date'].max().isoformat()})
        else:
            logger.warning("No daily price data collected")
            tracker.update_step("price_daily", "failed")
        
        # Hourly data for the past 90 days
        tracker.update_step("price_hourly", "starting")
        logger.info("Collecting hourly price data for past 90 days...")
        hourly_data = price_collector.get_historical_prices(days=90, interval='hourly')
        
        if not hourly_data.empty:
            hourly_data_path = os.path.join(data_dir, "bitcoin_hourly_prices.csv")
            hourly_data.to_csv(hourly_data_path, index=False)
            logger.info(f"Saved {len(hourly_data)} hourly price records to {hourly_data_path}")
            tracker.update_step("price_hourly", "completed", len(hourly_data), True, {"last_date": hourly_data['date'].max().isoformat()})
        else:
            logger.warning("No hourly price data collected")
            tracker.update_step("price_hourly", "failed")
        
        # 2. Collect whale transaction data (chunked)
        tracker.update_step("whale_transactions", "starting")
        logger.info("Collecting whale transaction data for past 2000 days...")
        tx_collector = WhaleTransactionCollector()
        
        total_days = 2000
        chunk_size = 500  # Chunk to stay within 1,000 calls/day
        all_whale_txs = pd.DataFrame()
        
        for start_offset in range(0, total_days, chunk_size):
            chunk_days = min(chunk_size, total_days - start_offset)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=start_offset + chunk_days)
            logger.info(f"Fetching whale transactions from {start_date.date()} to {end_date.date()}...")
            whale_txs = tx_collector.get_whale_transactions(days=chunk_days, start_date=start_date)
            
            if not whale_txs.empty:
                all_whale_txs = pd.concat([all_whale_txs, whale_txs])
        
        if not all_whale_txs.empty:
            whale_txs_path = os.path.join(data_dir, "whale_transactions.csv")
            all_whale_txs.to_csv(whale_txs_path, index=False)
            logger.info(f"Saved {len(all_whale_txs)} whale transactions to {whale_txs_path}")
            tracker.update_step("whale_transactions", "completed", len(all_whale_txs), True, {"last_block": int(all_whale_txs['block_height'].max())})
        else:
            logger.warning("No whale transactions collected")
            tracker.update_step("whale_transactions", "failed")
        
        # 3. Collect news data (chunked)
        tracker.update_step("news", "starting")
        logger.info("Collecting crypto news data for past 2000 days...")
        news_collector = CryptoNewsCollector()
        
        all_news_data = pd.DataFrame()
        for start_offset in range(0, total_days, chunk_size):
            chunk_days = min(chunk_size, total_days - start_offset)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=start_offset + chunk_days)
            logger.info(f"Fetching news articles from {start_date.date()} to {end_date.date()}...")
            news_data = news_collector.get_historical_news(days=chunk_days, start_date=start_date)
            
            if not news_data.empty:
                all_news_data = pd.concat([all_news_data, news_data])
        
        if not all_news_data.empty:
            news_path = os.path.join(data_dir, "crypto_news.csv")
            all_news_data.to_csv(news_path, index=False)
            logger.info(f"Saved {len(all_news_data)} news articles to {news_path}")
            tracker.update_step("news", "completed", len(all_news_data), True, {"last_date": all_news_data['date'].max().isoformat()})
        else:
            logger.warning("No news articles collected")
            tracker.update_step("news", "failed")
        
        tracker.complete()
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Completed historical data collection in {duration}")
        
    except Exception as e:
        logger.error(f"Error during data collection: {e}")
        if tracker.progress["steps"]:
            last_step = list(tracker.progress["steps"].keys())[-1]
            tracker.update_step(last_step, "failed")
        raise

if __name__ == "__main__":
    main()
