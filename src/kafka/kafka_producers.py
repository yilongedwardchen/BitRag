"""
Kafka producers for streaming real-time Bitcoin data.
"""
import json
import time
import logging
import threading
import schedule
from datetime import datetime
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Import data collectors
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_collection.price_collector import BitcoinPriceCollector
from data_collection.transaction_collector import WhaleTransactionCollector
from data_collection.news_collector import CryptoNewsCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'bitrag-kafka:9092'

class KafkaDataProducer:
    def __init__(self, topic):
        self.topic = topic
        self.producer = None
        self.connect_with_retries()
    
    def connect_with_retries(self, max_retries=5, retry_delay=10):
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=5,
                    retry_backoff_ms=1000
                )
                logger.info(f"Initialized Kafka producer for topic: {self.topic}")
                return
            except KafkaError as e:
                logger.error(f"Kafka connection attempt {attempt+1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        raise Exception("Failed to connect to Kafka after max retries")
    
    def send_message(self, message):
        try:
            future = self.producer.send(self.topic, message)
            self.producer.flush()
            record_metadata = future.get(timeout=10)
            logger.debug(f"Sent message to {record_metadata.topic} partition {record_metadata.partition}")
            return True
        except Exception as e:
            logger.error(f"Error sending message to Kafka: {e}")
            return False

class BitcoinPriceProducer(KafkaDataProducer):
    def __init__(self):
        super().__init__('bitcoin_price_updates')
        self.collector = BitcoinPriceCollector()
    
    def fetch_and_send_price(self):
        try:
            # Use CryptoCompare's /data/price endpoint for real-time price
            price_data = self.collector._make_request("price", params={'fsym': 'BTC', 'tsyms': 'USD'})
            
            if not price_data or 'USD' not in price_data:
                logger.warning("No price data retrieved")
                return
            
            price_dict = {
                'timestamp': datetime.now().isoformat(),
                'price': float(price_data['USD']),
                'market_cap': None,  # Not available in /price endpoint
                'volume': None  # Not available in /price endpoint
            }
            
            success = self.send_message(price_dict)
            if success:
                logger.info(f"Sent price update: ${price_dict['price']:.2f}")
        except Exception as e:
            logger.error(f"Error in price producer: {e}")

class WhaleTransactionProducer(KafkaDataProducer):
    def __init__(self, threshold_btc=100):
        super().__init__('whale_transactions')
        self.collector = WhaleTransactionCollector(api_key=os.environ.get('BLOCKCHAIR_API_KEY', ''))
        self.last_processed_tx = set()
    
    def fetch_and_send_transactions(self):
        try:
            # Fetch transactions from the last 10 minutes
            whale_txs = self.collector.get_whale_transactions(days=0.007, min_btc=100)  # ~10 minutes
            
            if whale_txs.empty:
                logger.info("No new whale transactions detected")
                return
            
            current_tx_hashes = set(whale_txs['tx_hash'])
            new_tx_hashes = current_tx_hashes - self.last_processed_tx
            
            if not new_tx_hashes:
                logger.info("No new whale transactions to process")
                return
            
            new_txs = whale_txs[whale_txs['tx_hash'].isin(new_tx_hashes)]
            
            for _, tx in new_txs.iterrows():
                tx_dict = {
                    'tx_hash': tx['tx_hash'],
                    'block_hash': None,  # Blockchair doesn't provide block_hash directly
                    'timestamp': tx['timestamp'].isoformat() if pd.notna(tx['timestamp']) else None,
                    'value_btc': float(tx['value_btc']),
                    'fee': None,  # Not available in free tier
                    'size': None,  # Not available in this endpoint
                    'input_count': int(tx['input_count']),
                    'output_count': int(tx['output_count']),
                    'is_coinbase': False  # Assume non-coinbase for whale txs
                }
                
                success = self.send_message(tx_dict)
                if success:
                    logger.info(f"Sent whale transaction: {tx['tx_hash']} - {tx['value_btc']:.2f} BTC")
            
            self.last_processed_tx = current_tx_hashes
            
        except Exception as e:
            logger.error(f"Error in whale transaction producer: {e}")

class CryptoNewsProducer(KafkaDataProducer):
    def __init__(self):
        super().__init__('crypto_news')
        self.collector = CryptoNewsCollector()
        self.last_processed_news = set()
    
    def fetch_and_send_news(self):
        try:
            news_data = self.collector.get_historical_news(days=1)
            
            if news_data.empty:
                logger.info("No news retrieved")
                return
            
            current_urls = set(news_data['link'])
            new_urls = current_urls - self.last_processed_news
            
            if not new_urls:
                logger.info("No new news articles to process")
                return
            
            new_articles = news_data[news_data['link'].isin(new_urls)]
            
            for _, article in new_articles.iterrows():
                article_dict = {
                    'title': article['title'],
                    'link': article['link'],
                    'summary': article['summary'],
                    'published_date': article['published_date'].isoformat() if pd.notna(article['published_date']) else None,
                    'source': article['source']
                }
                
                success = self.send_message(article_dict)
                if success:
                    logger.info(f"Sent news article: {article['title']}")
            
            self.last_processed_news = current_urls
            
        except Exception as e:
            logger.error(f"Error in news producer: {e}")

def main():
    logger.info("Starting Kafka producers...")
    
    price_producer = BitcoinPriceProducer()
    whale_producer = WhaleTransactionProducer(threshold_btc=100)
    news_producer = CryptoNewsProducer()
    
    schedule.every(5).minutes.do(price_producer.fetch_and_send_price)
    schedule.every(10).minutes.do(whale_producer.fetch_and_send_transactions)
    schedule.every(15).minutes.do(news_producer.fetch_and_send_news)
    
    price_producer.fetch_and_send_price()
    whale_producer.fetch_and_send_transactions()
    news_producer.fetch_and_send_news()
    
    logger.info("Producers are running. Press Ctrl+C to stop.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping Kafka producers...")
        price_producer.producer.close()
        whale_producer.producer.close()
        news_producer.producer.close()

if __name__ == "__main__":
    main()
