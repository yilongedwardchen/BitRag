"""
Pathway data processing pipeline for Bitcoin data.
This script processes data from Kafka streams and stores it in PostgreSQL and Milvus.
"""
import os
import json
import logging
from datetime import datetime
import threading
import time
import pandas as pd
import pathway as pw
from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType, utility
from sentence_transformers import SentenceTransformer
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'bitrag-kafka:9092')
KAFKA_TOPICS = {
    'prices': 'bitcoin_price_updates',
    'whales': 'whale_transactions',
    'news': 'crypto_news'
}

# PostgreSQL connection parameters
PG_HOST = os.environ.get('PG_HOST', 'bitrag-postgres')
PG_PORT = int(os.environ.get('PG_PORT', 5432))
PG_USER = os.environ.get('PG_USER', 'bitrag')
PG_PASS = os.environ.get('PG_PASS', 'bitragpassword')
PG_DB = os.environ.get('PG_DB', 'bitrag_db')

# Milvus connection parameters
MILVUS_HOST = os.environ.get('MILVUS_HOST', 'bitrag-milvus')
MILVUS_PORT = int(os.environ.get('MILVUS_PORT', 19530))
EMBEDDING_DIMENSION = 384

class ProcessingStats:
    def __init__(self):
        self.prices_processed = 0
        self.transactions_processed = 0
        self.news_processed = 0
        self.embeddings_generated = 0
        self.errors = 0
        self.start_time = datetime.now()
        self.last_update = self.start_time
    
    def update_price(self, count=1):
        self.prices_processed += count
        self.last_update = datetime.now()
    
    def update_transaction(self, count=1):
        self.transactions_processed += count
        self.last_update = datetime.now()
    
    def update_news(self, count=1):
        self.news_processed += count
        self.last_update = datetime.now()
    
    def update_embeddings(self, count=1):
        self.embeddings_generated += count
        self.last_update = datetime.now()
    
    def record_error(self):
        self.errors += 1
    
    def get_stats(self):
        runtime = datetime.now() - self.start_time
        return {
            "start_time": self.start_time.isoformat(),
            "current_time": datetime.now().isoformat(),
            "runtime_seconds": runtime.total_seconds(),
            "last_update": self.last_update.isoformat(),
            "prices_processed": self.prices_processed,
            "transactions_processed": self.transactions_processed,
            "news_processed": self.news_processed,
            "embeddings_generated": self.embeddings_generated,
            "errors": self.errors,
            "processing_rate": {
                "prices_per_minute": (self.prices_processed * 60) / max(1, runtime.total_seconds()),
                "transactions_per_minute": (self.transactions_processed * 60) / max(1, runtime.total_seconds()),
                "news_per_minute": (self.news_processed * 60) / max(1, runtime.total_seconds()),
                "embeddings_per_minute": (self.embeddings_generated * 60) / max(1, runtime.total_seconds())
            }
        }

class ProgressMonitor:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.stats = ProcessingStats()
        self.progress_file = os.path.join(data_dir, "processing_progress.json")
        self.running = False
        self.thread = None
    
    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop)
        self.thread.daemon = True
        self.thread.start()
        logger.info(f"Progress monitoring started. Progress will be saved to {self.progress_file}")
    
    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)
        self._save_progress()
        logger.info("Progress monitoring stopped")
    
    def _monitor_loop(self):
        while self.running:
            self._save_progress()
            time.sleep(10)
    
    def _save_progress(self):
        try:
            stats = self.stats.get_stats()
            with open(self.progress_file, 'w') as f:
                json.dump(stats, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving progress: {e}")
    
    def get_database_stats(self, pg_conn):
        try:
            stats = {}
            with pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT COUNT(*) as count FROM bitcoin_prices")
                stats["total_prices"] = cursor.fetchone()["count"]
                cursor.execute("SELECT COUNT(*) as count FROM whale_transactions")
                stats["total_transactions"] = cursor.fetchone()["count"]
                cursor.execute("SELECT COUNT(*) as count FROM crypto_news")
                stats["total_news"] = cursor.fetchone()["count"]
            return stats
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {}

class BitcoinDataProcessor:
    def __init__(self):
        self.pw_app = None
        self.pg_conn = None
        self.embedding_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
        
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data")
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.monitor = ProgressMonitor(self.data_dir)
        self.setup_connections_with_retries()
        self.monitor.start()
    
    def setup_connections_with_retries(self, max_retries=5, retry_delay=10):
        for attempt in range(max_retries):
            try:
                self.setup_postgres_connection()
                self.setup_milvus_connection()
                return
            except Exception as e:
                logger.error(f"Connection attempt {attempt+1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        raise Exception("Failed to establish connections after max retries")
    
    def setup_postgres_connection(self):
        self.pg_conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASS,
            dbname=PG_DB
        )
        logger.info("Connected to PostgreSQL")
    
    def setup_milvus_connection(self):
        connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT)
        logger.info("Connected to Milvus")
        if not self.collection_exists("news_embeddings"):
            self.create_news_collection()
    
    def collection_exists(self, collection_name):
        return utility.has_collection(collection_name)
    
    def create_news_collection(self):
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="news_id", dtype=DataType.INT64),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=EMBEDDING_DIMENSION),
            FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=500),
            FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=2000),
            FieldSchema(name="source", dtype=DataType.VARCHAR, max_length=100),
            FieldSchema(name="published_date", dtype=DataType.VARCHAR, max_length=30),
            FieldSchema(name="link", dtype=DataType.VARCHAR, max_length=500)
        ]
        schema = CollectionSchema(fields=fields, description="News article embeddings")
        collection = Collection(name="news_embeddings", schema=schema)
        index_params = {
            "metric_type": "L2",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 128}
        }
        collection.create_index(field_name="embedding", index_params=index_params)
        logger.info("Created news_embeddings collection in Milvus")
    
    def parse_price_data(self, data):
        try:
            data_dict = json.loads(data)
            timestamp = datetime.fromisoformat(data_dict['timestamp'])
            return {
                'timestamp': timestamp,
                'price': float(data_dict['price']),
                'market_cap': float(data_dict.get('market_cap', 0)),
                'volume': float(data_dict.get('volume', 0)),
                'date': timestamp.date()
            }
        except Exception as e:
            logger.error(f"Error parsing price data: {e}")
            self.monitor.stats.record_error()
            return None
    
    def parse_whale_transaction(self, data):
        try:
            data_dict = json.loads(data)
            timestamp = datetime.fromisoformat(data_dict['timestamp']) if data_dict['timestamp'] else None
            return {
                'tx_hash': data_dict['tx_hash'],
                'block_hash': data_dict.get('block_hash', ''),
                'timestamp': timestamp,
                'value_btc': float(data_dict['value_btc']),
                'fee': float(data_dict.get('fee', 0)),
                'size': int(data_dict.get('size', 0)),
                'input_count': int(data_dict['input_count']),
                'output_count': int(data_dict['output_count']),
                'is_coinbase': bool(data_dict.get('is_coinbase', False))
            }
        except Exception as e:
            logger.error(f"Error parsing whale transaction: {e}")
            self.monitor.stats.record_error()
            return None
    
    def parse_news_data(self, data):
        try:
            data_dict = json.loads(data)
            published_date = datetime.fromisoformat(data_dict['published_date']) if data_dict['published_date'] else None
            return {
                'title': data_dict['title'],
                'link': data_dict['link'],
                'summary': data_dict['summary'],
                'published_date': published_date,
                'source': data_dict['source']
            }
        except Exception as e:
            logger.error(f"Error parsing news data: {e}")
            self.monitor.stats.record_error()
            return None
    
    def store_price_in_postgres(self, prices):
        if not self.pg_conn:
            logger.error("No PostgreSQL connection available")
            return
        
        try:
            with self.pg_conn.cursor() as cursor:
                price_list = list(prices)
                if not price_list:
                    return
                
                query = """
                    INSERT INTO bitcoin_prices (timestamp, price, market_cap, volume, date)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp) DO UPDATE 
                    SET price = EXCLUDED.price, 
                        market_cap = EXCLUDED.market_cap, 
                        volume = EXCLUDED.volume
                """
                
                execute_batch(cursor, query, [
                    (
                        price['timestamp'],
                        price['price'],
                        price['market_cap'],
                        price['volume'],
                        price['date']
                    ) for price in price_list
                ])
                
                self.pg_conn.commit()
                self.monitor.stats.update_price(len(price_list))
                logger.info(f"Stored {len(price_list)} price records in PostgreSQL")
        except Exception as e:
            logger.error(f"Error storing price data in PostgreSQL: {e}")
            self.monitor.stats.record_error()
            self.pg_conn.rollback()
    
    def store_transaction_in_postgres(self, transactions):
        if not self.pg_conn:
            logger.error("No PostgreSQL connection available")
            return
        
        try:
            with self.pg_conn.cursor() as cursor:
                tx_list = list(transactions)
                if not tx_list:
                    return
                
                query = """
                    INSERT INTO whale_transactions 
                    (tx_hash, block_hash, timestamp, value_btc, fee, size, input_count, output_count, is_coinbase)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tx_hash) DO NOTHING
                """
                
                execute_batch(cursor, query, [
                    (
                        tx['tx_hash'],
                        tx['block_hash'],
                        tx['timestamp'],
                        tx['value_btc'],
                        tx['fee'],
                        tx['size'],
                        tx['input_count'],
                        tx['output_count'],
                        tx['is_coinbase']
                    ) for tx in tx_list
                ])
                
                self.pg_conn.commit()
                self.monitor.stats.update_transaction(len(tx_list))
                logger.info(f"Stored {len(tx_list)} whale transactions in PostgreSQL")
        except Exception as e:
            logger.error(f"Error storing transaction data in PostgreSQL: {e}")
            self.monitor.stats.record_error()
            self.pg_conn.rollback()
    
    def store_news_in_postgres(self, news_items):
        if not self.pg_conn:
            logger.error("No PostgreSQL connection available")
            return
        
        try:
            with self.pg_conn.cursor() as cursor:
                news_list = list(news_items)
                if not news_list:
                    return
                
                query = """
                    INSERT INTO crypto_news 
                    (title, link, summary, published_date, source)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (link) DO NOTHING
                    RETURNING id
                """
                
                news_with_ids = []
                for news in news_list:
                    cursor.execute(query, (
                        news['title'],
                        news['link'],
                        news['summary'],
                        news['published_date'],
                        news['source']
                    ))
                    result = cursor.fetchone()
                    if result:
                        news_id = result[0]
                        news_with_id = news.copy()
                        news_with_id['id'] = news_id
                        news_with_ids.append(news_with_id)
                
                self.pg_conn.commit()
                self.monitor.stats.update_news(len(news_list))
                logger.info(f"Stored {len(news_list)} news articles in PostgreSQL")
                
                if news_with_ids:
                    self.generate_and_store_embeddings(news_with_ids)
        except Exception as e:
            logger.error(f"Error storing news data in PostgreSQL: {e}")
            self.monitor.stats.record_error()
            self.pg_conn.rollback()
    
    def generate_and_store_embeddings(self, news_items):
        try:
            logger.info(f"Generating embeddings for {len(news_items)} news articles")
            texts = [f"{news['title']} {news['summary']}" for news in news_items]
            embeddings = self.embedding_model.encode(texts)
            
            milvus_data = []
            for i, news in enumerate(news_items):
                milvus_data.append({
                    'news_id': int(news['id']),
                    'embedding': embeddings[i].tolist(),
                    'title': news['title'],
                    'content': news['summary'],
                    'source': news['source'],
                    'published_date': news['published_date'].isoformat() if news['published_date'] else "",
                    'link': news['link']
                })
            
            collection = Collection("news_embeddings")
            collection.load()
            
            news_ids = [item['news_id'] for item in milvus_data]
            embeddings = [item['embedding'] for item in milvus_data]
            titles = [item['title'] for item in milvus_data]
            contents = [item['content'] for item in milvus_data]
            sources = [item['source'] for item in milvus_data]
            dates = [item['published_date'] for item in milvus_data]
            links = [item['link'] for item in milvus_data]
            
            collection.insert([
                news_ids, embeddings, titles, contents, sources, dates, links
            ])
            
            collection.flush()
            self.monitor.stats.update_embeddings(len(news_items))
            logger.info(f"Stored {len(news_items)} news embeddings in Milvus")
            collection.release()
        except Exception as e:
            logger.error(f"Error generating and storing embeddings: {e}")
            self.monitor.stats.record_error()
    
    def process_historical_data(self):
        if not self.pg_conn:
            logger.error("No PostgreSQL connection available")
            return
        
        try:
            db_stats_before = self.monitor.get_database_stats(self.pg_conn)
            logger.info(f"Database statistics before processing: {db_stats_before}")
            
            with self.pg_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT cn.id, cn.title, cn.link, cn.summary, cn.published_date, cn.source
                    FROM crypto_news cn
                    WHERE NOT EXISTS (
                        SELECT 1 FROM news_embeddings ne WHERE ne.news_id = cn.id
                    )
                    ORDER BY cn.published_date DESC
                """)
                
                news_items = [
                    {
                        'id': row[0],
                        'title': row[1],
                        'link': row[2],
                        'summary': row[3],
                        'published_date': row[4],
                        'source': row[5]
                    } for row in cursor.fetchall()
                ]
            
            if news_items:
                logger.info(f"Processing {len(news_items)} historical news articles")
                batch_size = 50
                total_batches = (len(news_items) + batch_size - 1) // batch_size
                
                for i in range(0, len(news_items), batch_size):
                    batch = news_items[i:i+batch_size]
                    logger.info(f"Processing batch {i//batch_size + 1}/{total_batches} ({len(batch)} articles)")
                    self.generate_and_store_embeddings(batch)
                
                db_stats_after = self.monitor.get_database_stats(self.pg_conn)
                logger.info(f"Database statistics after processing: {db_stats_after}")
            else:
                logger.info("No historical news articles to process")
        except Exception as e:
            logger.error(f"Error processing historical data: {e}")
            self.monitor.stats.record_error()
    
    def build_pathway_pipeline(self):
        price_input = pw.io.kafka.read(
            brokers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPICS['prices'],
            value_deserializer=self.parse_price_data,
            autocommit_duration_ms=1000
        )
        
        whale_input = pw.io.kafka.read(
            brokers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPICS['whales'],
            value_deserializer=self.parse_whale_transaction,
            autocommit_duration_ms=1000
        )
        
        news_input = pw.io.kafka.read(
            brokers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPICS['news'],
            value_deserializer=self.parse_news_data,
            autocommit_duration_ms=1000
        )
        
        price_input.sink_to(pw.io.python.callback(self.store_price_in_postgres))
        whale_input.sink_to(pw.io.python.callback(self.store_transaction_in_postgres))
        news_input.sink_to(pw.io.python.callback(self.store_news_in_postgres))
        
        self.pw_app = pw.App(
            network=price_input.node.graph
        )
    
    def run(self):
        try:
            logger.info("Processing historical data...")
            self.process_historical_data()
            
            logger.info("Building Pathway pipeline...")
            self.build_pathway_pipeline()
            
            logger.info("Starting Pathway pipeline...")
            self.pw_app.run(run_metadata=pw.RunMetadata(run_local=True))
        except Exception as e:
            logger.error(f"Error running data processor: {e}")
            self.monitor.stats.record_error()
        finally:
            self.monitor.stop()
            if self.pg_conn:
                self.pg_conn.close()

if __name__ == "__main__":
    processor = BitcoinDataProcessor()
    processor.run()
