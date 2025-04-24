"""
Script to set up PostgreSQL schema for the BitRag project.
Run this after starting the PostgreSQL container.
"""
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_batch
import pandas as pd
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# PostgreSQL connection parameters
PG_HOST = os.environ.get('PG_HOST', 'bitrag-postgres')
PG_PORT = int(os.environ.get('PG_PORT', 5432))
PG_USER = os.environ.get('PG_USER', 'bitrag')
PG_PASS = os.environ.get('PG_PASS', 'bitragpassword')
PG_DB = os.environ.get('PG_DB', 'bitrag_db')

# SQL statements to create schema
CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS bitcoin_prices (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    price NUMERIC(20, 2) NOT NULL,
    market_cap NUMERIC(20, 2),
    volume NUMERIC(20, 2),
    date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(timestamp)
);
CREATE INDEX IF NOT EXISTS idx_bitcoin_prices_timestamp ON bitcoin_prices(timestamp);
CREATE INDEX IF NOT EXISTS idx_bitcoin_prices_date ON bitcoin_prices(date);

CREATE TABLE IF NOT EXISTS whale_transactions (
    id SERIAL PRIMARY KEY,
    tx_hash VARCHAR(255) NOT NULL,
    block_hash VARCHAR(255),
    timestamp TIMESTAMP,
    value_btc NUMERIC(20, 8) NOT NULL,
    fee NUMERIC(20, 8),
    size INTEGER,
    input_count INTEGER NOT NULL,
    output_count INTEGER NOT NULL,
    is_coinbase BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tx_hash)
);
CREATE INDEX IF NOT EXISTS idx_whale_transactions_tx_hash ON whale_transactions(tx_hash);
CREATE INDEX IF NOT EXISTS idx_whale_transactions_timestamp ON whale_transactions(timestamp);

CREATE TABLE IF NOT EXISTS crypto_news (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    link TEXT NOT NULL,
    summary TEXT,
    published_date TIMESTAMP,
    source VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(link)
);
CREATE INDEX IF NOT EXISTS idx_crypto_news_published_date ON crypto_news(published_date);
"""

def connect_to_db():
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASS,
            dbname=PG_DB
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Connection error: {e}")
        return None

def setup_database():
    conn = connect_to_db()
    if not conn:
        logger.error("Failed to connect to PostgreSQL. Make sure it's running.")
        return False
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_TABLES)
            logger.info("Database schema created successfully")
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        if conn:
            conn.close()
        return False

def populate_db_from_csv():
    conn = connect_to_db()
    if not conn:
        logger.error("Failed to connect to PostgreSQL. Make sure it's running.")
        return False
    
    try:
        data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data")
        
        # Import Bitcoin daily prices
        daily_prices_path = os.path.join(data_dir, "bitcoin_daily_prices.csv")
        if os.path.exists(daily_prices_path):
            logger.info(f"Importing data from {daily_prices_path}")
            df = pd.read_csv(daily_prices_path)
            
            if df['timestamp'].dtype == 'object':
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            if 'date' not in df.columns:
                df['date'] = df['timestamp'].dt.date
            
            with conn.cursor() as cursor:
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
                        row['timestamp'],
                        row['price'],
                        row.get('market_cap', None),
                        row.get('volume', None),
                        row['date']
                    ) for _, row in df.iterrows()
                ])
            logger.info(f"Imported {len(df)} daily price records")
        
        # Import whale transactions
        whale_txs_path = os.path.join(data_dir, "whale_transactions.csv")
        if os.path.exists(whale_txs_path):
            logger.info(f"Importing data from {whale_txs_path}")
            df = pd.read_csv(whale_txs_path)
            
            if 'timestamp' in df.columns and df['timestamp'].dtype == 'object':
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            with conn.cursor() as cursor:
                query = """
                    INSERT INTO whale_transactions 
                    (tx_hash, block_hash, timestamp, value_btc, fee, size, input_count, output_count, is_coinbase)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tx_hash) DO NOTHING
                """
                execute_batch(cursor, query, [
                    (
                        row['tx_hash'],
                        row.get('block_hash', None),
                        row['timestamp'] if pd.notna(row.get('timestamp', pd.NA)) else None,
                        row['value_btc'],
                        row.get('fee', None),
                        row.get('size', None),
                        row['input_count'],
                        row['output_count'],
                        row.get('is_coinbase', False)
                    ) for _, row in df.iterrows()
                ])
            logger.info(f"Imported {len(df)} whale transactions")
        
        # Import crypto news
        news_path = os.path.join(data_dir, "crypto_news.csv")
        if os.path.exists(news_path):
            logger.info(f"Importing data from {news_path}")
            df = pd.read_csv(news_path)
            
            if 'published_date' in df.columns and df['published_date'].dtype == 'object':
                df['published_date'] = pd.to_datetime(df['published_date'])
            
            with conn.cursor() as cursor:
                query = """
                    INSERT INTO crypto_news 
                    (title, link, summary, published_date, source)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (link) DO NOTHING
                """
                execute_batch(cursor, query, [
                    (
                        row['title'],
                        row['link'],
                        row['summary'],
                        row['published_date'] if pd.notna(row.get('published_date', pd.NA)) else None,
                        row['source']
                    ) for _, row in df.iterrows()
                ])
            logger.info(f"Imported {len(df)} news articles")
        
        conn.commit()
        logger.info("Successfully imported historical data to PostgreSQL")
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error importing data: {e}")
        if conn:
            conn.close()
        return False

if __name__ == "__main__":
    logger.info("Setting up PostgreSQL database...")
    if setup_database():
        logger.info("Database setup complete.")
        if len(sys.argv) > 1 and sys.argv[1] == "--import":
            logger.info("Importing historical data...")
            if populate_db_from_csv():
                logger.info("Data import complete.")
            else:
                logger.error("Data import failed.")
    else:
        logger.error("Database setup failed.")
