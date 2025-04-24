"""
Bitcoin price data collection module using CryptoCompare API with optimized performance.
"""
import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BitcoinPriceCollector:
    """Collects Bitcoin price data from CryptoCompare API."""
    
    BASE_URL = "https://min-api.cryptocompare.com/data"
    
    def __init__(self, api_key=None):
        self.session = requests.Session()
        self.api_key = api_key or os.environ.get('CRYPTOCOMPARE_API_KEY', '')
        if not self.api_key:
            logger.error("CryptoCompare API key is required for full functionality. Set CRYPTOCOMPARE_API_KEY environment variable.")
            raise ValueError("Missing CryptoCompare API key")
    
    def _make_request(self, endpoint, params=None, timeout=10, retries=3):
        """Make a request to the CryptoCompare API with rate limiting and timeouts."""
        url = f"{self.BASE_URL}/{endpoint}"
        if params is None:
            params = {}
        params['api_key'] = self.api_key
        
        for attempt in range(retries):
            try:
                response = self.session.get(url, params=params, timeout=timeout)
                response.raise_for_status()
                time.sleep(0.5)
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"API request failed (attempt {attempt+1}/{retries}): {e}")
                if hasattr(e, 'response') and e.response and e.response.status_code == 429:
                    wait_time = min(60 * (attempt + 1), 300)
                    logger.warning(f"Rate limit hit, waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                elif attempt < retries - 1:
                    time.sleep(2 * (attempt + 1))
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                if attempt < retries - 1:
                    time.sleep(2)
        
        logger.error(f"Failed to get data after {retries} attempts")
        return None
    
    def get_historical_prices(self, days=90, interval='day'):
        """
        Get historical Bitcoin price data.
        
        Args:
            days: Number of days to fetch
            interval: 'day', 'hour', or 'minute' (minute limited in free tier)
        
        Returns:
            DataFrame with timestamp, price, volume, and open/close data
        """
        logger.info(f"Fetching {interval} historical prices for the past {days} days")
        
        if interval.lower() in ['day', 'daily']:
            days = min(days, 2000)
            endpoint = 'histoday'
        elif interval.lower() in ['hour', 'hourly']:
            days = min(days, 30)
            endpoint = 'histohour'
        else:
            days = min(days, 7)
            endpoint = 'histominute'
        
        logger.info(f"Using endpoint {endpoint} for {days} days of data")
        
        params = {
            'fsym': 'BTC',
            'tsym': 'USD',
            'limit': days,
            'aggregate': 1
        }
        
        data = self._make_request(f"v2/{endpoint}", params)
        
        if not data or 'Data' not in data or 'Data' not in data['Data']:
            logger.error("Failed to fetch historical price data")
            return pd.DataFrame(columns=['timestamp', 'price', 'volume', 'date'])
        
        df = pd.DataFrame(data['Data']['Data'])
        if df.empty:
            logger.warning("Received empty data set from API")
            return pd.DataFrame(columns=['timestamp', 'price', 'volume', 'date'])
            
        df['timestamp'] = pd.to_datetime(df['time'], unit='s')
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volumefrom', 'volumeto']]
        df.columns = ['timestamp', 'open', 'high', 'low', 'price', 'volume_btc', 'volume_usd']
        df['volume'] = df['volume_usd']
        df['date'] = df['timestamp'].dt.date
        
        logger.info(f"Retrieved {len(df)} data points")
        return df
    
    def get_extended_historical_data(self, start_date=None, end_date=None, interval='day'):
        """
        Get extended historical data using a more efficient approach.
        
        Args:
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format (defaults to today)
            interval: 'day', 'hour', or 'minute'
            
        Returns:
            DataFrame with all historical data
        """
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        if not start_date:
            if interval.lower() in ['day', 'daily']:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            elif interval.lower() in ['hour', 'hourly']:
                start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            else:
                start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        logger.info(f"Getting {interval} data from {start_date} to {end_date}")
        
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        days_diff = (end - start).days + 1
        
        if interval.lower() in ['day', 'daily'] and days_diff <= 2000:
            logger.info(f"Fetching entire range of {days_diff} days in one call")
            return self.get_historical_prices(days=days_diff, interval='day')
        
        max_chunk = 2000 if interval.lower() in ['day', 'daily'] else 30
        all_data = pd.DataFrame()
        current_end = end
        max_chunks = 5
        chunks_processed = 0
        
        while current_end >= start and chunks_processed < max_chunks:
            days = min((current_end - start).days + 1, max_chunk)
            if days <= 0:
                break
                
            logger.info(f"Fetching chunk {chunks_processed+1}: {days} days ending {current_end.strftime('%Y-%m-%d')}")
            
            chunk_data = self.get_historical_prices(days=days, interval=interval)
            
            if not chunk_data.empty:
                all_data = pd.concat([chunk_data, all_data])
                chunks_processed += 1
            else:
                logger.warning(f"Received empty chunk, skipping to next chunk")
            
            current_end = current_end - timedelta(days=days)
            
            if chunks_processed >= max_chunks and current_end >= start:
                logger.warning(f"Reached maximum chunk limit ({max_chunks}). Some historical data will be missing.")
        
        if not all_data.empty:
            all_data = all_data.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
            logger.info(f"Final dataset contains {len(all_data)} records")
        else:
            logger.warning("No data was collected")
        
        return all_data

if __name__ == "__main__":
    API_KEY = os.environ.get('CRYPTOCOMPARE_API_KEY', '')
    collector = BitcoinPriceCollector(api_key=API_KEY)
    daily_data = collector.get_historical_prices(days=30, interval='day')
    
    if not daily_data.empty:
        print(f"Retrieved {len(daily_data)} daily data points")
        print(daily_data.head())
        daily_data.to_csv("bitcoin_daily_prices_test.csv", index=False)
