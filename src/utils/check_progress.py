"""
Progress tracking utility for BitRag data collection and processing.
"""
import os
import json
import logging
import time
import argparse
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProgressTracker:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.collection_progress_file = os.path.join(data_dir, "collection_progress.json")
        self.processing_progress_file = os.path.join(data_dir, "processing_progress.json")
    
    def _load_progress(self, progress_file):
        if os.path.exists(progress_file):
            try:
                with open(progress_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading progress file {progress_file}: {e}")
                return self._init_progress(os.path.basename(progress_file))
        return self._init_progress(os.path.basename(progress_file))
    
    def _init_progress(self, file_name):
        current_time = datetime.now().isoformat()
        if file_name == "collection_progress.json":
            return {
                "status": "not_started",
                "start_time": current_time,
                "end_time": None,
                "steps": {
                    "price_daily": {"status": "pending", "count": 0, "completed": False, "last_date": None},
                    "price_hourly": {"status": "pending", "count": 0, "completed": False, "last_date": None},
                    "whale_transactions": {"status": "pending", "count": 0, "completed": False, "last_block": None},
                    "news": {"status": "pending", "count": 0, "completed": False, "last_date": None}
                },
                "overall_progress": 0
            }
        else:  # processing_progress.json
            return {
                "start_time": current_time,
                "current_time": current_time,
                "runtime_seconds": 0,
                "last_update": current_time,
                "prices_processed": 0,
                "transactions_processed": 0,
                "news_processed": 0,
                "embeddings_generated": 0,
                "errors": 0,
                "processing_rate": {
                    "prices_per_minute": 0,
                    "transactions_per_minute": 0,
                    "news_per_minute": 0,
                    "embeddings_per_minute": 0
                }
            }
    
    def display_progress(self, progress_type="all"):
        if progress_type in ["collection", "all"]:
            collection_progress = self._load_progress(self.collection_progress_file)
            logger.info("Collection Progress:")
            logger.info(f"Status: {collection_progress['status']}")
            logger.info(f"Start Time: {collection_progress['start_time']}")
            logger.info(f"End Time: {collection_progress.get('end_time', 'N/A')}")
            logger.info(f"Overall Progress: {collection_progress['overall_progress']:.2f}%")
            logger.info("Steps:")
            for step, details in collection_progress['steps'].items():
                logger.info(f"  {step}:")
                logger.info(f"    Status: {details['status']}")
                logger.info(f"    Count: {details['count']}")
                logger.info(f"    Completed: {details['completed']}")
                if 'last_date' in details and details['last_date']:
                    logger.info(f"    Last Date: {details['last_date']}")
                if 'last_block' in details and details['last_block']:
                    logger.info(f"    Last Block: {details['last_block']}")
        
        if progress_type in ["processing", "all"]:
            processing_progress = self._load_progress(self.processing_progress_file)
            logger.info("\nProcessing Progress:")
            logger.info(f"Start Time: {processing_progress['start_time']}")
            logger.info(f"Last Update: {processing_progress['last_update']}")
            logger.info(f"Runtime (seconds): {processing_progress['runtime_seconds']:.2f}")
            logger.info(f"Prices Processed: {processing_progress['prices_processed']}")
            logger.info(f"Transactions Processed: {processing_progress['transactions_processed']}")
            logger.info(f"News Processed: {processing_progress['news_processed']}")
            logger.info(f"Embeddings Generated: {processing_progress['embeddings_generated']}")
            logger.info(f"Errors: {processing_progress['errors']}")
            logger.info("Processing Rates (per minute):")
            for key, value in processing_progress['processing_rate'].items():
                logger.info(f"  {key}: {value:.2f}")

def main():
    parser = argparse.ArgumentParser(description="BitRag Progress Tracker")
    parser.add_argument('--type', choices=['collection', 'processing', 'all'], default='all', help="Progress type to display")
    parser.add_argument('--watch', action='store_true', help="Watch progress with continuous updates")
    parser.add_argument('--interval', type=int, default=5, help="Watch interval in seconds")
    args = parser.parse_args()
    
    tracker = ProgressTracker(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"))
    
    if args.watch:
        while True:
            print("\033[H\033[J")  # Clear terminal
            tracker.display_progress(args.type)
            time.sleep(args.interval)
    else:
        tracker.display_progress(args.type)

if __name__ == "__main__":
    main()
