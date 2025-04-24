"""
Script to initialize directories and create missing files in the Docker container.
Run this before starting the collection and processing.
"""
import os
import logging
import json
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_directory_structure():
    """Create the necessary directory structure."""
    # Get project root directory
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Define directories to create
    directories = [
        os.path.join(root_dir, "data"),
        os.path.join(root_dir, "logs"),
    ]
    
    # Create directories
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
            logger.info(f"Created directory: {directory}")
        else:
            logger.info(f"Directory already exists: {directory}")
    
    return os.path.join(root_dir, "data")

def create_progress_files(data_dir):
    """Create empty progress files if they don't exist."""
    # Collection progress file
    collection_progress_file = os.path.join(data_dir, "collection_progress.json")
    if not os.path.exists(collection_progress_file):
        initial_collection_progress = {
            "status": "not_started",
            "start_time": None,
            "end_time": None,
            "steps": {
                "price_daily": {"status": "pending", "count": 0, "completed": False},
                "price_hourly": {"status": "pending", "count": 0, "completed": False},
                "whale_transactions": {"status": "pending", "count": 0, "completed": False},
                "news": {"status": "pending", "count": 0, "completed": False}
            },
            "overall_progress": 0
        }
        
        with open(collection_progress_file, 'w') as f:
            json.dump(initial_collection_progress, f, indent=2)
        logger.info(f"Created collection progress file: {collection_progress_file}")
    
    # Processing progress file
    processing_progress_file = os.path.join(data_dir, "processing_progress.json")
    if not os.path.exists(processing_progress_file):
        initial_processing_progress = {
            "start_time": datetime.now().isoformat(),
            "current_time": datetime.now().isoformat(),
            "runtime_seconds": 0,
            "last_update": datetime.now().isoformat(),
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
        
        with open(processing_progress_file, 'w') as f:
            json.dump(initial_processing_progress, f, indent=2)
        logger.info(f"Created processing progress file: {processing_progress_file}")

def check_import_paths():
    """Verify that module imports will work correctly."""
    try:
        # Test imports to verify paths
        import src.data_collection.price_collector
        import src.data_collection.transaction_collector
        import src.data_collection.news_collector
        logger.info("Import paths verified successfully")
        return True
    except ImportError as e:
        logger.error(f"Import path verification failed: {e}")
        logger.error("Please ensure the PYTHONPATH is set correctly")
        return False

def main():
    """Main initialization function."""
    logger.info("Starting Docker initialization...")
    
    # Create directory structure
    data_dir = create_directory_structure()
    
    # Create progress files
    create_progress_files(data_dir)
    
    # Check import paths
    if check_import_paths():
        logger.info("Docker initialization completed successfully")
    else:
        logger.warning("Docker initialization completed with warnings")

if __name__ == "__main__":
    main()
