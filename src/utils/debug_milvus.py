"""
Utility script to debug Milvus connection issues.
"""
import os
import sys
import time
import logging
import requests
from pymilvus import connections, utility, Collection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Milvus connection parameters
MILVUS_HOST = os.environ.get('MILVUS_HOST', 'bitrag-milvus')
MILVUS_PORT = int(os.environ.get('MILVUS_PORT', 19530))
ETCD_HOST = os.environ.get('ETCD_HOST', 'bitrag-etcd')
ETCD_PORT = int(os.environ.get('ETCD_PORT', 2379))
MINIO_HOST = os.environ.get('MINIO_HOST', 'bitrag-minio')
MINIO_PORT = int(os.environ.get('MINIO_PORT', 9000))

def check_connection():
    logger.info(f"Attempting to connect to Milvus at {MILVUS_HOST}:{MILVUS_PORT}")
    
    try:
        connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT)
        logger.info("Successfully connected to Milvus!")
        
        logger.info(f"Milvus server version: {utility.get_server_version()}")
        
        logger.info("Checking for existing collections...")
        existing_collections = utility.list_collections()
        if existing_collections:
            logger.info(f"Found collections: {existing_collections}")
            for collection_name in existing_collections:
                collection = Collection(collection_name)
                logger.info(f"Collection {collection_name}:")
                logger.info(f"  Schema: {collection.schema}")
                logger.info(f"  Index: {collection.indexes}")
        else:
            logger.info("No collections found")
        
        connections.disconnect("default")
        logger.info("Connection test completed successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to connect to Milvus: {e}")
        return False

def test_with_retry(max_retries=5, delay=10):
    for attempt in range(max_retries):
        logger.info(f"Connection attempt {attempt + 1}/{max_retries}")
        if check_connection():
            return True
        if attempt < max_retries - 1:
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
    logger.error(f"Failed to connect after {max_retries} attempts")
    return False

def check_dependencies():
    logger.info("Checking Milvus dependencies...")
    
    logger.info("Testing connection to etcd...")
    try:
        response = requests.get(f"http://{ETCD_HOST}:{ETCD_PORT}/health", timeout=5)
        if response.json().get("health") == "true":
            logger.info("etcd is healthy")
        else:
            logger.error(f"etcd health check failed: {response.text}")
    except Exception as e:
        logger.error(f"Failed to check etcd: {e}")
    
    logger.info("Testing connection to MinIO...")
    try:
        response = requests.get(f"http://{MINIO_HOST}:{MINIO_PORT}/minio/health/live", timeout=5)
        if response.status_code == 200:
            logger.info("MinIO is accessible")
        else:
            logger.error(f"MinIO health check failed: {response.text}")
    except Exception as e:
        logger.error(f"Failed to check MinIO: {e}")

def check_milvus_logs():
    logger.info("To check Milvus logs, run the following command:")
    logger.info("docker logs bitrag-milvus")
    logger.info("\nTo check etcd logs:")
    logger.info("docker logs bitrag-etcd")
    logger.info("\nTo check MinIO logs:")
    logger.info("docker logs bitrag-minio")

def main():
    logger.info("Starting Milvus connection debugging...")
    test_with_retry()
    check_dependencies()
    check_milvus_logs()
    logger.info("Debugging completed")

if __name__ == "__main__":
    main()
ţul

---

#### 7. `init_docker.py` (in `src/utils/`)
**Purpose**: Initializes directories (`data`, `logs`) and creates progress files (`collection_progress.json`, `processing_progress.json`) in the Docker container.

**Strengths**:
- Creates necessary directories (`data`, `logs`) with `os.makedirs(exist_ok=True)` to avoid errors if they exist.
- Initializes progress files with sensible defaults, aligning with `ProgressTracker` and `ProcessingStats` structures.
- Verifies import paths for key modules (`price_collector`, `transaction_collector`, `news_collector`), ensuring `PYTHONPATH` is correct.
- Uses logging to track initialization steps.

**Issues**:
- **Redundant Progress Structure**: The `collection_progress.json` structure lacks `last_date` and `last_block` fields added in the updated `collect_historical_data.py`, causing potential inconsistencies.
- **Log Directory Unused**: Creates a `logs` directory, but logs are written to `data_collection.log` in the `data` directory (per `collect_historical_data.py`). This directory is unused unless other scripts write logs there.
- **Error Handling**: Limited error handling for file creation; permissions issues in the Docker container could cause failures.
- **Hardcoded Paths**: Uses `os.path.dirname` to compute the root directory, which assumes a specific project structure.

**Recommendations**:
- Update `collection_progress.json` to include `last_date` and `last_block` fields.
- Remove or utilize the `logs` directory, or redirect logs to it.
- Add error handling for file permissions and directory creation.
- Make the root directory configurable via an environment variable.

**Fixes**:
- Below is an updated `init_docker.py`.

<xaiArtifact artifact_id="f1a8b039-78d7-49da-b8d6-0cfb69efc0f0" artifact_version_id="6d1f9199-2818-407e-9db3-f6d7fbb18c14" title="init_docker.py (updated)" contentType="text/python">
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
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "init_docker.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def create_directory_structure():
    root_dir = os.environ.get('BITRAG_ROOT_DIR', os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    directories = [
        os.path.join(root_dir, "data"),
        os.path.join(root_dir, "logs"),
    ]
    
    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
            os.chmod(directory, 0o777)  # Ensure write permissions
            logger.info(f"Created directory: {directory}")
        except Exception as e:
            logger.error(f"Failed to create directory {directory}: {e}")
            raise
    
    return os.path.join(root_dir, "data")

def create_progress_files(data_dir):
    collection_progress_file = os.path.join(data_dir, "collection_progress.json")
    if not os.path.exists(collection_progress_file):
        initial_collection_progress = {
            "status": "not_started",
            "start_time": None,
            "end_time": None,
            "steps": {
                "price_daily": {"status": "pending", "count": 0, "completed": False, "last_date": None},
                "price_hourly": {"status": "pending", "count": 0, "completed": False, "last_date": None},
                "whale_transactions": {"status": "pending", "count": 0, "completed": False, "last_block": None},
                "news": {"status": "pending", "count": 0, "completed": False, "last_date": None}
            },
            "overall_progress": 0
        }
        
        try:
            with open(collection_progress_file, 'w') as f:
                json.dump(initial_collection_progress, f, indent=2)
            os.chmod(collection_progress_file, 0o666)
            logger.info(f"Created collection progress file: {collection_progress_file}")
        except Exception as e:
            logger.error(f"Failed to create collection progress file: {e}")
            raise
    
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
        
        try:
            with open(processing_progress_file, 'w') as f:
                json.dump(initial_processing_progress, f, indent=2)
            os.chmod(processing_progress_file, 0o666)
            logger.info(f"Created processing progress file: {processing_progress_file}")
        except Exception as e:
            logger.error(f"Failed to create processing progress file: {e}")
            raise

def check_import_paths():
    try:
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
    logger.info("Starting Docker initialization...")
    
    data_dir = create_directory_structure()
    create_progress_files(data_dir)
    
    if check_import_paths():
        logger.info("Docker initialization completed successfully")
    else:
        logger.warning("Docker initialization completed with warnings")
        sys.exit(1)

if __name__ == "__main__":
    main()
