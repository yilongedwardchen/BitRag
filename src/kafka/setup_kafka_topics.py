"""
Script to set up Kafka topics for the BitRag project.
Run this after starting the Kafka container.
"""
import logging
import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'bitrag-kafka:9092')
TOPICS = [
    {'name': 'bitcoin_price_updates', 'num_partitions': 1, 'replication_factor': 1},
    {'name': 'whale_transactions', 'num_partitions': 1, 'replication_factor': 1},
    {'name': 'crypto_news', 'num_partitions': 1, 'replication_factor': 1}
]

def create_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id='bitrag-admin'
    )
    
    try:
        existing_topics = admin_client.list_topics()
        logger.info(f"Existing topics: {existing_topics}")
    except Exception as e:
        logger.error(f"Error listing topics: {e}")
        existing_topics = []
    
    new_topics = []
    for topic in TOPICS:
        if topic['name'] not in existing_topics:
            new_topics.append(NewTopic(
                name=topic['name'],
                num_partitions=topic['num_partitions'],
                replication_factor=topic['replication_factor']
            ))
    
    if new_topics:
        try:
            admin_client.create_topics(new_topics=new_topics)
            logger.info(f"Created topics: {[topic.name for topic in new_topics]}")
            for topic in new_topics:
                logger.info(f"Topic {topic.name}: {topic.num_partitions} partitions, {topic.replication_factor} replication factor")
        except TopicAlreadyExistsError:
            logger.warning("Some topics already exist.")
        except Exception as e:
            logger.error(f"Error creating topics: {e}")
    else:
        logger.info("All topics already exist.")
    
    admin_client.close()

if __name__ == "__main__":
    logger.info("Setting up Kafka topics...")
    create_topics()
    logger.info("Kafka setup complete.")
