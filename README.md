# Bitcoin Price Prediction System with Flink and DeepSeek

This comprehensive real-time system collects Bitcoin market data and crypto news, processes it with Apache Flink, stores structured data in PostgreSQL, provides vector search through Milvus, and uses DeepSeek models for advanced retrieval augmented generation (RAG).

## System Architecture

The system integrates multiple components:

1. **Real-time data collection** from Binance and CoinGecko (market data) and crypto news sources
2. **Data streaming with Kafka** for reliable message delivery
3. **Stream processing with Apache Flink** for real-time analytics
4. **Structured storage in PostgreSQL** for relational data and time series
5. **Vector embeddings in Milvus** for semantic search capabilities
6. **DeepSeek v3 models** for AI-powered analysis and natural language generation
7. **Spring AI integration** with hot-swap ability for model connections

## Key Features

- Real-time Bitcoin market data analysis
- News sentiment analysis with crypto-specific enhancements
- Combined market and sentiment correlation analysis
- Historical data storage and trend analysis
- Semantic search using vector embeddings
- AI-powered insights and predictions with DeepSeek v3
- Horizontally scalable architecture with Flink and Kafka
- Hot-swappable AI model integration through Spring AI

## System Components Diagram

```
Data Sources → Data Collection → Kafka → Flink Processing → Storage → Analytics/RAG → UI/API
   |               |               |           |              |            |           |
Binance       BTC Collector       Topics    Market Proc.   PostgreSQL   DeepSeek     Spring AI
CoinGecko   News Scraper          │         Sentiment      Milvus DB     RAG       Integration
News Sites                        │         Combined                    System
                                  │
                                  ↓
                           Flink Stream Processing
```

## Getting Started

### Prerequisites

- Docker and Docker Compose
- HuggingFace API token (for DeepSeek models)
- Java 11+ (for Spring AI integration)
- Python 3.10+

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/btc-prediction-system.git
   cd btc-prediction-system
   ```

2. Create a `.env` file from the template:
   ```bash
   cp env.example.txt .env
   ```
   The file will be a hidden one under current directory, use `ls -a` for checking.

3. Add your HuggingFace API token to the `.env` file:
   ```
   HF_TOKEN=your_huggingface_token_here
   ```

4. Build and start the system:
   ```bash
   docker-compose up -d
   ```

5. Monitor the logs:
   ```bash
   docker-compose logs -f
   ```

### Accessing the Services

- **Flink Dashboard**: http://localhost:8081
- **Kafka UI**: http://localhost:8080
- **pgAdmin**: http://localhost:5050 (login with admin@pgadmin.org / admin)
- **Milvus UI**: http://localhost:9001 (login with minioadmin / minioadmin)

## Data Flow

1. **Data Collection**:
   - `btc_kafka_producer.py` collects BTC market data from CoinGecko and Binance
   - `crypto_news_scraper.py` collects news from crypto news sources
   - Data is sent to respective Kafka topics

2. **Stream Processing with Flink**:
   - `market_data_processor.py` processes raw market data (prices, volume, orderbook)
   - `news_sentiment_processor.py` analyzes news sentiment with NLP techniques
   - `combined_analysis_processor.py` correlates market and sentiment data

3. **Data Storage**:
   - Structured data is stored in PostgreSQL (time series, articles, analysis)
   - Vector embeddings are stored in Milvus for semantic search
   
4. **Analytics & RAG**:
   - Combined market-sentiment analysis generates insights
   - DeepSeek embeddings provide vector representations
   - DeepSeek LLM powers the RAG system for natural language queries

5. **Integration**:
   - Spring AI connection provides a hot-swappable model architecture
   - Future addition: Trading agent AI for quantitative trading

## Flink Jobs

The system uses Apache Flink for stream processing, with three main jobs:

1. **Market Data Processor**:
   - Processes raw market data from Kafka
   - Calculates derived metrics (liquidity ratios, imbalances)
   - Stores processed data in PostgreSQL and forwards to combined analysis

2. **News Sentiment Processor**:
   - Analyzes news article sentiment with specialized crypto lexicon
   - Extracts entities and calculates relevance scores
   - Stores results in PostgreSQL and forwards to combined analysis

3. **Combined Analysis Processor**:
   - Joins market and sentiment data streams
   - Calculates correlations and generates insights
   - Stores analysis in both PostgreSQL and Milvus for retrieval

## Database Schema

The PostgreSQL database is organized into three schemas:

1. **btc_data**: Contains market-related tables
   - `market_snapshots`: Time-series BTC price and liquidity metrics
   - `order_book_snapshots`: Order book data in JSONB format

2. **news_data**: Contains news and sentiment-related tables
   - `articles`: News articles with content in text and metadata in JSONB
   - `sentiment_analysis`: Sentiment scores and entities

3. **analysis_data**: Contains combined analysis data
   - `combined_analysis`: Market and sentiment correlation with insights
   - `vector_embeddings`: References to Milvus vector IDs

## Milvus Vector Database

The system uses Milvus to store vector embeddings for:
- Market data snapshots
- News article content
- Combined analysis documents
- Generated insights

These embeddings enable semantic search and similarity matching for the RAG system.

## DeepSeek Integration

This system uses DeepSeek models for:

1. **Embeddings**: The `deepseek-ai/deepseek-v3-embedding` model converts text data into vector embeddings for semantic search.

2. **LLM**: The `deepseek-ai/deepseek-v3` model serves as the foundation for the RAG system, generating answers to user queries based on retrieved context.

## Spring AI Hot-Swap System

The system is designed with a modular architecture that allows for hot-swapping of AI models through Spring AI. This provides:

- Flexibility to swap models without downtime
- A/B testing capabilities for different models
- Gradual rollout of new model versions
- Integration with Java-based enterprise systems

## Quantitative Trading Integration

The system is designed to later add agent AI for direct quantitative trading:

1. First stage: Validate prediction performance using historical and real-time data
2. Second stage: Implement trading strategies based on insights and predictions
3. Final stage: Deploy agent AI for automated trading with risk management

## Configuration Options

Key configuration options in `.env`:

```
# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# DeepSeek models
EMBEDDING_MODEL_NAME=deepseek-ai/deepseek-v3-embedding
LLM_MODEL_NAME=deepseek-ai/deepseek-v3

# HuggingFace API token (required for DeepSeek models)
HF_TOKEN=your_huggingface_token_here

# Database configurations
MILVUS_HOST=milvus
POSTGRES_HOST=postgres

# GPU acceleration
USE_CUDA=false
```

## Development and Extension

### Adding New Data Sources

To add a new data source:
1. Create a new collector script following the pattern in `btc_kafka_producer.py`
2. Define a new Kafka topic for your data
3. Update or create a Flink processor for your data type
4. Modify the combined analysis job to incorporate your new data

### Creating New Flink Jobs

To add a new Flink job:
1. Create a new Python script in the `flink-jobs` directory
2. Use the existing jobs as templates
3. Add your job to the `submit_flink_jobs.py` script
4. Rebuild the Flink container with `docker-compose build flink-python-job`

### Integrating with Trading Systems

The system can be extended to connect with trading APIs:
1. Create a trading strategy module that consumes from the combined analysis topic
2. Implement risk management rules and position sizing logic
3. Connect to exchange APIs (Binance, etc.) for order execution
4. Monitor performance with additional Flink jobs

## Troubleshooting

### Common Issues

- **Flink jobs failing**: Check logs with `docker-compose logs flink-python-job`
- **Kafka connection issues**: Verify with `docker-compose logs kafka`
- **Database connection errors**: Ensure PostgreSQL is running with `docker-compose logs postgres`
- **Milvus connection failures**: Check status with `docker-compose logs milvus`

### Monitoring

Monitor the system with:
- Flink Dashboard for job status and metrics
- Kafka UI for topic inspection
- pgAdmin for database monitoring
- Docker stats for resource usage: `docker stats`

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Apache Flink community for the stream processing framework
- DeepSeek AI for the language models
- Milvus team for the vector database
- LangChain for the RAG framework
