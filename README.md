# BitRag

![BitRag Logo](path/to/logo.png) <!-- Replace with actual logo path if available -->

**BitRag** is an innovative, open-source Bitcoin price analysis system that harnesses the power of **Retrieval-Augmented Generation (RAG)** with the cutting-edge **DeepSeek V3** model. Designed to deliver real-time insights into Bitcoin market trends, BitRag collects, processes, and analyzes a wealth of data—including price, trading volume, whale transactions, and news—making it an essential tool for cryptocurrency enthusiasts, traders, and developers alike. With a robust, scalable architecture built on modern technologies like Kafka, Pathway, PostgreSQL, Milvus, and LangChain, BitRag is fully containerized using Docker for seamless deployment and operation.

---

## 🚀 Features

- **Comprehensive Data Collection**: Gathers both historical and real-time Bitcoin data from trusted APIs like CryptoCompare and Blockchair.
- **Real-Time Data Streaming**: Utilizes Kafka to deliver continuous updates, keeping you ahead of the market.
- **Low-Latency Processing**: Employs Pathway for lightning-fast data processing, storing raw data in PostgreSQL and vector embeddings in Milvus.
- **Intelligent RAG Analysis**: Combines LangChain with DeepSeek V3 to provide context-aware, insightful responses to your queries.
- **User-Friendly REST API**: Offers a FastAPI-powered interface for easy interaction with the system.
- **Progress Tracking**: Includes utility scripts to monitor data collection and processing in real time.
- **Dockerized for Simplicity**: Runs all services in containers, ensuring effortless setup, scalability, and portability.

---

## 🏗️ Architecture Overview

BitRag’s architecture is a symphony of interconnected components, designed for performance and scalability:

- **Data Collectors**: Custom Python scripts (`price_collector.py`, `transaction_collector.py`, `news_collector.py`) fetch data from external APIs with precision.
- **Kafka Streaming**: Handles real-time data flow through dedicated topics like `bitcoin_price_updates`, `whale_transactions`, and `crypto_news`.
- **Pathway Processing**: Processes Kafka streams with minimal latency, feeding raw data to PostgreSQL and embeddings to Milvus.
- **PostgreSQL Storage**: Archives raw data (prices, transactions, news) for historical analysis and querying.
- **Milvus Vector Store**: Manages vector embeddings of news articles, enabling efficient similarity searches.
- **LangChain & DeepSeek V3**: Drives the RAG engine, delivering smart, context-rich responses.
- **FastAPI Interface**: Provides a sleek REST API for seamless user interaction.

---

## 📋 Prerequisites

Before diving in, ensure you have the following:

- **Docker** and **Docker Compose**: Required for containerized deployment.
- **Python 3.8+**: Optional, for running scripts outside Docker.
- **API Keys**:
  - CryptoCompare API key (`CRYPTOCOMPARE_API_KEY`).
  - Blockchair API key (`BLOCKCHAIR_API_KEY`).

---

## 🛠️ Installation Guide

Follow these steps to get BitRag up and running:

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/BitRag.git
   cd BitRag
   ```

2. **Configure Environment Variables**:
   Create a `.env` file in the root directory and add your API keys:
   ```env
   CRYPTOCOMPARE_API_KEY=your_cryptocompare_api_key
   BLOCKCHAIR_API_KEY=your_blockchair_api_key
   ```

3. **Launch Docker Containers**:
   ```bash
   docker-compose up -d
   ```

4. **Initialize the Docker Environment**:
   ```bash
   docker exec -it bitrag-app python /app/src/utils/init_docker.py
   ```

5. **Set Up PostgreSQL Schema**:
   ```bash
   docker exec -it bitrag-app python /app/src/database/setup_postgres.py
   ```

6. **Create Kafka Topics**:
   ```bash
   docker exec -it bitrag-app python /app/src/kafka/setup_kafka_topics.py
   ```

7. **Collect Historical Data**:
   ```bash
   docker exec -it bitrag-app python /app/src/data_collection/collect_historical_data.py
   ```

8. **Start the Data Processing Pipeline**:
   ```bash
   docker exec -it bitrag-app python /app/src/pathway/data_processor.py
   ```

9. **Run Kafka Producers**:
   ```bash
   docker exec -it bitrag-app python /app/src/kafka/kafka_producers.py
   ```

10. **Launch the API Server**:
    ```bash
    docker exec -it bitrag-app python /app/src/rag/api.py
    ```

### Simplify with `docker-manage.sh`
For a smoother experience, use the provided management script:
- Start all services: `./docker-manage.sh start`
- Stop services: `./docker-manage.sh stop`
- View logs: `./docker-manage.sh logs app`
- Check progress: `./docker-manage.sh progress`

---

## 📖 How to Use BitRag

Once the system is running, interact with BitRag via its REST API:

- **Query the RAG Model**:
  ```bash
  curl -X POST http://localhost:8000/query -H 'Content-Type: application/json' -d '{"query": "What factors are influencing Bitcoin price today?"}'
  ```

- **Fetch Recent Price Data**:
  ```bash
  curl http://localhost:8000/recent-price
  ```

- **View Recent Whale Transactions**:
  ```bash
  curl http://localhost:8000/recent-whales
  ```

- **Monitor System Progress**:
  ```bash
  docker exec -it bitrag-app python /app/src/utils/check_progress.py --type all
  ```

- **Debug Milvus Integration**:
  ```bash
  docker exec -it bitrag-app python /app/src/utils/debug_milvus.py
  ```

---

## 📚 API Documentation

BitRag’s REST API offers the following endpoints:

- **GET /**: Returns a friendly welcome message.
- **GET /health**: Checks the API and RAG engine status.
- **POST /query**: Accepts a query (e.g., `{"query": "Bitcoin trend analysis"}`) and returns a detailed RAG-generated response.
- **GET /recent-price**: Retrieves Bitcoin price data for the last 7 days.
- **GET /recent-whales**: Lists the top 5 recent whale transactions.

For a full interactive experience, visit [http://localhost:8000/docs](http://localhost:8000/docs) when the API is active.

---

## 🤝 Contributing to BitRag

We’d love your help to make BitRag even better! Here’s how to contribute:

1. Fork the repository.
2. Create a branch for your feature or bugfix.
3. Commit your changes with descriptive messages.
4. Submit a pull request with a clear explanation of your updates.

---

## 📜 License

BitRag is proudly licensed under the MIT License. See [LICENSE](LICENSE) for more details.

---

## 🙏 Acknowledgments

- A huge thank you to the open-source community for powering BitRag with incredible tools like Kafka, Pathway, Milvus, LangChain, and DeepSeek.
- Special appreciation to all contributors who are helping shape the future of BitRag.

---

Built with passion for the crypto community, BitRag is your gateway to understanding Bitcoin market dynamics in real time. Get started today and explore the power of RAG-driven analysis!