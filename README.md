# BTC/USTC Price Prediction Pipeline

This repository contains a comprehensive pipeline for streaming, processing, and predicting BTC/USTC price trends using a Retrieval-Augmented Generation (RAG) system with DeepSeek V3, built on Apache Kafka and Docker, with potential future integration of an MCP AI Agent.

## Overview

- **Purpose:** Stream real-time BTC/USTC price data, process it with a RAG system, and predict price trends using DeepSeek V3, with plans for a future Multi-Component Predictive (MCP) AI Agent.

- **Components:**

  - **Kafka Streaming:** Ingests real-time price data from sources like Binance using Kafka with ZooKeeper and Kafka UI.

  - **RAG System:** Retrieves relevant data (prices, news) from storage and augments it for prediction.

  - **DeepSeek V3:** Generates price trend predictions based on retrieved data.

  - **Future MCP AI Agent:** Plans to integrate a multi-component AI agent for advanced prediction and decision-making.
