# Kafka Information Streaming Setup

This repository provides a Docker-based setup for running Apache Kafka with ZooKeeper and Kafka UI, designed for streaming data. It includes an optional configuration for exposing Kafka externally using ngrok, making it accessible beyond your local network.

## Overview

- **Purpose**: Set up a Kafka cluster to stream and process real-time BTC/USTC price data from sources like Binance, with optional external access via ngrok.
- **Components**:
  - **ZooKeeper**: Manages Kafka’s metadata.
  - **Kafka Broker**: Handles message streaming.
  - **Kafka UI**: A web interface for monitoring and managing Kafka topics.
- **External Access**: Uses ngrok to expose the Kafka broker to the internet.

## Prerequisites

- **Docker**: Installed on your system (e.g., Docker Desktop on macOS).
- **Docker Compose**: Included with Docker Desktop or installed separately.
- **ngrok**: Required for external access (optional; see [installation](#installing-ngrok)).
- **Python**: For running producer/consumer scripts (optional, if included in your repo).

## Setup Instructions

### 1. Clone the Repository
  ```bash
  git clone https://github.com/<your-username>/<your-repo-name>.git
  cd <your-repo-name>
  ```

### 2. Configure `docker-compose.yml`

The `docker-compose.yml` file in this repository defines the Kafka setup with ZooKeeper, a single Kafka broker, and Kafka UI. It uses environment variables like `NGROK_URL` for external access and includes volumes for data persistence.

- **Notes:**
  - Volumes ensure data persistence for ZooKeeper and Kafka.
  - `NGROK_URL` defaults to `localhost:9092` if not set.

### 3. Start Kafka Locally

Run the following command to start Kafka and related services:
  ```bash
  docker-compose up -d
  ```

- Verify services are running:
  ```bash
  docker ps
  ```

- Access Kafka UI at [http://localhost:8080](http://localhost:8080).

### 4. Enable External Access with ngrok (Optional)

To expose Kafka externally:

**Installing ngrok**
- Install Homebrew (if not already installed):
  ```bash
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  ```

- Add Homebrew to your PATH:
  ```bash
  echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
  eval "$(/opt/homebrew/bin/brew shellenv)"
  ```

- Install ngrok:
  ```bash
  brew install ngrok
  ```

- Sign up for an ngrok account at [ngrok](https://dashboard.ngrok.com/signup) and get your authtoken from [your authtoken page](https://dashboard.ngrok.com/get-started/your-authtoken).

- Configure the authtoken:
  ```bash
  ngrok authtoken YOUR_AUTH_TOKEN
  ```

**Running ngrok**

- Start ngrok to tunnel port 9092:
  ```bash
  ngrok tcp 9092
  ```

- Note the forwarding URL (e.g., `tcp://2.tcp.ngrok.io:14346`).

**Update Kafka with ngrok URL**

- Stop the current setup:
  ```bash
  docker-compose down
  ```

- Start with the ngrok URL:
  ```bash
  NGROK_URL=2.tcp.ngrok.io:14346 docker-compose up -d
  ```

Now external clients can connect to `2.tcp.ngrok.io:14346`.

















