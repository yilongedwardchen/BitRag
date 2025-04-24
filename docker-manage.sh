#!/bin/bash

# BitRag Docker Management Script with Progress Tracking

# Exit on error
set -e

# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Set compose file
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Trap errors
trap 'log_error "An error occurred. Exiting..."; exit 1' ERR

# Check if docker-compose file exists
if [ ! -f "$COMPOSE_FILE" ]; then
    log_error "Cannot find $COMPOSE_FILE in current directory!"
    log_info "Please set COMPOSE_FILE environment variable or ensure the file exists."
    exit 1
fi

# Function to clean all Docker resources
clean_all() {
    log_info "Stopping and removing all containers, volumes, and networks..."
    docker-compose -f "$COMPOSE_FILE" down -v --remove-orphans
    log_info "All Docker resources cleaned."
}

# Function to start infrastructure services only
start_infra() {
    log_info "Starting infrastructure services..."
    docker-compose -f "$COMPOSE_FILE" up -d zookeeper kafka postgres etcd minio milvus kafka-ui adminer
    log_info "Infrastructure services started. You can access:"
    log_info "- Kafka UI: http://localhost:8080"
    log_info "- PostgreSQL Admin: http://localhost:8081"
}

# Function to start all services
start_all() {
    log_info "Starting all BitRag services..."
    docker-compose -f "$COMPOSE_FILE" up -d
    log_info "All services started. You can access:"
    log_info "- Kafka UI: http://localhost:8080"
    log_info "- PostgreSQL Admin: http://localhost:8081"
    log_info "- BitRag API: http://localhost:8000"
    log_info "You can test the API with: curl -X POST http://localhost:8000/query -H 'Content-Type: application/json' -d '{\"query\":\"What is the current Bitcoin price trend?\"}'"
    log_info "You can monitor progress with: ./docker-manage.sh progress"
}

# Function to collect historical data
collect_data() {
    log_info "Ensuring prerequisites for data collection..."
    docker-compose -f "$COMPOSE_FILE" run --rm app python /app/src/database/setup_postgres.py
    docker-compose -f "$COMPOSE_FILE" run --rm app python /app/src/kafka/setup_kafka_topics.py
    log_info "Starting data collection container..."
    docker-compose -f "$COMPOSE_FILE" up -d collector
    log_info "Data collection process started."
    log_info "Check collection progress with: ./docker-manage.sh progress collection"
}

# Function to stop all services
stop_all() {
    log_info "Stopping all services..."
    docker-compose -f "$COMPOSE_FILE" down
    log_info "All services stopped."
}

# Function to check logs of a specific service
check_logs() {
    if [ -z "$1" ]; then
        log_error "Please specify a service name to check logs. Available services:"
        docker-compose -f "$COMPOSE_FILE" ps --services
        exit 1
    fi
    
    case "$1" in
        app)
            container="bitrag-app"
            ;;
        collector)
            container="bitrag-collector"
            ;;
        processor)
            container="bitrag-processor"
            ;;
        kafka-producer)
            container="bitrag-kafka-producer"
            ;;
        *)
            container="$1"
            ;;
    esac
    
    log_info "Showing logs for $container..."
    docker-compose -f "$COMPOSE_FILE" logs -f "$container"
}

# Function to check progress
check_progress() {
    progress_type="all"
    if [ -n "$1" ]; then
        progress_type="$1"
    fi
    
    log_info "Checking progress for $progress_type..."
    
    if ! docker-compose -f "$COMPOSE_FILE" ps | grep -q "bitrag-app.*Up"; then
        log_info "App container is not running. Starting a temporary container to check progress..."
        docker-compose -f "$COMPOSE_FILE" run --rm app python /app/src/utils/check_progress.py --type "$progress_type"
    else
        docker-compose -f "$COMPOSE_FILE" exec app python /app/src/utils/check_progress.py --type "$progress_type"
    fi
}

# Function to watch progress updates
watch_progress() {
    progress_type="all"
    if [ -n "$1" ]; then
        progress_type="$1"
    fi
    
    interval=5
    if [ -n "$2" ]; then
        interval="$2"
    fi
    
    log_info "Watching progress for $progress_type (update every $interval seconds)..."
    
    if ! docker-compose -f "$COMPOSE_FILE" ps | grep -q "bitrag-app.*Up"; then
        log_error "App container is not running. Please start it with './docker-manage.sh start' first."
        exit 1
    else
        docker-compose -f "$COMPOSE_FILE" exec app python /app/src/utils/check_progress.py --type "$progress_type" --watch --interval "$interval"
    fi
}

# Function to show current status
show_status() {
    log_info "Current Docker container status:"
    docker-compose -f "$COMPOSE_FILE" ps
}

# Function to initialize the system
initialize() {
    log_info "Initializing BitRag system..."
    
    log_info "Starting infrastructure services..."
    docker-compose -f "$COMPOSE_FILE" up -d zookeeper kafka postgres etcd minio milvus kafka-ui adminer
    
    log_info "Waiting for services to be ready (60 seconds)..."
    sleep 60
    
    log_info "Building application container..."
    docker-compose -f "$COMPOSE_FILE" build app
    
    log_info "Initializing directories and files..."
    docker-compose -f "$COMPOSE_FILE" run --rm app python /app/src/utils/init_docker.py
    
    log_info "Setting up PostgreSQL schema..."
    docker-compose -f "$COMPOSE_FILE" run --rm app python /app/src/database/setup_postgres.py
    
    log_info "Setting up Kafka topics..."
    docker-compose -f "$COMPOSE_FILE" run --rm app python /app/src/kafka/setup_kafka_topics.py
    
    log_info "Collecting historical data..."
    docker-compose -f "$COMPOSE_FILE" up -d collector
    
    log_info "Monitoring data collection progress..."
    until docker-compose -f "$COMPOSE_FILE" exec app python /app/src/utils/check_progress.py --type collection | grep -q "status: completed"; do
        log_info "Data collection in progress... Checking again in 30 seconds"
        sleep 30
    done
    
    log_info "Data collection completed."
    log_info "You can start the full system with: ./docker-manage.sh start"
}

# Display usage if no arguments provided
if [ $# -eq 0 ]; then
    echo "BitRag Docker Management Script"
    echo "-------------------------------"
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  init                - Initialize the system (infrastructure, schema, data collection)"
    echo "  clean               - Remove all containers, volumes, and networks"
    echo "  start-infra         - Start infrastructure services only"
    echo "  start               - Start all BitRag services"
    echo "  collect-data        - Start data collection process"
    echo "  stop                - Stop all services"
    echo "  logs [service]      - Show logs for a specific service"
    echo "  progress [type]     - Check progress (type: collection, processing, or all)"
    echo "  watch [type] [sec]  - Watch progress updates (type: collection, processing, or all)"
    echo "  status              - Show current Docker container status"
    echo ""
    echo "Examples:"
    echo "  $0 start"
    echo "  $0 progress collection"
    echo "  $0 watch processing 10"
    exit 0
fi

# Process commands
case "$1" in
    init)
        initialize
        ;;
    clean)
        clean_all
        ;;
    start-infra)
        start_infra
        ;;
    start)
        start_all
        ;;
    collect-data)
        collect_data
        ;;
    stop)
        stop_all
        ;;
    logs)
        check_logs "$2"
        ;;
    progress)
        check_progress "$2"
        ;;
    watch)
        watch_progress "$2" "$3"
        ;;
    status)
        show_status
        ;;
    *)
        log_error "Unknown command: $1"
        echo "Run $0 without arguments to see available commands."
        exit 1
        ;;
esac

exit 0