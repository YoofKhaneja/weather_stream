# Weather Data Streaming Platform

Real-time weather data pipeline using Kafka, Go, and PostgreSQL with analytics capabilities.

## What It Does

Streams weather data for 100+ US cities every 30 minutes, stores it in PostgreSQL, and provides analytics to compare temperature changes and weather patterns over time.

## Architecture

Open-Meteo API → Producer (Go) → Kafka → Consumer (Go) → PostgreSQL → Analytics

## Files

- **`producer/main.go`** - Fetches weather data from API, publishes to Kafka topics
- **`consumer/main.go`** - Reads from Kafka, writes to PostgreSQL database
- **`docker-compose.yml`** - Runs Kafka, Zookeeper, PostgreSQL, Kafka UI
- **`init.sql`** - Database schema (current_weather, forecast_comparison tables)
- **`cities.json`** - List of 100+ US cities to track
- **`analytics_queries.sql`** - SQL queries for temperature trends, forecast accuracy, regional stats
- **`analytics_service.go`** - Exports analytics data to JSON/CSV
- **`analytics_visualizations.py`** - Generates charts (Plotly/Matplotlib) and reports

## Quick Start

### Start services
docker-compose up -d

### Run producer (terminal 1)
go run producer/main.go

### Run consumer (terminal 2)
go run consumer/main.go

### After 24+ hours, run analytics
python3 analytics_visualizations.py or go run analytics_service/main.go

## Tech Stack

Go, Apache Kafka, PostgreSQL, Docker, Python (Pandas, Plotly)


