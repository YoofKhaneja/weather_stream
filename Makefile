.PHONY: help start stop logs analytics-go analytics-py analytics-sql clean

help:
	@echo "Weather Analytics Commands:"
	@echo "  make start          - Start all services (Kafka, PostgreSQL)"
	@echo "  make stop           - Stop all services"
	@echo "  make logs           - View service logs"
	@echo "  make analytics-go   - Run Go analytics service"
	@echo "  make analytics-py   - Run Python analytics script"
	@echo "  make analytics-sql  - Run SQL analytics queries"
	@echo "  make clean          - Clean generated files"

start:
	docker-compose up -d
	@echo "✅ Services started. Waiting for health checks..."
	@sleep 10

stop:
	docker-compose down

logs:
	docker-compose logs -f

analytics-go:
	@echo "🔬 Running Go analytics service..."
	go run analytics_service.go

analytics-py:
	@echo "🔬 Running Python analytics script..."
	python3 analytics_visualizations.py

analytics-sql:
	@echo "🔬 Running SQL analytics queries..."
	@echo "Connect to: psql -h localhost -p 5433 -U weather_user -d weather_db"
	@echo "Then run queries from: analytics_queries.sql"

clean:
	rm -rf analytics_output/
	rm -f *.json *.csv *.png *.html
	@echo "✅ Cleaned generated files"
