# Switchyard Monorepo Automation
.PHONY: up down proto test-go test-py lint help

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

up: ## Start local services via Docker Compose
	docker compose up -d

down: ## Stop local services via Docker Compose
	docker compose down

proto: ## Generate protobuf definitions (placeholder)
	@echo "Generating protobuf files..."
	@# TODO: add buf generate or protoc compilation commands

test-go: ## Run Go tests with race detection in services/broker
	@echo "Running Go tests..."
	cd services/broker && go test -race ./...

test-py: ## Run Python tests in services/worker and services/mcp-servers
	@echo "Running Python tests..."
	pytest services/worker services/mcp-servers

lint: ## Run linters across Go and Python codebases
	@echo "Linting Go..."
	cd services/broker && golangci-lint run ./... || true
	@echo "Linting Python..."
	ruff check services/worker services/mcp-servers
