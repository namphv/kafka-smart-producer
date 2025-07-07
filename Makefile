# Makefile for kafka-smart-producer
# Provides common development commands

.PHONY: help install test lint format type-check security clean docs build upload pre-commit dev-setup

# Default target
help: ## Show this help message
	@echo "kafka-smart-producer development commands:"
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

# Development setup
dev-setup: ## Set up development environment
	uv sync --dev
	uv run pre-commit install
	uv run pre-commit install --hook-type commit-msg
	@echo "Development environment ready!"

install: ## Install package in development mode
	uv sync

# Testing
test: ## Run tests
	uv run pytest tests/ -v

test-cov: ## Run tests with coverage
	uv run pytest tests/ -v --cov=src/kafka_smart_producer --cov-report=html --cov-report=term

test-fast: ## Run tests without slow tests
	uv run pytest tests/ -v -m "not slow"

# Code quality
lint: ## Run linting
	uv run ruff check src/ tests/
	uv run pydocstyle src/

lint-fix: ## Run linting with auto-fix
	uv run ruff check --fix src/ tests/
	uv run autoflake --in-place --recursive src/ tests/

format: ## Format code
	uv run black src/ tests/
	uv run isort src/ tests/

type-check: ## Run type checking
	uv run mypy src/kafka_smart_producer/

security: ## Run security checks
	uv run bandit -r src/ -f json
	uv run safety check

# Quality gate (run all checks)
check: format lint type-check security test ## Run all quality checks

# Pre-commit hooks
pre-commit: ## Run pre-commit hooks on all files
	uv run pre-commit run --all-files

pre-commit-update: ## Update pre-commit hooks
	uv run pre-commit autoupdate

# Documentation
docs: ## Generate documentation (placeholder)
	@echo "Documentation generation not implemented yet"

# Build and distribution
clean: ## Clean build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf src/*.egg-info/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean ## Build package
	uv build

upload-test: build ## Upload to Test PyPI
	uv publish --repository testpypi

upload: build ## Upload to PyPI
	uv publish

# Development utilities
deps-update: ## Update dependencies
	uv lock --upgrade

deps-show: ## Show dependency tree
	uv tree

version-bump-patch: ## Bump patch version
	@echo "Manual version bump required in pyproject.toml"

version-bump-minor: ## Bump minor version
	@echo "Manual version bump required in pyproject.toml"

version-bump-major: ## Bump major version
	@echo "Manual version bump required in pyproject.toml"

# Task management (following .tasks/ structure)
task-list: ## List available implementation tasks
	@echo "Available implementation tasks:"
	@ls -1 .tasks/Task-*.md | sed 's/.*Task-/  Task-/' | sed 's/\.md//'

task-show: ## Show specific task (usage: make task-show TASK=01)
	@if [ -z "$(TASK)" ]; then echo "Usage: make task-show TASK=01"; exit 1; fi
	@cat .tasks/Task-$(TASK)-*.md 2>/dev/null || echo "Task $(TASK) not found"

# CI/CD helpers
ci-setup: ## Set up CI environment
	uv sync --dev

ci-test: ## Run CI tests
	uv run pytest tests/ -v --cov=src/kafka_smart_producer --cov-report=xml

ci-check: ## Run CI quality checks
	uv run ruff check src/ tests/
	uv run mypy src/kafka_smart_producer/
	uv run bandit -r src/
