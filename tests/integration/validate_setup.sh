#!/bin/bash

# Infrastructure Validation Script for Kafka Smart Producer
# This script validates that the complete test infrastructure is working correctly.

set -e

echo "============================================================"
echo "🧪 Kafka Smart Producer - Infrastructure Validation"
echo "============================================================"

# Change to integration test directory
cd "$(dirname "$0")"

echo ""
echo "🔍 Checking Docker Compose infrastructure..."

# Check if required services are running
RUNNING_SERVICES=$(docker-compose ps --services --filter status=running 2>/dev/null || echo "")

if echo "$RUNNING_SERVICES" | grep -q "kafka" && echo "$RUNNING_SERVICES" | grep -q "zookeeper"; then
    echo "✅ Docker Compose services are running"
else
    echo "🚀 Starting Docker Compose infrastructure..."
    docker-compose up -d
    echo "⏳ Waiting for services to be ready..."
    sleep 10
fi

echo ""
echo "🔍 Checking service health..."

# Check Kafka health
if docker-compose ps kafka | grep -q "healthy"; then
    echo "✅ Kafka is healthy"
else
    echo "⚠️ Kafka health check not yet complete - this is normal on first startup"
fi

# Check Redis health
if docker-compose ps redis | grep -q "healthy"; then
    echo "✅ Redis is healthy"
else
    echo "⚠️ Redis health check not yet complete - this is normal on first startup"
fi

echo ""
echo "🔍 Testing Kafka connectivity..."

# Change to project root for uv commands
cd "../.."

# Run a simple connectivity test
if uv run python -c "
from confluent_kafka.admin import AdminClient
import sys
try:
    admin = AdminClient({'bootstrap.servers': 'localhost:9092'})
    metadata = admin.list_topics(timeout=10)
    print(f'✅ Kafka accessible - {len(metadata.topics)} topics available')
except Exception as e:
    print(f'❌ Kafka connectivity failed: {e}')
    sys.exit(1)
"; then
    echo "✅ Kafka connectivity test passed"
else
    echo "❌ Kafka connectivity test failed"
    exit 1
fi

echo ""
echo "🔍 Testing Redis connectivity..."

if uv run python -c "
import redis
import sys
try:
    r = redis.Redis(host='localhost', port=6380, decode_responses=True)
    if r.ping():
        print('✅ Redis accessible')
    else:
        print('❌ Redis ping failed')
        sys.exit(1)
except ImportError:
    print('⚠️ Redis client not available - cache tests will be skipped')
except Exception as e:
    print(f'❌ Redis connectivity failed: {e}')
    sys.exit(1)
"; then
    echo "✅ Redis connectivity test passed"
else
    echo "❌ Redis connectivity test failed"
    exit 1
fi

echo ""
echo "🔍 Running infrastructure validation tests..."

if uv run python -m pytest tests/integration/test_infrastructure_setup.py::TestInfrastructureSetup::test_kafka_cluster_connectivity -v --tb=short -q; then
    echo "✅ Infrastructure validation test passed"
else
    echo "❌ Infrastructure validation test failed"
    exit 1
fi

echo ""
echo "============================================================"
echo "🎉 All infrastructure checks passed!"
echo "🚀 Infrastructure is ready for integration testing"
echo ""
echo "Available commands:"
echo "  • Run all infrastructure tests:"
echo "    uv run python -m pytest tests/integration/test_infrastructure_setup.py -v"
echo ""
echo "  • Run specific scenario tests:"
echo "    uv run python -m pytest tests/integration/test_scenario_*.py -v"
echo ""
echo "  • Stop infrastructure:"
echo "    cd tests/integration && docker-compose down"
echo "============================================================"
