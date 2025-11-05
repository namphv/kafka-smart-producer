#!/usr/bin/env python3
"""
Quick Infrastructure Validation Script

Run this script to validate that the complete test infrastructure is working correctly.
This script provides a quick health check before running the full integration test suite.
"""

import os
import subprocess
import sys
import time


def check_docker_compose() -> bool:
    """Check if Docker Compose services are running."""
    print("🔍 Checking Docker Compose infrastructure...")

    try:
        result = subprocess.run(
            ["docker-compose", "ps", "--services", "--filter", "status=running"],
            cwd=".",
            capture_output=True,
            text=True,
            check=True,
        )

        running_services = result.stdout.strip().split("\n")
        required_services = {"kafka", "zookeeper", "redis"}

        if set(running_services) >= required_services:
            print("✅ All required Docker services are running")
            return True
        else:
            missing = required_services - set(running_services)
            print(f"❌ Missing services: {missing}")
            return False

    except subprocess.CalledProcessError as e:
        print(f"❌ Docker Compose check failed: {e}")
        return False
    except FileNotFoundError:
        print("❌ docker-compose command not found")
        return False


def check_kafka_connectivity() -> bool:
    """Check Kafka cluster connectivity."""
    print("🔍 Checking Kafka connectivity...")

    try:
        from confluent_kafka.admin import AdminClient

        admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
        metadata = admin_client.list_topics(timeout=5)

        print(f"✅ Kafka accessible - {len(metadata.topics)} topics available")
        return True

    except Exception as e:
        print(f"❌ Kafka connectivity failed: {e}")
        return False


def check_redis_connectivity() -> bool:
    """Check Redis connectivity."""
    print("🔍 Checking Redis connectivity...")

    try:
        import redis

        r = redis.Redis(host="localhost", port=6380, decode_responses=True)

        if r.ping():
            print("✅ Redis accessible")
            return True
        else:
            print("❌ Redis ping failed")
            return False

    except ImportError:
        print("⚠️ Redis client not available - skipping cache tests")
        return True  # Not critical for basic tests
    except Exception as e:
        print(f"❌ Redis connectivity failed: {e}")
        return False


def check_python_dependencies() -> bool:
    """Check required Python dependencies."""
    print("🔍 Checking Python dependencies...")

    required_packages = ["confluent_kafka", "pytest"]

    missing_packages = []

    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        print(f"❌ Missing Python packages: {missing_packages}")
        return False
    else:
        print("✅ All required Python packages available")
        return True


def run_quick_test() -> bool:
    """Run a quick infrastructure test."""
    print("🔍 Running quick infrastructure test...")

    try:
        # Change to project root to use uv
        project_root = os.path.dirname(os.path.dirname(os.getcwd()))
        subprocess.run(
            [
                "uv",
                "run",
                "python",
                "-m",
                "pytest",
                "tests/integration/test_infrastructure_setup.py::TestInfrastructureSetup::test_kafka_cluster_connectivity",
                "-v",
                "--tb=short",
            ],
            cwd=project_root,
            capture_output=True,
            text=True,
            check=True,
        )

        print("✅ Quick infrastructure test passed")
        return True

    except subprocess.CalledProcessError as e:
        print("❌ Quick infrastructure test failed")
        print("Error output:", e.stderr)
        return False


def start_infrastructure() -> bool:
    """Start Docker Compose infrastructure if not running."""
    print("🚀 Starting Docker Compose infrastructure...")

    try:
        subprocess.run(
            ["docker-compose", "up", "-d"],
            cwd=".",
            capture_output=True,
            text=True,
            check=True,
        )

        print("✅ Docker Compose infrastructure started")
        print("⏳ Waiting for services to be ready...")
        time.sleep(10)  # Give services time to start
        return True

    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to start infrastructure: {e}")
        print("Error output:", e.stderr)
        return False


def main():
    """Main validation routine."""
    print("=" * 60)
    print("🧪 Kafka Smart Producer - Infrastructure Validation")
    print("=" * 60)

    # Change to integration test directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    checks = [
        ("Python Dependencies", check_python_dependencies),
        ("Docker Compose Status", check_docker_compose),
        ("Kafka Connectivity", check_kafka_connectivity),
        ("Redis Connectivity", check_redis_connectivity),
        ("Quick Test", run_quick_test),
    ]

    all_passed = True

    for check_name, check_func in checks:
        print(f"\n{check_name}:")
        if not check_func():
            all_passed = False

            if check_name == "Docker Compose Status":
                print("🔧 Attempting to start infrastructure...")
                if start_infrastructure():
                    print("✅ Infrastructure started - re-running checks...")
                    # Re-run connectivity checks
                    if not check_kafka_connectivity():
                        all_passed = False
                    if not check_redis_connectivity():
                        all_passed = False
                else:
                    all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 All infrastructure checks passed!")
        print("🚀 Ready to run integration tests:")
        print("   uv run python -m pytest test_infrastructure_setup.py -v")
        sys.exit(0)
    else:
        print("❌ Some infrastructure checks failed!")
        print("🔧 Please fix the issues above before running integration tests")
        sys.exit(1)


if __name__ == "__main__":
    main()
