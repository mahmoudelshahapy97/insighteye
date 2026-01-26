# Testing Guide for InsightEye

## Overview
This document provides instructions for running the comprehensive test suite for the InsightEye project.

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures and configuration
├── pytest.ini               # Pytest configuration
├── unit/                    # Unit tests
│   ├── test_auth_service.py
│   ├── test_session_service.py
│   ├── test_otp_service.py
│   ├── test_camera_service.py
│   ├── test_user_service.py
│   ├── test_workspace_service.py
│   ├── test_analytics_service.py
│   ├── test_shared_stream_service.py
│   └── test_stream_management.py
├── api/                     # API endpoint tests
│   ├── test_auth_router.py
│   ├── test_camera_router.py
│   └── test_stream_router.py
├── integration/             # Integration tests
│   └── test_database_integration.py
├── e2e/                     # End-to-end tests
│   └── test_user_journey.py
└── stress_test_streams.py   # Load tests
```

## Prerequisites

### Install Test Dependencies
```bash
pip install pytest pytest-asyncio pytest-mock pytest-cov httpx
```

### Environment Setup
Create a `.env.test` file or set environment variables:
```bash
export TEST_DATABASE_URL="postgresql://test_user:test_pass@localhost:5432/test_insighteye"
export TEST_REDIS_URL="redis://localhost:6379/1"
export TEST_QDRANT_URL="http://localhost:6333"
export TEST_ELASTICSEARCH_URL="http://localhost:9200"
```

## Running Tests

### Run All Tests
```bash
pytest tests/ -v
```

### Run Specific Test Categories

#### Unit Tests Only
```bash
pytest tests/unit/ -v
```

#### API Tests Only
```bash
pytest tests/api/ -v
```

#### Integration Tests Only
```bash
pytest tests/integration/ -v -m integration
```

#### E2E Tests Only
```bash
pytest tests/e2e/ -v -m e2e
```

### Run Specific Test File
```bash
pytest tests/unit/test_auth_service.py -v
```

### Run Specific Test Function
```bash
pytest tests/unit/test_auth_service.py::test_create_token_pair -v
```

### Run Tests by Marker
```bash
# Run only async tests
pytest -m asyncio -v

# Run only integration tests
pytest -m integration -v

# Run only slow tests
pytest -m slow -v

# Exclude slow tests
pytest -m "not slow" -v
```

## Coverage Reports

### Generate Coverage Report
```bash
pytest tests/ --cov=app --cov-report=html --cov-report=term-missing
```

### View HTML Coverage Report
```bash
# Open htmlcov/index.html in your browser
start htmlcov/index.html  # Windows
open htmlcov/index.html   # macOS
xdg-open htmlcov/index.html  # Linux
```

### Coverage by Module
```bash
pytest tests/ --cov=app --cov-report=term-missing
```

## Advanced Options

### Run Tests in Parallel
```bash
# Install pytest-xdist first
pip install pytest-xdist

# Run with auto-detected CPU cores
pytest tests/ -n auto
```

### Run with Verbose Output
```bash
pytest tests/ -vv
```

### Show Print Statements
```bash
pytest tests/ -s
```

### Stop on First Failure
```bash
pytest tests/ -x
```

### Run Last Failed Tests
```bash
pytest tests/ --lf
```

### Run Failed Tests First
```bash
pytest tests/ --ff
```

### Show Slowest Tests
```bash
pytest tests/ --durations=10
```

## Test Markers

Available markers:
- `@pytest.mark.asyncio` - Async tests
- `@pytest.mark.integration` - Integration tests (require external services)
- `@pytest.mark.e2e` - End-to-end tests
- `@pytest.mark.slow` - Slow-running tests
- `@pytest.mark.unit` - Unit tests

## Continuous Integration

### GitHub Actions
Tests run automatically on push/PR via GitHub Actions workflow.

### Pre-commit Hook
Add to `.git/hooks/pre-commit`:
```bash
#!/bin/bash
pytest tests/unit/ -v
if [ $? -ne 0 ]; then
    echo "Unit tests failed. Commit aborted."
    exit 1
fi
```

## Troubleshooting

### Database Connection Issues
Ensure PostgreSQL is running and test database exists:
```bash
psql -U postgres -c "CREATE DATABASE test_insighteye;"
```

### Import Errors
Ensure you're in the project root and PYTHONPATH is set:
```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

### Async Test Failures
Ensure pytest-asyncio is installed and asyncio_mode is set in pytest.ini

### Mock Issues
Ensure pytest-mock is installed:
```bash
pip install pytest-mock
```

## Best Practices

1. **Isolation**: Each test should be independent
2. **Cleanup**: Use fixtures to setup and teardown
3. **Mocking**: Mock external dependencies in unit tests
4. **Naming**: Use descriptive test names
5. **AAA Pattern**: Arrange, Act, Assert
6. **Coverage**: Aim for 85%+ code coverage
7. **Speed**: Keep unit tests fast (<1s each)

## Current Test Coverage

| Component | Coverage | Target |
|-----------|----------|--------|
| Authentication | ~60% | 90% |
| User Service | ~40% | 85% |
| Camera Service | ~20% | 85% |
| Stream Service | ~50% | 90% |
| API Routers | ~10% | 85% |
| **Overall** | **~35%** | **85%+** |

## Next Steps

1. Run existing tests: `pytest tests/unit/ -v`
2. Review coverage: `pytest tests/ --cov=app --cov-report=html`
3. Implement missing tests based on coverage gaps
4. Achieve 85%+ overall coverage
