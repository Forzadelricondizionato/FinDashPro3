# Test Reports Directory

This directory contains test execution reports:

- `coverage/`: HTML coverage reports
- `mutmut/`: Mutation testing reports
- `junit/`: JUnit XML reports for CI

Run full test suite with reports:
```bash
pytest --cov=fdp --cov-report=html:reports/coverage --junitxml=reports/junit/test-results.xml
