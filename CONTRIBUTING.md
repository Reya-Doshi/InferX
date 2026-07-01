# Contributing to InferX

Thank you for your interest in contributing to InferX! As a cloud-native infrastructure project, we maintain strict code quality, testing, and documentation standards.

---

## 1. Development Setup

### Requirements
*   Python 3.13+
*   NVIDIA GPU Driver (optional, mocks are available for local tests)
*   Docker & Docker Compose

### Clone and Environment Setup
```bash
git clone https://github.com/inferx-ai/inferx.git
cd inferx
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## 2. Code Quality & Format Standards

We enforce styling and typing checks before code ingestion:
*   **Formatting:** We use `black` for formatting.
*   **Linting:** We use `ruff` to identify syntax patterns.
*   **Static Type Checking:** We use `mypy` for static type verification.

Verify quality locally using pre-commit hooks:
```bash
pip install pre-commit
pre-commit install
```

---

## 3. Running Test Suites

Every code change must pass the test discovery suites. We require new features to include unit tests and validation checks.

### Run All Unit Tests
```bash
python -m unittest discover tests/
```

### Run Performance Benchmarks
```bash
python -m tests.benchmark_performance
```

---

## 4. Pull Request Guidelines

1.  **Branch Naming:** Use descriptive branch prefixes: `feature/`, `bugfix/`, or `docs/`.
2.  **Linting & Typing:** Ensure `pre-commit` runs cleanly before pushing.
3.  **Atomic Commits:** Keep commits focused.
4.  **SLA Verification:** Changes to scheduling or runtime core must run performance benchmarks, verifying that tail latencies satisfy SLA thresholds.
