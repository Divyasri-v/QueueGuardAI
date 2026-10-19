# 🛡️ QueueGuard AI

**AI-Powered Background Job Monitoring & Recovery System**

A complete, production-ready Python application that monitors real background jobs using Redis + RQ (Redis Queue), detects failures, explains them with a local AI-powered analyzer, and automatically retries eligible jobs with exponential backoff.

---

## 📐 Architecture

```
Streamlit Dashboard (app.py)
        │
        ▼
QueueGuard Service (queueguard/job_service.py)
        │
        ├──► Redis + RQ Queue ◄──► RQ Worker (scripts/start_worker.py)
        │         │                          │
        │         ▼                          ▼
        │    Job Status/Registry      Task Execution
        │         │                (queueguard/worker_tasks.py)
        │         ▼
        └──► Job History (Redis Hashes + Sorted Sets)
                  │
                  ▼
        Failure Analyzer (local, no API needed)
                  │
                  ▼
        Recovery Engine → Retry with Exponential Backoff
```

---

## ✨ Features

| Feature | Status |
|---|---|
| Real Redis + RQ job execution | ✅ |
| 4 pre-defined safe job types | ✅ |
| Persisted job history (Redis-backed) | ✅ |
| Local AI failure analysis (no API key) | ✅ |
| Manual + automatic retry | ✅ |
| Exponential backoff | ✅ |
| Dead-letter queue tracking | ✅ |
| Retry audit history | ✅ |
| Real-time Streamlit dashboard | ✅ |
| Plotly analytics charts | ✅ |
| Redis offline warning | ✅ |
| Docker Compose for Redis | ✅ |
| Pytest test suite | ✅ |

---

## 🚀 Quick Start (Windows PowerShell)

### Step 1 — Clone & navigate

```powershell
cd C:\Users\DIVYA\.gemini\antigravity\scratch\QueueGuardAI
```

### Step 2 — Create Python virtual environment

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Step 3 — Configure environment

```powershell
Copy-Item .env.example .env
# Edit .env if needed (default settings work with Docker Redis)
```

### Step 4 — Start Redis with Docker

```powershell
docker-compose up -d
```

Verify Redis is running:
```powershell
docker ps
docker exec queueguard-redis redis-cli ping
# Should print: PONG
```

### Step 5 — Run health check

```powershell
python scripts/check_setup.py
```

### Step 6 — Open 3 separate PowerShell terminals

**Terminal 1 — RQ Worker** (executes background jobs):
```powershell
cd C:\Users\DIVYA\.gemini\antigravity\scratch\QueueGuardAI
.venv\Scripts\Activate.ps1
python scripts/start_worker.py
```

**Terminal 2 — RQ Scheduler** (handles delayed retries with backoff):
```powershell
cd C:\Users\DIVYA\.gemini\antigravity\scratch\QueueGuardAI
.venv\Scripts\Activate.ps1
python scripts/start_scheduler.py
```

**Terminal 3 — Streamlit Dashboard**:
```powershell
cd C:\Users\DIVYA\.gemini\antigravity\scratch\QueueGuardAI
.venv\Scripts\Activate.ps1
streamlit run app.py
```

Open **http://localhost:8501** in your browser.

---

## 🧪 Running Tests

Tests do not require Redis — they use mocks.

```powershell
# Activate venv first
.venv\Scripts\Activate.ps1

# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_failure_analyzer.py -v
pytest tests/test_recovery.py -v
pytest tests/test_job_service.py -v
```

---

## 📂 Project Structure

```
QueueGuardAI/
├── app.py                          # Streamlit dashboard (6 tabs)
├── config.py                       # Environment-based configuration
├── requirements.txt
├── .env.example
├── docker-compose.yml              # Redis service
├── Dockerfile
├── conftest.py                     # Pytest root config
├── queueguard/
│   ├── __init__.py
│   ├── redis_connection.py         # Redis client + health check
│   ├── worker_tasks.py             # 4 pre-defined task functions
│   ├── job_service.py              # Enqueue, status, metrics
│   ├── failure_analyzer.py         # Local failure classification
│   ├── recovery.py                 # Retry + exponential backoff
│   └── history.py                  # Redis-backed persistent history
├── scripts/
│   ├── start_worker.py             # RQ worker process
│   ├── start_scheduler.py          # RQ scheduler (for delayed retries)
│   └── check_setup.py              # Health check utility
└── tests/
    ├── conftest.py
    ├── test_failure_analyzer.py    # 20+ tests for failure classification
    ├── test_recovery.py            # Tests for retry logic & backoff
    └── test_job_service.py         # Tests for enqueue, validation, stats
```

---

## ⚙️ Configuration (.env)

| Variable | Default | Description |
|---|---|---|
| `REDIS_HOST` | `localhost` | Redis server hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_DB` | `0` | Redis database index |
| `REDIS_PASSWORD` | *(empty)* | Redis password (if any) |
| `QUEUE_NAME` | `queueguard` | RQ queue name |
| `MAX_RETRIES` | `3` | Maximum retry attempts per job |
| `RETRY_BACKOFF_BASE` | `2` | Exponential backoff base (seconds) |
| `JOB_TIMEOUT` | `180` | Job execution timeout (seconds) |
| `LOG_LEVEL` | `INFO` | Logging level |

---

## 🔄 Retry & Recovery Logic

### Backoff Formula
```
delay = min(RETRY_BACKOFF_BASE ^ attempt, RETRY_BACKOFF_MAX)
```

| Attempt | Backoff |
|---|---|
| 1st retry | 2s |
| 2nd retry | 4s |
| 3rd retry | 8s |

### Retryable Tasks
Only idempotent tasks are auto-retried:
- `successful_task` ✅
- `slow_task` ✅
- `data_processing_task` ✅
- `failing_task` ❌ (never auto-retried — intentional failures)

### Dead-letter Queue
When a job exceeds `MAX_RETRIES`, it is marked as `dead` and logged. No further retries occur.

### Scheduler Note
Delayed retries (backoff > 0s) require the RQ Scheduler process.
If the scheduler is not running, jobs are enqueued immediately (backoff applied in the UI only).

---

## 🐛 Troubleshooting

### Redis not connecting
```powershell
# Check Docker is running
docker ps

# Restart Redis
docker-compose restart redis

# Test manually
docker exec queueguard-redis redis-cli ping
```

### Worker not picking up jobs
```powershell
# Make sure the worker is running in its own terminal
python scripts/start_worker.py

# Check worker registered
python -c "
from queueguard.redis_connection import get_redis_client
from rq import Worker
r = get_redis_client()
print([w.name for w in Worker.all(connection=r)])
"
```

### rq-scheduler not installed
```powershell
pip install rq-scheduler
```

### Streamlit import errors
```powershell
# Make sure venv is activated and requirements installed
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### PowerShell execution policy
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

---

## 🔒 Security

- Only pre-defined task functions are executable (whitelist enforced).
- No user-supplied code is ever evaluated or executed.
- Redis credentials loaded from environment variables, never hardcoded.
- Stack traces shown only in expandable diagnostic sections.
- API keys (optional LLM integration) loaded from env, never logged.

---

## 📜 License

MIT — for educational and demonstration purposes.
