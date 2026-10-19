"""
QueueGuard AI — Health Check Script
Verifies Redis connectivity, required packages, and queue state.

Usage:
    python scripts/check_setup.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def check_packages():
    required = [
        "streamlit", "redis", "rq", "pandas",
        "plotly", "dotenv", "pytest",
    ]
    print("\n📦 Checking required packages...")
    all_ok = True
    for pkg in required:
        try:
            import importlib
            importlib.import_module(pkg)
            print(f"  ✅ {pkg}")
        except ImportError:
            print(f"  ❌ {pkg} — NOT installed")
            all_ok = False
    return all_ok


def check_redis():
    print("\n🔌 Checking Redis connection...")
    try:
        from queueguard.redis_connection import check_redis_health
        health = check_redis_health()
        if health["connected"]:
            print(f"  ✅ Redis connected at {health['host']}:{health['port']}")
            print(f"     Latency: {health['latency_ms']}ms")
            return True
        else:
            print(f"  ❌ Redis NOT connected: {health.get('error')}")
            print("     → Start Redis with: docker-compose up -d")
            return False
    except Exception as e:
        print(f"  ❌ Error checking Redis: {e}")
        return False


def check_queue():
    print("\n📋 Checking queue state...")
    try:
        from queueguard.redis_connection import get_redis_client
        from rq import Queue
        from rq.registry import StartedJobRegistry, FailedJobRegistry, FinishedJobRegistry
        from config import QUEUE_NAME

        r = get_redis_client()
        q = Queue(QUEUE_NAME, connection=r)

        queued = q.count
        started = StartedJobRegistry(QUEUE_NAME, connection=r).count
        finished = FinishedJobRegistry(QUEUE_NAME, connection=r).count
        failed = FailedJobRegistry(QUEUE_NAME, connection=r).count

        print(f"  Queue: {QUEUE_NAME!r}")
        print(f"  Queued:   {queued}")
        print(f"  Started:  {started}")
        print(f"  Finished: {finished}")
        print(f"  Failed:   {failed}")
        return True
    except Exception as e:
        print(f"  ❌ Could not read queue: {e}")
        return False


def check_workers():
    print("\n👷 Checking active workers...")
    try:
        from queueguard.redis_connection import get_redis_client
        from rq import Worker
        from config import QUEUE_NAME

        r = get_redis_client()
        workers = Worker.all(connection=r)
        if workers:
            for w in workers:
                print(f"  ✅ Worker: {w.name} | State: {w.state} | Queues: {[q.name for q in w.queues]}")
        else:
            print("  ⚠️  No active workers found.")
            print("     → Start a worker with: python scripts/start_worker.py")
        return True
    except Exception as e:
        print(f"  ❌ Could not check workers: {e}")
        return False


def main():
    print("=" * 55)
    print("  QueueGuard AI — Setup Health Check")
    print("=" * 55)

    pkg_ok = check_packages()
    redis_ok = check_redis()

    if redis_ok:
        check_queue()
        check_workers()

    print("\n" + "=" * 55)
    if pkg_ok and redis_ok:
        print("  ✅ All checks passed! Ready to run QueueGuard AI.")
        print("\n  Start commands:")
        print("    Terminal 1 (Worker):    python scripts/start_worker.py")
        print("    Terminal 2 (Scheduler): python scripts/start_scheduler.py")
        print("    Terminal 3 (Dashboard): streamlit run app.py")
    else:
        print("  ❌ Some checks failed. Fix the issues above before starting.")
    print("=" * 55)


if __name__ == "__main__":
    main()
