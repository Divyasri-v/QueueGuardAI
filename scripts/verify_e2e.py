"""Live end-to-end verification script for QueueGuard AI."""
import sys
import json
import time
sys.path.insert(0, ".")

print("=== QueueGuard AI — Live E2E Verification ===")
print()


def wait_for_terminal_status(job, timeout=15, poll=0.3):
    """Poll job status until it reaches a terminal state or timeout."""
    terminal = {"finished", "failed", "stopped", "cancelled"}
    deadline = time.time() + timeout
    while time.time() < deadline:
        job.refresh()
        status = str(job.get_status()).lower()
        for t in terminal:
            if t in status:
                return status
        time.sleep(poll)
    return str(job.get_status()).lower()


# 1. Redis health
from queueguard.redis_connection import check_redis_health, get_rq_redis_client, get_redis_client
health = check_redis_health()
print(f"[1] Redis Connected : {health['connected']}")
if not health["connected"]:
    print("    SKIP: Redis offline -", health.get("error"))
    sys.exit(1)
print(f"    Host/Port       : {health['host']}:{health['port']}")
print(f"    Latency         : {health.get('latency_ms')}ms")

# 2. get_queue_stats
from queueguard.job_service import get_queue_stats
stats = get_queue_stats()
print()
print(f"[2] Queue Stats     : {json.dumps(stats)}")
assert "total" in stats, "stats must contain total"
assert "error" not in stats, f"stats returned error: {stats}"
print("    OK")

# 3. Successful task -> FINISHED
from rq import Queue, SimpleWorker
from rq.job import Job
from queueguard.worker_tasks import successful_task
from config import QUEUE_NAME

r = get_rq_redis_client()
q = Queue(QUEUE_NAME, connection=r)

print()
print("[3] successful_task (duration=0.3s)")
job1 = q.enqueue(successful_task, kwargs={"duration": 0.3, "payload": "e2e-verify"})
print(f"    Enqueued id     : {job1.id}")
w = SimpleWorker([q], connection=r)
w.work(burst=True)
status1 = wait_for_terminal_status(job1)
print(f"    Final status    : {status1}")
j1 = job1
print(f"    Result          : {j1.result}")
assert "finished" in status1, f"Expected finished, got {status1}"
print("    PASSED")

# 4. Failing task -> FAILED
from queueguard.worker_tasks import failing_task
print()
print("[4] failing_task (error_type=runtime)")
job2 = q.enqueue(failing_task, kwargs={"error_type": "runtime"})
print(f"    Enqueued id     : {job2.id}")
w2 = SimpleWorker([q], connection=r)
w2.work(burst=True)
status2 = wait_for_terminal_status(job2)
print(f"    Final status    : {status2}")
assert "failed" in status2, f"Expected failed, got {status2}"
print("    PASSED")

# 5. Worker.all() uses raw bytes client (System Health bug fix verification)
from rq import Worker
print()
print("[5] Worker.all() with raw bytes client (System Health bug fix)")
try:
    workers = Worker.all(connection=r)
    print(f"    Workers found   : {len(workers)}")
    print("    No AttributeError raised — PASSED")
except AttributeError as exc:
    print(f"    FAILED: {exc}")
    sys.exit(1)

# 6. History persistence
from queueguard.history import list_all_jobs
r_decoded = get_redis_client()
all_jobs = list_all_jobs(r_decoded)
print()
print(f"[6] Persisted job history: {len(all_jobs)} records")
assert len(all_jobs) >= 2, f"Expected >=2 jobs, got {len(all_jobs)}"
print("    PASSED")

# 7. get_queue_stats reflects updated data
stats2 = get_queue_stats()
print()
print("[7] Updated queue stats after job execution:")
print(f"    total={stats2.get('total',0)}  finished={stats2.get('finished',0)}  "
      f"failed={stats2.get('failed',0)}")
assert stats2.get("total", 0) >= 2, f"total should be >=2 after 2 jobs, got {stats2}"
print("    PASSED")

# 8. Theme token dicts verify correctly (import via mock)
print()
print("[8] Theme token dicts accessible (import with Streamlit mocked)")
import types
import unittest.mock as mock


class _FakeSS(dict):
    def __getattr__(self, k):
        return self[k]
    def __setattr__(self, k, v):
        self[k] = v


fake_st = types.SimpleNamespace(
    set_page_config=lambda **_: None,
    markdown=lambda *a, **kw: None,
    session_state=_FakeSS({"theme": "dark"}),
)
with mock.patch.dict(sys.modules, {
    "streamlit":          fake_st,
    "pandas":             mock.MagicMock(),
    "plotly":             mock.MagicMock(),
    "plotly.express":     mock.MagicMock(),
    "plotly.graph_objects": mock.MagicMock(),
}):
    if "app" in sys.modules:
        del sys.modules["app"]
    import app as _app

    assert "text_strong" in _app.DARK,  "DARK missing text_strong"
    assert "text_strong" in _app.LIGHT, "LIGHT missing text_strong"
    assert _app.DARK["bg_solid"]  == "#0f0f1a", f"Wrong DARK bg_solid: {_app.DARK['bg_solid']}"
    assert _app.LIGHT["bg_solid"] == "#f1f5f9", f"Wrong LIGHT bg_solid: {_app.LIGHT['bg_solid']}"

    # Verify chart_layout produces theme-differing output
    fake_st.session_state["theme"] = "dark"
    dark_layout = _app.chart_layout()
    fake_st.session_state["theme"] = "light"
    light_layout = _app.chart_layout()
    assert dark_layout["font"]["color"] != light_layout["font"]["color"], \
        "chart_layout must produce different font colors per theme"

    # Verify highlight_status for all statuses, both themes
    for status in ["queued", "started", "finished", "failed", "retrying", "dead"]:
        fake_st.session_state["theme"] = "dark"
        css = _app.highlight_status(status)
        assert "background-color" in css, f"Dark highlight missing bg for {status}"
        fake_st.session_state["theme"] = "light"
        css = _app.highlight_status(status)
        assert "background-color" in css, f"Light highlight missing bg for {status}"

print(f"    DARK['bg_solid']  = {_app.DARK['bg_solid']}")
print(f"    LIGHT['bg_solid'] = {_app.LIGHT['bg_solid']}")
print(f"    chart_layout dark font  = {dark_layout['font']['color']}")
print(f"    chart_layout light font = {light_layout['font']['color']}")
print("    PASSED")

print()
print("=== ALL LIVE E2E CHECKS PASSED ===")
