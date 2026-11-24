import time

import pytest
import redis as sync_redis
from fastapi.testclient import TestClient

from app.main import create_app

A = {"X-API-Key": "key-alpha"}
B = {"X-API-Key": "key-beta"}


@pytest.fixture
def client(settings):
    r = sync_redis.Redis.from_url(settings.redis_url)
    r.flushdb()
    settings.embedded_worker = True  # exercise the free-tier deployment mode end to end
    with TestClient(create_app(settings)) as c:
        yield c
    r.flushdb()
    r.close()


def _wait_status(client, job_id, headers, want, timeout=5):
    end = time.time() + timeout
    while time.time() < end:
        body = client.get(f"/v1/jobs/{job_id}", headers=headers).json()
        if body["status"] in want:
            return body
        time.sleep(0.05)
    raise AssertionError(body)


def test_health_and_ready(client):
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").json() == {"status": "ready"}


def test_requires_valid_api_key(client):
    assert client.post("/v1/jobs", json={"type": "echo"}).status_code == 401
    assert client.post("/v1/jobs", json={"type": "echo"}, headers={"X-API-Key": "nope"}).status_code == 401
    assert client.get("/v1/queue/stats").status_code == 401


def test_enqueue_and_complete_end_to_end(client):
    r = client.post("/v1/jobs", json={"type": "echo", "payload": {"hello": "world"}}, headers=A)
    assert r.status_code == 202
    body = r.json()
    assert r.headers["Location"] == f"/v1/jobs/{body['id']}"
    job = _wait_status(client, body["id"], A, {"succeeded"})
    assert job["result"] == {"hello": "world"} and job["attempts"] == 1


def test_idempotency_key_replays_original(client):
    h = {**A, "Idempotency-Key": "order-42"}
    first = client.post("/v1/jobs", json={"type": "echo", "payload": {"n": 1}}, headers=h)
    second = client.post("/v1/jobs", json={"type": "echo", "payload": {"n": 1}}, headers=h)
    assert first.status_code == 202 and second.status_code == 200
    assert first.json()["id"] == second.json()["id"] and second.json()["duplicate"] is True


def test_tenants_cannot_see_each_others_jobs(client):
    jid = client.post("/v1/jobs", json={"type": "echo"}, headers=A).json()["id"]
    assert client.get(f"/v1/jobs/{jid}", headers=A).status_code == 200
    assert client.get(f"/v1/jobs/{jid}", headers=B).status_code == 404


def test_validation_errors(client):
    assert client.post("/v1/jobs", json={"type": "nope-handler"}, headers=A).status_code == 422
    assert client.post("/v1/jobs", json={"type": "Bad Type!"}, headers=A).status_code == 422
    assert client.post("/v1/jobs", json={"type": "echo", "max_attempts": 99}, headers=A).status_code == 422
    assert (
        client.post("/v1/jobs", json={"type": "echo"}, headers={**A, "Idempotency-Key": "bad key!"}).status_code == 422
    )
    assert client.get("/v1/jobs/not-a-valid-id", headers=A).status_code == 422
    big = {"type": "echo", "payload": {"blob": "x" * 70_000}}
    assert client.post("/v1/jobs", json=big, headers=A).status_code == 413


def test_failed_job_can_be_retried_from_dlq(client):
    jid = client.post("/v1/jobs", json={"type": "fail", "max_attempts": 1}, headers=A).json()["id"]
    _wait_status(client, jid, A, {"dead"})
    assert client.post(f"/v1/jobs/{jid}/retry", headers=A).status_code == 202
    assert client.post(f"/v1/jobs/{jid}/retry", headers=B).status_code == 404
    _wait_status(client, jid, A, {"dead"})  # fails again, back in the DLQ
    ok = client.post("/v1/jobs", json={"type": "echo"}, headers=A).json()["id"]
    _wait_status(client, ok, A, {"succeeded"})
    assert client.post(f"/v1/jobs/{ok}/retry", headers=A).status_code == 409  # not dead


def test_rate_limit_returns_429_with_headers(settings):
    settings.rate_limit_capacity = 3
    settings.rate_limit_refill_per_sec = 0.5
    r = sync_redis.Redis.from_url(settings.redis_url)
    r.flushdb()
    with TestClient(create_app(settings)) as c:
        codes = [c.post("/v1/jobs", json={"type": "echo"}, headers=A) for _ in range(5)]
        assert [x.status_code for x in codes] == [202, 202, 202, 429, 429]
        assert codes[0].headers["RateLimit-Remaining"] == "2"
        assert int(codes[3].headers["Retry-After"]) >= 1
        # another API key has its own bucket
        assert c.post("/v1/jobs", json={"type": "echo"}, headers=B).status_code == 202
    r.flushdb()
    r.close()


def test_metrics_exposes_queue_and_http_stats(client):
    jid = client.post("/v1/jobs", json={"type": "echo"}, headers=A).json()["id"]
    _wait_status(client, jid, A, {"succeeded"})
    text = client.get("/metrics").text
    assert 'taskforge_queue_depth{queue="ready"}' in text
    assert 'taskforge_jobs_total{event="succeeded"} 1.0' in text
    assert "taskforge_http_requests_total" in text
    assert 'route="/v1/jobs"' in text


def test_metrics_publish_all_counters_from_zero(client):
    """Counters must exist at 0 before any event, or Prometheus misses the first one."""
    text = client.get("/metrics").text
    for event in ("enqueued", "succeeded", "retried", "dead", "reaped"):
        assert f'taskforge_jobs_total{{event="{event}"}} 0.0' in text


def test_metrics_token_protects_endpoint(settings):
    settings.metrics_token = "s3cret"
    with TestClient(create_app(settings)) as c:
        assert c.get("/metrics").status_code == 401
        assert c.get("/metrics", headers={"Authorization": "Bearer wrong"}).status_code == 401
        assert c.get("/metrics", headers={"Authorization": "Bearer s3cret"}).status_code == 200


def test_redis_outage_returns_503(settings):
    settings.redis_url = "redis://localhost:1/0"  # nothing listens here
    with TestClient(create_app(settings)) as c:
        assert c.get("/healthz").status_code == 200  # liveness does not depend on Redis
        assert c.get("/readyz").status_code == 503
        assert c.post("/v1/jobs", json={"type": "echo"}, headers=A).status_code == 503
