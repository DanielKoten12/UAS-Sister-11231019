from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from aggregator.app.database import Database
from aggregator.app.main import create_app


def sample_event(event_id: str = "event-1", topic: str = "app") -> dict[str, object]:
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "pytest",
        "payload": {"level": "INFO", "message": "hello", "counter": 1},
    }


def client_for(tmp_path, worker_count: int = 4) -> TestClient:
    app = create_app(str(tmp_path / "aggregator.db"), worker_count=worker_count)
    return TestClient(app)


def drain(client: TestClient) -> None:
    response = client.post("/admin/drain", params={"timeout": 10})
    assert response.status_code == 200
    assert response.json()["drained"] is True


def test_health_endpoint(tmp_path):
    with client_for(tmp_path) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


def test_publish_single_event_is_accepted(tmp_path):
    with client_for(tmp_path) as client:
        response = client.post("/publish", json=sample_event())
        assert response.status_code == 200
        assert response.json()["accepted"] == 1
        drain(client)
        assert client.get("/stats").json()["unique_processed"] == 1


def test_publish_batch_is_accepted(tmp_path):
    batch = [sample_event(f"event-{index}") for index in range(5)]
    with client_for(tmp_path) as client:
        response = client.post("/publish", json=batch)
        assert response.status_code == 200
        assert response.json()["accepted"] == 5
        drain(client)
        assert client.get("/stats").json()["unique_processed"] == 5


def test_empty_batch_is_rejected(tmp_path):
    with client_for(tmp_path) as client:
        response = client.post("/publish", json=[])
        assert response.status_code == 400
        assert client.get("/stats").json()["received"] == 0


def test_missing_event_id_is_rejected_without_incrementing_received(tmp_path):
    event = sample_event()
    event.pop("event_id")
    with client_for(tmp_path) as client:
        response = client.post("/publish", json=event)
        assert response.status_code == 422
        assert client.get("/stats").json()["received"] == 0


def test_payload_must_be_object(tmp_path):
    event = sample_event()
    event["payload"] = "not-an-object"
    with client_for(tmp_path) as client:
        response = client.post("/publish", json=event)
        assert response.status_code == 422


def test_duplicate_event_is_processed_once(tmp_path):
    event = sample_event("same-id")
    with client_for(tmp_path) as client:
        for _ in range(3):
            assert client.post("/publish", json=event).status_code == 200
        drain(client)
        stats = client.get("/stats").json()
        assert stats["received"] == 3
        assert stats["unique_processed"] == 1
        assert stats["duplicate_dropped"] == 2
        assert len(client.get("/events").json()) == 1


def test_same_event_id_on_different_topics_is_unique_per_topic(tmp_path):
    with client_for(tmp_path) as client:
        assert client.post("/publish", json=sample_event("shared", "app")).status_code == 200
        assert client.post("/publish", json=sample_event("shared", "auth")).status_code == 200
        drain(client)
        stats = client.get("/stats").json()
        assert stats["unique_processed"] == 2
        assert stats["duplicate_dropped"] == 0


def test_events_can_be_filtered_by_topic(tmp_path):
    with client_for(tmp_path) as client:
        client.post("/publish", json=[sample_event("a", "app"), sample_event("b", "auth")])
        drain(client)
        response = client.get("/events", params={"topic": "auth"})
        assert response.status_code == 200
        events = response.json()
        assert len(events) == 1
        assert events[0]["topic"] == "auth"


def test_stats_contains_topic_counts_and_worker_count(tmp_path):
    with client_for(tmp_path, worker_count=3) as client:
        client.post("/publish", json=[sample_event("a", "app"), sample_event("b", "app")])
        drain(client)
        stats = client.get("/stats").json()
        assert stats["topics"] == {"app": 2}
        assert stats["worker_count"] == 3
        assert stats["queue_size"] == 0
        assert "uptime" in stats


def test_database_dedup_is_safe_under_concurrent_workers(tmp_path):
    db = Database(str(tmp_path / "race.db"))
    db.init()
    event = sample_event("race-id")

    def process(index: int) -> bool:
        return db.process_event(event, f"worker-{index}")

    with ThreadPoolExecutor(max_workers=16) as executor:
        results = list(executor.map(process, range(40)))

    stats = db.get_stats()
    assert results.count(True) == 1
    assert stats["unique_processed"] == 1
    assert stats["duplicate_dropped"] == 39


def test_persistence_blocks_reprocessing_after_restart(tmp_path):
    db_path = tmp_path / "persistent.db"
    event = sample_event("persisted")

    with TestClient(create_app(str(db_path), worker_count=2)) as client:
        client.post("/publish", json=event)
        drain(client)
        assert client.get("/stats").json()["unique_processed"] == 1

    with TestClient(create_app(str(db_path), worker_count=2)) as client:
        client.post("/publish", json=event)
        drain(client)
        stats = client.get("/stats").json()
        assert stats["received"] == 2
        assert stats["unique_processed"] == 1
        assert stats["duplicate_dropped"] == 1


def test_batch_with_invalid_item_is_rejected_atomically_by_validation(tmp_path):
    valid = sample_event("valid")
    invalid = sample_event("invalid")
    invalid["timestamp"] = "not-a-date"
    with client_for(tmp_path) as client:
        response = client.post("/publish", json=[valid, invalid])
        assert response.status_code == 422
        stats = client.get("/stats").json()
        assert stats["received"] == 0
        assert stats["unique_processed"] == 0


def test_small_stress_run_keeps_consistent_counts(tmp_path):
    unique = [sample_event(f"stress-{index}", "stress") for index in range(70)]
    duplicates = unique[:30]
    with client_for(tmp_path, worker_count=6) as client:
        response = client.post("/publish", json=unique + duplicates)
        assert response.status_code == 200
        drain(client)
        stats = client.get("/stats").json()
        assert stats["received"] == 100
        assert stats["unique_processed"] == 70
        assert stats["duplicate_dropped"] == 30


def test_event_limit_is_respected(tmp_path):
    batch = [sample_event(f"limit-{index}") for index in range(4)]
    with client_for(tmp_path) as client:
        client.post("/publish", json=batch)
        drain(client)
        response = client.get("/events", params={"limit": 2})
        assert response.status_code == 200
        assert len(response.json()) == 2


def test_audit_log_records_processed_and_duplicate_attempts(tmp_path):
    db_path = tmp_path / "audit.db"
    with TestClient(create_app(str(db_path), worker_count=2)) as client:
        event = sample_event("audit")
        client.post("/publish", json=[event, event])
        drain(client)

    db = Database(str(db_path))
    assert db.count_audit_rows() == 2
