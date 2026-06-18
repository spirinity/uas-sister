from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from aggregator_app.main import app, get_broker, get_repository
from aggregator_app.models import EventIn, PublishResult, Stats


class FakeRepository:
    def __init__(self):
        self.events = {}
        self.received = 0
        self.unique_processed = 0
        self.duplicate_dropped = 0

    async def publish_many(self, events: list[EventIn]) -> PublishResult:
        unique = 0
        duplicates = 0
        self.received += len(events)

        for event in events:
            key = (event.topic, event.event_id)
            if key in self.events:
                duplicates += 1
                continue
            self.events[key] = event
            unique += 1

        self.unique_processed += unique
        self.duplicate_dropped += duplicates
        return PublishResult(
            received=len(events),
            unique_processed=unique,
            duplicate_dropped=duplicates,
        )

    async def list_events(self, topic: str | None = None, limit: int = 100):
        items = [
            event
            for (event_topic, _), event in self.events.items()
            if topic is None or event_topic == topic
        ]
        return [
            {
                **event.model_dump(),
                "processed_at": datetime.now(tz=UTC),
            }
            for event in items[:limit]
        ]

    async def get_stats(self) -> Stats:
        topics = sorted({topic for topic, _ in self.events})
        return Stats(
            received=self.received,
            unique_processed=self.unique_processed,
            duplicate_dropped=self.duplicate_dropped,
            topics=topics,
            uptime_seconds=1.0,
        )


class FakeBroker:
    def __init__(self, repository: FakeRepository):
        self.repository = repository

    async def publish_and_wait(self, events: list[EventIn]) -> PublishResult:
        return await self.repository.publish_many(events)


@pytest.fixture
def fake_repository():
    repository = FakeRepository()
    app.dependency_overrides[get_repository] = lambda: repository
    app.dependency_overrides[get_broker] = lambda: FakeBroker(repository)
    yield repository
    app.dependency_overrides.clear()


@pytest.fixture
def client(fake_repository):
    test_client = TestClient(app)
    yield test_client
    test_client.close()


def event(event_id: str = "evt-1", topic: str = "auth") -> dict:
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": "2026-06-16T10:00:00Z",
        "source": "test",
        "payload": {"ok": True},
    }


def test_health_is_not_available_without_lifespan_pool():
    assert app.title == "UAS Pub-Sub Log Aggregator"


def test_publish_single_event(client):
    response = client.post("/publish", json=event())
    assert response.status_code == 200
    assert response.json() == {
        "received": 1,
        "unique_processed": 1,
        "duplicate_dropped": 0,
    }


def test_publish_batch_events(client):
    response = client.post(
        "/publish",
        json=[event("evt-1"), event("evt-2")],
    )
    assert response.status_code == 200
    assert response.json()["unique_processed"] == 2


def test_duplicate_event_is_dropped(client):
    client.post("/publish", json=event())
    response = client.post("/publish", json=event())
    assert response.json()["unique_processed"] == 0
    assert response.json()["duplicate_dropped"] == 1


def test_duplicate_inside_batch_is_dropped(client):
    response = client.post(
        "/publish",
        json=[event("same"), event("same")],
    )
    assert response.json()["received"] == 2
    assert response.json()["unique_processed"] == 1
    assert response.json()["duplicate_dropped"] == 1


def test_invalid_event_missing_topic(client):
    invalid = event()
    del invalid["topic"]
    response = client.post("/publish", json=invalid)
    assert response.status_code == 422


def test_invalid_event_rejects_extra_field(client):
    invalid = event()
    invalid["unexpected"] = True
    response = client.post("/publish", json=invalid)
    assert response.status_code == 422


def test_get_events_returns_processed_events(client):
    client.post("/publish", json=event("evt-1", "auth"))
    response = client.get("/events")
    assert response.status_code == 200
    assert response.json()[0]["event_id"] == "evt-1"


def test_get_events_filters_by_topic(client):
    client.post("/publish", json=event("evt-1", "auth"))
    client.post("/publish", json=event("evt-2", "payment"))
    response = client.get("/events?topic=payment")
    assert [item["topic"] for item in response.json()] == ["payment"]


def test_get_events_limit_validation(client):
    response = client.get("/events?limit=0")
    assert response.status_code == 422


def test_get_stats_counts_received_unique_and_duplicates(client):
    client.post("/publish", json=[event("a"), event("b"), event("a")])
    response = client.get("/stats")
    assert response.status_code == 200
    assert response.json()["received"] == 3
    assert response.json()["unique_processed"] == 2
    assert response.json()["duplicate_dropped"] == 1


def test_get_stats_lists_topics(client):
    client.post("/publish", json=event("a", "auth"))
    client.post("/publish", json=event("b", "payment"))
    response = client.get("/stats")
    assert response.json()["topics"] == ["auth", "payment"]


def test_demo_publish_single(client):
    response = client.post("/demo/publish-single")
    assert response.status_code == 200
    assert response.json()["result"]["unique_processed"] == 1


def test_demo_publish_duplicate(client):
    response = client.post("/demo/publish-duplicate?copies=4")
    assert response.status_code == 200
    assert response.json()["result"] == {
        "received": 4,
        "unique_processed": 1,
        "duplicate_dropped": 3,
    }


def test_demo_publish_batch(client):
    response = client.post("/demo/publish-batch?total=10&duplicate_rate=0.30")
    assert response.status_code == 200
    assert response.json()["result"] == {
        "received": 10,
        "unique_processed": 7,
        "duplicate_dropped": 3,
    }


def test_demo_concurrency(client):
    response = client.post("/demo/concurrency?requests=8")
    assert response.status_code == 200
    assert response.json()["result"] == {
        "received": 8,
        "unique_processed": 1,
        "duplicate_dropped": 7,
    }
