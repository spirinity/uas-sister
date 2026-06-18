import asyncio
import random
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated
from uuid import uuid4

import asyncpg
from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request
from redis.asyncio import Redis

from aggregator_app.broker import EventBroker
from aggregator_app.database import create_pool, init_schema
from aggregator_app.models import DemoResponse, EventIn, EventOut, PublishResult, Stats
from aggregator_app.repository import EventRepository
from aggregator_app.settings import settings


EventBody = EventIn | list[EventIn]

PUBLISH_EXAMPLES = {
    "single_event": {
        "summary": "Single event",
        "description": "Use this to publish one event manually.",
        "value": {
            "topic": "auth",
            "event_id": "swagger-auth-1",
            "timestamp": "2026-06-16T10:00:00Z",
            "source": "swagger-ui",
            "payload": {"message": "single login event"},
        },
    },
    "batch_with_duplicate": {
        "summary": "Batch with duplicate",
        "description": "Two items use the same topic and event_id, so one is dropped.",
        "value": [
            {
                "topic": "payment",
                "event_id": "swagger-payment-duplicate",
                "timestamp": "2026-06-16T10:00:00Z",
                "source": "swagger-ui",
                "payload": {"amount": 50000},
            },
            {
                "topic": "payment",
                "event_id": "swagger-payment-duplicate",
                "timestamp": "2026-06-16T10:00:01Z",
                "source": "swagger-ui-retry",
                "payload": {"amount": 50000},
            },
        ],
    },
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    started_at = datetime.now(tz=UTC)
    pool = await create_pool(settings.database_url)
    await init_schema(pool)
    redis = Redis.from_url(settings.broker_url, decode_responses=True)
    await redis.ping()
    app.state.pool = pool
    app.state.redis = redis
    app.state.broker = EventBroker(redis=redis, settings=settings)
    app.state.repository = EventRepository(pool=pool, started_at=started_at)
    yield
    await redis.aclose()
    await pool.close()


app = FastAPI(
    title="UAS Pub-Sub Log Aggregator",
    version="1.0.0",
    description=(
        "API untuk demo Pub-Sub Log Aggregator dengan idempotent consumer, "
        "deduplication persisten, transaksi, dan kontrol konkurensi. "
        "Buka tag Demo Use Cases untuk menjalankan skenario presentasi dari Swagger."
    ),
    servers=[{"url": "http://127.0.0.1:8080", "description": "Local Docker Compose"}],
    lifespan=lifespan,
)


def get_repository(request: Request) -> EventRepository:
    return request.app.state.repository


def get_broker(request: Request) -> EventBroker:
    return request.app.state.broker


async def publish_through_broker(
    broker: EventBroker,
    events: list[EventIn],
) -> PublishResult:
    try:
        return await broker.publish_and_wait(events)
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail=str(exc)) from exc


def build_demo_event(topic: str, event_id: str, sequence: int = 1) -> EventIn:
    return EventIn(
        topic=topic,
        event_id=event_id,
        timestamp=datetime.now(tz=UTC),
        source="swagger-demo",
        payload={"sequence": sequence, "message": f"demo event for {topic}"},
    )


def build_batch_events(total: int, duplicate_rate: float) -> list[EventIn]:
    unique_count = max(1, int(total * (1 - duplicate_rate)))
    topics = ["auth", "payment", "inventory", "shipping"]
    unique_events = [
        build_demo_event(
            topic=random.choice(topics),
            event_id=f"swagger-batch-{uuid4()}",
            sequence=index,
        )
        for index in range(unique_count)
    ]
    duplicate_count = total - unique_count
    duplicates = [random.choice(unique_events) for _ in range(duplicate_count)]
    events = unique_events + duplicates
    random.shuffle(events)
    return events


async def demo_response(
    repository: EventRepository,
    use_case: str,
    explanation: str,
    result: PublishResult,
) -> DemoResponse:
    return DemoResponse(
        use_case=use_case,
        explanation=explanation,
        result=result,
        stats=await repository.get_stats(),
    )


@app.get("/health", tags=["Core API"], summary="Health check")
async def health(request: Request) -> dict[str, str]:
    pool: asyncpg.Pool = request.app.state.pool
    redis: Redis = request.app.state.redis
    await pool.fetchval("SELECT 1")
    await redis.ping()
    return {"status": "ok", "database": "ok", "broker": "ok"}


@app.post(
    "/publish",
    response_model=PublishResult,
    tags=["Core API"],
    summary="Publish single or batch event",
    description="Menerima satu event atau list event. Dedup dilakukan dengan topic + event_id.",
)
async def publish(
    body: Annotated[EventBody, Body(openapi_examples=PUBLISH_EXAMPLES)],
    broker: Annotated[EventBroker, Depends(get_broker)],
) -> PublishResult:
    events = body if isinstance(body, list) else [body]
    return await publish_through_broker(broker, events)


@app.get(
    "/events",
    response_model=list[EventOut],
    tags=["Core API"],
    summary="List processed unique events",
)
async def events(
    repository: Annotated[EventRepository, Depends(get_repository)],
    topic: Annotated[str | None, Query(min_length=1)] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> list[EventOut]:
    return await repository.list_events(topic=topic, limit=limit)


@app.get("/stats", response_model=Stats, tags=["Core API"], summary="Get aggregate stats")
async def stats(
    repository: Annotated[EventRepository, Depends(get_repository)],
) -> Stats:
    return await repository.get_stats()


@app.post(
    "/demo/publish-single",
    response_model=DemoResponse,
    tags=["Demo Use Cases"],
    summary="Use Case 1 - publish one event",
)
async def demo_publish_single(
    repository: Annotated[EventRepository, Depends(get_repository)],
    broker: Annotated[EventBroker, Depends(get_broker)],
    topic: Annotated[str, Query(min_length=1)] = "auth",
    event_id: Annotated[str | None, Query(min_length=1)] = None,
) -> DemoResponse:
    event = build_demo_event(topic=topic, event_id=event_id or f"swagger-single-{uuid4()}")
    result = await publish_through_broker(broker, [event])
    return await demo_response(
        repository,
        use_case="Publish one event",
        explanation="Satu event dikirim ke aggregator dan harus masuk sebagai unique_processed = 1.",
        result=result,
    )


@app.post(
    "/demo/publish-duplicate",
    response_model=DemoResponse,
    tags=["Demo Use Cases"],
    summary="Use Case 2 - publish duplicate events",
)
async def demo_publish_duplicate(
    repository: Annotated[EventRepository, Depends(get_repository)],
    broker: Annotated[EventBroker, Depends(get_broker)],
    topic: Annotated[str, Query(min_length=1)] = "payment",
    event_id: Annotated[str | None, Query(min_length=1)] = None,
    copies: Annotated[int, Query(ge=2, le=100)] = 2,
) -> DemoResponse:
    shared_event_id = event_id or f"swagger-duplicate-{uuid4()}"
    events = [build_demo_event(topic=topic, event_id=shared_event_id, sequence=i) for i in range(copies)]
    result = await publish_through_broker(broker, events)
    return await demo_response(
        repository,
        use_case="Publish duplicate events",
        explanation="Beberapa event memakai topic dan event_id yang sama. Hanya satu diproses, sisanya duplicate_dropped.",
        result=result,
    )


@app.post(
    "/demo/publish-batch",
    response_model=DemoResponse,
    tags=["Demo Use Cases"],
    summary="Use Case 3 - publish batch with duplicate rate",
)
async def demo_publish_batch(
    repository: Annotated[EventRepository, Depends(get_repository)],
    broker: Annotated[EventBroker, Depends(get_broker)],
    total: Annotated[int, Query(ge=1, le=20000)] = 100,
    duplicate_rate: Annotated[float, Query(ge=0, le=0.95)] = 0.30,
) -> DemoResponse:
    events = build_batch_events(total=total, duplicate_rate=duplicate_rate)
    result = await publish_through_broker(broker, events)
    return await demo_response(
        repository,
        use_case="Publish batch events",
        explanation="Batch berisi event unik dan duplikat sesuai duplicate_rate. Gunakan total=20000 untuk demo performa.",
        result=result,
    )


@app.post(
    "/demo/concurrency",
    response_model=DemoResponse,
    tags=["Demo Use Cases"],
    summary="Use Case 4 - concurrent duplicate publish",
)
async def demo_concurrency(
    repository: Annotated[EventRepository, Depends(get_repository)],
    broker: Annotated[EventBroker, Depends(get_broker)],
    requests: Annotated[int, Query(ge=2, le=200)] = 50,
    topic: Annotated[str, Query(min_length=1)] = "concurrency-demo",
    event_id: Annotated[str | None, Query(min_length=1)] = None,
) -> DemoResponse:
    shared_event_id = event_id or f"swagger-concurrency-{uuid4()}"
    event = build_demo_event(topic=topic, event_id=shared_event_id)
    results = await asyncio.gather(
        *[publish_through_broker(broker, [event]) for _ in range(requests)]
    )
    result = PublishResult(
        received=sum(item.received for item in results),
        unique_processed=sum(item.unique_processed for item in results),
        duplicate_dropped=sum(item.duplicate_dropped for item in results),
    )
    return await demo_response(
        repository,
        use_case="Concurrent duplicate publish",
        explanation="Request paralel mengirim event yang sama. Unique constraint Postgres membuat hanya satu request memproses event.",
        result=result,
    )
