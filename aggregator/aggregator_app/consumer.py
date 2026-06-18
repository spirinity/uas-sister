import asyncio
import json
import logging
import socket
from uuid import uuid4

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from aggregator_app.broker import EventBroker
from aggregator_app.database import create_pool, init_schema
from aggregator_app.models import EventIn
from aggregator_app.repository import EventRepository
from aggregator_app.settings import settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("event-consumer")


async def ensure_consumer_group(redis: Redis) -> None:
    try:
        await redis.xgroup_create(
            name=settings.redis_stream,
            groupname=settings.redis_consumer_group,
            id="0",
            mkstream=True,
        )
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def process_message(
    redis: Redis,
    repository: EventRepository,
    broker: EventBroker,
    message_id: str,
    fields: dict[str, str],
) -> None:
    request_id = fields["request_id"]
    raw_events = json.loads(fields["events"])
    events = [EventIn.model_validate(item) for item in raw_events]
    result = await repository.publish_many(events)

    await redis.set(
        broker.result_key(request_id),
        result.model_dump_json(),
        ex=settings.result_ttl_seconds,
    )
    await redis.xack(
        settings.redis_stream,
        settings.redis_consumer_group,
        message_id,
    )
    logger.info(
        "processed message=%s received=%s unique=%s duplicates=%s",
        message_id,
        result.received,
        result.unique_processed,
        result.duplicate_dropped,
    )


async def claim_stale_messages(
    redis: Redis,
    repository: EventRepository,
    broker: EventBroker,
    consumer_name: str,
) -> None:
    response = await redis.xautoclaim(
        name=settings.redis_stream,
        groupname=settings.redis_consumer_group,
        consumername=consumer_name,
        min_idle_time=settings.claim_idle_ms,
        start_id="0-0",
        count=10,
    )
    for message_id, fields in response[1]:
        await process_message(redis, repository, broker, message_id, fields)


async def worker(
    redis: Redis,
    repository: EventRepository,
    broker: EventBroker,
    worker_index: int,
) -> None:
    consumer_name = (
        f"{socket.gethostname()}-{worker_index}-{uuid4().hex[:6]}"
    )
    logger.info("consumer worker started name=%s", consumer_name)

    while True:
        try:
            await claim_stale_messages(redis, repository, broker, consumer_name)
            messages = await redis.xreadgroup(
                groupname=settings.redis_consumer_group,
                consumername=consumer_name,
                streams={settings.redis_stream: ">"},
                count=1,
                block=5_000,
            )
            for _, entries in messages:
                for message_id, fields in entries:
                    await process_message(
                        redis,
                        repository,
                        broker,
                        message_id,
                        fields,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("consumer worker failed; retrying")
            await asyncio.sleep(1)


async def run() -> None:
    pool = await create_pool(settings.database_url)
    await init_schema(pool)
    redis = Redis.from_url(settings.broker_url, decode_responses=True)
    await redis.ping()
    await ensure_consumer_group(redis)

    repository = EventRepository(pool=pool, started_at=None)
    broker = EventBroker(redis=redis, settings=settings)
    tasks = [
        asyncio.create_task(worker(redis, repository, broker, index))
        for index in range(settings.consumer_workers)
    ]

    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await redis.aclose()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run())
