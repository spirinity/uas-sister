import asyncio
import json
from time import monotonic
from uuid import uuid4

from redis.asyncio import Redis

from aggregator_app.models import EventIn, PublishResult
from aggregator_app.settings import Settings


class EventBroker:
    def __init__(self, redis: Redis, settings: Settings):
        self.redis = redis
        self.settings = settings

    def result_key(self, request_id: str) -> str:
        return f"publish-result:{request_id}"

    async def publish_and_wait(self, events: list[EventIn]) -> PublishResult:
        request_id = str(uuid4())
        payload = json.dumps([event.model_dump(mode="json") for event in events])
        await self.redis.xadd(
            self.settings.redis_stream,
            {"request_id": request_id, "events": payload},
            maxlen=10_000,
            approximate=True,
        )

        deadline = monotonic() + self.settings.publish_timeout_seconds
        key = self.result_key(request_id)
        while monotonic() < deadline:
            result = await self.redis.get(key)
            if result is not None:
                await self.redis.delete(key)
                return PublishResult.model_validate_json(result)
            await asyncio.sleep(0.05)

        raise TimeoutError(f"consumer result timed out for request {request_id}")
