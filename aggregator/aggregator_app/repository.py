import json
from datetime import datetime

import asyncpg

from aggregator_app.models import EventIn, EventOut, PublishResult, Stats


def parse_payload(value: object) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {"value": parsed}
    return {"value": value}


class EventRepository:
    def __init__(self, pool: asyncpg.Pool, started_at: datetime | None):
        self.pool = pool
        self.started_at = started_at

    async def publish_many(self, events: list[EventIn]) -> PublishResult:
        unique_processed = 0
        duplicate_dropped = 0

        async with self.pool.acquire() as connection:
            async with connection.transaction(isolation="read_committed"):
                await connection.execute(
                    """
                    UPDATE stats
                    SET received = received + $1
                    WHERE key = 'global'
                    """,
                    len(events),
                )

                for event in events:
                    inserted = await connection.fetchval(
                        """
                        INSERT INTO processed_events (topic, event_id)
                        VALUES ($1, $2)
                        ON CONFLICT (topic, event_id) DO NOTHING
                        RETURNING id
                        """,
                        event.topic,
                        event.event_id,
                    )

                    if inserted is None:
                        duplicate_dropped += 1
                        continue

                    await connection.execute(
                        """
                        INSERT INTO events (
                            topic,
                            event_id,
                            event_timestamp,
                            source,
                            payload
                        )
                        VALUES ($1, $2, $3, $4, $5::jsonb)
                        ON CONFLICT (topic, event_id) DO NOTHING
                        """,
                        event.topic,
                        event.event_id,
                        event.timestamp,
                        event.source,
                        json.dumps(event.payload),
                    )
                    unique_processed += 1

                await connection.execute(
                    """
                    UPDATE stats
                    SET unique_processed = unique_processed + $1,
                        duplicate_dropped = duplicate_dropped + $2
                    WHERE key = 'global'
                    """,
                    unique_processed,
                    duplicate_dropped,
                )

        return PublishResult(
            received=len(events),
            unique_processed=unique_processed,
            duplicate_dropped=duplicate_dropped,
        )

    async def list_events(self, topic: str | None = None, limit: int = 100) -> list[EventOut]:
        query = """
            SELECT topic, event_id, event_timestamp, source, payload, processed_at
            FROM events
        """
        args: list[object] = []

        if topic:
            query += " WHERE topic = $1"
            args.append(topic)

        query += f" ORDER BY processed_at DESC LIMIT ${len(args) + 1}"
        args.append(limit)

        rows = await self.pool.fetch(query, *args)
        return [
            EventOut(
                topic=row["topic"],
                event_id=row["event_id"],
                timestamp=row["event_timestamp"],
                source=row["source"],
                payload=parse_payload(row["payload"]),
                processed_at=row["processed_at"],
            )
            for row in rows
        ]

    async def get_stats(self) -> Stats:
        row = await self.pool.fetchrow(
            """
            SELECT received, unique_processed, duplicate_dropped
            FROM stats
            WHERE key = 'global'
            """
        )
        topic_rows = await self.pool.fetch(
            "SELECT DISTINCT topic FROM events ORDER BY topic ASC"
        )

        uptime_seconds = 0.0
        if self.started_at is not None:
            now = datetime.now(tz=self.started_at.tzinfo)
            uptime_seconds = (now - self.started_at).total_seconds()
        return Stats(
            received=row["received"],
            unique_processed=row["unique_processed"],
            duplicate_dropped=row["duplicate_dropped"],
            topics=[topic_row["topic"] for topic_row in topic_rows],
            uptime_seconds=uptime_seconds,
        )
