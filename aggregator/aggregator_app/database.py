import asyncpg


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS processed_events (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    event_id TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (topic, event_id)
);

CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    payload JSONB NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (topic, event_id)
);

CREATE TABLE IF NOT EXISTS stats (
    key TEXT PRIMARY KEY CHECK (key = 'global'),
    received BIGINT NOT NULL DEFAULT 0,
    unique_processed BIGINT NOT NULL DEFAULT 0,
    duplicate_dropped BIGINT NOT NULL DEFAULT 0
);

INSERT INTO stats (key) VALUES ('global')
ON CONFLICT (key) DO NOTHING;
"""


async def create_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn=database_url,
        min_size=1,
        max_size=10,
        command_timeout=30,
    )


async def init_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as connection:
        async with connection.transaction():
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtext('pubsub-log-aggregator-schema'))"
            )
            await connection.execute(SCHEMA_SQL)
