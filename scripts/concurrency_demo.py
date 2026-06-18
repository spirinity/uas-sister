import argparse
import asyncio
from datetime import UTC, datetime

import httpx


def build_duplicate_event() -> dict:
    return {
        "topic": "concurrency-demo",
        "event_id": "shared-event-id",
        "timestamp": datetime.now(tz=UTC).isoformat(),
        "source": "concurrency-demo",
        "payload": {"same_event_sent_many_times": True},
    }


async def send_once(client: httpx.AsyncClient, target_url: str) -> dict:
    response = await client.post(target_url, json=build_duplicate_event())
    response.raise_for_status()
    return response.json()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="http://localhost:8080/publish")
    parser.add_argument("--requests", type=int, default=50)
    args = parser.parse_args()

    async with httpx.AsyncClient(timeout=30) as client:
        results = await asyncio.gather(
            *[send_once(client, args.target) for _ in range(args.requests)]
        )

    print(
        {
            "requests": args.requests,
            "received": sum(result["received"] for result in results),
            "unique_processed": sum(result["unique_processed"] for result in results),
            "duplicate_dropped": sum(result["duplicate_dropped"] for result in results),
        }
    )


if __name__ == "__main__":
    asyncio.run(main())
