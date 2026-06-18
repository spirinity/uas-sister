import os
import random
import time
from datetime import UTC, datetime
from uuid import uuid4

import httpx


def build_events(total_events: int, duplicate_rate: float) -> list[dict]:
    unique_count = max(1, int(total_events * (1 - duplicate_rate)))
    topics = ["auth", "payment", "inventory", "shipping"]
    unique_events: list[dict] = []

    for index in range(unique_count):
        topic = random.choice(topics)
        unique_events.append(
            {
                "topic": topic,
                "event_id": f"{topic}-{uuid4()}",
                "timestamp": datetime.now(tz=UTC).isoformat(),
                "source": "publisher-simulator",
                "payload": {
                    "sequence": index,
                    "message": f"simulated {topic} log",
                },
            }
        )

    duplicate_count = total_events - unique_count
    duplicates = [random.choice(unique_events) for _ in range(duplicate_count)]
    events = unique_events + duplicates
    random.shuffle(events)
    return events


def chunks(items: list[dict], size: int):
    for index in range(0, len(items), size):
        yield items[index : index + size]


def main() -> None:
    target_url = os.getenv("TARGET_URL", "http://localhost:8080/publish")
    total_events = int(os.getenv("TOTAL_EVENTS", "20000"))
    duplicate_rate = float(os.getenv("DUPLICATE_RATE", "0.30"))
    batch_size = int(os.getenv("BATCH_SIZE", "250"))

    events = build_events(total_events=total_events, duplicate_rate=duplicate_rate)
    started = time.perf_counter()
    received = 0
    unique_processed = 0
    duplicate_dropped = 0

    with httpx.Client(timeout=30) as client:
        for batch in chunks(events, batch_size):
            response = client.post(target_url, json=batch)
            response.raise_for_status()
            result = response.json()
            received += result["received"]
            unique_processed += result["unique_processed"]
            duplicate_dropped += result["duplicate_dropped"]

    elapsed = time.perf_counter() - started
    throughput = received / elapsed if elapsed else received
    print(
        {
            "received": received,
            "unique_processed": unique_processed,
            "duplicate_dropped": duplicate_dropped,
            "elapsed_seconds": round(elapsed, 3),
            "throughput_events_per_second": round(throughput, 2),
        }
    )


if __name__ == "__main__":
    main()
