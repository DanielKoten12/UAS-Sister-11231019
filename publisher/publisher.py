from __future__ import annotations

import argparse
import asyncio
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx


LEVELS = ("INFO", "WARN", "ERROR", "DEBUG")


def build_events(total: int, duplicate_rate: float, topics: list[str], source: str) -> list[dict[str, Any]]:
    duplicate_rate = max(0.0, min(duplicate_rate, 0.95))
    unique_total = max(1, int(total * (1 - duplicate_rate)))
    duplicate_total = total - unique_total
    started_at = datetime.now(timezone.utc)

    unique_events: list[dict[str, Any]] = []
    for index in range(unique_total):
        topic = topics[index % len(topics)]
        event_id = f"{source}-{index}-{uuid.uuid4().hex[:12]}"
        unique_events.append(
            {
                "topic": topic,
                "event_id": event_id,
                "timestamp": (started_at + timedelta(milliseconds=index)).isoformat(),
                "source": source,
                "payload": {
                    "level": random.choice(LEVELS),
                    "message": f"log event {index}",
                    "sequence": index,
                    "monotonic_counter": index,
                },
            }
        )

    duplicate_events = [random.choice(unique_events).copy() for _ in range(duplicate_total)]
    events = unique_events + duplicate_events
    random.shuffle(events)
    return events


def chunks(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


async def post_batch(client: httpx.AsyncClient, target_url: str, batch: list[dict[str, Any]]) -> None:
    delay = 0.25
    for attempt in range(1, 5):
        try:
            response = await client.post(target_url, json=batch, timeout=30)
            response.raise_for_status()
            return
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            if attempt == 4:
                raise RuntimeError(f"failed to publish batch after retries: {exc}") from exc
            await asyncio.sleep(delay)
            delay *= 2


async def run(args: argparse.Namespace) -> None:
    topics = [topic.strip() for topic in args.topics.split(",") if topic.strip()]
    events = build_events(args.events, args.duplicate_rate, topics, args.source)
    batches = chunks(events, args.batch_size)
    started = time.perf_counter()

    limits = httpx.Limits(max_connections=args.concurrency, max_keepalive_connections=args.concurrency)
    async with httpx.AsyncClient(limits=limits) as client:
        semaphore = asyncio.Semaphore(args.concurrency)

        async def guarded(batch: list[dict[str, Any]]) -> None:
            async with semaphore:
                await post_batch(client, args.target_url, batch)

        await asyncio.gather(*(guarded(batch) for batch in batches))

        drain_url = args.target_url.rsplit("/", 1)[0] + "/admin/drain"
        try:
            await client.post(drain_url, params={"timeout": 120}, timeout=130)
        except httpx.HTTPError:
            pass

    elapsed = time.perf_counter() - started
    throughput = len(events) / elapsed if elapsed else 0
    print(f"sent={len(events)} duplicate_rate={args.duplicate_rate:.2f}")
    print(f"batches={len(batches)} batch_size={args.batch_size} concurrency={args.concurrency}")
    print(f"elapsed_seconds={elapsed:.3f} throughput_events_per_second={throughput:.2f}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulator publisher for the log aggregator.")
    parser.add_argument("--target-url", default=os.getenv("TARGET_URL", "http://localhost:8080/publish"))
    parser.add_argument("--events", type=int, default=int(os.getenv("EVENT_COUNT", "20000")))
    parser.add_argument("--duplicate-rate", type=float, default=float(os.getenv("DUPLICATE_RATE", "0.30")))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "500")))
    parser.add_argument("--concurrency", type=int, default=int(os.getenv("PUBLISH_CONCURRENCY", "4")))
    parser.add_argument("--topics", default=os.getenv("TOPICS", "app,auth,payment,system"))
    parser.add_argument("--source", default=os.getenv("SOURCE", "publisher-1"))
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
