from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query

from .database import Database
from .models import Event, PublishResponse
from .processor import EventProcessor

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def create_app(
    database_path: str | None = None,
    worker_count: int | None = None,
) -> FastAPI:
    db_path = database_path or os.getenv("DATABASE_PATH", "./data/aggregator.db")
    workers = worker_count or int(os.getenv("WORKER_COUNT", "4"))
    database = Database(db_path)
    processor = EventProcessor(database, worker_count=workers)
    started_at = time.monotonic()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        database.init()
        processor.start()
        app.state.database = database
        app.state.processor = processor
        app.state.started_at = started_at
        yield
        processor.stop()

    app = FastAPI(
        title="Pub-Sub Log Aggregator",
        version="1.0.0",
        description="Idempotent log aggregator with SQLite transactions and concurrent workers.",
        lifespan=lifespan,
    )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/publish", response_model=PublishResponse)
    def publish(payload: Event | list[Event]) -> PublishResponse:
        events = payload if isinstance(payload, list) else [payload]
        if not events:
            raise HTTPException(status_code=400, detail="batch must contain at least one event")

        records = [event.to_record() for event in events]
        database.increment_received(len(records))
        for record in records:
            processor.submit(record)

        return PublishResponse(
            accepted=len(records),
            queued=processor.queue_size(),
            message="event accepted for asynchronous processing",
        )

    @app.get("/events")
    def events(
        topic: str | None = None,
        limit: Annotated[int, Query(ge=1, le=5000)] = 1000,
    ) -> list[dict[str, object]]:
        return database.get_events(topic=topic, limit=limit)

    @app.get("/stats")
    def stats() -> dict[str, object]:
        values = database.get_stats()
        values["uptime"] = round(time.monotonic() - started_at, 3)
        values["worker_count"] = processor.worker_count
        values["queue_size"] = processor.queue_size()
        return values

    @app.post("/admin/drain")
    def drain(timeout: Annotated[float, Query(gt=0, le=120)] = 30) -> dict[str, object]:
        drained = processor.drain(timeout=timeout)
        return {"drained": drained, "queue_size": processor.queue_size()}

    return app


app = create_app()
