from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Any

from .database import Database

logger = logging.getLogger("aggregator.processor")


class EventProcessor:
    def __init__(self, database: Database, worker_count: int = 4) -> None:
        self.database = database
        self.worker_count = max(1, worker_count)
        self.queue: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self._threads: list[threading.Thread] = []
        self._started = False
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            for index in range(self.worker_count):
                worker_id = f"worker-{index + 1}"
                thread = threading.Thread(
                    target=self._run_worker,
                    name=worker_id,
                    args=(worker_id,),
                    daemon=True,
                )
                thread.start()
                self._threads.append(thread)
            self._started = True
            logger.info("started %s worker(s)", self.worker_count)

    def stop(self) -> None:
        with self._lock:
            if not self._started:
                return
            for _ in self._threads:
                self.queue.put(None)
            for thread in self._threads:
                thread.join(timeout=5)
            self._threads.clear()
            self._started = False
            logger.info("stopped workers")

    def submit(self, event: dict[str, Any]) -> None:
        self.queue.put(event)

    def drain(self, timeout: float = 30) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.queue.unfinished_tasks == 0:
                return True
            time.sleep(0.02)
        return self.queue.unfinished_tasks == 0

    def queue_size(self) -> int:
        return self.queue.qsize()

    def _run_worker(self, worker_id: str) -> None:
        while True:
            event = self.queue.get()
            try:
                if event is None:
                    return
                inserted = self.database.process_event(event, worker_id)
                if inserted:
                    logger.info(
                        "processed topic=%s event_id=%s worker=%s",
                        event["topic"],
                        event["event_id"],
                        worker_id,
                    )
                else:
                    logger.info(
                        "duplicate dropped topic=%s event_id=%s worker=%s",
                        event["topic"],
                        event["event_id"],
                        worker_id,
                    )
            except Exception:
                logger.exception("worker failed while processing event")
            finally:
                self.queue.task_done()
