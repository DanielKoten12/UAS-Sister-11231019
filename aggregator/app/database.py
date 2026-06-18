from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any


STAT_NAMES = ("received", "unique_processed", "duplicate_dropped")


class Database:
    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def init(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS processed_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    source TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    received_at TEXT NOT NULL DEFAULT (datetime('now')),
                    processed_at TEXT NOT NULL DEFAULT (datetime('now')),
                    UNIQUE(topic, event_id)
                );

                CREATE INDEX IF NOT EXISTS idx_processed_events_topic
                ON processed_events(topic);

                CREATE TABLE IF NOT EXISTS stats (
                    name TEXT PRIMARY KEY,
                    count INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                """
            )
            for name in STAT_NAMES:
                conn.execute(
                    "INSERT OR IGNORE INTO stats(name, count) VALUES (?, 0)",
                    (name,),
                )
            conn.commit()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            timeout=30,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def increment_received(self, amount: int) -> None:
        if amount <= 0:
            return
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE stats SET count = count + ? WHERE name = 'received'",
                (amount,),
            )
            conn.commit()

    def process_event(self, event: dict[str, Any], worker_id: str) -> bool:
        payload_json = json.dumps(event["payload"], sort_keys=True, separators=(",", ":"))
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

        def attempt() -> bool:
            with self.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO processed_events
                        (topic, event_id, timestamp, source, payload, payload_hash, worker_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event["topic"],
                        event["event_id"],
                        event["timestamp"],
                        event["source"],
                        payload_json,
                        payload_hash,
                        worker_id,
                    ),
                )
                inserted = cursor.rowcount == 1
                stat_name = "unique_processed" if inserted else "duplicate_dropped"
                action = "processed" if inserted else "duplicate"
                conn.execute(
                    "UPDATE stats SET count = count + 1 WHERE name = ?",
                    (stat_name,),
                )
                conn.execute(
                    """
                    INSERT INTO audit_log(topic, event_id, action, worker_id)
                    VALUES (?, ?, ?, ?)
                    """,
                    (event["topic"], event["event_id"], action, worker_id),
                )
                conn.commit()
                return inserted

        delay = 0.025
        for retry in range(6):
            try:
                return attempt()
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or retry == 5:
                    raise
                time.sleep(delay)
                delay *= 2
        raise RuntimeError("unreachable retry state")

    def get_events(self, topic: str | None = None, limit: int = 1000) -> list[dict[str, Any]]:
        limit = max(1, min(limit, 5000))
        query = """
            SELECT topic, event_id, timestamp, source, payload, payload_hash, worker_id,
                   received_at, processed_at
            FROM processed_events
        """
        params: tuple[Any, ...]
        if topic:
            query += " WHERE topic = ?"
            params = (topic, limit)
        else:
            params = (limit,)
        query += " ORDER BY id ASC LIMIT ?"

        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        events: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item["payload"])
            events.append(item)
        return events

    def get_stats(self) -> dict[str, Any]:
        with self.connect() as conn:
            counts = {
                row["name"]: row["count"]
                for row in conn.execute("SELECT name, count FROM stats").fetchall()
            }
            topics = {
                row["topic"]: row["count"]
                for row in conn.execute(
                    """
                    SELECT topic, COUNT(*) AS count
                    FROM processed_events
                    GROUP BY topic
                    ORDER BY topic ASC
                    """
                ).fetchall()
            }
        for name in STAT_NAMES:
            counts.setdefault(name, 0)
        counts["topics"] = topics
        return counts

    def count_audit_rows(self) -> int:
        with self.connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS count FROM audit_log").fetchone()
        return int(row["count"])
