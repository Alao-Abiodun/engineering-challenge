import json
import sqlite3
from datetime import UTC, datetime
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS workflow_events (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,  -- global order of events
    run_id     TEXT NOT NULL,
    event      TEXT NOT NULL,
    event_key  TEXT NOT NULL,                      -- what makes this event unique within the run
    data       TEXT NOT NULL,                      -- JSON details (attempt, decision, reasons...)
    created_at TEXT NOT NULL,
    UNIQUE (run_id, event_key)
);
"""


class EventLog:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.executescript(SCHEMA)

    def record(self, run_id: str, event: str, key: str, **data: Any) -> bool:
        """Append an event. Returns False if this run already has an event with this key."""
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO workflow_events (run_id, event, event_key, data, created_at) VALUES (?, ?, ?, ?, ?)",
            (run_id, event, key, json.dumps(data, default=str, sort_keys=True), datetime.now(UTC).isoformat()),
        )
        self.conn.commit()
        return cur.rowcount == 1

    def for_run(self, run_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT event, data, created_at FROM workflow_events WHERE run_id = ? ORDER BY seq", (run_id,)
        ).fetchall()
        return [{"event": event, **json.loads(data), "at": created_at} for event, data, created_at in rows]

    def names(self, run_id: str) -> list[str]:
        return [e["event"] for e in self.for_run(run_id)]