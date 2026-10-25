import sqlite3
from dataclasses import dataclass
from pathlib import Path

from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.checkpoint.sqlite import SqliteSaver

from .events import EventLog
from .models import ContentItem, Decision, EvaluationResult, ReviewDecision, Status, WorkflowState

CHECKPOINT_TYPES = [WorkflowState, ContentItem, EvaluationResult, ReviewDecision, Decision, Status]


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


@dataclass
class Storage:
    checkpointer: SqliteSaver
    events: EventLog
    _connections: tuple[sqlite3.Connection, ...]

    def close(self) -> None:
        for conn in self._connections:
            conn.close()


def open_storage(db_path: Path) -> Storage:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_conn, events_conn = _connect(db_path), _connect(db_path)
    checkpointer = SqliteSaver(checkpoint_conn, serde=JsonPlusSerializer(allowed_msgpack_modules=CHECKPOINT_TYPES))
    return Storage(checkpointer, EventLog(events_conn), (checkpoint_conn, events_conn))