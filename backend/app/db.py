from __future__ import annotations
import sqlite3
from contextlib import contextmanager
from app.config import DB_PATH


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 8000")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


@contextmanager
def db():
    conn = connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS images (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          tag TEXT NOT NULL,
          pull_state TEXT NOT NULL,
          layers_done INTEGER NOT NULL DEFAULT 0,
          layers_total INTEGER NOT NULL DEFAULT 4,
          ready_at TEXT
        );
        CREATE TABLE IF NOT EXISTS pods (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          image_id INTEGER,
          run_state TEXT NOT NULL,
          sidecar TEXT,
          cpu_quota REAL NOT NULL DEFAULT 1.0,
          mem_quota_mb INTEGER NOT NULL DEFAULT 512,
          FOREIGN KEY(image_id) REFERENCES images(id)
        );
        CREATE TABLE IF NOT EXISTS start_orders (
          id INTEGER PRIMARY KEY,
          pod_id INTEGER NOT NULL,
          action TEXT NOT NULL,
          target_image_id INTEGER,
          status TEXT NOT NULL,
          note TEXT,
          created_at TEXT NOT NULL,
          FOREIGN KEY(pod_id) REFERENCES pods(id)
        );
        CREATE TABLE IF NOT EXISTS probes (
          id INTEGER PRIMARY KEY,
          pod_id INTEGER NOT NULL UNIQUE,
          color TEXT NOT NULL,
          fail_streak INTEGER NOT NULL DEFAULT 0,
          updated_at TEXT,
          FOREIGN KEY(pod_id) REFERENCES pods(id)
        );
        CREATE TABLE IF NOT EXISTS quotas (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          cpu_used REAL NOT NULL,
          cpu_limit REAL NOT NULL,
          mem_used_mb INTEGER NOT NULL,
          mem_limit_mb INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS restart_policy (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          mode TEXT NOT NULL,
          backoff_sec INTEGER NOT NULL DEFAULT 5
        );
        CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY,
          kind TEXT NOT NULL,
          ref TEXT,
          detail TEXT NOT NULL,
          at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS settings (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS envelopes (
          id INTEGER PRIMARY KEY,
          order_id INTEGER NOT NULL UNIQUE,
          pod_id INTEGER NOT NULL,
          action TEXT NOT NULL,
          phase TEXT NOT NULL,
          snapshot TEXT NOT NULL,
          token TEXT NOT NULL UNIQUE,
          quota_cpu REAL NOT NULL DEFAULT 0,
          quota_mem INTEGER NOT NULL DEFAULT 0,
          persisted_ms INTEGER,
          created_ms INTEGER NOT NULL,
          updated_ms INTEGER NOT NULL,
          FOREIGN KEY(order_id) REFERENCES start_orders(id),
          FOREIGN KEY(pod_id) REFERENCES pods(id)
        );
        CREATE TABLE IF NOT EXISTS pod_locks (
          pod_id INTEGER PRIMARY KEY,
          holder TEXT NOT NULL,
          envelope_id INTEGER NOT NULL,
          acquired_ms INTEGER NOT NULL,
          expires_ms INTEGER NOT NULL,
          dirty INTEGER NOT NULL DEFAULT 0,
          FOREIGN KEY(envelope_id) REFERENCES envelopes(id)
        );
        CREATE TABLE IF NOT EXISTS yield_gates (
          id INTEGER PRIMARY KEY,
          pod_id INTEGER NOT NULL,
          bucket_id INTEGER NOT NULL,
          waiter_order_id INTEGER NOT NULL,
          blocker_order_id INTEGER NOT NULL,
          state TEXT NOT NULL,
          created_ms INTEGER NOT NULL,
          resolved_ms INTEGER
        );
        CREATE TABLE IF NOT EXISTS contention_buckets (
          id INTEGER PRIMARY KEY,
          pod_id INTEGER NOT NULL,
          start_order_id INTEGER NOT NULL,
          stop_order_id INTEGER NOT NULL,
          arm TEXT NOT NULL,
          verdict TEXT,
          reason TEXT,
          pre_snapshot TEXT NOT NULL,
          final_state TEXT,
          state TEXT NOT NULL,
          replay_log TEXT NOT NULL DEFAULT '[]',
          created_ms INTEGER NOT NULL,
          closed_ms INTEGER
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uq_bucket_open ON contention_buckets(pod_id) WHERE state='在途';
        """
    )
