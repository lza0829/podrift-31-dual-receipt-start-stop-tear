import os
import pathlib
import sqlite3
import sys
import tempfile

# 必须在任何 app 模块导入前定库路径（TestClient 烟测用）
os.environ.setdefault(
    "PODRIFT_DB",
    os.path.join(tempfile.mkdtemp(prefix="podrift-test-"), "test.db"),
)
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import pytest  # noqa: E402

from app.db import init_schema  # noqa: E402


@pytest.fixture
def connect_fn(tmp_path):
    """每测一库的连接工厂：镜像已就绪、配额清零。"""
    path = tmp_path / "t.db"

    def factory():
        conn = sqlite3.connect(str(path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 8000")
        return conn

    conn = factory()
    init_schema(conn)
    conn.execute(
        "INSERT INTO images(name, tag, pull_state, layers_done, layers_total, ready_at)"
        " VALUES ('app/core', '1.0', '已就绪', 4, 4, '2026-01-01T00:00:00Z')"
    )
    conn.execute(
        "INSERT INTO quotas(id, cpu_used, cpu_limit, mem_used_mb, mem_limit_mb)"
        " VALUES (1, 0.0, 4.0, 0, 2048)"
    )
    conn.commit()
    conn.close()
    return factory


@pytest.fixture
def pod_stopped(connect_fn):
    return add_pod(connect_fn, run_state="已停止")


@pytest.fixture
def pod_running(connect_fn):
    return add_pod(connect_fn, run_state="运行中")


def add_pod(connect_fn, name="web-a", run_state="已停止", cpu=0.5, mem=256):
    with connect_fn() as conn:
        cur = conn.execute(
            "INSERT INTO pods(name, image_id, run_state, sidecar, cpu_quota, mem_quota_mb)"
            " VALUES (?, 1, ?, NULL, ?, ?)",
            (name, run_state, cpu, mem),
        )
        if run_state == "运行中":
            conn.execute(
                "UPDATE quotas SET cpu_used=cpu_used+?, mem_used_mb=mem_used_mb+? WHERE id=1",
                (cpu, mem),
            )
        conn.commit()
        return cur.lastrowid


def one(connect_fn, sql, args=()):
    with connect_fn() as conn:
        return conn.execute(sql, args).fetchone()


def all_rows(connect_fn, sql, args=()):
    with connect_fn() as conn:
        return conn.execute(sql, args).fetchall()
