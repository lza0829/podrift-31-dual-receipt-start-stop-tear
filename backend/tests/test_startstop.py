"""启停对砸夹具：启动与停止对砸、让路闸超时、双痕迹回滚、半截脏锁、互证与重放。"""
import json
import threading
import time

from app.services import startstop as ss
from app.services.common import now_ms
from tests.conftest import add_pod, all_rows, one

POLL = 20
BIG_TTL = 30000


def _run(fn):
    box = {}
    t = threading.Thread(target=lambda: box.__setitem__("r", fn()), daemon=True)
    t.start()
    return t, box


def _wait_for(pred, timeout=5.0, what="条件"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        got = pred()
        if got:
            return got
        time.sleep(0.02)
    raise AssertionError(f"超时未等到:{what}")


# ---------------------------------------------------------------- 对砸·让路闸臂（双成）

def test_gate_arm_both_succeed(connect_fn, pod_stopped):
    pod_id = pod_stopped
    traced = threading.Event()
    release = threading.Event()

    def hook(action, order_id, env_id):
        traced.set()
        release.wait(10)

    t_start, start_box = _run(lambda: ss.submit_order(
        connect_fn, pod_id, "启动", hooks={"after_trace": hook}, poll_ms=POLL, lock_ttl_ms=BIG_TTL))
    assert traced.wait(5), "启动未落痕迹"
    # 启动信封在途：舱体启动中、锁在启动手
    assert one(connect_fn, "SELECT run_state FROM pods WHERE id=?", (pod_id,))["run_state"] == "启动中"

    t_stop, stop_box = _run(lambda: ss.submit_order(
        connect_fn, pod_id, "停止", poll_ms=POLL, gate_timeout_ms=5000, lock_ttl_ms=BIG_TTL))
    gate = _wait_for(lambda: one(connect_fn, "SELECT * FROM yield_gates WHERE state='等待中'"),
                     what="停止进让路闸")
    # 启动未落库，停止不得改舱体已停止
    assert one(connect_fn, "SELECT run_state FROM pods WHERE id=?", (pod_id,))["run_state"] == "启动中"
    assert gate["state"] == "等待中"

    release.set()
    t_start.join(10)
    t_stop.join(10)
    assert start_box["r"]["status"] == "已完成"
    assert stop_box["r"]["status"] == "已完成"

    # 让路闸臂：停止后落，舱体已停止，配额归零
    assert one(connect_fn, "SELECT run_state FROM pods WHERE id=?", (pod_id,))["run_state"] == "已停止"
    q = one(connect_fn, "SELECT * FROM quotas WHERE id=1")
    assert q["cpu_used"] == 0 and q["mem_used_mb"] == 0

    bucket = one(connect_fn, "SELECT * FROM contention_buckets")
    assert bucket["arm"] == "让路闸"
    assert bucket["verdict"] == "双成" and bucket["state"] == "已收口"
    assert bucket["final_state"] == "已停止"
    assert one(connect_fn, "SELECT state FROM yield_gates")["state"] == "已放行"
    assert one(connect_fn, "SELECT COUNT(*) c FROM pod_locks")["c"] == 0
    # 双灯补齐可核
    assert "争用#" in start_box["r"]["note"] and "争用#" in stop_box["r"]["note"]
    # 互证全绿、读侧重放一致
    with connect_fn() as conn:
        checks = ss.verify_contention(conn, bucket["id"])
    assert all(c["通过"] for c in checks), checks
    rp = ss.replay_bucket(json.loads(bucket["replay_log"]))
    assert rp["verdict"] == "双成" and rp["pod"] == "已停止"
    assert rp["start_light"] == "已完成" and rp["stop_light"] == "已完成"


# ---------------------------------------------------------------- 对砸·停止抢先臂（双成）

def test_preempt_arm_both_succeed(connect_fn, pod_running):
    pod_id = pod_running
    stop_traced = threading.Event()
    release_stop = threading.Event()

    def hook(action, order_id, env_id):
        stop_traced.set()
        release_stop.wait(10)

    t_stop, stop_box = _run(lambda: ss.submit_order(
        connect_fn, pod_id, "停止", hooks={"after_trace": hook}, poll_ms=POLL, lock_ttl_ms=BIG_TTL))
    assert stop_traced.wait(5), "停止未落痕迹"
    assert one(connect_fn, "SELECT run_state FROM pods WHERE id=?", (pod_id,))["run_state"] == "停止中"

    t_start, start_box = _run(lambda: ss.submit_order(
        connect_fn, pod_id, "启动", poll_ms=POLL, lock_ttl_ms=BIG_TTL))
    bucket = _wait_for(lambda: one(connect_fn, "SELECT * FROM contention_buckets"),
                       what="争用桶开桶")
    # 停止抢先臂；启动未落库前桶必须挂在在途，不得装成无在途却已停止收口
    assert bucket["arm"] == "停止抢先" and bucket["state"] == "在途"

    release_stop.set()
    t_stop.join(10)
    t_start.join(10)
    assert stop_box["r"]["status"] == "已完成"
    assert start_box["r"]["status"] == "已完成"

    # 停止先落、启动后落：舱体运行中，配额净回原位
    assert one(connect_fn, "SELECT run_state FROM pods WHERE id=?", (pod_id,))["run_state"] == "运行中"
    q = one(connect_fn, "SELECT * FROM quotas WHERE id=1")
    assert q["cpu_used"] == 0.5 and q["mem_used_mb"] == 256

    bucket = one(connect_fn, "SELECT * FROM contention_buckets")
    assert bucket["verdict"] == "双成" and bucket["final_state"] == "运行中"
    # 停止抢先臂不得有闸痕；启动灯补齐可核
    assert one(connect_fn, "SELECT COUNT(*) c FROM yield_gates")["c"] == 0
    assert "争用#" in start_box["r"]["note"]
    with connect_fn() as conn:
        checks = ss.verify_contention(conn, bucket["id"])
    assert all(c["通过"] for c in checks), checks


# ---------------------------------------------------------------- 让路闸超时（整笔退回争用前快照）

def test_gate_timeout_full_rollback(connect_fn, pod_stopped):
    pod_id = pod_stopped
    traced = threading.Event()
    release = threading.Event()

    def hook(action, order_id, env_id):
        traced.set()
        release.wait(10)

    t_start, start_box = _run(lambda: ss.submit_order(
        connect_fn, pod_id, "启动", hooks={"after_trace": hook}, poll_ms=POLL, lock_ttl_ms=BIG_TTL))
    assert traced.wait(5)

    # 启动迟迟不落库，停止闸内超时 → 整笔回滚
    r_stop = ss.submit_order(connect_fn, pod_id, "停止",
                             gate_timeout_ms=300, poll_ms=POLL, lock_ttl_ms=BIG_TTL)
    assert r_stop["status"] == "已回滚"
    assert "让路闸超时" in r_stop["note"]
    # 舱体退回争用前快照
    assert one(connect_fn, "SELECT run_state FROM pods WHERE id=?", (pod_id,))["run_state"] == "已停止"

    release.set()
    t_start.join(10)
    # 启动迟到落库被相位岗挡住，同笔回滚
    assert start_box["r"]["status"] == "已回滚"

    s_order = one(connect_fn, "SELECT * FROM start_orders WHERE action='启动'")
    t_order = one(connect_fn, "SELECT * FROM start_orders WHERE action='停止'")
    assert s_order["status"] == "已回滚" and t_order["status"] == "已回滚"
    bucket = one(connect_fn, "SELECT * FROM contention_buckets")
    assert bucket["state"] == "已回滚" and bucket["verdict"] == "整笔回滚"
    assert bucket["reason"] == "让路闸超时" and bucket["final_state"] == "已停止"
    assert one(connect_fn, "SELECT state FROM yield_gates")["state"] == "已超时"
    assert one(connect_fn, "SELECT COUNT(*) c FROM pod_locks")["c"] == 0
    q = one(connect_fn, "SELECT * FROM quotas WHERE id=1")
    assert q["cpu_used"] == 0 and q["mem_used_mb"] == 0
    with connect_fn() as conn:
        checks = ss.verify_contention(conn, bucket["id"])
    assert all(c["通过"] for c in checks), checks
    rp = ss.replay_bucket(json.loads(bucket["replay_log"]))
    assert rp["verdict"] == "整笔回滚" and rp["pod"] == "已停止"
    assert rp["start_light"] == "已回滚" and rp["stop_light"] == "已回滚"


# ---------------------------------------------------------------- 双痕迹回滚

def test_double_trace_rollback(connect_fn, pod_stopped):
    pod_id = pod_stopped
    now = now_ms()
    with connect_fn() as conn:
        # 种双痕迹：启动在途舱体启动中，停止却已落库且单灯已完成
        s_order = conn.execute(
            "INSERT INTO start_orders(pod_id, action, target_image_id, status, note, created_at)"
            " VALUES (?, '启动', 1, '在途', '', '2026-01-01T00:00:00Z')", (pod_id,)).lastrowid
        t_order = conn.execute(
            "INSERT INTO start_orders(pod_id, action, target_image_id, status, note, created_at)"
            " VALUES (?, '停止', 1, '已完成', '停止成功', '2026-01-01T00:00:00Z')", (pod_id,)).lastrowid
        s_env = conn.execute(
            "INSERT INTO envelopes(order_id, pod_id, action, phase, snapshot, token, created_ms, updated_ms)"
            " VALUES (?, ?, '启动', '在途', ?, ?, ?, ?)",
            (s_order, pod_id, json.dumps({"run_state": "已停止", "image_id": 1}),
             f"env-{s_order}-plant", now, now)).lastrowid
        conn.execute(
            "INSERT INTO envelopes(order_id, pod_id, action, phase, snapshot, token, persisted_ms, created_ms, updated_ms)"
            " VALUES (?, ?, '停止', '已落库', ?, ?, ?, ?, ?)",
            (t_order, pod_id, json.dumps({"run_state": "启动中", "image_id": 1}),
             f"env-{t_order}-plant", now, now, now)).lastrowid
        conn.execute(
            "INSERT INTO pod_locks(pod_id, holder, envelope_id, acquired_ms, expires_ms, dirty)"
            " VALUES (?, ?, ?, ?, ?, 0)",
            (pod_id, f"env-{s_order}-plant", s_env, now, now + 60000))
        log = json.dumps([{"seq": 1, "at_ms": now, "step": "bucket.open", "actor": "system",
                           "detail": {"arm": "让路闸", "pre_state": "已停止",
                                      "start_order_id": s_order, "stop_order_id": t_order}}],
                         ensure_ascii=False)
        bucket_id = conn.execute(
            "INSERT INTO contention_buckets(pod_id, start_order_id, stop_order_id, arm, pre_snapshot,"
            " state, replay_log, created_ms) VALUES (?, ?, ?, '让路闸', ?, '在途', ?, ?)",
            (pod_id, s_order, t_order, json.dumps({"run_state": "已停止", "image_id": 1}),
             log, now)).lastrowid
        conn.execute(
            "INSERT INTO yield_gates(pod_id, bucket_id, waiter_order_id, blocker_order_id, state, created_ms)"
            " VALUES (?, ?, ?, ?, '等待中', ?)", (pod_id, bucket_id, t_order, s_order, now))
        conn.execute("UPDATE pods SET run_state='启动中' WHERE id=?", (pod_id,))
        conn.commit()

    with connect_fn() as conn:
        actions = ss.reconcile_pod(conn, pod_id)
        conn.commit()
    assert any("双痕迹" in a for a in actions), actions

    # 整笔退回争用前快照，双灯回写已回滚
    assert one(connect_fn, "SELECT run_state FROM pods WHERE id=?", (pod_id,))["run_state"] == "已停止"
    s_order = one(connect_fn, "SELECT * FROM start_orders WHERE action='启动'")
    t_order = one(connect_fn, "SELECT * FROM start_orders WHERE action='停止'")
    assert s_order["status"] == "已回滚" and t_order["status"] == "已回滚"
    assert "双痕迹" in t_order["note"]
    envs = all_rows(connect_fn, "SELECT * FROM envelopes")
    assert all(e["phase"] == "已回滚" for e in envs)
    bucket = one(connect_fn, "SELECT * FROM contention_buckets")
    assert bucket["state"] == "已回滚" and bucket["reason"] == "双痕迹"
    assert one(connect_fn, "SELECT state FROM yield_gates")["state"] == "已作废"
    assert one(connect_fn, "SELECT COUNT(*) c FROM pod_locks")["c"] == 0
    with connect_fn() as conn:
        checks = ss.verify_contention(conn, bucket["id"])
    assert all(c["通过"] for c in checks), checks


# ---------------------------------------------------------------- 半截脏锁

def test_half_cut_dirty_lock_recovered(connect_fn, pod_stopped):
    pod_id = pod_stopped
    now = now_ms()
    with connect_fn() as conn:
        # 种半截脏锁：启动信封崩在半路，锁已逾期，舱体挂着启动中
        order_id = conn.execute(
            "INSERT INTO start_orders(pod_id, action, target_image_id, status, note, created_at)"
            " VALUES (?, '启动', 1, '在途', '', '2026-01-01T00:00:00Z')", (pod_id,)).lastrowid
        env_id = conn.execute(
            "INSERT INTO envelopes(order_id, pod_id, action, phase, snapshot, token, created_ms, updated_ms)"
            " VALUES (?, ?, '启动', '在途', ?, ?, ?, ?)",
            (order_id, pod_id, json.dumps({"run_state": "已停止", "image_id": 1}),
             f"env-{order_id}-plant", now - 10000, now - 10000)).lastrowid
        conn.execute(
            "INSERT INTO pod_locks(pod_id, holder, envelope_id, acquired_ms, expires_ms, dirty)"
            " VALUES (?, ?, ?, ?, ?, 0)",
            (pod_id, f"env-{order_id}-plant", env_id, now - 10000, now - 1))
        conn.execute("UPDATE pods SET run_state='启动中' WHERE id=?", (pod_id,))
        conn.commit()

    with connect_fn() as conn:
        actions = ss.reconcile_pod(conn, pod_id)
        conn.commit()
    assert any("半截脏锁" in a for a in actions), actions
    # 舱体退回信封快照，灯回写已回滚，锁清
    assert one(connect_fn, "SELECT run_state FROM pods WHERE id=?", (pod_id,))["run_state"] == "已停止"
    assert one(connect_fn, "SELECT status FROM start_orders WHERE id=?", (order_id,))["status"] == "已回滚"
    assert one(connect_fn, "SELECT phase FROM envelopes WHERE id=?", (env_id,))["phase"] == "已回滚"
    assert one(connect_fn, "SELECT COUNT(*) c FROM pod_locks")["c"] == 0

    # 回收后新启动照常走完，不是只弹锁提示条
    r = ss.submit_order(connect_fn, pod_id, "启动", poll_ms=POLL)
    assert r["status"] == "已完成"
    assert one(connect_fn, "SELECT run_state FROM pods WHERE id=?", (pod_id,))["run_state"] == "运行中"


# ---------------------------------------------------------------- 互证：两臂让路结论不得并存

def test_dual_arm_contradiction_caught(connect_fn, pod_running):
    pod_id = pod_running
    now = now_ms()
    with connect_fn() as conn:
        # 种一只臂=停止抢先却带闸痕的桶：两臂结论并存，互证必须抓出
        s_order = conn.execute(
            "INSERT INTO start_orders(pod_id, action, target_image_id, status, note, created_at)"
            " VALUES (?, '启动', 1, '已完成', '启动成功', '2026-01-01T00:00:00Z')", (pod_id,)).lastrowid
        t_order = conn.execute(
            "INSERT INTO start_orders(pod_id, action, target_image_id, status, note, created_at)"
            " VALUES (?, '停止', 1, '已完成', '停止成功', '2026-01-01T00:00:00Z')", (pod_id,)).lastrowid
        conn.execute(
            "INSERT INTO envelopes(order_id, pod_id, action, phase, snapshot, token, persisted_ms, created_ms, updated_ms)"
            " VALUES (?, ?, '启动', '已确认', ?, ?, ?, ?, ?)",
            (s_order, pod_id, json.dumps({"run_state": "已停止", "image_id": 1}),
             f"env-{s_order}-plant", now + 2, now, now))
        conn.execute(
            "INSERT INTO envelopes(order_id, pod_id, action, phase, snapshot, token, persisted_ms, created_ms, updated_ms)"
            " VALUES (?, ?, '停止', '已确认', ?, ?, ?, ?, ?)",
            (t_order, pod_id, json.dumps({"run_state": "运行中", "image_id": 1}),
             f"env-{t_order}-plant", now + 1, now, now))
        log = json.dumps([
            {"seq": 1, "at_ms": now, "step": "bucket.open", "actor": "system",
             "detail": {"arm": "停止抢先", "pre_state": "运行中",
                        "start_order_id": s_order, "stop_order_id": t_order}},
            {"seq": 2, "at_ms": now + 1, "step": "env.persist", "actor": f"停止#{t_order}", "detail": {"state": "已停止"}},
            {"seq": 3, "at_ms": now + 1, "step": "env.confirm", "actor": f"停止#{t_order}", "detail": {}},
            {"seq": 4, "at_ms": now + 2, "step": "env.persist", "actor": f"启动#{s_order}", "detail": {"state": "运行中"}},
            {"seq": 5, "at_ms": now + 2, "step": "env.confirm", "actor": f"启动#{s_order}", "detail": {}},
            {"seq": 6, "at_ms": now + 3, "step": "bucket.close", "actor": "system",
             "detail": {"verdict": "双成", "final_state": "运行中"}},
        ], ensure_ascii=False)
        bucket_id = conn.execute(
            "INSERT INTO contention_buckets(pod_id, start_order_id, stop_order_id, arm, verdict, reason,"
            " pre_snapshot, final_state, state, replay_log, created_ms, closed_ms)"
            " VALUES (?, ?, ?, '停止抢先', '双成', NULL, ?, '运行中', '已收口', ?, ?, ?)",
            (pod_id, s_order, t_order, json.dumps({"run_state": "运行中", "image_id": 1}),
             log, now, now + 3)).lastrowid
        # 闸痕混进停止抢先臂
        conn.execute(
            "INSERT INTO yield_gates(pod_id, bucket_id, waiter_order_id, blocker_order_id, state, created_ms)"
            " VALUES (?, ?, ?, ?, '等待中', ?)", (pod_id, bucket_id, t_order, s_order, now))
        conn.commit()

    with connect_fn() as conn:
        checks = ss.verify_contention(conn, bucket_id)
    by_name = {c["名称"]: c["通过"] for c in checks}
    assert by_name["臂唯一"] is False, "双臂并存未被臂唯一抓出"
    assert not all(c["通过"] for c in checks)
    # 其余各证仍成立，说明抓的是臂并存而非误报
    assert by_name["信封证"] and by_name["灯证"] and by_name["重放证"]


# ---------------------------------------------------------------- 同舱对砸多轮：两臂不并存

def test_threaded_collision_single_arm(connect_fn, pod_stopped):
    pod_id = pod_stopped
    for rnd in range(12):
        barrier = threading.Barrier(2)
        boxes = {}

        def hit(action, key):
            barrier.wait()
            boxes[key] = ss.submit_order(connect_fn, pod_id, action,
                                         poll_ms=10, gate_timeout_ms=5000, lock_ttl_ms=15000)

        t1 = threading.Thread(target=hit, args=("启动", "s"))
        t2 = threading.Thread(target=hit, args=("停止", "t"))
        t1.start(); t2.start()
        t1.join(15); t2.join(15)
        assert not t1.is_alive() and not t2.is_alive(), f"第{rnd}轮卡死"

        # 双灯都到终态，不得在途并挂
        s = one(connect_fn, "SELECT * FROM start_orders WHERE id=?", (boxes["s"]["id"],))
        t = one(connect_fn, "SELECT * FROM start_orders WHERE id=?", (boxes["t"]["id"],))
        assert s["status"] in ("已完成", "已回滚"), s["status"]
        assert t["status"] in ("已完成", "已回滚"), t["status"]

        bucket = one(connect_fn, "SELECT * FROM contention_buckets ORDER BY id DESC LIMIT 1")
        assert bucket is not None and bucket["state"] in ("已收口", "已回滚")
        # 同舱不得挂着开着的桶
        assert one(connect_fn, "SELECT COUNT(*) c FROM contention_buckets WHERE state='在途'")["c"] == 0
        # 两臂让路结论不得并存：闸痕当且仅当让路闸臂
        gates = all_rows(connect_fn, "SELECT * FROM yield_gates WHERE bucket_id=?", (bucket["id"],))
        assert (len(gates) > 0) == (bucket["arm"] == "让路闸")
        # 舱体终态与桶记一致，互证全绿
        pod = one(connect_fn, "SELECT * FROM pods WHERE id=?", (pod_id,))
        assert pod["run_state"] == bucket["final_state"]
        with connect_fn() as conn:
            checks = ss.verify_contention(conn, bucket["id"])
        assert all(c["通过"] for c in checks), (rnd, checks)
        # 双成时两单都已完成且带争用回执
        if bucket["verdict"] == "双成":
            assert s["status"] == "已完成" and t["status"] == "已完成"
            assert "争用#" in s["note"] and "争用#" in t["note"]

        # 复位
        with connect_fn() as conn:
            conn.execute("DELETE FROM pod_locks")
            conn.execute("DELETE FROM yield_gates")
            conn.execute("DELETE FROM contention_buckets")
            conn.execute("DELETE FROM envelopes")
            conn.execute("UPDATE pods SET run_state='已停止' WHERE id=?", (pod_id,))
            conn.execute("UPDATE quotas SET cpu_used=0, mem_used_mb=0 WHERE id=1")
            conn.commit()


# ---------------------------------------------------------------- 顺序启停与接口烟测

def test_sequential_start_stop_no_contention(connect_fn, pod_stopped):
    pod_id = pod_stopped
    r1 = ss.submit_order(connect_fn, pod_id, "启动", poll_ms=POLL)
    assert r1["status"] == "已完成"
    assert one(connect_fn, "SELECT run_state FROM pods WHERE id=?", (pod_id,))["run_state"] == "运行中"
    r2 = ss.submit_order(connect_fn, pod_id, "停止", poll_ms=POLL)
    assert r2["status"] == "已完成"
    assert one(connect_fn, "SELECT run_state FROM pods WHERE id=?", (pod_id,))["run_state"] == "已停止"
    q = one(connect_fn, "SELECT * FROM quotas WHERE id=1")
    assert q["cpu_used"] == 0 and q["mem_used_mb"] == 0
    # 近窗内仍记一只让路闸桶（启动先确认），结局双成
    bucket = one(connect_fn, "SELECT * FROM contention_buckets")
    assert bucket["arm"] == "让路闸" and bucket["verdict"] == "双成"
    with connect_fn() as conn:
        checks = ss.verify_contention(conn, bucket["id"])
    assert all(c["通过"] for c in checks), checks


def test_api_smoke():
    from fastapi.testclient import TestClient
    from app.main import app

    client = TestClient(app)
    r = client.post("/api/启停单", json={"pod_id": 1, "action": "启动"})
    assert r.status_code == 200 and r.json()["status"] == "已完成"
    r = client.post("/api/启停单", json={"pod_id": 1, "action": "停止"})
    assert r.status_code == 200 and r.json()["status"] == "已完成"
    data = client.get("/api/对读").json()
    assert "争用桶" in data
    assert all(b["互证通过"] for b in data["争用桶"])
    r = client.post("/api/舱体/1/理账")
    assert r.status_code == 200 and r.json()["ok"] is True
