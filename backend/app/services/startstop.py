"""启停对砸并发引擎。

同舱启动写口与停止写口几乎同时按下时，两只信封（启动信封/停止信封）要么同成，
要么整笔退回争用前舱体快照。让路规则只有两条：

1. 让路闸：启动信封落库完成前停止请求进让路闸——启动未落库则停止不改舱体已停止，
   让路闸待启动落库后再放行。
2. 停止抢先：停止可抢先改舱体并回写启停单灯——停止可先落，但启动灯必须补齐可核；
   对读屏争用桶不得装成无在途却已停止收口。

让路闸超时与双痕迹都可回滚；半截脏锁由理账（reconcile）回收。
启动信封、停止信封、舱体锁、对读屏争用桶通过 verify_contention 互证，
两臂让路结论不得并存。关停与探活抢点不在此题。

终态裁决（全局唯一，不得混用）——四侧同屏可核：
  双完成（双成）必须能由 舱体锁旁读(pod_lock_events) ∪ 锁水位桶(contention_buckets)
  ∪ 启停回执对(start_orders/envelopes) ∪ 审计(events) 同屏可核重放。缺任一侧，
  就把“后到那笔”退回（单成退后），先到那笔维持；不以面板灯号宣称双完成。
  同舱一只开桶只认一对单；余留在途、第三笔连点、跨桶同单一律后到退回。
"""
from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager

from app.config import (
    CONTENTION_WINDOW_MS,
    GATE_TIMEOUT_MS,
    LOCK_TTL_MS,
    LOCK_WAIT_MS,
    POLL_MS,
)
from app.services.common import now_iso, now_ms

# 信封相位
PH_INFLIGHT = "在途"
PH_PERSISTED = "已落库"
PH_CONFIRMED = "已确认"
PH_ROLLED = "已回滚"
TERMINAL_PHASES = (PH_CONFIRMED, PH_ROLLED)

# 启停单灯
L_INFLIGHT = "在途"
L_DONE = "已完成"
L_FAILED = "失败"
L_ROLLED = "已回滚"

# 舱体态
P_STOPPED = "已停止"
P_STARTING = "启动中"
P_RUNNING = "运行中"
P_STOPPING = "停止中"

# 让路臂（同一争用只允许一臂）
ARM_GATE = "让路闸"
ARM_PREEMPT = "停止抢先"

# 争用结局
V_BOTH = "双成"
V_ROLLBACK = "整笔回滚"
V_SINGLE = "单成退后"

# 让路闸态
G_WAITING = "等待中"
G_RELEASED = "已放行"
G_TIMEOUT = "已超时"
G_VOID = "已作废"

# 争用桶态
B_OPEN = "在途"
B_CLOSED = "已收口"
B_ROLLED = "已回滚"

ACT_START = "启动"
ACT_STOP = "停止"

_START_ALIASES = ("启动", "start", "Start")
_STOP_ALIASES = ("停止", "stop", "Stop")


def normalize_action(raw: str) -> str | None:
    a = (raw or "").strip()
    if a in _START_ALIASES:
        return ACT_START
    if a in _STOP_ALIASES:
        return ACT_STOP
    return None


@contextmanager
def _tx(connect_fn):
    """单写者事务：BEGIN IMMEDIATE 让并发开信封严格串行，避免快照过期。

    WAL 下并发取写锁偶尔会绕过 busy_timeout 直接抛 database is locked；
    连点叠令时这不是业务失败，只作短暂争用处理——有界重试后再放弃。
    """
    conn = connect_fn()
    conn.isolation_level = None
    deadline = time.monotonic() + 5.0
    while True:
        try:
            conn.execute("BEGIN IMMEDIATE")
            break
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or time.monotonic() >= deadline:
                conn.close()
                raise
            time.sleep(0.02)
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------- 基础读写

def _pod(conn, pod_id: int):
    return conn.execute("SELECT * FROM pods WHERE id=?", (pod_id,)).fetchone()


def _quota(conn):
    return conn.execute("SELECT * FROM quotas WHERE id=1").fetchone()


def _snapshot_of(pod_row) -> str:
    return json.dumps(
        {"run_state": pod_row["run_state"], "image_id": pod_row["image_id"]},
        ensure_ascii=False,
    )


def _restore_pod(conn, pod_id: int, snapshot_json: str) -> None:
    snap = json.loads(snapshot_json)
    conn.execute(
        "UPDATE pods SET run_state=?, image_id=? WHERE id=?",
        (snap["run_state"], snap["image_id"], pod_id),
    )


def _event(conn, kind: str, ref: str, detail: str) -> None:
    conn.execute(
        "INSERT INTO events(kind, ref, detail, at) VALUES (?,?,?,?)",
        (kind, ref, detail, now_iso()),
    )


def _env(conn, env_id: int):
    return conn.execute("SELECT * FROM envelopes WHERE id=?", (env_id,)).fetchone()


def _env_by_order(conn, order_id: int):
    return conn.execute("SELECT * FROM envelopes WHERE order_id=?", (order_id,)).fetchone()


def _order(conn, order_id: int):
    return conn.execute("SELECT * FROM start_orders WHERE id=?", (order_id,)).fetchone()


def _open_bucket(conn, pod_id: int):
    return conn.execute(
        "SELECT * FROM contention_buckets WHERE pod_id=? AND state=? ORDER BY id DESC LIMIT 1",
        (pod_id, B_OPEN),
    ).fetchone()


def _bucket_of_order(conn, pod_id: int, order_id: int):
    return conn.execute(
        """
        SELECT * FROM contention_buckets
        WHERE pod_id=? AND (start_order_id=? OR stop_order_id=?)
        ORDER BY id DESC LIMIT 1
        """,
        (pod_id, order_id, order_id),
    ).fetchone()


def _bucket_step(conn, bucket_id: int, step: str, actor: str, detail: dict) -> None:
    row = conn.execute(
        "SELECT replay_log FROM contention_buckets WHERE id=?", (bucket_id,)
    ).fetchone()
    if not row:
        return
    log = json.loads(row["replay_log"])
    log.append({"seq": len(log) + 1, "at_ms": now_ms(), "step": step, "actor": actor, "detail": detail})
    conn.execute(
        "UPDATE contention_buckets SET replay_log=? WHERE id=?",
        (json.dumps(log, ensure_ascii=False), bucket_id),
    )


def _bucket_step_if_open(conn, pod_id: int, step: str, actor: str, detail: dict) -> None:
    bucket = _open_bucket(conn, pod_id)
    if bucket:
        _bucket_step(conn, bucket["id"], step, actor, detail)


# ---------------------------------------------------------------- 舱体锁

def _lock_row(conn, pod_id: int):
    return conn.execute("SELECT * FROM pod_locks WHERE pod_id=?", (pod_id,)).fetchone()


def _lock_side_read(conn, pod_id: int, envelope_id: int | None, order_id: int | None,
                    action: str | None, event: str, holder: str | None) -> None:
    """舱体锁旁读留痕：取锁/放锁/回收都写耐久事件，供双完成四侧同屏核验。"""
    conn.execute(
        """
        INSERT INTO pod_lock_events(pod_id, envelope_id, order_id, action, event, holder, at_ms)
        VALUES (?,?,?,?,?,?,?)
        """,
        (pod_id, envelope_id, order_id, action, event, holder, now_ms()),
    )


def _try_lock(conn, pod_id: int, token: str, env_id: int, ttl_ms: int) -> bool:
    now = now_ms()
    try:
        conn.execute(
            "INSERT INTO pod_locks(pod_id, holder, envelope_id, acquired_ms, expires_ms, dirty) VALUES (?,?,?,?,?,0)",
            (pod_id, token, env_id, now, now + ttl_ms),
        )
    except sqlite3.IntegrityError:
        return False
    env = _env(conn, env_id)
    _lock_side_read(conn, pod_id, env_id, env["order_id"] if env else None,
                    env["action"] if env else None, "取锁", token)
    return True


def _release_lock(conn, pod_id: int, token: str) -> None:
    lock = _lock_row(conn, pod_id)
    if lock and lock["holder"] == token:
        env = _env(conn, lock["envelope_id"])
        _lock_side_read(conn, pod_id, lock["envelope_id"],
                        env["order_id"] if env else None,
                        env["action"] if env else None, "放锁", token)
    conn.execute("DELETE FROM pod_locks WHERE pod_id=? AND holder=?", (pod_id, token))


def _force_release_locks(conn, pod_id: int, env_ids: set[int]) -> None:
    lock = _lock_row(conn, pod_id)
    if lock and lock["envelope_id"] in env_ids:
        env = _env(conn, lock["envelope_id"])
        _lock_side_read(conn, pod_id, lock["envelope_id"],
                        env["order_id"] if env else None,
                        env["action"] if env else None, "回收放锁", lock["holder"])
        conn.execute("DELETE FROM pod_locks WHERE pod_id=?", (pod_id,))


# ---------------------------------------------------------------- 信封

def _open_envelope(conn, pod_id: int, order_id: int, action: str, snapshot: str) -> tuple[int, str]:
    token = f"env-{order_id}-{uuid.uuid4().hex[:8]}"
    now = now_ms()
    cur = conn.execute(
        """
        INSERT INTO envelopes(order_id, pod_id, action, phase, snapshot, token, created_ms, updated_ms)
        VALUES (?,?,?,?,?,?,?,?)
        """,
        (order_id, pod_id, action, PH_INFLIGHT, snapshot, token, now, now),
    )
    return cur.lastrowid, token


def _set_phase(conn, env_id: int, expect: str, to: str) -> bool:
    now = now_ms()
    persisted = now if to == PH_PERSISTED else None
    cur = conn.execute(
        """
        UPDATE envelopes SET phase=?, updated_ms=?,
               persisted_ms=COALESCE(?, persisted_ms)
        WHERE id=? AND phase=?
        """,
        (to, now, persisted, env_id, expect),
    )
    return cur.rowcount == 1


def _record_quota_delta(conn, env_id: int, cpu: float, mem: int) -> None:
    conn.execute(
        "UPDATE envelopes SET quota_cpu=quota_cpu+?, quota_mem=quota_mem+? WHERE id=?",
        (cpu, mem, env_id),
    )


def _rollback_envelope(conn, env, reason: str, restore_snapshot: str | None = None,
                       step_bucket: bool = True) -> None:
    """信封回滚：可选恢复舱体快照、反冲配额、回写启停单灯。幂等（已回滚则跳过）。"""
    if env["phase"] == PH_ROLLED:
        return
    if restore_snapshot is not None:
        _restore_pod(conn, env["pod_id"], restore_snapshot)
    cpu = float(env["quota_cpu"])
    mem = int(env["quota_mem"])
    if cpu or mem:
        conn.execute(
            "UPDATE quotas SET cpu_used=MAX(0, cpu_used-?), mem_used_mb=MAX(0, mem_used_mb-?) WHERE id=1",
            (cpu, mem),
        )
        conn.execute("UPDATE envelopes SET quota_cpu=0, quota_mem=0 WHERE id=?", (env["id"],))
    conn.execute(
        "UPDATE envelopes SET phase=?, updated_ms=? WHERE id=?",
        (PH_ROLLED, now_ms(), env["id"]),
    )
    conn.execute(
        "UPDATE start_orders SET status=?, note=? WHERE id=?",
        (L_ROLLED, reason, env["order_id"]),
    )
    if step_bucket:
        _bucket_step_if_open(
            conn, env["pod_id"], "env.rollback", f"{env['action']}#{env['order_id']}", {"reason": reason}
        )


# ---------------------------------------------------------------- 争用桶

def _find_rival(conn, pod_id: int, action: str, window_ms: int, exclude_order_id: int):
    """找对面信封：在途，或近窗内刚确认且尚未归属任何争用桶的。

    已归属某只桶（在途或已收口）的单号一律不算对面：否则同一笔会被后到的连点
    当成新对面再挂进第二只桶，回执对与锁水位桶就双花了。
    """
    since = now_ms() - window_ms
    return conn.execute(
        """
        SELECT e.* FROM envelopes e
        WHERE e.pod_id=? AND e.action=? AND e.order_id<>?
          AND (e.phase NOT IN (?, ?) OR (e.phase=? AND e.updated_ms>=?))
          AND e.order_id NOT IN (
                SELECT start_order_id FROM contention_buckets WHERE pod_id=?
                UNION
                SELECT stop_order_id FROM contention_buckets WHERE pod_id=?)
        ORDER BY e.id DESC LIMIT 1
        """,
        (
            pod_id,
            action,
            exclude_order_id,
            PH_CONFIRMED,
            PH_ROLLED,
            PH_CONFIRMED,
            since,
            pod_id,
            pod_id,
        ),
    ).fetchone()


def _ensure_bucket(conn, pod_row, start_order_id: int, stop_order_id: int, arm: str, pre_snapshot: str, rival=None):
    """开争用桶；同舱同刻只许一只开着的桶，撞唯一索引则读现成的那只。

    返回 (桶, 是否含本对)。若现成的桶挂的是别的单对，本笔属后到，须退回。
    对面信封若已落库/已确认，开桶时把它的痕迹补记进日志，保证日志自含、读侧可重放。
    """
    pod_id = pod_row["id"]
    members = {start_order_id, stop_order_id}

    def _matched(b):
        return b is not None and {b["start_order_id"], b["stop_order_id"]} == members

    existing = _open_bucket(conn, pod_id)
    if existing:
        return existing, _matched(existing)
    # 开桶即清扫同舱余留在途信封（非本对成员）：锁水位桶只认本桶两笔，
    # 余留者在途会把“双完成”撑成三笔双花，必须先退回。
    for stray in conn.execute(
        "SELECT * FROM envelopes WHERE pod_id=? AND phase IN (?, ?)", (pod_id, PH_INFLIGHT, PH_PERSISTED)
    ).fetchall():
        if stray["order_id"] not in members:
            _return_intruder(conn, "开桶清扫", stray, "锁水位桶:余留在途")
    log = [
        {
            "seq": 1,
            "at_ms": now_ms(),
            "step": "bucket.open",
            "actor": "system",
            "detail": {
                "arm": arm,
                "pre_state": json.loads(pre_snapshot)["run_state"],
                "start_order_id": start_order_id,
                "stop_order_id": stop_order_id,
            },
        }
    ]
    if rival is not None and rival["phase"] in (PH_PERSISTED, PH_CONFIRMED):
        actor = f"{rival['action']}#{rival['order_id']}"
        landed = P_RUNNING if rival["action"] == ACT_START else P_STOPPED
        log.append({
            "seq": len(log) + 1,
            "at_ms": rival["persisted_ms"] or now_ms(),
            "step": "env.persist",
            "actor": actor,
            "detail": {"state": landed},
        })
        if rival["phase"] == PH_CONFIRMED:
            log.append({
                "seq": len(log) + 1,
                "at_ms": rival["updated_ms"],
                "step": "env.confirm",
                "actor": actor,
                "detail": {},
            })
    try:
        cur = conn.execute(
            """
            INSERT INTO contention_buckets(pod_id, start_order_id, stop_order_id, arm, pre_snapshot,
                                           state, replay_log, created_ms)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                pod_id,
                start_order_id,
                stop_order_id,
                arm,
                pre_snapshot,
                B_OPEN,
                json.dumps(log, ensure_ascii=False),
                now_ms(),
            ),
        )
        # 对面已确认的单灯在此补齐争用回执，可核可溯
        if rival is not None and rival["phase"] == PH_CONFIRMED:
            conn.execute(
                "UPDATE start_orders SET note = note || ? WHERE id=? AND note NOT LIKE '%争用#%'",
                (f" · 争用#{cur.lastrowid}:{arm}", rival["order_id"]),
            )
        _event(conn, "启停对砸", str(cur.lastrowid), f"争用开桶 臂={arm} 舱={pod_row['name']}")
        return _open_bucket(conn, pod_id), True
    except sqlite3.IntegrityError:
        b = _open_bucket(conn, pod_id)
        return b, _matched(b)


def _rollback_bucket(conn, bucket, reason: str) -> None:
    """整笔退回争用前舱体快照：两信封回滚、闸作废、锁释放、桶收口。"""
    pod_id = bucket["pod_id"]
    s_env = _env_by_order(conn, bucket["start_order_id"])
    t_env = _env_by_order(conn, bucket["stop_order_id"])
    note = f"争用#{bucket['id']} 整笔回滚:{reason}"
    for env in (s_env, t_env):
        if env is not None:
            _rollback_envelope(conn, env, note)
    _restore_pod(conn, pod_id, bucket["pre_snapshot"])
    _bucket_step(conn, bucket["id"], "pod.restore", "system", {"state": json.loads(bucket["pre_snapshot"])["run_state"]})
    conn.execute(
        "UPDATE yield_gates SET state=?, resolved_ms=? WHERE bucket_id=? AND state=?",
        (G_VOID, now_ms(), bucket["id"], G_WAITING),
    )
    _force_release_locks(conn, pod_id, {e["id"] for e in (s_env, t_env) if e})
    final = json.loads(bucket["pre_snapshot"])["run_state"]
    conn.execute(
        """
        UPDATE contention_buckets SET state=?, verdict=?, reason=?, final_state=?, closed_ms=?
        WHERE id=?
        """,
        (B_ROLLED, V_ROLLBACK, reason, final, now_ms(), bucket["id"]),
    )
    _bucket_step(conn, bucket["id"], "bucket.close", "system", {"verdict": V_ROLLBACK, "reason": reason, "final_state": final})
    _event(conn, "启停对砸", str(bucket["id"]), f"整笔回滚 原因={reason}")


# ---------------------------------------------------------------- 四侧同屏可核

def _landed_state(action: str) -> str:
    return P_RUNNING if action == ACT_START else P_STOPPED


def _four_side_check(conn, bucket) -> list[str]:
    """双完成四侧同屏可核：锁旁读 ∪ 锁水位桶 ∪ 启停回执对 ∪ 审计。

    返回缺失侧清单；空清单才允许宣称双完成。
    """
    pod_id = bucket["pod_id"]
    s_env = _env_by_order(conn, bucket["start_order_id"])
    t_env = _env_by_order(conn, bucket["stop_order_id"])
    missing: list[str] = []
    # 锁水位桶：桶行必须真挂着一启动一停止两只信封
    if s_env is None or t_env is None:
        missing.append("锁水位桶:信封缺")
    elif s_env["action"] != ACT_START or t_env["action"] != ACT_STOP:
        missing.append(f"锁水位桶:成员动作错({s_env['action']}/{t_env['action']})")
    for env in (s_env, t_env):
        if env is None:
            continue
        # 锁旁读：该信封必须留下取锁旁读痕迹
        got = conn.execute(
            "SELECT 1 FROM pod_lock_events WHERE pod_id=? AND envelope_id=? AND event='取锁' LIMIT 1",
            (pod_id, env["id"]),
        ).fetchone()
        if not got:
            missing.append(f"锁旁读:{env['action']}#{env['order_id']}")
        # 启停回执对：该笔的回执行必须在
        if _order(conn, env["order_id"]) is None:
            missing.append(f"回执对:{env['action']}#{env['order_id']}")
    # 审计：争用开桶必须有可核审计痕
    ev = conn.execute(
        "SELECT 1 FROM events WHERE kind='启停对砸' AND ref=? LIMIT 1", (str(bucket["id"]),)
    ).fetchone()
    if not ev:
        missing.append("审计:争用痕缺")
    return missing


def _settle_single(conn, bucket, confirmed_env, rolled_env, missing: list[str]) -> None:
    """单成退后收口：一确认、一回滚；舱体态取存活那笔的落库终态。"""
    final = _landed_state(confirmed_env["action"])
    conn.execute("UPDATE pods SET run_state=? WHERE id=?", (final, bucket["pod_id"]))
    if rolled_env is not None:
        conn.execute(
            "UPDATE start_orders SET note = note || ? WHERE id=?",
            (f" · 退后#{rolled_env['order_id']}", confirmed_env["order_id"]),
        )
    reason = ("缺侧退回:" + ",".join(missing)) if missing else "单臂未确认"
    conn.execute(
        """
        UPDATE contention_buckets SET state=?, verdict=?, reason=?, final_state=?, closed_ms=?
        WHERE id=?
        """,
        (B_ROLLED, V_SINGLE, reason, final, now_ms(), bucket["id"]),
    )
    _bucket_step(
        conn, bucket["id"], "bucket.close", "system",
        {"verdict": V_SINGLE, "reason": reason, "final_state": final,
         "returned": rolled_env["order_id"] if rolled_env is not None else None},
    )
    _event(conn, "启停对砸", str(bucket["id"]),
           f"单成退后 原因={reason} 存活#{confirmed_env['order_id']}")


def _return_later(conn, bucket, env, missing: list[str]) -> None:
    """缺侧退回后到那笔：只回滚本笔（反冲配额），先到那笔的落库维持不动。"""
    reason = "缺侧退回:" + ",".join(missing)
    rival_order_id = bucket["stop_order_id"] if env["action"] == ACT_START else bucket["start_order_id"]
    rival_env = _env_by_order(conn, rival_order_id)
    # 若后到那笔已落库，回滚要把它改过的舱体/镜像退回争用前快照
    restore = bucket["pre_snapshot"] if env["phase"] == PH_PERSISTED else None
    _rollback_envelope(conn, env, f"争用#{bucket['id']} {reason}", restore_snapshot=restore)
    confirmed_env = rival_env if (rival_env is not None and rival_env["phase"] == PH_CONFIRMED) else None
    if confirmed_env is not None:
        _settle_single(conn, bucket, confirmed_env, env, missing)
    else:
        final = _pod(conn, bucket["pod_id"])["run_state"]
        conn.execute(
            """
            UPDATE contention_buckets SET state=?, verdict=?, reason=?, final_state=?, closed_ms=?
            WHERE id=?
            """,
            (B_ROLLED, V_SINGLE, reason, final, now_ms(), bucket["id"]),
        )
        _bucket_step(
            conn, bucket["id"], "bucket.close", "system",
            {"verdict": V_SINGLE, "reason": reason, "final_state": final, "returned": env["order_id"]},
        )
        _event(conn, "启停对砸", str(bucket["id"]), f"单成退后 缺侧={','.join(missing)} 退回#{env['order_id']}")


def _return_intruder(conn, bucket_id, env, reason: str) -> None:
    """后到单不在已开桶成员里：只退本笔，桶与先到那笔维持不动，不污桶日志。"""
    if env is None:
        return
    restore = env["snapshot"] if env["phase"] == PH_PERSISTED else None
    _rollback_envelope(
        conn, env, f"争用#{bucket_id} 后到退回:{reason}",
        restore_snapshot=restore, step_bucket=False,
    )
    _force_release_locks(conn, env["pod_id"], {env["id"]})
    _event(conn, "启停对砸", str(bucket_id), f"后到退回#{env['order_id']} 原因={reason}")


def _guard_existing_bucket(conn, pod_id: int, order_id: int) -> dict | None:
    """同舱已开着争用桶时，本笔若不在桶成员里即“锁水位桶缺席”的后到单，须退回。

    返回该桶表示本笔为后到；返回 None 表示无冲突（或本笔正是桶成员）。
    """
    bucket = _open_bucket(conn, pod_id)
    if bucket and order_id not in (bucket["start_order_id"], bucket["stop_order_id"]):
        return bucket
    return None


def _maybe_close_bucket(conn, pod_id: int) -> None:
    """两只信封都到终态时收口：双确认且四侧可核=双成，缺侧退回后到那笔。"""
    bucket = _open_bucket(conn, pod_id)
    if not bucket:
        return
    s_env = _env_by_order(conn, bucket["start_order_id"])
    t_env = _env_by_order(conn, bucket["stop_order_id"])
    if not s_env or not t_env:
        return
    if s_env["phase"] not in TERMINAL_PHASES or t_env["phase"] not in TERMINAL_PHASES:
        return
    if s_env["phase"] != PH_CONFIRMED or t_env["phase"] != PH_CONFIRMED:
        # 只有一方确认、另一方回滚：先到那笔是单成，收口记单成退后而非“整笔回滚”，
        # 免得好端端一笔已完成的回执被桶说成整笔回滚，跟回执对各说各话。
        if PH_CONFIRMED in (s_env["phase"], t_env["phase"]):
            confirmed_env = s_env if s_env["phase"] == PH_CONFIRMED else t_env
            rolled_env = t_env if confirmed_env is s_env else s_env
            _settle_single(conn, bucket, confirmed_env, rolled_env, [])
            return
        final = _pod(conn, pod_id)["run_state"]
        verdict, reason, state = V_ROLLBACK, "单臂未确认", B_ROLLED
        conn.execute(
            """
            UPDATE contention_buckets SET state=?, verdict=?, reason=?, final_state=?, closed_ms=?
            WHERE id=?
            """,
            (state, verdict, reason, final, now_ms(), bucket["id"]),
        )
        _bucket_step(conn, bucket["id"], "bucket.close", "system", {"verdict": verdict, "reason": reason, "final_state": final})
        _event(conn, "启停对砸", str(bucket["id"]), f"争用收口 结局={verdict} 臂={bucket['arm']}")
        return
    # 双确认：必须四侧同屏可核；缺任一侧就把后到那笔退回
    missing = _four_side_check(conn, bucket)
    if missing:
        s_p = s_env["persisted_ms"] or 0
        t_p = t_env["persisted_ms"] or 0
        later = s_env if s_p >= t_p else t_env
        _return_later(conn, bucket, later, missing)
        return
    final = _pod(conn, pod_id)["run_state"]
    verdict, reason, state = V_BOTH, "", B_CLOSED
    conn.execute(
        """
        UPDATE contention_buckets SET state=?, verdict=?, reason=?, final_state=?, closed_ms=?
        WHERE id=?
        """,
        (state, verdict, reason, final, now_ms(), bucket["id"]),
    )
    _bucket_step(conn, bucket["id"], "bucket.close", "system", {"verdict": verdict, "reason": reason, "final_state": final})
    _event(conn, "启停对砸", str(bucket["id"]), f"争用收口 结局={verdict} 臂={bucket['arm']}")


# ---------------------------------------------------------------- 理账：半截脏锁 + 双痕迹

def _persist_order_violates_arm(arm: str, start_env, stop_env) -> bool:
    """落库次序违背让路臂即双痕迹：让路闸臂停止先落，或停止抢先臂启动先落。"""
    s_p = start_env["persisted_ms"]
    t_p = stop_env["persisted_ms"]
    if arm == ARM_GATE:
        return t_p is not None and (s_p is None or t_p < s_p)
    return s_p is not None and (t_p is None or s_p < t_p)


def reconcile_pod(conn, pod_id: int) -> list[str]:
    """理账：回收半截脏锁、检测双痕迹并整笔回滚。返回动作说明。"""
    actions: list[str] = []
    now = now_ms()
    lock = _lock_row(conn, pod_id)
    if lock and lock["expires_ms"] < now:
        env = _env(conn, lock["envelope_id"])
        if env and env["phase"] not in TERMINAL_PHASES:
            conn.execute("UPDATE pod_locks SET dirty=1 WHERE pod_id=?", (pod_id,))
            bucket = _bucket_of_order(conn, pod_id, env["order_id"])
            if bucket and bucket["state"] == B_OPEN:
                _rollback_bucket(conn, bucket, "半截脏锁")
            else:
                _rollback_envelope(conn, env, "半截脏锁回收", restore_snapshot=env["snapshot"])
            actions.append(f"半截脏锁回收:信封#{env['id']}")
            _event(conn, "舱体锁", str(pod_id), f"半截脏锁回收 信封#{env['id']}")
        conn.execute("DELETE FROM pod_locks WHERE pod_id=?", (pod_id,))
    bucket = _open_bucket(conn, pod_id)
    if bucket:
        s_env = _env_by_order(conn, bucket["start_order_id"])
        t_env = _env_by_order(conn, bucket["stop_order_id"])
        if s_env and t_env and _persist_order_violates_arm(bucket["arm"], s_env, t_env):
            _rollback_bucket(conn, bucket, "双痕迹")
            actions.append(f"双痕迹回滚:争用#{bucket['id']}")
            _event(conn, "启停对砸", str(bucket["id"]), "双痕迹并挂，整笔回滚")
        elif (s_env and t_env and s_env["phase"] in TERMINAL_PHASES and t_env["phase"] in TERMINAL_PHASES
              and {s_env["phase"], t_env["phase"]} == {PH_CONFIRMED}):
            # 双确认却仍开桶：此前已双记成功；补收口按四侧核，缺侧退后到那笔，
            # 不再让“假双完成”长期与舱体锁旁读各说各话。
            _maybe_close_bucket(conn, pod_id)
            actions.append(f"双确认补收口:争用#{bucket['id']}")
    actions.extend(_sweep_orphan_double_done(conn, pod_id))
    return actions


def _sweep_orphan_double_done(conn, pod_id: int) -> list[str]:
    """双完成而锁水位桶空：一对真并发过的启动/停止都确认了，却没有桶背书。

    “被桶背书”指该单号出现在某只争用桶的启动/停止成员里（无论桶态）。只有两笔
    信封寿命真实交叠（后开的那笔在先前那笔结束前就已开信封）才算“对砸”，才可判
    成假双完成；先后落库、时间不交叠的普通顺序启停不受影响。命中则退回后到那笔，
    先到那笔维持，使回执对与舱体锁旁读恢复同屏可核。
    """
    actions: list[str] = []
    confirmed = conn.execute(
        """
        SELECT e.* FROM envelopes e
        WHERE e.pod_id=? AND e.phase=?
          AND e.order_id NOT IN (
              SELECT start_order_id FROM contention_buckets WHERE pod_id=?
              UNION
              SELECT stop_order_id FROM contention_buckets WHERE pod_id=?)
        ORDER BY e.created_ms ASC, e.id ASC
        """,
        (pod_id, PH_CONFIRMED, pod_id, pod_id),
    ).fetchall()
    starts = [e for e in confirmed if e["action"] == ACT_START]
    stops = [e for e in confirmed if e["action"] == ACT_STOP]
    for s_env in starts:
        for t_env in stops:
            # 寿命交叠才属对砸：两笔在对方结束前都已开信封
            overlap = (t_env["created_ms"] < s_env["updated_ms"]
                       and s_env["created_ms"] < t_env["updated_ms"])
            if not overlap:
                continue
            later = s_env if (s_env["persisted_ms"] or 0) >= (t_env["persisted_ms"] or 0) else t_env
            _rollback_envelope(conn, later, "假双完成退回:锁水位桶空", step_bucket=False)
            _force_release_locks(conn, pod_id, {later["id"]})
            _event(conn, "启停对砸", str(pod_id),
                   f"假双完成退回#{later['order_id']} 缺锁水位桶")
            actions.append(f"假双完成退回:单#{later['order_id']}")
            return actions
    return actions


# ---------------------------------------------------------------- 轮询等待

def _wait_lock(connect_fn, pod_id: int, token: str, env_id: int, action: str,
               lock_wait_ms: int, lock_ttl_ms: int, poll_ms: int) -> bool:
    """等锁：持有者逾期则顺手理账回收；拿到锁即落痕迹（启动→启动中，停止→停止中）。"""
    deadline = now_ms() + lock_wait_ms
    while now_ms() < deadline:
        with _tx(connect_fn) as conn:
            reconcile_pod(conn, pod_id)
            holder = _lock_row(conn, pod_id)
            if holder is None and _try_lock(conn, pod_id, token, env_id, lock_ttl_ms):
                env = _env(conn, env_id)
                if env["phase"] != PH_INFLIGHT:
                    # 等锁期间自家信封已被理账整笔回滚：放锁退回，不留脏锁
                    _release_lock(conn, pod_id, token)
                    return False
                pod = _pod(conn, pod_id)
                if action == ACT_START:
                    conn.execute("UPDATE pods SET run_state=? WHERE id=?", (P_STARTING, pod_id))
                    _bucket_step_if_open(conn, pod_id, "pod.trace", f"启动#{env['order_id']}", {"state": P_STARTING})
                elif pod["run_state"] == P_RUNNING:
                    conn.execute("UPDATE pods SET run_state=? WHERE id=?", (P_STOPPING, pod_id))
                    _bucket_step_if_open(conn, pod_id, "pod.trace", f"停止#{env['order_id']}", {"state": P_STOPPING})
                return True
        time.sleep(poll_ms / 1000)
    return False


def _wait_gate(connect_fn, gate_id: int, blocker_env_id: int, gate_timeout_ms: int, poll_ms: int) -> str:
    """让路闸等待：启动落库（或已确认/已回滚）即放行；超时由调用方整笔回滚。"""
    deadline = now_ms() + gate_timeout_ms
    while now_ms() < deadline:
        with _tx(connect_fn) as conn:
            env = _env(conn, blocker_env_id)
            if env is None or env["phase"] != PH_INFLIGHT:
                cur = conn.execute(
                    "UPDATE yield_gates SET state=?, resolved_ms=? WHERE id=? AND state=?",
                    (G_RELEASED, now_ms(), gate_id, G_WAITING),
                )
                if cur.rowcount == 1:
                    gate = conn.execute("SELECT * FROM yield_gates WHERE id=?", (gate_id,)).fetchone()
                    _bucket_step(conn, gate["bucket_id"], "gate.release", f"停止#{gate['waiter_order_id']}", {})
                return "released"
        time.sleep(poll_ms / 1000)
    return "timeout"


# ---------------------------------------------------------------- 下单主流程

def _fail_order(conn, pod_id: int, action: str, img_id, note: str) -> dict:
    cur = conn.execute(
        "INSERT INTO start_orders(pod_id, action, target_image_id, status, note, created_at) VALUES (?,?,?,?,?,?)",
        (pod_id, action, img_id, L_FAILED, note, now_iso()),
    )
    _event(conn, "启停单", str(cur.lastrowid), f"{action} → {L_FAILED} {note}")
    return {"id": cur.lastrowid, "status": L_FAILED, "note": note}


def _current_result(conn, order_id: int) -> dict:
    od = _order(conn, order_id)
    return {"id": order_id, "status": od["status"], "note": od["note"]}


def submit_order(connect_fn, pod_id: int, action_raw: str, target_image_id: int | None = None, *,
                 gate_timeout_ms: int = GATE_TIMEOUT_MS, lock_ttl_ms: int = LOCK_TTL_MS,
                 lock_wait_ms: int = LOCK_WAIT_MS, poll_ms: int = POLL_MS,
                 window_ms: int = CONTENTION_WINDOW_MS, hooks: dict | None = None) -> dict:
    action = normalize_action(action_raw)
    hooks = hooks or {}
    with _tx(connect_fn) as conn:
        pod = _pod(conn, pod_id)
        if not pod:
            return {"错误": "舱体不存在"}
        if action is None:
            img_id = target_image_id or pod["image_id"]
            return _fail_order(conn, pod_id, (action_raw or "").strip(), img_id, f"未知动作:{(action_raw or '').strip()}")
    if action == ACT_START:
        return _submit_start(connect_fn, pod_id, target_image_id,
                             gate_timeout_ms=gate_timeout_ms, lock_ttl_ms=lock_ttl_ms,
                             lock_wait_ms=lock_wait_ms, poll_ms=poll_ms, window_ms=window_ms, hooks=hooks)
    return _submit_stop(connect_fn, pod_id, target_image_id,
                        gate_timeout_ms=gate_timeout_ms, lock_ttl_ms=lock_ttl_ms,
                        lock_wait_ms=lock_wait_ms, poll_ms=poll_ms, window_ms=window_ms, hooks=hooks)


def _submit_start(connect_fn, pod_id: int, target_image_id, *, gate_timeout_ms, lock_ttl_ms,
                  lock_wait_ms, poll_ms, window_ms, hooks) -> dict:
    # 事务一：理账、校验、开信封、发现争用、抢锁落痕迹
    with _tx(connect_fn) as conn:
        reconcile_pod(conn, pod_id)
        pod = _pod(conn, pod_id)
        img_id = target_image_id or pod["image_id"]
        img = conn.execute("SELECT * FROM images WHERE id=?", (img_id,)).fetchone()
        if not img or img["pull_state"] != "已就绪":
            return _fail_order(conn, pod_id, ACT_START, img_id, "镜像未就绪")
        q = _quota(conn)
        need_cpu = float(pod["cpu_quota"])
        need_mem = int(pod["mem_quota_mb"])
        if float(q["cpu_used"]) + need_cpu > float(q["cpu_limit"]) or int(q["mem_used_mb"]) + need_mem > int(q["mem_limit_mb"]):
            return _fail_order(conn, pod_id, ACT_START, img_id, "配额不足")
        cur = conn.execute(
            "INSERT INTO start_orders(pod_id, action, target_image_id, status, note, created_at) VALUES (?,?,?,?,?,?)",
            (pod_id, ACT_START, img_id, L_INFLIGHT, "", now_iso()),
        )
        order_id = cur.lastrowid
        env_id, token = _open_envelope(conn, pod_id, order_id, ACT_START, _snapshot_of(pod))
        intruder = _guard_existing_bucket(conn, pod_id, order_id)
        if intruder is not None:
            # 已开桶不认本笔：锁水位桶缺席，本笔属后到，只退本笔
            _return_intruder(conn, intruder["id"], _env_by_order(conn, order_id), "锁水位桶:桶已属他单")
            return _current_result(conn, order_id)
        rival = _find_rival(conn, pod_id, ACT_STOP, window_ms, order_id)
        in_bucket = True
        bucket = None
        if rival:
            # 停止先伸手 → 停止抢先臂；争用前快照取先开的停止信封
            bucket, in_bucket = _ensure_bucket(conn, pod, order_id, rival["order_id"], ARM_PREEMPT, rival["snapshot"], rival=rival)
            if not in_bucket:
                # 开着的桶挂的是别的单对：本笔属后到，锁水位桶缺席，只退本笔
                _return_intruder(conn, bucket["id"], _env_by_order(conn, order_id), "锁水位桶:桶已属他单")
                return _current_result(conn, order_id)
        got = _try_lock(conn, pod_id, token, env_id, lock_ttl_ms)
        if got:
            conn.execute("UPDATE pods SET run_state=? WHERE id=?", (P_STARTING, pod_id))
            _bucket_step_if_open(conn, pod_id, "pod.trace", f"启动#{order_id}", {"state": P_STARTING})
    if not got:
        got = _wait_lock(connect_fn, pod_id, token, env_id, ACT_START, lock_wait_ms, lock_ttl_ms, poll_ms)
        if not got:
            with _tx(connect_fn) as conn:
                env = _env(conn, env_id)
                if env["phase"] not in TERMINAL_PHASES:
                    _rollback_envelope(conn, env, "锁等待超时")
                return _current_result(conn, order_id)
    if hooks.get("after_trace"):
        hooks["after_trace"](ACT_START, order_id, env_id)
    # 事务二：落库（相位+锁双岗，被整笔回滚后这里必然挡住）
    with _tx(connect_fn) as conn:
        env = _env(conn, env_id)
        lock = _lock_row(conn, pod_id)
        if env["phase"] != PH_INFLIGHT or not lock or lock["holder"] != token:
            return _current_result(conn, order_id)
        conn.execute("UPDATE pods SET run_state=?, image_id=? WHERE id=?", (P_RUNNING, img_id, pod_id))
        conn.execute(
            "UPDATE quotas SET cpu_used=cpu_used+?, mem_used_mb=mem_used_mb+? WHERE id=1",
            (need_cpu, need_mem),
        )
        _record_quota_delta(conn, env_id, need_cpu, need_mem)
        _set_phase(conn, env_id, PH_INFLIGHT, PH_PERSISTED)
        _bucket_step_if_open(conn, pod_id, "env.persist", f"启动#{order_id}", {"state": P_RUNNING})
    # 事务三：确认、回写启停单灯、放锁、争用收口
    with _tx(connect_fn) as conn:
        env = _env(conn, env_id)
        if env["phase"] != PH_PERSISTED:
            return _current_result(conn, order_id)
        bucket = _bucket_of_order(conn, pod_id, order_id)
        note = "启动成功" + (f" · 争用#{bucket['id']}:{bucket['arm']}" if bucket else "")
        conn.execute("UPDATE start_orders SET status=?, note=? WHERE id=?", (L_DONE, note, order_id))
        _set_phase(conn, env_id, PH_PERSISTED, PH_CONFIRMED)
        _release_lock(conn, pod_id, token)
        _bucket_step_if_open(conn, pod_id, "env.confirm", f"启动#{order_id}", {})
        _maybe_close_bucket(conn, pod_id)
        _event(conn, "启停单", str(order_id), f"启动 → {L_DONE} {note}")
        return {"id": order_id, "status": L_DONE, "note": note}


def _submit_stop(connect_fn, pod_id: int, target_image_id, *, gate_timeout_ms, lock_ttl_ms,
                 lock_wait_ms, poll_ms, window_ms, hooks) -> dict:
    gate_id = None
    blocker_env_id = None
    # 事务一：理账、开信封、发现争用（启动在途→进让路闸；启动已落→即放闸留痕）
    with _tx(connect_fn) as conn:
        reconcile_pod(conn, pod_id)
        pod = _pod(conn, pod_id)
        img_id = pod["image_id"]
        cur = conn.execute(
            "INSERT INTO start_orders(pod_id, action, target_image_id, status, note, created_at) VALUES (?,?,?,?,?,?)",
            (pod_id, ACT_STOP, img_id, L_INFLIGHT, "", now_iso()),
        )
        order_id = cur.lastrowid
        env_id, token = _open_envelope(conn, pod_id, order_id, ACT_STOP, _snapshot_of(pod))
        intruder = _guard_existing_bucket(conn, pod_id, order_id)
        if intruder is not None:
            _return_intruder(conn, intruder["id"], _env_by_order(conn, order_id), "锁水位桶:桶已属他单")
            return _current_result(conn, order_id)
        rival = _find_rival(conn, pod_id, ACT_START, window_ms, order_id)
        got = False
        if rival:
            # 启动先伸手 → 让路闸臂；争用前快照取先开的启动信封
            bucket, in_bucket = _ensure_bucket(conn, pod, rival["order_id"], order_id, ARM_GATE, rival["snapshot"], rival=rival)
            if not in_bucket:
                _return_intruder(conn, bucket["id"], _env_by_order(conn, order_id), "锁水位桶:桶已属他单")
                return _current_result(conn, order_id)
            blocker_env_id = rival["id"]
            if rival["phase"] == PH_INFLIGHT:
                # 启动未落库：停止不改舱体，进让路闸待落库后放行
                gcur = conn.execute(
                    "INSERT INTO yield_gates(pod_id, bucket_id, waiter_order_id, blocker_order_id, state, created_ms) VALUES (?,?,?,?,?,?)",
                    (pod_id, bucket["id"], order_id, rival["order_id"], G_WAITING, now_ms()),
                )
                gate_id = gcur.lastrowid
                _bucket_step(conn, bucket["id"], "gate.enter", f"停止#{order_id}", {"blocker": f"启动#{rival['order_id']}"})
            else:
                gcur = conn.execute(
                    "INSERT INTO yield_gates(pod_id, bucket_id, waiter_order_id, blocker_order_id, state, created_ms, resolved_ms) VALUES (?,?,?,?,?,?,?)",
                    (pod_id, bucket["id"], order_id, rival["order_id"], G_RELEASED, now_ms(), now_ms()),
                )
                _bucket_step(conn, bucket["id"], "gate.release", f"停止#{order_id}", {"instant": True})
        if gate_id is None:
            got = _try_lock(conn, pod_id, token, env_id, lock_ttl_ms)
            if got and pod["run_state"] == P_RUNNING:
                conn.execute("UPDATE pods SET run_state=? WHERE id=?", (P_STOPPING, pod_id))
                _bucket_step_if_open(conn, pod_id, "pod.trace", f"停止#{order_id}", {"state": P_STOPPING})
    # 让路闸外等待：启动落库后放行；超时则整笔退回争用前舱体快照
    if gate_id is not None:
        outcome = _wait_gate(connect_fn, gate_id, blocker_env_id, gate_timeout_ms, poll_ms)
        if outcome == "timeout":
            with _tx(connect_fn) as conn:
                gate = conn.execute("SELECT * FROM yield_gates WHERE id=?", (gate_id,)).fetchone()
                if gate["state"] == G_WAITING:
                    blocker = _env(conn, blocker_env_id)
                    if blocker and blocker["phase"] == PH_INFLIGHT:
                        conn.execute(
                            "UPDATE yield_gates SET state=?, resolved_ms=? WHERE id=?",
                            (G_TIMEOUT, now_ms(), gate_id),
                        )
                        bucket = conn.execute(
                            "SELECT * FROM contention_buckets WHERE id=?", (gate["bucket_id"],)
                        ).fetchone()
                        _bucket_step(conn, bucket["id"], "gate.timeout", f"停止#{order_id}", {})
                        _rollback_bucket(conn, bucket, "让路闸超时")
                        return _current_result(conn, order_id)
                # 竞态消散：启动已落库或已被理账，按放行继续
        # 放行后走锁路径
    if not got:
        got = _wait_lock(connect_fn, pod_id, token, env_id, ACT_STOP, lock_wait_ms, lock_ttl_ms, poll_ms)
        if not got:
            with _tx(connect_fn) as conn:
                env = _env(conn, env_id)
                if env["phase"] not in TERMINAL_PHASES:
                    _rollback_envelope(conn, env, "锁等待超时")
                return _current_result(conn, order_id)
    if hooks.get("after_trace"):
        hooks["after_trace"](ACT_STOP, order_id, env_id)
    # 事务二：落库（运行中才退配额；已停止为幂等空停）
    with _tx(connect_fn) as conn:
        env = _env(conn, env_id)
        lock = _lock_row(conn, pod_id)
        if env["phase"] != PH_INFLIGHT or not lock or lock["holder"] != token:
            return _current_result(conn, order_id)
        cur_pod = _pod(conn, pod_id)
        if cur_pod["run_state"] in (P_RUNNING, P_STOPPING):
            conn.execute("UPDATE pods SET run_state=? WHERE id=?", (P_STOPPED, pod_id))
            conn.execute(
                "UPDATE quotas SET cpu_used=MAX(0, cpu_used-?), mem_used_mb=MAX(0, mem_used_mb-?) WHERE id=1",
                (float(cur_pod["cpu_quota"]), int(cur_pod["mem_quota_mb"])),
            )
            _record_quota_delta(conn, env_id, -float(cur_pod["cpu_quota"]), -int(cur_pod["mem_quota_mb"]))
        else:
            conn.execute("UPDATE pods SET run_state=? WHERE id=?", (P_STOPPED, pod_id))
        _set_phase(conn, env_id, PH_INFLIGHT, PH_PERSISTED)
        _bucket_step_if_open(conn, pod_id, "env.persist", f"停止#{order_id}", {"state": P_STOPPED})
    # 事务三：确认、回写启停单灯、放锁、争用收口
    with _tx(connect_fn) as conn:
        env = _env(conn, env_id)
        if env["phase"] != PH_PERSISTED:
            return _current_result(conn, order_id)
        bucket = _bucket_of_order(conn, pod_id, order_id)
        note = "停止成功" + (f" · 争用#{bucket['id']}:{bucket['arm']}" if bucket else "")
        conn.execute("UPDATE start_orders SET status=?, note=? WHERE id=?", (L_DONE, note, order_id))
        _set_phase(conn, env_id, PH_PERSISTED, PH_CONFIRMED)
        _release_lock(conn, pod_id, token)
        _bucket_step_if_open(conn, pod_id, "env.confirm", f"停止#{order_id}", {})
        _maybe_close_bucket(conn, pod_id)
        _event(conn, "启停单", str(order_id), f"停止 → {L_DONE} {note}")
        return {"id": order_id, "status": L_DONE, "note": note}


# ---------------------------------------------------------------- 重放与互证

def replay_bucket(log: list[dict]) -> dict:
    """纯函数：把争用桶重放日志叠成读侧投影，结局必须可重放。"""
    st = {"arm": None, "pod": None, "start_light": L_INFLIGHT, "stop_light": L_INFLIGHT,
          "verdict": None, "final": None}
    for e in sorted(log, key=lambda x: x["seq"]):
        step = e.get("step")
        actor = e.get("actor", "")
        detail = e.get("detail") or {}
        if step == "bucket.open":
            st["arm"] = detail.get("arm")
            st["pod"] = detail.get("pre_state")
        elif step in ("pod.trace", "env.persist", "pod.restore"):
            st["pod"] = detail.get("state", st["pod"])
        elif step == "env.confirm":
            if actor.startswith("启动#"):
                st["start_light"] = L_DONE
            elif actor.startswith("停止#"):
                st["stop_light"] = L_DONE
        elif step == "env.rollback":
            if actor.startswith("启动#"):
                st["start_light"] = L_ROLLED
            elif actor.startswith("停止#"):
                st["stop_light"] = L_ROLLED
        elif step == "bucket.close":
            st["verdict"] = detail.get("verdict")
            st["final"] = detail.get("final_state")
            st["pod"] = detail.get("final_state", st["pod"])
    return st


def verify_contention(conn, bucket_id: int) -> list[dict]:
    """互证：启动信封、停止信封、舱体锁、对读屏争用桶互相印证；两臂结论不得并存。"""
    b = conn.execute("SELECT * FROM contention_buckets WHERE id=?", (bucket_id,)).fetchone()
    if not b:
        return [{"名称": "桶在", "通过": False, "详情": f"争用#{bucket_id} 不存在"}]
    pod = _pod(conn, b["pod_id"])
    s_env = _env_by_order(conn, b["start_order_id"])
    t_env = _env_by_order(conn, b["stop_order_id"])
    s_ord = _order(conn, b["start_order_id"])
    t_ord = _order(conn, b["stop_order_id"])
    gates = conn.execute("SELECT * FROM yield_gates WHERE bucket_id=?", (bucket_id,)).fetchall()
    lock = _lock_row(conn, b["pod_id"])
    checks: list[dict] = []

    # 臂唯一：闸痕当且仅当让路闸臂，两臂让路结论不得并存
    ok = b["arm"] in (ARM_GATE, ARM_PREEMPT) and ((len(gates) > 0) == (b["arm"] == ARM_GATE))
    checks.append({"名称": "臂唯一", "通过": ok, "详情": f"臂={b['arm']} 闸痕={len(gates)}"})

    # 锁证：在途桶锁必属两信封之一；收口/回滚后两信封不得持锁
    holders = {e["token"] for e in (s_env, t_env) if e}
    if b["state"] == B_OPEN:
        ok = lock is None or lock["holder"] in holders
        detail = f"在途桶 锁持={lock['holder'] if lock else '无'}"
    else:
        ok = lock is None or lock["holder"] not in holders
        detail = f"收口后 锁持={lock['holder'] if lock else '无'}"
    checks.append({"名称": "锁证", "通过": ok, "详情": detail})

    # 信封证：双成→双确认；整笔回滚→双回滚；单成退后→先到确认、后到回滚
    if b["state"] == B_OPEN:
        ok = not _persist_order_violates_arm(b["arm"], s_env, t_env)
        detail = "在途无违臂落库"
    elif b["verdict"] == V_BOTH:
        ok = s_env["phase"] == PH_CONFIRMED and t_env["phase"] == PH_CONFIRMED
        detail = f"双成 启动={s_env['phase']} 停止={t_env['phase']}"
    elif b["verdict"] == V_SINGLE:
        phases = {s_env["phase"], t_env["phase"]}
        ok = PH_ROLLED in phases and PH_CONFIRMED in phases
        detail = f"单成退后 启动={s_env['phase']} 停止={t_env['phase']}"
    else:
        ok = s_env["phase"] == PH_ROLLED and t_env["phase"] == PH_ROLLED
        detail = f"回滚 启动={s_env['phase']} 停止={t_env['phase']}"
    checks.append({"名称": "信封证", "通过": ok, "详情": detail})

    # 灯证：启停单灯与信封相位一致，桶收口后不得在途（启动灯必须补齐可核）
    light_map = {PH_INFLIGHT: (L_INFLIGHT,), PH_PERSISTED: (L_INFLIGHT,),
                 PH_CONFIRMED: (L_DONE,), PH_ROLLED: (L_ROLLED,)}
    ok = s_ord["status"] in light_map[s_env["phase"]] and t_ord["status"] in light_map[t_env["phase"]]
    if b["state"] != B_OPEN:
        ok = ok and s_ord["status"] != L_INFLIGHT and t_ord["status"] != L_INFLIGHT
    checks.append({"名称": "灯证", "通过": ok, "详情": f"启动灯={s_ord['status']} 停止灯={t_ord['status']}"})

    # 舱体证：在途桶必须确有在途信封（不得装成无在途却已停止收口）；收口后舱体态=桶记终态
    if b["state"] == B_OPEN:
        ok = s_env["phase"] not in TERMINAL_PHASES or t_env["phase"] not in TERMINAL_PHASES
        detail = f"在途桶 启动={s_env['phase']} 停止={t_env['phase']}"
    else:
        ok = pod["run_state"] == b["final_state"]
        detail = f"舱体={pod['run_state']} 桶记={b['final_state']}"
    checks.append({"名称": "舱体证", "通过": ok, "详情": detail})

    # 四侧证：宣称双完成时，锁旁读∪锁水位桶∪回执对∪审计必须同屏可核；缺任一即不得双成
    if b["state"] != B_OPEN and b["verdict"] == V_BOTH:
        missing = _four_side_check(conn, b)
        checks.append({"名称": "四侧证", "通过": not missing,
                       "详情": "四侧齐" if not missing else "缺:" + ",".join(missing)})
    else:
        checks.append({"名称": "四侧证", "通过": True, "详情": "非双成不适用"})

    # 重放证：读侧重放结局须与落库一致
    rp = replay_bucket(json.loads(b["replay_log"]))
    ok = rp["arm"] == b["arm"]
    if b["state"] != B_OPEN:
        ok = (ok and rp["verdict"] == b["verdict"] and rp["final"] == b["final_state"]
              and rp["pod"] == pod["run_state"]
              and rp["start_light"] == s_ord["status"] and rp["stop_light"] == t_ord["status"])
    checks.append({"名称": "重放证", "通过": ok, "详情": f"重放={json.dumps(rp, ensure_ascii=False)}"})
    return checks
