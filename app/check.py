"""后台一致性核对：确认副本是否漏录、误留或改坏标本。

工作流程（全程分批处理，内存占用不随标本/日志总量增长）
=====================================================

1. PIN_REPLICA —— 固定副本截面
   以只读快照（WAL 读事务 + query_only）打开运行中的副本，在同一快照里
   读出 sync_meta 的 last_applied_seq（副本已应用序号 R）与全部
   specimens，分页誊写到核对库 staged_replica。期间同步器可继续写副本、
   查询可继续读副本，互不影响（核对只持读事务，绝不持写事务）。

2. PIN_SOURCE —— 固定源库同一读取快照中的水位、日志与标本
   以只读事务打开源库，在同一个读快照里：
     * 记录源水位 W = MAX(_change_log.seq)；
     * 把日志切片 (R, W] 分页誊写到 staged_log；
     * 把 specimens 分页誊写到 staged_source。
   源库写入、原同步与查询全部继续可用（WAL 读写并发）。

3. CATCH_UP —— 把副本截面补齐到源水位
   按 seq 升序把 staged_log 重放到 staged_replica（I/U=覆盖，D=删除）。
   “正常同步延迟”（副本尚未追上的那部分日志）在此被补齐，不计入差异。

4. COMPARE —— 以源快照为准，逐 id 比较 taxon / locality
   staged_source 与补齐后的 staged_replica 双向分页对比：
     * 源有副本无 -> MISSING（漏录）
     * 副本有源无 -> EXTRA（误留）
     * 字段不同     -> MISMATCH（改坏），记录双方值

5. 发布 / 失败
   差异随批次写入核对库，但只有状态 = DONE 的报告才会被 API 发布；
   任何失败都清理该次核对的中间数据与部分差异，不发布部分结论。
   进程重启时，未完成的核对一律标记为 FAILED；已完成的报告完整保留。

核对数据写到独立的第三个 SQLite 库（CHECK_DB），绝不改写源库与副本。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Optional

from . import replica_db
from .source_db import connect as source_connect

# 核对各阶段每批处理的行数（誊写截面、重放日志、逐 id 比较共用）
CHECK_BATCH_SIZE = int(os.environ.get("CHECK_BATCH_SIZE", "500"))

# 核对运行状态
STATUS_RUNNING = "RUNNING"
STATUS_DONE = "DONE"
STATUS_FAILED = "FAILED"

# 核对阶段（用于进度展示）
PHASE_PIN_REPLICA = "PIN_REPLICA"
PHASE_PIN_SOURCE = "PIN_SOURCE"
PHASE_CATCH_UP = "CATCH_UP"
PHASE_COMPARE = "COMPARE"
PHASE_DONE = "DONE"

# 差异类别
CAT_MISSING = "MISSING"    # 源有、副本无：漏录
CAT_EXTRA = "EXTRA"        # 副本有、源无：误留
CAT_MISMATCH = "MISMATCH"  # 双方都有、字段不同：改坏
CATEGORIES = (CAT_MISSING, CAT_EXTRA, CAT_MISMATCH)


class ReplicaNotReadyError(RuntimeError):
    """副本未就绪（phase != RUNNING），一致性核对仅受理已就绪副本。"""


# --------------------------------------------------------------------------- #
# 核对库（第三个独立 SQLite：运行记录 + 差异报告 + 分页誊写的中间截面）
# --------------------------------------------------------------------------- #

def connect(path: str, *, timeout: float = 30.0) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=timeout, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


@contextmanager
def open_check(path: str):
    conn = connect(path)
    try:
        yield conn
    finally:
        conn.close()


def init_check_db(path: str) -> None:
    """创建核对库结构（幂等）。"""
    with open_check(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS check_runs (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                status              TEXT NOT NULL
                                    CHECK (status IN ('RUNNING','DONE','FAILED')),
                phase               TEXT NOT NULL,
                replica_seq         INTEGER NOT NULL DEFAULT 0,
                source_watermark    INTEGER NOT NULL DEFAULT 0,
                staged_replica_rows INTEGER NOT NULL DEFAULT 0,
                staged_source_rows  INTEGER NOT NULL DEFAULT 0,
                staged_log_entries  INTEGER NOT NULL DEFAULT 0,
                applied_log_entries INTEGER NOT NULL DEFAULT 0,
                compared_rows       INTEGER NOT NULL DEFAULT 0,
                missing_count       INTEGER NOT NULL DEFAULT 0,
                extra_count         INTEGER NOT NULL DEFAULT 0,
                mismatch_count      INTEGER NOT NULL DEFAULT 0,
                error               TEXT,
                created_at          TEXT NOT NULL DEFAULT (datetime('now')),
                finished_at         TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS check_diffs (
                run_id           INTEGER NOT NULL,
                category         TEXT NOT NULL
                                 CHECK (category IN ('MISSING','EXTRA','MISMATCH')),
                specimen_id      INTEGER NOT NULL,
                source_taxon     TEXT,
                source_locality  TEXT,
                replica_taxon    TEXT,
                replica_locality TEXT,
                PRIMARY KEY (run_id, category, specimen_id)
            )
            """
        )
        # 中间截面：誊写完成后即删除，不作为报告内容
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS staged_replica (
                run_id   INTEGER NOT NULL,
                id       INTEGER NOT NULL,
                taxon    TEXT,
                locality TEXT,
                PRIMARY KEY (run_id, id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS staged_source (
                run_id   INTEGER NOT NULL,
                id       INTEGER NOT NULL,
                taxon    TEXT,
                locality TEXT,
                PRIMARY KEY (run_id, id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS staged_log (
                run_id      INTEGER NOT NULL,
                seq         INTEGER NOT NULL,
                op          TEXT NOT NULL,
                specimen_id INTEGER NOT NULL,
                taxon       TEXT,
                locality    TEXT,
                PRIMARY KEY (run_id, seq)
            )
            """
        )


def recover_interrupted(path: str) -> int:
    """进程重启恢复：未完成的核对一律标记失败，并清除其部分结论与中间数据。

    已完成（DONE）的报告及其差异完整保留。返回被标记失败的核对数。
    """
    with open_check(path) as conn:
        ids = [
            r["id"]
            for r in conn.execute(
                "SELECT id FROM check_runs WHERE status = ?", (STATUS_RUNNING,)
            )
        ]
        if not ids:
            return 0
        conn.execute("BEGIN IMMEDIATE")
        for run_id in ids:
            _purge_run_data(conn, run_id)
        conn.execute(
            """
            UPDATE check_runs
               SET status = ?, error = ?, finished_at = datetime('now')
             WHERE status = ?
            """,
            (STATUS_FAILED, "进程重启，核对中断（未完成不发布结论）", STATUS_RUNNING),
        )
        conn.execute("COMMIT")
        return len(ids)


def _purge_run_data(conn, run_id: int) -> None:
    """删除一次核对的差异与中间截面（调用方须已开启事务）。"""
    for table in ("check_diffs", "staged_replica", "staged_source", "staged_log"):
        conn.execute(f"DELETE FROM {table} WHERE run_id = ?", (run_id,))


# --------------------------------------------------------------------------- #
# 报告查询（Web 层使用）
# --------------------------------------------------------------------------- #

def get_run(path: str, run_id: int) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    with open_check(path) as conn:
        row = conn.execute(
            "SELECT * FROM check_runs WHERE id = ?", (run_id,)
        ).fetchone()
        return dict(row) if row is not None else None


def list_runs(path: str, limit: int = 20) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open_check(path) as conn:
        rows = conn.execute(
            "SELECT * FROM check_runs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def get_diffs(
    path: str,
    run_id: int,
    category: Optional[str] = None,
    offset: int = 0,
    limit: int = 50,
) -> dict:
    """分页读取报告差异。调用方负责只在 status = DONE 时发布。"""
    where = "run_id = ?"
    params: list = [run_id]
    if category in CATEGORIES:
        where += " AND category = ?"
        params.append(category)
    with open_check(path) as conn:
        total = int(
            conn.execute(
                f"SELECT COUNT(*) AS c FROM check_diffs WHERE {where}", params
            ).fetchone()["c"]
        )
        rows = conn.execute(
            f"""
            SELECT category, specimen_id,
                   source_taxon, source_locality, replica_taxon, replica_locality
              FROM check_diffs
             WHERE {where}
             ORDER BY specimen_id ASC
             LIMIT ? OFFSET ?
            """,
            (*params, limit, offset),
        ).fetchall()
    return {
        "run_id": run_id,
        "category": category or "ALL",
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": [dict(r) for r in rows],
    }


# --------------------------------------------------------------------------- #
# 核对执行器（后台线程，单实例运行）
# --------------------------------------------------------------------------- #

class CheckRunner:
    """一次只允许运行一个核对；进度与结果全部持久化在核对库中。"""

    def __init__(
        self,
        source_path: str,
        replica_path: str,
        check_path: str,
        batch_size: int = CHECK_BATCH_SIZE,
    ):
        self.source_path = source_path
        self.replica_path = replica_path
        self.check_path = check_path
        self.batch_size = batch_size
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    # ----------------------------- 生命周期 ----------------------------- #
    def start(self) -> tuple[bool, Optional[int]]:
        """受理一次核对。返回 (started, run_id)。

        副本未就绪时抛 ReplicaNotReadyError；已有核对进行中时返回 (False, None)。
        """
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False, None
            # 仅接收已就绪副本
            if not replica_db.is_ready(self.replica_path):
                raise ReplicaNotReadyError(
                    "副本未就绪（phase != RUNNING），一致性核对仅受理已就绪副本"
                )
            run_id = self._create_run()
            t = threading.Thread(
                target=self._run_safely,
                args=(run_id,),
                name="consistency-check",
                daemon=True,
            )
            self._thread = t
            t.start()
            return True, run_id

    def wait_until_idle(self, timeout: Optional[float] = None) -> None:
        """供测试使用：等待核对线程退出。"""
        t = self._thread
        if t is not None:
            t.join(timeout)

    def is_alive(self) -> bool:
        t = self._thread
        return t is not None and t.is_alive()

    def _create_run(self) -> int:
        with open_check(self.check_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "INSERT INTO check_runs(status, phase) VALUES (?, ?)",
                (STATUS_RUNNING, PHASE_PIN_REPLICA),
            )
            run_id = int(cur.lastrowid)
            conn.execute("COMMIT")
            return run_id

    def _run_safely(self, run_id: int) -> None:
        try:
            self._run(run_id)
        except Exception as exc:  # 失败：不发布任何部分结论
            self._fail(run_id, exc)

    def _run(self, run_id: int) -> None:
        replica_seq = self._pin_replica(run_id)
        self._pin_source(run_id, replica_seq)
        self._catch_up(run_id, replica_seq)
        self._compare_forward(run_id)
        self._compare_extra(run_id)
        self._finalize(run_id)

    def _fail(self, run_id: int, exc: Exception) -> None:
        try:
            with open_check(self.check_path) as conn:
                conn.execute("BEGIN IMMEDIATE")
                # 不发布部分结论：清除已写入的差异与中间截面，差异计数归零
                _purge_run_data(conn, run_id)
                conn.execute(
                    """
                    UPDATE check_runs
                       SET status = ?, error = ?,
                           missing_count = 0, extra_count = 0, mismatch_count = 0,
                           finished_at = datetime('now')
                     WHERE id = ?
                    """,
                    (STATUS_FAILED, f"{type(exc).__name__}: {exc}", run_id),
                )
                conn.execute("COMMIT")
        except Exception:
            # 核对库本身不可写时，留待重启恢复逻辑把这次核对标记为失败
            pass

    # ----------------------------- 小工具 ----------------------------- #
    def _update_run(self, run_id: int, sets: str, params: tuple) -> None:
        with open_check(self.check_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                f"UPDATE check_runs SET {sets} WHERE id = ?", (*params, run_id)
            )
            conn.execute("COMMIT")

    def _stage_specimens(
        self, run_id: int, table: str, counter: str, rows: list
    ) -> None:
        """把一批标本誊写到中间截面表，并同事务更新进度计数。"""
        with open_check(self.check_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany(
                f"INSERT INTO {table}(run_id, id, taxon, locality)"
                " VALUES (?, ?, ?, ?)",
                [(run_id, r["id"], r["taxon"], r["locality"]) for r in rows],
            )
            conn.execute(
                f"UPDATE check_runs SET {counter} = {counter} + ? WHERE id = ?",
                (len(rows), run_id),
            )
            conn.execute("COMMIT")

    # --------------------------- 阶段 1：固定副本 --------------------------- #
    def _pin_replica(self, run_id: int) -> int:
        """固定副本数据及其已应用序号；返回副本截面序号 R。"""
        self._update_run(run_id, "phase = ?", (PHASE_PIN_REPLICA,))
        conn = replica_db.connect(self.replica_path)
        try:
            # 只读快照：query_only 保证本连接不可能改写运行中的副本
            conn.execute("PRAGMA query_only = ON")
            conn.execute("BEGIN")
            meta = conn.execute(
                "SELECT phase, last_applied_seq FROM sync_meta WHERE id = 1"
            ).fetchone()
            if meta is None or meta["phase"] != replica_db.PHASE_RUNNING:
                raise ReplicaNotReadyError(
                    "取截面时副本不处于 RUNNING"
                    f"（phase={meta['phase'] if meta else '无 meta'}）"
                )
            replica_seq = int(meta["last_applied_seq"])
            self._update_run(run_id, "replica_seq = ?", (replica_seq,))

            last_id: Optional[int] = None
            while True:
                if last_id is None:
                    rows = conn.execute(
                        "SELECT id, taxon, locality FROM specimens"
                        " ORDER BY id ASC LIMIT ?",
                        (self.batch_size,),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT id, taxon, locality FROM specimens"
                        " WHERE id > ? ORDER BY id ASC LIMIT ?",
                        (last_id, self.batch_size),
                    ).fetchall()
                if not rows:
                    break
                self._stage_specimens(
                    run_id, "staged_replica", "staged_replica_rows", rows
                )
                last_id = int(rows[-1]["id"])
                if len(rows) < self.batch_size:
                    break
            return replica_seq
        finally:
            conn.close()

    # --------------------------- 阶段 2：固定源库 --------------------------- #
    def _pin_source(self, run_id: int, replica_seq: int) -> None:
        """在源库同一读取快照中固定水位、日志切片 (R, W] 与标本。"""
        self._update_run(run_id, "phase = ?", (PHASE_PIN_SOURCE,))
        src = source_connect(self.source_path, readonly=True)
        try:
            src.execute("BEGIN")  # 同一读取快照：水位、日志、标本三者一致
            watermark = int(
                src.execute(
                    "SELECT COALESCE(MAX(seq), 0) AS m FROM _change_log"
                ).fetchone()["m"]
            )
            self._update_run(run_id, "source_watermark = ?", (watermark,))

            # 日志切片 (R, W]：副本尚未追上的部分（正常同步延迟）
            last_seq = replica_seq
            while True:
                rows = src.execute(
                    """
                    SELECT seq, op, id, taxon, locality
                      FROM _change_log
                     WHERE seq > ? AND seq <= ?
                     ORDER BY seq ASC
                     LIMIT ?
                    """,
                    (last_seq, watermark, self.batch_size),
                ).fetchall()
                if not rows:
                    break
                with open_check(self.check_path) as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.executemany(
                        "INSERT INTO staged_log"
                        "(run_id, seq, op, specimen_id, taxon, locality)"
                        " VALUES (?, ?, ?, ?, ?, ?)",
                        [
                            (
                                run_id,
                                r["seq"],
                                r["op"],
                                r["id"],
                                r["taxon"],
                                r["locality"],
                            )
                            for r in rows
                        ],
                    )
                    conn.execute(
                        "UPDATE check_runs"
                        "   SET staged_log_entries = staged_log_entries + ?"
                        " WHERE id = ?",
                        (len(rows), run_id),
                    )
                    conn.execute("COMMIT")
                last_seq = int(rows[-1]["seq"])
                if len(rows) < self.batch_size:
                    break

            # 标本截面
            last_id: Optional[int] = None
            while True:
                if last_id is None:
                    rows = src.execute(
                        "SELECT id, taxon, locality FROM specimens"
                        " ORDER BY id ASC LIMIT ?",
                        (self.batch_size,),
                    ).fetchall()
                else:
                    rows = src.execute(
                        "SELECT id, taxon, locality FROM specimens"
                        " WHERE id > ? ORDER BY id ASC LIMIT ?",
                        (last_id, self.batch_size),
                    ).fetchall()
                if not rows:
                    break
                self._stage_specimens(
                    run_id, "staged_source", "staged_source_rows", rows
                )
                last_id = int(rows[-1]["id"])
                if len(rows) < self.batch_size:
                    break
        finally:
            src.close()

    # ----------------------- 阶段 3：补齐到源水位 ----------------------- #
    def _catch_up(self, run_id: int, replica_seq: int) -> None:
        """把日志切片 (R, W] 重放到副本截面：正常同步延迟不计入差异。"""
        self._update_run(run_id, "phase = ?", (PHASE_CATCH_UP,))
        last_seq = replica_seq
        while True:
            with open_check(self.check_path) as conn:
                entries = conn.execute(
                    "SELECT seq, op, specimen_id, taxon, locality"
                    "  FROM staged_log"
                    " WHERE run_id = ? AND seq > ?"
                    " ORDER BY seq ASC LIMIT ?",
                    (run_id, last_seq, self.batch_size),
                ).fetchall()
                if not entries:
                    break
                conn.execute("BEGIN IMMEDIATE")
                for e in entries:
                    if e["op"] in ("I", "U"):
                        conn.execute(
                            "INSERT OR REPLACE INTO staged_replica"
                            "(run_id, id, taxon, locality) VALUES (?, ?, ?, ?)",
                            (run_id, e["specimen_id"], e["taxon"], e["locality"]),
                        )
                    else:  # 'D'
                        conn.execute(
                            "DELETE FROM staged_replica"
                            " WHERE run_id = ? AND id = ?",
                            (run_id, e["specimen_id"]),
                        )
                conn.execute(
                    "UPDATE check_runs"
                    "   SET applied_log_entries = applied_log_entries + ?"
                    " WHERE id = ?",
                    (len(entries), run_id),
                )
                conn.execute("COMMIT")
            last_seq = int(entries[-1]["seq"])
            if len(entries) < self.batch_size:
                break

    # ----------------------- 阶段 4：逐 id 比较 ----------------------- #
    def _compare_forward(self, run_id: int) -> None:
        """以源快照为准正向比较：源有副本无 = 缺失；字段不同 = 改坏。"""
        self._update_run(run_id, "phase = ?", (PHASE_COMPARE,))
        last_id: Optional[int] = None
        while True:
            with open_check(self.check_path) as conn:
                if last_id is None:
                    batch = conn.execute(
                        "SELECT id, taxon, locality FROM staged_source"
                        " WHERE run_id = ? ORDER BY id ASC LIMIT ?",
                        (run_id, self.batch_size),
                    ).fetchall()
                else:
                    batch = conn.execute(
                        "SELECT id, taxon, locality FROM staged_source"
                        " WHERE run_id = ? AND id > ? ORDER BY id ASC LIMIT ?",
                        (run_id, last_id, self.batch_size),
                    ).fetchall()
                if not batch:
                    break
                ids = [r["id"] for r in batch]
                placeholders = ",".join("?" for _ in ids)
                rep_map = {
                    r["id"]: r
                    for r in conn.execute(
                        f"SELECT id, taxon, locality FROM staged_replica"
                        f" WHERE run_id = ? AND id IN ({placeholders})",
                        (run_id, *ids),
                    )
                }
                diffs = []
                for s in batch:
                    r = rep_map.get(s["id"])
                    if r is None:
                        diffs.append(
                            (run_id, CAT_MISSING, s["id"],
                             s["taxon"], s["locality"], None, None)
                        )
                    elif (
                        r["taxon"] != s["taxon"] or r["locality"] != s["locality"]
                    ):
                        diffs.append(
                            (run_id, CAT_MISMATCH, s["id"],
                             s["taxon"], s["locality"], r["taxon"], r["locality"])
                        )
                n_missing = sum(1 for d in diffs if d[1] == CAT_MISSING)
                conn.execute("BEGIN IMMEDIATE")
                conn.executemany(
                    "INSERT INTO check_diffs"
                    "(run_id, category, specimen_id, source_taxon,"
                    " source_locality, replica_taxon, replica_locality)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    diffs,
                )
                conn.execute(
                    "UPDATE check_runs"
                    "   SET compared_rows = compared_rows + ?,"
                    "       missing_count = missing_count + ?,"
                    "       mismatch_count = mismatch_count + ?"
                    " WHERE id = ?",
                    (len(batch), n_missing, len(diffs) - n_missing, run_id),
                )
                conn.execute("COMMIT")
            last_id = int(batch[-1]["id"])
            if len(batch) < self.batch_size:
                break

    def _compare_extra(self, run_id: int) -> None:
        """反向比较：副本截面有而源快照无 = 误留（多余）。"""
        last_id: Optional[int] = None
        while True:
            with open_check(self.check_path) as conn:
                if last_id is None:
                    batch = conn.execute(
                        "SELECT id, taxon, locality FROM staged_replica"
                        " WHERE run_id = ? ORDER BY id ASC LIMIT ?",
                        (run_id, self.batch_size),
                    ).fetchall()
                else:
                    batch = conn.execute(
                        "SELECT id, taxon, locality FROM staged_replica"
                        " WHERE run_id = ? AND id > ? ORDER BY id ASC LIMIT ?",
                        (run_id, last_id, self.batch_size),
                    ).fetchall()
                if not batch:
                    break
                ids = [r["id"] for r in batch]
                placeholders = ",".join("?" for _ in ids)
                src_ids = {
                    row["id"]
                    for row in conn.execute(
                        f"SELECT id FROM staged_source"
                        f" WHERE run_id = ? AND id IN ({placeholders})",
                        (run_id, *ids),
                    )
                }
                diffs = [
                    (run_id, CAT_EXTRA, r["id"], None, None,
                     r["taxon"], r["locality"])
                    for r in batch
                    if r["id"] not in src_ids
                ]
                conn.execute("BEGIN IMMEDIATE")
                conn.executemany(
                    "INSERT INTO check_diffs"
                    "(run_id, category, specimen_id, source_taxon,"
                    " source_locality, replica_taxon, replica_locality)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    diffs,
                )
                conn.execute(
                    "UPDATE check_runs"
                    "   SET extra_count = extra_count + ? WHERE id = ?",
                    (len(diffs), run_id),
                )
                conn.execute("COMMIT")
            last_id = int(batch[-1]["id"])
            if len(batch) < self.batch_size:
                break

    # --------------------------- 阶段 5：发布 --------------------------- #
    def _finalize(self, run_id: int) -> None:
        """同一事务：发布 DONE 报告并清理中间截面。"""
        with open_check(self.check_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            for table in ("staged_replica", "staged_source", "staged_log"):
                conn.execute(f"DELETE FROM {table} WHERE run_id = ?", (run_id,))
            conn.execute(
                """
                UPDATE check_runs
                   SET status = ?, phase = ?, finished_at = datetime('now')
                 WHERE id = ?
                """,
                (STATUS_DONE, PHASE_DONE, run_id),
            )
            conn.execute("COMMIT")
