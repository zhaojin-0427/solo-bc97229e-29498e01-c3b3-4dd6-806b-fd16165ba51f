"""一致性核对测试：固定截面、补齐、逐 id 比较、失败与重启语义、Web API。"""
import os
import threading
import time

import pytest

# 必须在导入 app.web 前指向临时目录，避免模块级 app 使用默认 /data 路径
os.environ.setdefault("SOURCE_DB", "/tmp/museum-test-checkdb/source.db")
os.environ.setdefault("REPLICA_DB", "/tmp/museum-test-checkdb/replica.db")
os.environ.setdefault("CHECK_DB", "/tmp/museum-test-checkdb/check.db")

from app import check, replica_db, source_db  # noqa: E402
from app.check import CheckRunner  # noqa: E402
from app.source_db import connect as source_connect  # noqa: E402
from app.sync import SyncEngine, sync_once  # noqa: E402
from app.web import create_app  # noqa: E402


# --------------------------------------------------------------------------- #
# 辅助
# --------------------------------------------------------------------------- #
@pytest.fixture()
def paths(tmp_path):
    src = str(tmp_path / "source.db")
    rep = str(tmp_path / "replica.db")
    chk = str(tmp_path / "check.db")
    source_db.init_demo(src, rows=[(i, f"taxon-{i}", f"loc-{i}") for i in range(1, 9)])
    check.init_check_db(chk)
    return src, rep, chk


def replica_rows(path):
    with replica_db.open_replica(path) as conn:
        return {r["id"]: (r["taxon"], r["locality"])
                for r in conn.execute("SELECT * FROM specimens ORDER BY id")}


def source_rows(path):
    conn = source_connect(path, readonly=True)
    try:
        return {r["id"]: (r["taxon"], r["locality"])
                for r in conn.execute("SELECT * FROM specimens ORDER BY id")}
    finally:
        conn.close()


def run_check(src, rep, chk, batch_size=500):
    """同步执行一次核对并返回最终运行记录。"""
    runner = CheckRunner(src, rep, chk, batch_size=batch_size)
    started, run_id = runner.start()
    assert started, "核对未被受理"
    runner.wait_until_idle(60)
    return check.get_run(chk, run_id)


def wait_phase(rep, phase, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if replica_db.get_meta(rep)["phase"] == phase:
            return True
        time.sleep(0.02)
    return False


# --------------------------------------------------------------------------- #
# 1. 正常核对：一致 / 同步延迟 / 三类差异
# --------------------------------------------------------------------------- #
class TestCheckHappyPath:
    def test_consistent_replica_reports_no_diffs(self, paths):
        src, rep, chk = paths
        sync_once(src, rep)
        run = run_check(src, rep, chk)
        assert run["status"] == check.STATUS_DONE
        assert run["phase"] == check.PHASE_DONE
        assert (run["missing_count"], run["extra_count"], run["mismatch_count"]) \
            == (0, 0, 0)
        # 两端水位标明：副本已追平，二者相等
        assert run["replica_seq"] == source_db.max_change_seq(src)
        assert run["source_watermark"] == run["replica_seq"]
        assert run["compared_rows"] == 8
        assert run["finished_at"]
        # 中间截面已清理，不作为报告内容
        with check.open_check(chk) as conn:
            for table in ("staged_replica", "staged_source", "staged_log"):
                n = conn.execute(
                    f"SELECT COUNT(*) AS c FROM {table} WHERE run_id = ?",
                    (run["id"],),
                ).fetchone()["c"]
                assert n == 0

    def test_sync_lag_is_not_a_difference(self, paths):
        """正常同步延迟不算差异：副本落后于源水位时先补齐再比较。"""
        src, rep, chk = paths
        sync_once(src, rep)
        replica_seq = replica_db.get_meta(rep)["last_applied_seq"]
        # 源库继续写入但不同步：副本落后（正常同步延迟）
        for i in range(3):
            source_db.insert_specimen(src, 100 + i, f"lag-{i}", "not-yet-synced")
        watermark = source_db.max_change_seq(src)
        assert watermark > replica_seq

        run = run_check(src, rep, chk)
        assert run["status"] == check.STATUS_DONE
        # 两端水位如实标明：副本截面序号 < 源水位
        assert run["replica_seq"] == replica_seq
        assert run["source_watermark"] == watermark
        # 延迟部分被补齐（重放了 (R, W] 全部日志），不计入差异
        assert run["staged_log_entries"] == watermark - replica_seq
        assert run["applied_log_entries"] == watermark - replica_seq
        assert (run["missing_count"], run["extra_count"], run["mismatch_count"]) \
            == (0, 0, 0)

    def test_missing_extra_mismatch_detected(self, paths):
        """漏录 / 误留 / 改坏 都能被逐 id 比对发现，并记录双方值。"""
        src, rep, chk = paths
        sync_once(src, rep)
        with replica_db.open_replica(rep) as conn:
            conn.execute("DELETE FROM specimens WHERE id = 2")           # 漏录
            conn.execute(
                "INSERT INTO specimens VALUES (900, 'ghost', 'nowhere')"  # 误留
            )
            conn.execute(
                "UPDATE specimens SET taxon = 'corrupted' WHERE id = 3"   # 改坏
            )

        run = run_check(src, rep, chk)
        assert run["status"] == check.STATUS_DONE
        assert (run["missing_count"], run["extra_count"], run["mismatch_count"]) \
            == (1, 1, 1)

        missing = check.get_diffs(chk, run["id"], check.CAT_MISSING)["items"]
        assert [d["specimen_id"] for d in missing] == [2]
        assert missing[0]["source_taxon"] == "taxon-2"
        assert missing[0]["source_locality"] == "loc-2"
        assert missing[0]["replica_taxon"] is None

        extra = check.get_diffs(chk, run["id"], check.CAT_EXTRA)["items"]
        assert [d["specimen_id"] for d in extra] == [900]
        assert extra[0]["replica_taxon"] == "ghost"
        assert extra[0]["source_taxon"] is None

        mismatch = check.get_diffs(chk, run["id"], check.CAT_MISMATCH)["items"]
        assert [d["specimen_id"] for d in mismatch] == [3]
        assert mismatch[0]["source_taxon"] == "taxon-3"
        assert mismatch[0]["replica_taxon"] == "corrupted"
        assert mismatch[0]["source_locality"] == "loc-3"
        assert mismatch[0]["replica_locality"] == "loc-3"

    def test_not_ready_replica_rejected(self, paths):
        """仅接收已就绪副本：INIT / STOPPED 都拒绝受理。"""
        src, rep, chk = paths
        runner = CheckRunner(src, rep, chk)
        with pytest.raises(check.ReplicaNotReadyError):
            runner.start()  # 从未同步（INIT）

        engine = SyncEngine(src, rep, poll_interval=0.05)
        engine.start()
        assert wait_phase(rep, replica_db.PHASE_RUNNING)
        engine.stop()
        engine.wait_until_idle(10)
        assert replica_db.get_meta(rep)["phase"] == replica_db.PHASE_STOPPED
        with pytest.raises(check.ReplicaNotReadyError):
            runner.start()  # 已停止（非 RUNNING）

    def test_large_dataset_fully_batched(self, tmp_path):
        """数据量远大于批大小：全程分批，结果仍正确（覆盖分页路径）。"""
        src = str(tmp_path / "source.db")
        rep = str(tmp_path / "replica.db")
        chk = str(tmp_path / "check.db")
        source_db.init_demo(
            src, rows=[(i, f"t-{i}", f"l-{i}") for i in range(1, 251)]
        )
        check.init_check_db(chk)
        sync_once(src, rep)
        with replica_db.open_replica(rep) as conn:
            conn.execute("DELETE FROM specimens WHERE id IN (5, 250)")
            conn.execute("INSERT INTO specimens VALUES (999, 'x', 'y')")
            conn.execute("UPDATE specimens SET locality = 'bad' WHERE id = 7")

        run = run_check(src, rep, chk, batch_size=10)
        assert run["status"] == check.STATUS_DONE
        assert (run["missing_count"], run["extra_count"], run["mismatch_count"]) \
            == (2, 1, 1)
        assert run["compared_rows"] == 250
        assert run["staged_replica_rows"] == 249  # 250 - 2 删 + 1 增
        assert run["staged_source_rows"] == 250


# --------------------------------------------------------------------------- #
# 2. 不干扰源库 / 副本 / 原同步
# --------------------------------------------------------------------------- #
class TestNonInterference:
    def test_check_does_not_modify_source_or_replica(self, paths):
        src, rep, chk = paths
        sync_once(src, rep)
        before_src = source_rows(src)
        before_rep = replica_rows(rep)
        before_meta = replica_db.get_meta(rep)
        before_seq = source_db.max_change_seq(src)

        run = run_check(src, rep, chk)
        assert run["status"] == check.STATUS_DONE

        # 源库与运行中的副本均未被打扰
        assert source_rows(src) == before_src
        assert replica_rows(rep) == before_rep
        assert source_db.max_change_seq(src) == before_seq
        after_meta = replica_db.get_meta(rep)
        assert after_meta["last_applied_seq"] == before_meta["last_applied_seq"]
        assert after_meta["phase"] == before_meta["phase"]

    def test_source_writes_and_sync_continue_during_check(self, tmp_path):
        """取截面 / 补齐 / 比较期间：源写入不阻塞、原同步继续推进。"""
        src = str(tmp_path / "source.db")
        rep = str(tmp_path / "replica.db")
        chk = str(tmp_path / "check.db")
        source_db.init_demo(
            src, rows=[(i, f"t-{i}", f"l-{i}") for i in range(1, 301)]
        )
        check.init_check_db(chk)
        sync_once(src, rep)

        engine = SyncEngine(src, rep, poll_interval=0.05)
        engine.start()
        runner = CheckRunner(src, rep, chk, batch_size=7)
        started, run_id = runner.start()
        assert started
        try:
            # 核对进行期间源库持续写入：不得被核对持有的读事务阻塞
            t0 = time.time()
            for i in range(30):
                source_db.insert_specimen(src, 1000 + i, f"new-{i}", "during-check")
            elapsed = time.time() - t0
            assert elapsed < 15.0, f"源写入疑似被核对阻塞（{elapsed:.1f}s）"
            # 原同步继续可用：副本应能追上源端水位
            deadline = time.time() + 20
            while time.time() < deadline:
                meta = replica_db.get_meta(rep)
                if meta["last_applied_seq"] >= source_db.max_change_seq(src):
                    break
                time.sleep(0.05)
            else:
                pytest.fail("核对期间原同步未能继续推进")
        finally:
            runner.wait_until_idle(60)
            engine.stop()
            engine.wait_until_idle(20)

        run = check.get_run(chk, run_id)
        assert run["status"] == check.STATUS_DONE
        # 截面在各自读快照内一致 + 延迟已补齐 => 无差异
        assert (run["missing_count"], run["extra_count"], run["mismatch_count"]) \
            == (0, 0, 0)

    def test_only_one_check_at_a_time(self, paths, monkeypatch):
        src, rep, chk = paths
        sync_once(src, rep)
        gate = threading.Event()
        original = CheckRunner._pin_source

        def blocking(self, run_id, replica_seq):
            gate.wait(10)
            return original(self, run_id, replica_seq)

        monkeypatch.setattr(CheckRunner, "_pin_source", blocking)
        runner = CheckRunner(src, rep, chk)
        started1, run_id = runner.start()
        assert started1
        try:
            time.sleep(0.2)  # 等第一次核对进入阻塞点
            started2, run_id2 = runner.start()
            assert not started2 and run_id2 is None
        finally:
            gate.set()
            runner.wait_until_idle(30)
        assert check.get_run(chk, run_id)["status"] == check.STATUS_DONE


# --------------------------------------------------------------------------- #
# 3. 失败与重启语义
# --------------------------------------------------------------------------- #
class TestFailureSemantics:
    def test_failure_publishes_no_partial_conclusions(self, paths, monkeypatch):
        src, rep, chk = paths
        sync_once(src, rep)
        with replica_db.open_replica(rep) as conn:
            conn.execute("DELETE FROM specimens WHERE id = 2")  # 会产生 MISSING

        def boom(self, run_id):
            raise RuntimeError("simulated compare crash")

        # 正向比较（已写入 MISSING 差异）之后崩溃
        monkeypatch.setattr(CheckRunner, "_compare_extra", boom)
        runner = CheckRunner(src, rep, chk)
        started, run_id = runner.start()
        assert started
        runner.wait_until_idle(30)

        run = check.get_run(chk, run_id)
        assert run["status"] == check.STATUS_FAILED
        assert "simulated compare crash" in run["error"]
        # 部分结论被清除：差异、中间截面都不保留，计数归零
        assert check.get_diffs(chk, run_id)["total"] == 0
        assert (run["missing_count"], run["extra_count"], run["mismatch_count"]) \
            == (0, 0, 0)
        with check.open_check(chk) as conn:
            for table in ("staged_replica", "staged_source", "staged_log"):
                n = conn.execute(
                    f"SELECT COUNT(*) AS c FROM {table} WHERE run_id = ?",
                    (run_id,),
                ).fetchone()["c"]
                assert n == 0

    def test_restart_marks_unfinished_failed_and_keeps_reports(self, paths):
        src, rep, chk = paths
        sync_once(src, rep)
        with replica_db.open_replica(rep) as conn:
            conn.execute("DELETE FROM specimens WHERE id = 2")
        done_run = run_check(src, rep, chk)
        assert done_run["status"] == check.STATUS_DONE
        done_total = check.get_diffs(chk, done_run["id"])["total"]
        assert done_total == 1

        # 伪造一次“崩溃时仍在进行”的核对及其部分结论与中间数据
        with check.open_check(chk) as conn:
            cur = conn.execute(
                "INSERT INTO check_runs(status, phase) VALUES (?, ?)",
                (check.STATUS_RUNNING, check.PHASE_COMPARE),
            )
            stale_id = int(cur.lastrowid)
            conn.execute(
                "INSERT INTO check_diffs(run_id, category, specimen_id,"
                " source_taxon, source_locality)"
                " VALUES (?, 'MISSING', 5, 't', 'l')",
                (stale_id,),
            )
            conn.execute(
                "INSERT INTO staged_replica(run_id, id, taxon, locality)"
                " VALUES (?, 1, 'x', 'y')",
                (stale_id,),
            )

        recovered = check.recover_interrupted(chk)
        assert recovered == 1

        stale = check.get_run(chk, stale_id)
        assert stale["status"] == check.STATUS_FAILED
        assert stale["error"]
        assert stale["finished_at"]
        assert check.get_diffs(chk, stale_id)["total"] == 0
        with check.open_check(chk) as conn:
            n = conn.execute(
                "SELECT COUNT(*) AS c FROM staged_replica WHERE run_id = ?",
                (stale_id,),
            ).fetchone()["c"]
            assert n == 0

        # 已完成的完整报告在重启后保留
        kept = check.get_run(chk, done_run["id"])
        assert kept["status"] == check.STATUS_DONE
        assert check.get_diffs(chk, done_run["id"])["total"] == done_total


# --------------------------------------------------------------------------- #
# 4. Web API
# --------------------------------------------------------------------------- #
class TestCheckWeb:
    @pytest.fixture()
    def web(self, tmp_path):
        src = str(tmp_path / "source.db")
        rep = str(tmp_path / "replica.db")
        chk = str(tmp_path / "check.db")
        source_db.init_demo(
            src, rows=[(i, f"t-{i}", f"l-{i}") for i in range(1, 6)]
        )
        app = create_app(
            src, rep, poll_interval=0.05, auto_start=False,
            check_db_path=chk, check_batch_size=2,
        )
        app.config["TESTING"] = True
        with app.test_client() as c:
            yield c, app, src, rep, chk
        engine = app.config["SYNC_ENGINE"]
        engine.stop()
        engine.wait_until_idle(10)
        app.config["CHECK_RUNNER"].wait_until_idle(10)

    @staticmethod
    def _make_ready(c, app):
        assert c.post("/api/sync/start").get_json()["started"] is True
        engine = app.config["SYNC_ENGINE"]
        assert wait_phase(engine.replica_path, replica_db.PHASE_RUNNING)

    def test_check_page_served(self, web):
        c, *_ = web
        r = c.get("/check")
        assert r.status_code == 200
        assert "一致性核对".encode() in r.data

    def test_start_rejected_when_replica_not_ready(self, web):
        c, *_ = web
        r = c.post("/api/check/start")
        assert r.status_code == 409
        assert r.get_json()["error"] == "REPLICA_NOT_READY"

    def test_full_check_flow_via_api(self, web):
        c, app, src, rep, chk = web
        self._make_ready(c, app)

        # 演示注错：副本误留一条标本
        r = c.post("/api/demo/tamper_replica",
                   json={"op": "insert", "id": 900,
                         "taxon": "ghost", "locality": "nowhere"})
        assert r.status_code == 200

        r = c.post("/api/check/start")
        assert r.status_code == 200
        run_id = r.get_json()["run_id"]

        deadline = time.time() + 30
        while True:
            run = c.get(f"/api/check/runs/{run_id}").get_json()
            if run["status"] == check.STATUS_DONE:
                break
            assert time.time() < deadline, "核对未在时限内完成"
            time.sleep(0.1)

        assert run["extra_count"] == 1
        assert run["missing_count"] == 0 and run["mismatch_count"] == 0
        # 报告标明两端水位
        assert run["replica_seq"] >= 5
        assert run["source_watermark"] >= run["replica_seq"]

        # 分页差异：双方值都在
        d = c.get(f"/api/check/runs/{run_id}/diffs?category=EXTRA&limit=10")
        assert d.status_code == 200
        body = d.get_json()
        assert body["total"] == 1
        assert body["items"][0]["specimen_id"] == 900
        assert body["items"][0]["replica_taxon"] == "ghost"
        assert body["items"][0]["source_taxon"] is None

        # 非法类别 / 不存在核对
        assert c.get(
            f"/api/check/runs/{run_id}/diffs?category=BOGUS"
        ).status_code == 400
        assert c.get("/api/check/runs/999").status_code == 404
        assert c.get("/api/check/runs/999/diffs").status_code == 404

        # 历史列表包含本次核对
        runs = c.get("/api/check/runs").get_json()["runs"]
        assert runs and runs[0]["id"] == run_id

    def test_diffs_not_published_unless_done(self, web):
        c, app, src, rep, chk = web
        # 进行中与失败的核对都不发布部分结论
        with check.open_check(chk) as conn:
            cur = conn.execute(
                "INSERT INTO check_runs(status, phase) VALUES ('RUNNING', 'COMPARE')"
            )
            running_id = int(cur.lastrowid)
            cur = conn.execute(
                "INSERT INTO check_runs(status, phase, error)"
                " VALUES ('FAILED', 'COMPARE', 'boom')"
            )
            failed_id = int(cur.lastrowid)
            conn.execute(
                "INSERT INTO check_diffs(run_id, category, specimen_id)"
                " VALUES (?, 'MISSING', 1)",
                (failed_id,),
            )
        for rid in (running_id, failed_id):
            r = c.get(f"/api/check/runs/{rid}/diffs")
            assert r.status_code == 409
            assert r.get_json()["error"] == "REPORT_NOT_PUBLISHED"

    def test_double_start_conflict(self, web, monkeypatch):
        c, app, src, rep, chk = web
        self._make_ready(c, app)
        gate = threading.Event()
        runner = app.config["CHECK_RUNNER"]
        original = CheckRunner._pin_source

        def blocking(self, run_id, replica_seq):
            gate.wait(10)
            return original(self, run_id, replica_seq)

        monkeypatch.setattr(CheckRunner, "_pin_source", blocking)
        try:
            r1 = c.post("/api/check/start")
            assert r1.status_code == 200
            time.sleep(0.2)
            r2 = c.post("/api/check/start")
            assert r2.status_code == 409
            assert r2.get_json()["error"] == "CHECK_ALREADY_RUNNING"
        finally:
            gate.set()
            runner.wait_until_idle(30)
