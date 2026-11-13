"""自然博物馆标本登记库同步 Web 控制台（Flask）。

页面与 API：
  GET  /                 控制台页面（进度、错误、操作按钮、查询框）
  POST /api/sync/start   启动后台同步
  POST /api/sync/stop    请求停止后台同步
  GET  /api/status       同步进度与错误（JSON）
  GET  /api/specimen/<id>  按标本号查询副本（就绪前返回 503）

  # 一致性核对（后台线程，只读源库与副本，结论写入独立核对库）
  GET  /check                       核对页面（进度、差异报告、历史）
  POST /api/check/start             受理一次核对（仅已就绪副本；进行中返回 409）
  GET  /api/check/runs              最近核对列表
  GET  /api/check/runs/<id>         单次核对详情（进度 / 两端水位 / 差异计数）
  GET  /api/check/runs/<id>/diffs   分页差异（仅 DONE 发布；失败/进行中 409）

  # 演示用：直接对源库提交增 / 改 / 撤（鉴定员的日常操作）
  POST /api/demo/insert
  POST /api/demo/update
  POST /api/demo/delete
  GET  /api/source/specimens  查看源库当前标本（仅演示辅助）
  POST /api/demo/tamper_replica  直接改写副本（仅演示：让核对能发现差异）
"""
from __future__ import annotations

import os

from flask import Flask, jsonify, render_template, request

from . import check, replica_db, source_db
from .sync import SyncEngine

SOURCE_DB = os.environ.get("SOURCE_DB", "/data/source/specimens.db")
REPLICA_DB = os.environ.get("REPLICA_DB", "/data/replica/specimens_replica.db")
CHECK_DB = os.environ.get("CHECK_DB", "/data/check/consistency_check.db")
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "1.0"))
# 容器启动时是否自动开始同步（1/0）
AUTO_START = os.environ.get("AUTO_START", "1") == "1"


def create_app(
    source_db_path: str | None = None,
    replica_db_path: str | None = None,
    poll_interval: float | None = None,
    auto_start: bool | None = None,
    check_db_path: str | None = None,
    check_batch_size: int | None = None,
) -> Flask:
    app = Flask(__name__)
    source_path = source_db_path or SOURCE_DB
    replica_path = replica_db_path or REPLICA_DB
    check_path = check_db_path or CHECK_DB
    interval = POLL_INTERVAL if poll_interval is None else poll_interval
    do_auto_start = AUTO_START if auto_start is None else auto_start

    engine = SyncEngine(source_path, replica_path, poll_interval=interval)
    app.config["SYNC_ENGINE"] = engine

    # 核对库：初始化结构；重启恢复——未完成的核对标记失败，已完成报告保留
    check.init_check_db(check_path)
    check.recover_interrupted(check_path)
    check_runner = check.CheckRunner(
        source_path,
        replica_path,
        check_path,
        batch_size=check_batch_size or check.CHECK_BATCH_SIZE,
    )
    app.config["CHECK_RUNNER"] = check_runner
    app.config["CHECK_DB_PATH"] = check_path

    # 确保示例源库与副本结构存在（源库不存在则播种演示数据）
    source_db.init_demo(source_path)
    replica_db.init_replica(replica_path)
    if do_auto_start:
        engine.start()

    # ----------------------------- 页面 ----------------------------- #
    @app.get("/")
    def index():
        return render_template("index.html")

    # --------------------------- 同步控制 --------------------------- #
    @app.post("/api/sync/start")
    def sync_start():
        # 每次启动前确保源库 CDC 对象齐全（兼容外部替换的库文件）
        source_db.init_source(source_path)
        started = engine.start()
        return jsonify({"started": started, "status": engine.status()})

    @app.post("/api/sync/stop")
    def sync_stop():
        stopped = engine.stop()
        return jsonify({"stop_requested": stopped, "status": engine.status()})

    @app.get("/api/status")
    def status():
        st = engine.status()
        st["source"] = {
            "path": source_path,
            "specimen_count": _safe(source_db.count_specimens, source_path),
            "max_change_seq": _safe(source_db.max_change_seq, source_path),
        }
        return jsonify(st)

    # --------------------------- 副本查询 --------------------------- #
    @app.get("/api/specimen/<int:specimen_id>")
    def get_specimen(specimen_id: int):
        # 就绪前不供查询
        if not replica_db.is_ready(replica_path):
            meta = replica_db.get_meta(replica_path)
            return (
                jsonify(
                    {
                        "error": "REPLICA_NOT_READY",
                        "message": "副本尚未完成首次复制，暂不提供查询",
                        "phase": meta["phase"],
                    }
                ),
                503,
            )
        row = replica_db.get_specimen(replica_path, specimen_id)
        if row is None:
            return (
                jsonify(
                    {
                        "error": "NOT_FOUND",
                        "message": f"副本中不存在标本 {specimen_id}"
                        "（可能已被鉴定员撤回）",
                        "id": specimen_id,
                    }
                ),
                404,
            )
        return jsonify(row)

    # --------------------- 演示：源库增 / 改 / 撤 --------------------- #
    @app.post("/api/demo/insert")
    def demo_insert():
        data = request.get_json(force=True)
        try:
            specimen_id = int(data["id"])
            source_db.insert_specimen(
                source_path, specimen_id, str(data["taxon"]), str(data["locality"])
            )
        except KeyError as exc:
            return jsonify({"error": "BAD_REQUEST", "message": str(exc)}), 400
        except Exception as exc:  # 主键冲突等
            return jsonify({"error": "SOURCE_ERROR", "message": str(exc)}), 409
        return jsonify({"ok": True})

    @app.post("/api/demo/update")
    def demo_update():
        data = request.get_json(force=True)
        try:
            specimen_id = int(data["id"])
            source_db.update_specimen(
                source_path, specimen_id, str(data["taxon"]), str(data["locality"])
            )
        except KeyError as exc:
            return jsonify({"error": "NOT_FOUND", "message": str(exc)}), 404
        except Exception as exc:
            return jsonify({"error": "SOURCE_ERROR", "message": str(exc)}), 409
        return jsonify({"ok": True})

    @app.post("/api/demo/delete")
    def demo_delete():
        data = request.get_json(force=True)
        try:
            specimen_id = int(data["id"])
            source_db.delete_specimen(source_path, specimen_id)
        except KeyError as exc:
            return jsonify({"error": "NOT_FOUND", "message": str(exc)}), 404
        return jsonify({"ok": True})

    @app.get("/api/source/specimens")
    def source_specimens():
        conn = source_db.connect(source_path, readonly=True)
        try:
            rows = conn.execute(
                "SELECT id, taxon, locality FROM specimens ORDER BY id"
            ).fetchall()
        finally:
            conn.close()
        return jsonify([dict(r) for r in rows])

    # --------------------------- 一致性核对 --------------------------- #
    @app.get("/check")
    def check_page():
        return render_template("check.html")

    @app.post("/api/check/start")
    def check_start():
        try:
            started, run_id = check_runner.start()
        except check.ReplicaNotReadyError as exc:
            return (
                jsonify({"error": "REPLICA_NOT_READY", "message": str(exc)}),
                409,
            )
        if not started:
            return (
                jsonify(
                    {
                        "error": "CHECK_ALREADY_RUNNING",
                        "message": "已有一次核对正在进行，请等待其完成",
                    }
                ),
                409,
            )
        return jsonify(
            {"started": True, "run_id": run_id, "run": check.get_run(check_path, run_id)}
        )

    @app.get("/api/check/runs")
    def check_runs():
        limit = _int_arg("limit", 20, 1, 100)
        return jsonify({"runs": check.list_runs(check_path, limit)})

    @app.get("/api/check/runs/<int:run_id>")
    def check_run_detail(run_id: int):
        run = check.get_run(check_path, run_id)
        if run is None:
            return (
                jsonify({"error": "NOT_FOUND", "message": f"核对 {run_id} 不存在"}),
                404,
            )
        return jsonify(run)

    @app.get("/api/check/runs/<int:run_id>/diffs")
    def check_run_diffs(run_id: int):
        run = check.get_run(check_path, run_id)
        if run is None:
            return (
                jsonify({"error": "NOT_FOUND", "message": f"核对 {run_id} 不存在"}),
                404,
            )
        # 失败不发布部分结论：只有完成的核对才提供差异报告
        if run["status"] != check.STATUS_DONE:
            return (
                jsonify(
                    {
                        "error": "REPORT_NOT_PUBLISHED",
                        "message": "核对未完成或已失败，不发布部分结论",
                        "status": run["status"],
                    }
                ),
                409,
            )
        category = request.args.get("category") or None
        if category is not None and category not in check.CATEGORIES:
            return (
                jsonify(
                    {
                        "error": "BAD_CATEGORY",
                        "message": f"category 须为 {list(check.CATEGORIES)} 之一",
                    }
                ),
                400,
            )
        offset = _int_arg("offset", 0, 0, 1 << 62)
        limit = _int_arg("limit", 50, 1, 200)
        return jsonify(check.get_diffs(check_path, run_id, category, offset, limit))

    # --------------------- 演示：往副本注入问题 --------------------- #
    @app.post("/api/demo/tamper_replica")
    def demo_tamper_replica():
        """仅演示：直接改写副本（绕过同步器），模拟漏录/误留/改坏，
        供一致性核对发现。真实系统中副本只由同步器写入。"""
        data = request.get_json(force=True)
        op = data.get("op")
        try:
            specimen_id = int(data["id"])
        except (KeyError, TypeError, ValueError):
            return jsonify({"error": "BAD_REQUEST", "message": "需要整数 id"}), 400
        taxon = str(data.get("taxon", "tampered-taxon"))
        locality = str(data.get("locality", "tampered-locality"))
        with replica_db.open_replica(replica_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            if op == "delete":  # 模拟漏录
                conn.execute("DELETE FROM specimens WHERE id = ?", (specimen_id,))
            elif op == "insert":  # 模拟误留
                conn.execute(
                    "INSERT OR REPLACE INTO specimens(id, taxon, locality)"
                    " VALUES (?, ?, ?)",
                    (specimen_id, taxon, locality),
                )
            elif op == "update":  # 模拟改坏
                conn.execute(
                    "UPDATE specimens SET taxon = ?, locality = ? WHERE id = ?",
                    (taxon, locality, specimen_id),
                )
            else:
                conn.execute("ROLLBACK")
                return (
                    jsonify(
                        {
                            "error": "BAD_REQUEST",
                            "message": "op 须为 delete/insert/update",
                        }
                    ),
                    400,
                )
            conn.execute("COMMIT")
        return jsonify({"ok": True, "tampered": {"op": op, "id": specimen_id}})

    @app.get("/healthz")
    def healthz():
        return jsonify({"ok": True})

    return app


def _int_arg(name: str, default: int, lo: int, hi: int) -> int:
    """解析整数查询参数并夹取到 [lo, hi]，非法值回退默认。"""
    try:
        value = int(request.args.get(name, default))
    except (TypeError, ValueError):
        value = default
    return max(lo, min(hi, value))


def _safe(fn, *args):
    """源库可能被外部占用/锁定，状态接口不应因此 500。"""
    try:
        return fn(*args)
    except Exception as exc:  # pragma: no cover - 防御性
        return f"error: {type(exc).__name__}: {exc}"


app = create_app()
