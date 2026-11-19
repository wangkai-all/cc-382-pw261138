#!/usr/bin/env python3
"""加工调度器（独立进程，端口 7802）

职责：
- 生成源数据（/seed）并向登记服务登记源表结构
- 按 DAG 执行加工作业：源表 → 清洗表 → 聚合表 → 导出（staging）
- 每次运行把列级血缘（含表达式来源、转换方式、聚合最小组大小）上报登记服务
- 支持“模拟旧任务晚完成”：以过去的 started_at 执行作业，产物在分类变更后才到达发布环节

注意：调度器自身不做分类判断——它与登记服务独立运行，只负责上报血缘；
是否可发布由发布服务在发布时按登记服务的当前分类判定。
"""
import json
import os
import sqlite3

from common import (Service, DATA_DIR, EXPORT_DIR, REGISTRY_URL, ensure_dirs,
                    now_iso, post, get)
from jobs import JOBS, DAG_ORDER
import seed_data

PORT = 7802
DB = os.path.join(DATA_DIR, "scheduler.db")

svc = Service("scheduler")


def db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    ensure_dirs()
    conn = db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS runs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job TEXT, started_at TEXT, finished_at TEXT, status TEXT, rows_written INTEGER,
        classification_version_at_start INTEGER, classification_version_at_end INTEGER,
        simulated_late INTEGER DEFAULT 0, note TEXT);
    """)
    conn.commit()
    conn.close()


def registry_version():
    st, res = get(REGISTRY_URL + "/classification_state")
    return res.get("version", -1) if st == 200 else -1


def run_job(job_name, started_at=None, actor="scheduler"):
    """执行一个作业：计算 → 写产物 → 登记数据集 → 上报血缘 → 记录运行。"""
    if job_name not in JOBS:
        return 404, {"error": f"未知作业 {job_name}"}
    spec = JOBS[job_name]
    v0 = registry_version()
    t0 = now_iso()
    rows_written, meta = spec["compute"]()
    t1 = now_iso()
    v1 = registry_version()
    simulated_late = started_at is not None

    output = spec["output"]
    # 导出物写 sidecar 元数据（发布服务据此识别“晚完成”的旧任务产物）
    if spec["kind"] == "export":
        sidecar = {
            "export": output, "job": job_name,
            "started_at": started_at or t0, "finished_at": t1,
            "classification_version_at_start": v0,
            "simulated_late": simulated_late,
        }
        with open(os.path.join(EXPORT_DIR, output + ".meta.json"), "w", encoding="utf-8") as f:
            json.dump(sidecar, f, ensure_ascii=False, indent=1)

    # 登记数据集结构
    post(REGISTRY_URL + "/datasets", {
        "dataset": output, "kind": spec["kind"],
        "fields": [c["name"] for c in spec["columns"]], "actor": actor,
    })
    # 上报列级血缘：拼接、派生表达式的来源都保留
    edges = []
    for c in spec["columns"]:
        for src in c["sources"]:
            src_ds, src_col = src.split(".", 1)
            edges.append({
                "src_dataset": src_ds, "src_column": src_col,
                "dst_dataset": output, "dst_column": c["name"],
                "transform": c["transform"],
                "meta": {**meta, "expr": c["expr"]},
            })
    post(REGISTRY_URL + "/lineage", {"job": job_name, "edges": edges, "actor": actor})

    drift = v0 != v1
    note = "执行期间分类发生变更（版本漂移）" if drift else ""
    conn = db()
    cur = conn.execute(
        "INSERT INTO runs(job,started_at,finished_at,status,rows_written,"
        " classification_version_at_start,classification_version_at_end,simulated_late,note)"
        " VALUES(?,?,?,?,?,?,?,?,?)",
        (job_name, started_at or t0, t1, "done", rows_written, v0, v1,
         1 if simulated_late else 0, note))
    conn.commit()
    run_id = cur.lastrowid
    conn.close()
    return 200, {
        "run_id": run_id, "job": job_name, "output": output, "kind": spec["kind"],
        "rows": rows_written, "started_at": started_at or t0, "finished_at": t1,
        "classification_version_at_start": v0, "classification_version_at_end": v1,
        "classification_drift": drift, "simulated_late": simulated_late,
        "meta": meta, "note": note,
    }


@svc.route("GET", "/health")
def health(q, body=None):
    return {"service": "scheduler", "ok": True}


@svc.route("POST", "/seed")
def seed(q, body=None):
    stats = seed_data.generate()
    for ds, cols in {
        "customers": ["customer_id", "first_name", "last_name", "email", "phone",
                      "city", "signup_date"],
        "orders": ["order_id", "customer_id", "amount", "order_date", "channel"],
    }.items():
        post(REGISTRY_URL + "/datasets",
             {"dataset": ds, "kind": "source", "fields": cols, "actor": "seed"})
    return {"ok": True, "stats": stats}


@svc.route("POST", "/run")
def run(q, body):
    return run_job(body["job"], started_at=body.get("started_at"),
                   actor=body.get("actor", "scheduler"))


@svc.route("POST", "/run_all")
def run_all(q, body=None):
    results = []
    for name in DAG_ORDER:
        code, res = run_job(name)
        if code != 200:
            return code, {"error": f"作业 {name} 失败", "detail": res}
        results.append(res)
    return {"ok": True, "runs": results}


@svc.route("GET", "/jobs")
def list_jobs(q, body=None):
    return {"jobs": [{
        "name": name,
        "output": spec["output"],
        "kind": spec["kind"],
        "columns": [{"name": c["name"], "expr": c["expr"],
                     "transform": c["transform"], "sources": c["sources"]}
                    for c in spec["columns"]],
    } for name, spec in JOBS.items()], "dag_order": DAG_ORDER}


@svc.route("GET", "/runs")
def list_runs(q, body=None):
    conn = db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 100")]
    conn.close()
    return {"runs": rows}


if __name__ == "__main__":
    init_db()
    svc.run(PORT)
