#!/usr/bin/env python3
"""发布服务（独立进程，端口 7803）

职责：
- 发布前按登记服务的【当前】分类重新评估导出物 —— 这是“旧加工任务晚完成”
  也能被挡住的关键：判定发生在发布时，而不是任务调度时
- 三级处置：
    ok             → 直接发布到分发目录
    needs_approval → 敏感来源仅经假名化（掩码）进入产物，需登记服务中有对应
                     （导出名, 内容哈希）的人工审批才发布 → “安全替代版”流程
    blocked        → 阻断，记录判断依据；未知转换一律按阻断处理
- /enforce：分类变更后复核所有已分发产物，不再合规的移入隔离区
- /scan：用可追踪标记（CANARY）扫描分发/隔离目录，核对被阻断数据未再分发
"""
import hashlib
import json
import os
import re
import shutil
import sqlite3

from common import (Service, DATA_DIR, EXPORT_DIR, DIST_DIR, QUAR_DIR,
                    REGISTRY_URL, MARKER, ensure_dirs, now_iso, post, get)

PORT = 7803
DB = os.path.join(DATA_DIR, "publisher.db")
MARKER_RE = re.compile(MARKER, re.IGNORECASE)

svc = Service("publisher")


def db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    ensure_dirs()
    conn = db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS publish_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, export TEXT, content_hash TEXT,
        decision TEXT, reasons TEXT, actor TEXT);
    """)
    conn.commit()
    conn.close()


def log_decision(export, chash, decision, reasons, actor):
    conn = db()
    conn.execute("INSERT INTO publish_log(ts,export,content_hash,decision,reasons,actor)"
                 " VALUES(?,?,?,?,?,?)",
                 (now_iso(), export, chash, decision,
                  json.dumps(reasons, ensure_ascii=False), actor))
    conn.commit()
    conn.close()


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def staged_path(export):
    return os.path.join(EXPORT_DIR, export + ".csv")


def evaluate(export):
    st, res = post(REGISTRY_URL + "/evaluate", {"dataset": export})
    if st != 200:
        return None, res
    return res, None


def late_job_warning(export):
    """若产物由分类变更前启动的旧任务生成（晚完成），给出提示。判定仍按当前分类。"""
    meta_path = os.path.join(EXPORT_DIR, export + ".meta.json")
    if not os.path.exists(meta_path):
        return None
    with open(meta_path, encoding="utf-8") as f:
        meta = json.load(f)
    st, state = get(REGISTRY_URL + "/classification_state")
    if st == 200 and state.get("last_change_at") and meta.get("started_at"):
        if meta["started_at"] < state["last_change_at"]:
            return (f"该产物由分类变更前启动的旧任务生成（started_at={meta['started_at']} "
                    f"< 变更时间={state['last_change_at']}，任务版本 v"
                    f"{meta.get('classification_version_at_start')} < 当前 v{state['version']}），"
                    f"属晚完成产物，已按当前分类重新评估")
    return None


def decide(export, actor):
    """核心判定。返回 (decision, reasons, content_hash)。"""
    path = staged_path(export)
    if not os.path.exists(path):
        return "error", [f"导出 {export} 尚未生成，请先运行加工作业"], None
    chash = sha256_of(path)
    ev, err = evaluate(export)
    if ev is None:
        return "error", [f"登记服务评估失败: {err}"], chash

    reasons = []
    for r in ev["reasons"]:
        reasons.append(f"[阻断依据] 列 {r['column']}：{r['path']}")
    for u in ev["unknowns"]:
        reasons.append(f"[未知转换→按敏感阻断] 列 {u['column']}：{u['path']}")
    warn = late_job_warning(export)
    if warn:
        reasons.append("[晚完成] " + warn)

    verdict = ev["verdict"]
    if verdict == "ok":
        return "published", reasons or ["无敏感来源到达产物，可发布"], chash
    if verdict == "needs_approval":
        st, ap = get(REGISTRY_URL + f"/approvals?export={export}&content_hash={chash}")
        if st == 200 and ap.get("approved"):
            a = ap["approval"]
            reasons.append(f"安全替代版已获审批（{a['actor']} @ {a['created_at']}），准予发布")
            return "published", reasons, chash
        reasons.append("敏感来源仅经假名化（掩码）进入产物：需人工审批该内容哈希后才可发布")
        return "needs_approval", reasons, chash
    return "blocked", reasons, chash


@svc.route("GET", "/health")
def health(q, body=None):
    return {"service": "publisher", "ok": True}


@svc.route("POST", "/publish")
def publish(q, body):
    export = body["export"]
    actor = body.get("actor", "anonymous")
    decision, reasons, chash = decide(export, actor)
    if decision == "published":
        shutil.copyfile(staged_path(export), os.path.join(DIST_DIR, export + ".csv"))
    if decision != "error":
        log_decision(export, chash, decision, reasons, actor)
    return {"export": export, "decision": decision, "reasons": reasons,
            "content_hash": chash}


@svc.route("POST", "/enforce")
def enforce(q, body=None):
    """分类变更后复核所有已分发产物：不再合规的移入隔离区（不再被分发）。"""
    actor = (body or {}).get("actor", "enforce")
    actions = []
    for fname in sorted(os.listdir(DIST_DIR)):
        if not fname.endswith(".csv"):
            continue
        export = fname[:-4]
        dist_file = os.path.join(DIST_DIR, fname)
        chash = sha256_of(dist_file)
        ev, err = evaluate(export)
        if ev is None:
            # 登记服务都查不到血缘的产物：未知情况，fail-safe 隔离
            quar = os.path.join(QUAR_DIR, f"{export}.{now_iso().replace(':', '-')}.csv")
            shutil.move(dist_file, quar)
            actions.append({"export": export, "action": "quarantine",
                            "reason": f"无法评估（{err}），未知情况按不合规处理"})
            log_decision(export, chash, "quarantine", actions[-1]["reason"], actor)
            continue
        noncompliant = ev["verdict"] == "blocked"
        if ev["verdict"] == "needs_approval":
            st, ap = get(REGISTRY_URL + f"/approvals?export={export}&content_hash={chash}")
            noncompliant = not (st == 200 and ap.get("approved"))
        if noncompliant:
            quar = os.path.join(QUAR_DIR, f"{export}.{now_iso().replace(':', '-')}.csv")
            shutil.move(dist_file, quar)
            reason = (f"复核判定 {ev['verdict']}：" + ev["policy"])
            actions.append({"export": export, "action": "quarantine", "reason": reason})
            log_decision(export, chash, "quarantine", [reason], actor)
        else:
            actions.append({"export": export, "action": "keep",
                            "reason": f"复核判定 {ev['verdict']}，继续分发"})
    return {"ok": True, "actions": actions}


@svc.route("GET", "/status")
def status(q, body=None):
    """各导出物的当前状态：staging、判定、分发中/隔离、最近决策。"""
    st, res = get(REGISTRY_URL + "/fields")
    exports = sorted({f["dataset"] for f in res.get("fields", []) if f["kind"] == "export"}) \
        if st == 200 else []
    for fname in os.listdir(EXPORT_DIR):
        if fname.endswith(".csv") and fname[:-4] not in exports:
            exports.append(fname[:-4])
    conn = db()
    out = []
    for export in sorted(exports):
        staged = os.path.exists(staged_path(export))
        chash = sha256_of(staged_path(export)) if staged else None
        ev, _ = evaluate(export) if staged else (None, None)
        last = conn.execute(
            "SELECT * FROM publish_log WHERE export=? ORDER BY id DESC LIMIT 1",
            (export,)).fetchone()
        out.append({
            "export": export,
            "staged": staged,
            "staged_hash": chash,
            "distributed": os.path.exists(os.path.join(DIST_DIR, export + ".csv")),
            "verdict": ev["verdict"] if ev else None,
            "reasons": (ev["reasons"] + ev["unknowns"]) if ev else [],
            "policy": ev["policy"] if ev else None,
            "last_decision": dict(last) if last else None,
        })
    conn.close()
    return {"exports": out}


@svc.route("GET", "/scan")
def scan(q, body=None):
    """用可追踪标记扫描目录：核对被阻断的数据是否仍出现在分发目录。"""
    result = {}
    for label, d in (("distribution", DIST_DIR), ("quarantine", QUAR_DIR)):
        files = {}
        for fname in sorted(os.listdir(d)):
            if not fname.endswith(".csv"):
                continue
            with open(os.path.join(d, fname), encoding="utf-8") as f:
                hits = len(MARKER_RE.findall(f.read()))
            if hits:
                files[fname] = hits
        result[label] = {"files": files, "total": sum(files.values())}
    result["marker"] = MARKER
    result["ok"] = result["distribution"]["total"] == 0
    return result


@svc.route("GET", "/log")
def get_log(q, body=None):
    conn = db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM publish_log ORDER BY id DESC LIMIT 200")]
    conn.close()
    for r in rows:
        try:
            r["reasons"] = json.loads(r["reasons"])
        except Exception:
            pass
    return {"log": rows}


if __name__ == "__main__":
    init_db()
    svc.run(PORT)
