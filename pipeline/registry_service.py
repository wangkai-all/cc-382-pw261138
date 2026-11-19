#!/usr/bin/env python3
"""分类登记服务（独立进程，端口 7801）

职责：
- 登记数据集与字段分类（normal / internal / sensitive）
- 接收加工调度器上报的列级血缘（含拼接、派生表达式的来源，而非只看最终列名）
- 分类变更时做影响分析：沿血缘向下游传播，给出每个节点的判断依据
- 对导出物给出三级判定：ok / needs_approval / blocked
- 安全替代版审批、审计日志、分类变更后通知发布服务复核（webhook）

传播规则（判断依据）：
- copy/concat/substring/lower/trim/hash ：保留来源数据，敏感性原样传播
  （hash 对手机号这类低熵值可彩虹表逆推，视为保留）
- mask（仅留后4位）：假名化，降级为 needs_approval（可链接，需人工审批）
- aggregate 且 min_group_size >= K：匿名化，敏感不再向下传播
- aggregate 且 min_group_size < K：小样本组可反推，按保留处理
- unknown（不可解析的外部表达式/UDF）：未知情况 → fail-safe 按敏感阻断，并列入人工核查
"""
import json
import os
import sqlite3
from collections import defaultdict, deque

from common import (Service, DATA_DIR, PUBLISHER_URL, ensure_dirs, now_iso, post)

PORT = 7801
DB = os.path.join(DATA_DIR, "registry.db")
K_MIN = 10  # 聚合匿名化的最小组大小

PROPAGATING = {"copy", "concat", "substring", "lower", "trim", "hash"}
PSEUDO = {"mask"}
UNKNOWN = "unknown"

TRANSFORM_LABEL = {
    "copy": "直接复制",
    "concat": "拼接（保留来源数据）",
    "substring": "截取（保留部分来源）",
    "lower": "小写化（保留数据）",
    "trim": "去空白（保留数据）",
    "hash": "哈希（低熵值可逆，视为保留）",
    "mask": "掩码（仅留后4位，假名化）",
    "aggregate": "聚合",
    "unknown": "未知表达式（fail-safe 按敏感处理）",
}

TIER_OK, TIER_PSEUDO, TIER_BLOCKED = 0, 1, 2
VERDICT = {TIER_OK: "ok", TIER_PSEUDO: "needs_approval", TIER_BLOCKED: "blocked"}

svc = Service("registry")


def db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    ensure_dirs()
    conn = db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS datasets(
        dataset TEXT PRIMARY KEY, kind TEXT, registered_at TEXT);
    CREATE TABLE IF NOT EXISTS fields(
        dataset TEXT, column_name TEXT, classification TEXT DEFAULT 'normal',
        updated_at TEXT, updated_by TEXT, PRIMARY KEY(dataset, column_name));
    CREATE TABLE IF NOT EXISTS lineage(
        id INTEGER PRIMARY KEY AUTOINCREMENT, job TEXT,
        src_dataset TEXT, src_column TEXT, dst_dataset TEXT, dst_column TEXT,
        transform TEXT, meta TEXT, created_at TEXT);
    CREATE TABLE IF NOT EXISTS approvals(
        export TEXT, content_hash TEXT, actor TEXT, note TEXT, created_at TEXT,
        PRIMARY KEY(export, content_hash));
    CREATE TABLE IF NOT EXISTS audit(
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, actor TEXT, action TEXT, detail TEXT);
    CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
    """)
    conn.execute("INSERT OR IGNORE INTO meta(k,v) VALUES('classification_version','0')")
    conn.execute("INSERT OR IGNORE INTO meta(k,v) VALUES('last_change_at','')")
    conn.commit()
    conn.close()


def audit(conn, actor, action, detail):
    conn.execute("INSERT INTO audit(ts,actor,action,detail) VALUES(?,?,?,?)",
                 (now_iso(), actor, action, json.dumps(detail, ensure_ascii=False)))


def load_state():
    conn = db()
    datasets = {r["dataset"]: dict(r) for r in conn.execute("SELECT * FROM datasets")}
    fields = {}
    for r in conn.execute("SELECT * FROM fields"):
        fields[(r["dataset"], r["column_name"])] = dict(r)
    edges = [dict(r) for r in conn.execute("SELECT * FROM lineage")]
    conn.close()
    return datasets, fields, edges


def edge_label(t, meta):
    label = TRANSFORM_LABEL.get(t, f"未登记转换 {t}（按未知处理）")
    if t == "aggregate":
        k = meta.get("min_group_size")
        label += f"（最小组 k={k}，{'≥%d 匿名化' % K_MIN if (k or 0) >= K_MIN else '<%d 不匿名' % K_MIN}）"
    return label


def edge_effect(t, meta, in_tier, in_unk):
    """一条血缘边对敏感性的影响。返回 (新等级, 未知标记)；等级为 None 表示匿名化、不再传播。"""
    if t == "aggregate":
        if (meta.get("min_group_size") or 0) >= K_MIN:
            return (None, False)  # k 足够大，匿名化成立
        return (in_tier, in_unk)  # 小样本组，按保留处理
    if t in PROPAGATING:
        return (in_tier, in_unk)
    if t in PSEUDO:
        return (min(in_tier, TIER_PSEUDO), in_unk)
    # unknown 或任何未登记的转换：未知情况 fail-safe
    return (TIER_BLOCKED, True)


def compute_tiers(fields, edges):
    """从所有敏感字段出发沿血缘前向传播，返回每个列节点的 (等级, 未知标记, 判断依据路径)。"""
    fwd = defaultdict(list)
    for e in edges:
        fwd[(e["src_dataset"], e["src_column"])].append(e)
    tier, unk, witness = {}, {}, {}
    dq = deque()
    for key, f in fields.items():
        if f["classification"] == "sensitive":
            tier[key] = TIER_BLOCKED
            unk[key] = False
            witness[key] = f"{key[0]}.{key[1]} 分类=敏感"
            dq.append(key)
    while dq:
        node = dq.popleft()
        for e in fwd.get(node, []):
            dst = (e["dst_dataset"], e["dst_column"])
            meta = json.loads(e["meta"] or "{}")
            nt, nu = edge_effect(e["transform"], meta, tier[node], unk[node])
            if nt is None:
                continue  # 匿名化，敏感到此为止
            cur = (tier.get(dst, TIER_OK), unk.get(dst, False))
            if (nt, nu) > cur:  # 只向更坏的方向松弛，图是 DAG，必然终止
                tier[dst], unk[dst] = nt, nu
                witness[dst] = (witness[node]
                                + f" --[{edge_label(e['transform'], meta)}]--> {dst[0]}.{dst[1]}")
                dq.append(dst)
    return tier, unk, witness


def evaluate_dataset(dataset):
    """三级判定：ok / needs_approval / blocked，附判断依据与未知项。"""
    datasets, fields, edges = load_state()
    if dataset not in datasets:
        return None
    tier, unk, witness = compute_tiers(fields, edges)
    cols = [c for (d, c) in fields if d == dataset]
    worst = TIER_OK
    reasons, unknowns = [], []
    for c in sorted(cols):
        key = (dataset, c)
        t = tier.get(key, TIER_OK)
        u = unk.get(key, False)
        eff = TIER_BLOCKED if u else t  # 未知一律按敏感处理
        if eff > worst:
            worst = eff
        if eff == TIER_OK:
            continue
        item = {"column": c, "tier": eff, "unknown": u,
                "path": witness.get(key, "")}
        (unknowns if u else reasons).append(item)
    return {"dataset": dataset, "verdict": VERDICT[worst],
            "reasons": reasons, "unknowns": unknowns,
            "policy": ("存在敏感来源且未经任何脱敏直达产物，阻断"
                       if worst == TIER_BLOCKED else
                       "敏感来源仅经假名化（掩码）进入产物，需人工审批后发布"
                       if worst == TIER_PSEUDO else
                       "无敏感来源到达产物，可发布")}


def impact_tree(dataset, column):
    """从某字段出发的前向影响树，每条边标注转换方式与判断依据。"""
    datasets, fields, edges = load_state()
    fwd = defaultdict(list)
    for e in edges:
        fwd[(e["src_dataset"], e["src_column"])].append(e)

    def build(node, in_tier, in_unk, seen, depth):
        node_ds, node_col = node
        info = {"node": f"{node_ds}.{node_col}", "tier": in_tier, "unknown": in_unk,
                "children": []}
        if depth > 12:
            return info
        for e in fwd.get(node, []):
            dst = (e["dst_dataset"], e["dst_column"])
            if dst in seen:
                continue
            meta = json.loads(e["meta"] or "{}")
            nt, nu = edge_effect(e["transform"], meta, in_tier, in_unk)
            if nt is None:
                child = {"node": f"{dst[0]}.{dst[1]}", "tier": TIER_OK, "unknown": False,
                         "edge": edge_label(e["transform"], meta),
                         "note": "聚合匿名化成立，敏感不再向下传播", "children": []}
            else:
                child = build(dst, nt, nu, seen | {dst}, depth + 1)
                child["edge"] = edge_label(e["transform"], meta)
                if nu and not in_unk:
                    child["note"] = "未知转换：无法证明不含敏感来源，fail-safe 按敏感处理"
            info["children"].append(child)
        return info

    return build((dataset, column), TIER_BLOCKED, False, {(dataset, column)}, 0)


def exports_overview():
    """所有导出物的当前判定，用于影响摘要与‘精确阻断 vs 全部停止’对比。"""
    datasets, fields, edges = load_state()
    exports = sorted(d for d, info in datasets.items() if info["kind"] == "export")
    out = []
    for name in exports:
        ev = evaluate_dataset(name)
        out.append({"export": name, "verdict": ev["verdict"],
                    "n_reasons": len(ev["reasons"]), "n_unknowns": len(ev["unknowns"])})
    return out


# ---------------- API ----------------

@svc.route("GET", "/health")
def health(q, body=None):
    return {"service": "registry", "ok": True}


@svc.route("POST", "/datasets")
def register_dataset(q, body):
    """调度器登记数据集结构。已存在的字段保留原分类。"""
    dataset, kind = body["dataset"], body.get("kind", "derived")
    cols = body.get("fields", [])
    conn = db()
    conn.execute("INSERT OR REPLACE INTO datasets(dataset,kind,registered_at) VALUES(?,?,?)",
                 (dataset, kind, now_iso()))
    for c in cols:
        conn.execute("INSERT OR IGNORE INTO fields(dataset,column_name,classification,updated_at,updated_by)"
                     " VALUES(?,?,?,?,?)", (dataset, c, "normal", now_iso(), "system"))
    audit(conn, body.get("actor", "scheduler"), "register_dataset",
          {"dataset": dataset, "kind": kind, "fields": cols})
    conn.commit()
    conn.close()
    return {"ok": True, "dataset": dataset, "fields": cols}


@svc.route("POST", "/lineage")
def register_lineage(q, body):
    """调度器上报一次作业产生的列级血缘（含表达式来源与转换方式）。"""
    job = body["job"]
    edges = body.get("edges", [])
    conn = db()
    conn.execute("DELETE FROM lineage WHERE job=?", (job,))  # 重跑幂等
    for e in edges:
        conn.execute(
            "INSERT INTO lineage(job,src_dataset,src_column,dst_dataset,dst_column,transform,meta,created_at)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (job, e["src_dataset"], e["src_column"], e["dst_dataset"], e["dst_column"],
             e["transform"], json.dumps(e.get("meta", {}), ensure_ascii=False), now_iso()))
    audit(conn, body.get("actor", "scheduler"), "register_lineage",
          {"job": job, "n_edges": len(edges)})
    conn.commit()
    conn.close()
    return {"ok": True, "job": job, "n_edges": len(edges)}


@svc.route("GET", "/fields")
def list_fields(q, body=None):
    """所有字段：源字段给登记分类，派生字段另给传播计算出的有效状态。"""
    datasets, fields, edges = load_state()
    tier, unk, witness = compute_tiers(fields, edges)
    rows = []
    for (ds, col), f in sorted(fields.items()):
        key = (ds, col)
        t = tier.get(key, TIER_OK)
        u = unk.get(key, False)
        rows.append({
            "dataset": ds, "column": col, "kind": datasets.get(ds, {}).get("kind", "?"),
            "classification": f["classification"],
            "effective": VERDICT[TIER_BLOCKED if u else t],
            "unknown": u,
            "path": witness.get(key, ""),
            "updated_by": f.get("updated_by"), "updated_at": f.get("updated_at"),
        })
    return {"fields": rows}


@svc.route("POST", "/classifications")
def change_classification(q, body):
    """修改字段分类。返回影响摘要，并 webhook 通知发布服务复核已分发产物。"""
    dataset, column = body["dataset"], body["column"]
    new_cls = body["classification"]
    actor = body.get("actor", "anonymous")
    if new_cls not in ("normal", "internal", "sensitive"):
        return 400, {"error": "classification 须为 normal/internal/sensitive"}
    conn = db()
    row = conn.execute("SELECT classification FROM fields WHERE dataset=? AND column_name=?",
                       (dataset, column)).fetchone()
    if not row:
        conn.close()
        return 404, {"error": f"字段不存在: {dataset}.{column}"}
    old = row["classification"]
    conn.execute("UPDATE fields SET classification=?,updated_at=?,updated_by=?"
                 " WHERE dataset=? AND column_name=?",
                 (new_cls, now_iso(), actor, dataset, column))
    ver = int(conn.execute("SELECT v FROM meta WHERE k='classification_version'").fetchone()["v"]) + 1
    conn.execute("UPDATE meta SET v=? WHERE k='classification_version'", (str(ver),))
    conn.execute("UPDATE meta SET v=? WHERE k='last_change_at'", (now_iso(),))
    audit(conn, actor, "classification_change",
          {"field": f"{dataset}.{column}", "old": old, "new": new_cls, "version": ver})
    conn.commit()
    conn.close()

    exports = exports_overview()
    impacted = [e for e in exports if e["verdict"] != "ok"]
    # 通知发布服务：对已分发产物按新分类复核（独立服务间通过 HTTP 协作）
    enforce_result = None
    try:
        _, enforce_result = post(PUBLISHER_URL + "/enforce",
                                 {"actor": actor, "trigger": "classification_change"})
    except Exception:
        enforce_result = {"error": "发布服务不可达，需人工触发 /enforce"}
    return {
        "ok": True, "version": ver, "field": f"{dataset}.{column}",
        "old": old, "new": new_cls,
        "impacted_exports": impacted,
        "strategy_compare": {
            "stop_all": f"全部停止：{len(exports)} 个导出全部下架，安全但影响面最大",
            "precise": (f"精确阻断：{sum(1 for e in exports if e['verdict'] == 'blocked')} 个阻断、"
                        f"{sum(1 for e in exports if e['verdict'] == 'needs_approval')} 个待审批、"
                        f"{sum(1 for e in exports if e['verdict'] == 'ok')} 个不受影响"),
        },
        "enforce": enforce_result,
    }


@svc.route("GET", "/classification_state")
def classification_state(q, body=None):
    conn = db()
    ver = conn.execute("SELECT v FROM meta WHERE k='classification_version'").fetchone()["v"]
    ts = conn.execute("SELECT v FROM meta WHERE k='last_change_at'").fetchone()["v"]
    conn.close()
    return {"version": int(ver), "last_change_at": ts}


@svc.route("GET", "/impact")
def impact(q, body=None):
    """某字段的影响路径：前向传播树 + 受影响导出 + 两种策略对比。"""
    dataset, column = q["dataset"], q["column"]
    tree = impact_tree(dataset, column)
    exports = exports_overview()
    affected = [e for e in exports if e["verdict"] != "ok"]
    return {
        "root": f"{dataset}.{column}",
        "tree": tree,
        "affected_exports": affected,
        "total_exports": len(exports),
        "strategy_compare": {
            "stop_all": f"全部停止：{len(exports)} 个导出全部下架",
            "precise": f"精确阻断：仅 {len(affected)} 个受影响（依赖表达式级血缘，而非最终列名）",
        },
    }


@svc.route("POST", "/evaluate")
def evaluate(q, body):
    ev = evaluate_dataset(body["dataset"])
    if ev is None:
        return 404, {"error": f"未知数据集 {body['dataset']}"}
    return ev


@svc.route("POST", "/approvals")
def approve(q, body):
    """批准安全替代版：审批绑定 导出名+内容哈希，内容变了需重新审批。"""
    export, chash = body["export"], body["content_hash"]
    actor = body.get("actor", "anonymous")
    note = body.get("note", "")
    conn = db()
    conn.execute("INSERT OR REPLACE INTO approvals(export,content_hash,actor,note,created_at)"
                 " VALUES(?,?,?,?,?)", (export, chash, actor, note, now_iso()))
    audit(conn, actor, "approve_safe_version",
          {"export": export, "content_hash": chash, "note": note})
    conn.commit()
    conn.close()
    return {"ok": True, "export": export, "content_hash": chash, "actor": actor}


@svc.route("GET", "/approvals")
def check_approval(q, body=None):
    conn = db()
    if "export" in q:
        row = conn.execute("SELECT * FROM approvals WHERE export=? AND content_hash=?",
                           (q["export"], q.get("content_hash", ""))).fetchone()
        conn.close()
        return {"approved": bool(row), "approval": dict(row) if row else None}
    rows = [dict(r) for r in conn.execute("SELECT * FROM approvals ORDER BY created_at DESC")]
    conn.close()
    return {"approvals": rows}


@svc.route("GET", "/audit")
def get_audit(q, body=None):
    conn = db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM audit ORDER BY id DESC LIMIT 300")]
    conn.close()
    return {"audit": rows}


@svc.route("GET", "/")
def ui(q, body=None):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "web", "index.html"), encoding="utf-8") as f:
        return f.read()


if __name__ == "__main__":
    init_db()
    svc.run(PORT)
