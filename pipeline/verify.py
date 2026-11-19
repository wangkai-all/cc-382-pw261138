#!/usr/bin/env python3
"""端到端核对脚本：启动三个独立服务，完整走一遍“分类变更 → 精确阻断”场景。

场景步骤：
 1. 生成含 CANARY 标记的虚构源数据，运行全部加工任务
 2. 发布全部 4 个导出 —— 基线：数据正常分发，标记可见
 3. 把 customers.phone 从 normal 改为 sensitive
 4. 发布服务复核：billing/marketing 被隔离，分发目录标记清零
 5. 直接重发 billing → 阻断（直接复制路径）
 6. 模拟旧任务晚完成：变更前启动的 export_marketing 现在才产出 → 仍被阻断
 7. 安全替代版 export_marketing_safe：需审批 → 批准 → 发布，掩码值不含标记
 8. 未知转换 profile_note 的阻断依据存在
 9. partner_feed（k≥10 聚合）全程不受影响 —— 精确阻断而非全部停止
10. 终检：分发目录 0 标记，隔离区有标记（被阻断数据确实未再分发）

用法：python3 pipeline/verify.py [--keep]（--keep 结束后保留服务便于打开网页）
"""
import os
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from common import (DATA_DIR, DIST_DIR, QUAR_DIR, MARKER,  # noqa: E402
                    REGISTRY_URL, SCHEDULER_URL, PUBLISHER_URL, get, post)

PASS, FAIL = "✅", "❌"
checks = []


def check(cond, label, detail=""):
    checks.append((bool(cond), label, detail))
    print(f"  {PASS if cond else FAIL} {label}" + (f"  —— {detail}" if detail and not cond else ""))
    if not cond and detail:
        print(f"     详情: {detail[:600]}")


def step(msg):
    print(f"\n=== {msg} ===")


def wait_health(url, name, timeout=20):
    t0 = time.time()
    while time.time() - t0 < timeout:
        st, _ = get(url + "/health", timeout=2)
        if st == 200:
            return True
        time.sleep(0.3)
    raise RuntimeError(f"{name} 启动超时")


def scan_dir(d):
    import re
    hits = {}
    for f in sorted(os.listdir(d)):
        if f.endswith(".csv"):
            txt = open(os.path.join(d, f), encoding="utf-8").read()
            n = len(re.findall(MARKER, txt, re.IGNORECASE))
            if n:
                hits[f] = n
    return hits


def main():
    keep = "--keep" in sys.argv

    # 干净环境
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.makedirs(os.path.join(DATA_DIR, "logs"), exist_ok=True)

    procs = []
    for name in ("registry_service", "scheduler_service", "publisher_service"):
        log = open(os.path.join(DATA_DIR, "logs", name + ".log"), "w")
        procs.append(subprocess.Popen(
            [sys.executable, os.path.join(HERE, name + ".py")],
            stdout=log, stderr=subprocess.STDOUT))
    try:
        wait_health(REGISTRY_URL, "registry")
        wait_health(SCHEDULER_URL, "scheduler")
        wait_health(PUBLISHER_URL, "publisher")
        print("三个独立服务已启动：registry:7801 scheduler:7802 publisher:7803")

        step("1. 生成源数据（含 CANARY 标记）并运行全部加工任务")
        st, seed = post(SCHEDULER_URL + "/seed")
        check(st == 200 and seed["stats"]["canary_customers"] == 12, "源数据生成，含 12 个标记客户")
        st, runs = post(SCHEDULER_URL + "/run_all")
        check(st == 200 and len(runs["runs"]) == 7, "7 个加工作业全部完成",
              str(runs)[:300])

        step("2. 基线：全部分类为 normal，4 个导出均可发布，标记随数据流出")
        for ex in ("export_billing", "export_marketing", "export_marketing_safe",
                   "export_partner_feed"):
            st, r = post(PUBLISHER_URL + "/publish", {"export": ex, "actor": "verify"})
            check(st == 200 and r["decision"] == "published", f"{ex} 发布成功", str(r)[:300])
        hits = scan_dir(DIST_DIR)
        check("export_billing.csv" in hits and "export_marketing.csv" in hits,
              "基线确认：标记确实随 billing/marketing 流出（证明管道真实连通）",
              str(hits))

        step("3. 分类变更：customers.phone  normal → sensitive")
        st, chg = post(REGISTRY_URL + "/classifications",
                       {"dataset": "customers", "column": "phone",
                        "classification": "sensitive", "actor": "data-governor"})
        check(st == 200 and chg["ok"], "分类变更成功", str(chg)[:300])
        verdicts = {e["export"]: e["verdict"] for e in chg["impacted_exports"]}
        check(verdicts.get("export_billing") == "blocked", "影响分析：export_billing 应阻断")
        check(verdicts.get("export_marketing") == "blocked", "影响分析：export_marketing 应阻断")
        check(verdicts.get("export_marketing_safe") == "needs_approval",
              "影响分析：export_marketing_safe 需审批（仅掩码）")
        check("export_partner_feed" not in verdicts,
              "影响分析：export_partner_feed（k≥10 聚合）不受影响")

        step("4. 发布服务复核已分发产物（分类变更触发的 webhook）")
        # 分类变更时登记服务已同步调用发布服务 /enforce（webhook），
        # 这里再显式复核一次（幂等），然后核对目录终态
        webhook_actions = (chg.get("enforce") or {}).get("actions", [])
        quar_by_hook = {a["export"] for a in webhook_actions if a["action"] == "quarantine"}
        check({"export_billing", "export_marketing", "export_marketing_safe"} <= quar_by_hook,
              "分类变更 webhook 触发复核：billing / marketing / 未审批的 marketing_safe 被隔离",
              str(webhook_actions)[:400])
        st, enf = post(PUBLISHER_URL + "/enforce", {"actor": "verify"})
        quar_files = " ".join(sorted(os.listdir(QUAR_DIR)))
        check(all(n in quar_files for n in
                  ("export_billing", "export_marketing", "export_marketing_safe")),
              "隔离区中确实存在这三个产物文件", quar_files[:300])
        dist_files = sorted(os.listdir(DIST_DIR))
        check(dist_files == ["export_partner_feed.csv"],
              "分发目录仅剩 partner_feed（精确阻断，非全部停止）", str(dist_files))
        hits = scan_dir(DIST_DIR)
        check(not hits, "复核后分发目录标记数 = 0", str(hits))

        step("5. 尝试重发 export_billing → 应阻断，且给出直接复制的判断依据")
        st, r = post(PUBLISHER_URL + "/publish", {"export": "export_billing", "actor": "verify"})
        check(r["decision"] == "blocked", "export_billing 被阻断")
        check(any("copy" in x or "直接复制" in x for x in r["reasons"]),
              "阻断依据包含传播路径（copy 直达）", str(r["reasons"])[:400])
        check(not os.path.exists(os.path.join(DIST_DIR, "export_billing.csv")),
              "阻断后分发目录无 billing 文件")

        step("6. 模拟旧加工任务晚完成（变更前启动，现在才产出）")
        st, state = get(REGISTRY_URL + "/classification_state")
        from datetime import datetime, timedelta
        started = (datetime.fromisoformat(state["last_change_at"])
                   - timedelta(minutes=10)).isoformat()
        st, run = post(SCHEDULER_URL + "/run",
                       {"job": "export_marketing", "started_at": started,
                        "actor": "late-job-sim"})
        check(st == 200 and run["simulated_late"], "旧任务以过去的 started_at 晚完成")
        st, r = post(PUBLISHER_URL + "/publish", {"export": "export_marketing",
                                                  "actor": "late-job-sim"})
        check(r["decision"] == "blocked", "晚完成的旧任务产物仍被阻断（发布时判定）")
        check(any("晚完成" in x for x in r["reasons"]),
              "阻断依据中明确标注“晚完成”", str(r["reasons"])[:500])
        check("export_marketing.csv" not in scan_dir(DIST_DIR),
              "晚完成产物未进入分发目录")

        step("7. 安全替代版：export_marketing_safe 需审批 → 批准 → 发布")
        st, r = post(PUBLISHER_URL + "/publish",
                     {"export": "export_marketing_safe", "actor": "verify"})
        check(r["decision"] == "needs_approval", "未审批时不可发布（needs_approval）")
        chash = r["content_hash"]
        st, ap = post(REGISTRY_URL + "/approvals",
                      {"export": "export_marketing_safe", "content_hash": chash,
                       "actor": "dpo", "note": "安全替代版：敏感列已去除或掩码"})
        check(st == 200 and ap["ok"], "DPO 批准安全替代版（绑定内容哈希）")
        st, r = post(PUBLISHER_URL + "/publish",
                     {"export": "export_marketing_safe", "actor": "verify"})
        check(r["decision"] == "published", "审批后发布成功")
        safe_file = os.path.join(DIST_DIR, "export_marketing_safe.csv")
        content = open(safe_file, encoding="utf-8").read()
        check("***-***-" in content, "安全版中手机号为掩码形式")
        check(MARKER.lower() not in content.lower(), "安全版不含任何 CANARY 标记")
        check("contact_blob" not in content and "profile_note" not in content,
              "安全版已去除拼接列与未知UDF列")

        step("8. 未知转换的处理：profile_note（外部UDF，表达式不可解析）")
        st, ev = post(REGISTRY_URL + "/evaluate", {"dataset": "export_marketing"})
        check(ev["verdict"] == "blocked" and len(ev["unknowns"]) >= 1,
              "未知转换 fail-safe 按敏感阻断")
        check(any("profile_note" in u["column"] for u in ev["unknowns"]),
              "未知项明确指向 profile_note 列", str(ev["unknowns"])[:300])
        # 只看最终列名的规则会漏掉 contact_blob：列名不含 phone，却携带 phone 数据
        st, fields = get(REGISTRY_URL + "/fields")
        cb = [f for f in fields["fields"]
              if f["dataset"] == "stg_customers" and f["column"] == "contact_blob"]
        check(cb and cb[0]["effective"] == "blocked" and "phone" not in "contact_blob",
              "contact_blob 列名不含 phone 仍被血缘判定为敏感（列名规则会漏判）")

        step("9. 审计与版本")
        st, aud = get(REGISTRY_URL + "/audit")
        actions = [a["action"] for a in aud["audit"]]
        check("classification_change" in actions and "approve_safe_version" in actions,
              "审计包含分类变更与安全版审批")
        st, plog = get(PUBLISHER_URL + "/log")
        decisions = [r["decision"] for r in plog["log"]]
        check("blocked" in decisions and "quarantine" in decisions,
              "发布日志包含阻断与隔离决策")

        step("10. 终检：被阻断的数据确实未再分发")
        dist_hits = scan_dir(DIST_DIR)
        quar_hits = scan_dir(QUAR_DIR)
        check(not dist_hits, "分发目录（对外）标记数 = 0", str(dist_hits))
        check(sum(quar_hits.values()) > 0, "隔离区存有被阻断的标记数据（受控，未外发）",
              str(quar_hits))
        st, scan = get(PUBLISHER_URL + "/scan")
        check(scan["ok"], "发布服务 /scan 自检通过")

    finally:
        if not keep:
            for p in procs:
                p.terminate()
        else:
            print("\n[--keep] 服务保留运行：")
            print(f"  网页控制台: {REGISTRY_URL}/")
            print(f"  调度器:     {SCHEDULER_URL}/jobs")
            print(f"  发布服务:   {PUBLISHER_URL}/status")

    n_fail = sum(1 for ok, _, _ in checks if not ok)
    print(f"\n{'='*56}")
    print(f"核对完成：{len(checks) - n_fail}/{len(checks)} 项通过"
          + (f"，{n_fail} 项失败" if n_fail else "，全部通过 ✅"))
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
