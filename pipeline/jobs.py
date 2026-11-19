#!/usr/bin/env python3
"""加工作业定义：源表 → 清洗表 → 聚合表 → 导出。

每个作业声明列级血缘（sources + transform），调度器执行后上报登记服务。
关键演示点：
- contact_blob 由 email 与 phone 拼接而成，最终列名不含 "phone"，
  只看列名的规则会漏判，表达式级血缘不会
- profile_note 由外部 UDF 生成、表达式不可解析 → transform=unknown，
  登记服务 fail-safe 按敏感处理
"""
import os
from collections import defaultdict

from common import SRC_DIR, WAREHOUSE_DIR, EXPORT_DIR, read_csv, write_csv


def mask_phone(v):
    return ("***-***-" + v[-4:]) if v else v


def _src(name):
    return os.path.join(SRC_DIR, name + ".csv")


def _wh(name):
    return os.path.join(WAREHOUSE_DIR, name + ".csv")


def _ex(name):
    return os.path.join(EXPORT_DIR, name + ".csv")


# ---------- 计算函数 ----------

def compute_clean_customers():
    rows = read_csv(_src("customers"))
    out = [{
        "customer_id": r["customer_id"],
        "full_name": r["first_name"] + " " + r["last_name"],
        "email": r["email"].lower(),
        "phone": r["phone"],
        "phone_masked": mask_phone(r["phone"]),
        "city": r["city"],
        "contact_blob": r["email"].lower() + "|" + r["phone"],
        # 外部 UDF：实现上确实含 phone 片段，若外发可被标记扫描发现
        "profile_note": "pf_" + r["phone"][:8],
    } for r in rows]
    header = ["customer_id", "full_name", "email", "phone", "phone_masked",
              "city", "contact_blob", "profile_note"]
    write_csv(_wh("stg_customers"), out, header)
    return len(out), {}


def compute_clean_orders():
    rows = read_csv(_src("orders"))
    header = ["order_id", "customer_id", "amount", "order_date", "channel"]
    write_csv(_wh("stg_orders"), rows, header)
    return len(rows), {}


def compute_agg_daily_sales():
    orders = read_csv(_wh("stg_orders"))
    customers = {r["customer_id"]: r for r in read_csv(_wh("stg_customers"))}
    groups = defaultdict(list)
    for o in orders:
        city = customers[o["customer_id"]]["city"]
        groups[(o["order_date"], city)].append(float(o["amount"]))
    out = []
    for (day, city), amounts in sorted(groups.items()):
        out.append({
            "day": day, "city": city,
            "orders_count": len(amounts),
            "total_revenue": f"{sum(amounts):.2f}",
            "avg_basket": f"{sum(amounts) / len(amounts):.2f}",
        })
    header = ["day", "city", "orders_count", "total_revenue", "avg_basket"]
    write_csv(_wh("agg_daily_sales"), out, header)
    min_group = min(len(v) for v in groups.values())
    return len(out), {"min_group_size": min_group}


def _export_from_stg(name, cols):
    rows = read_csv(_wh("stg_customers"))
    write_csv(_ex(name), [{c: r[c] for c in cols} for r in rows], cols)
    return len(rows), {}


def compute_export_billing():
    return _export_from_stg("export_billing", ["customer_id", "full_name", "phone"])


def compute_export_marketing():
    return _export_from_stg("export_marketing",
                            ["customer_id", "full_name", "email",
                             "contact_blob", "phone_masked", "profile_note"])


def compute_export_marketing_safe():
    # 安全替代版：去掉拼接列与未知 UDF 列，手机号只留掩码
    return _export_from_stg("export_marketing_safe",
                            ["customer_id", "full_name", "city", "phone_masked"])


def compute_export_partner_feed():
    rows = read_csv(_wh("agg_daily_sales"))
    cols = ["day", "city", "orders_count", "total_revenue"]
    write_csv(_ex("export_partner_feed"), [{c: r[c] for c in cols} for r in rows], cols)
    return len(rows), {}


# ---------- 作业声明（含列级血缘） ----------

def col(name, sources, transform, expr):
    return {"name": name, "sources": sources, "transform": transform, "expr": expr}


JOBS = {
    "clean_customers": {
        "output": "stg_customers", "kind": "table",
        "compute": compute_clean_customers,
        "columns": [
            col("customer_id", ["customers.customer_id"], "copy", "customer_id"),
            col("full_name", ["customers.first_name", "customers.last_name"], "concat",
                "first_name || ' ' || last_name"),
            col("email", ["customers.email"], "lower", "lower(email)"),
            col("phone", ["customers.phone"], "copy", "phone"),
            col("phone_masked", ["customers.phone"], "mask", "mask(phone, keep=4)"),
            col("city", ["customers.city"], "copy", "city"),
            col("contact_blob", ["customers.email", "customers.phone"], "concat",
                "lower(email) || '|' || phone"),
            col("profile_note", ["customers.phone", "customers.signup_date"], "unknown",
                "udf_profile(phone, signup_date)  -- 外部UDF，表达式不可解析"),
        ],
    },
    "clean_orders": {
        "output": "stg_orders", "kind": "table",
        "compute": compute_clean_orders,
        "columns": [
            col("order_id", ["orders.order_id"], "copy", "order_id"),
            col("customer_id", ["orders.customer_id"], "copy", "customer_id"),
            col("amount", ["orders.amount"], "copy", "amount"),
            col("order_date", ["orders.order_date"], "copy", "order_date"),
            col("channel", ["orders.channel"], "copy", "channel"),
        ],
    },
    "agg_daily_sales": {
        "output": "agg_daily_sales", "kind": "table",
        "compute": compute_agg_daily_sales,
        "columns": [
            col("day", ["stg_orders.order_date"], "substring", "date(order_date)"),
            col("city", ["stg_customers.city"], "copy", "city  -- 分组键"),
            col("orders_count", ["stg_orders.order_id"], "aggregate", "count(*)"),
            col("total_revenue", ["stg_orders.amount"], "aggregate", "sum(amount)"),
            col("avg_basket", ["stg_orders.amount"], "aggregate", "avg(amount)"),
        ],
    },
    "export_billing": {
        "output": "export_billing", "kind": "export",
        "compute": compute_export_billing,
        "columns": [
            col("customer_id", ["stg_customers.customer_id"], "copy", "customer_id"),
            col("full_name", ["stg_customers.full_name"], "copy", "full_name"),
            col("phone", ["stg_customers.phone"], "copy", "phone"),
        ],
    },
    "export_marketing": {
        "output": "export_marketing", "kind": "export",
        "compute": compute_export_marketing,
        "columns": [
            col("customer_id", ["stg_customers.customer_id"], "copy", "customer_id"),
            col("full_name", ["stg_customers.full_name"], "copy", "full_name"),
            col("email", ["stg_customers.email"], "copy", "email"),
            col("contact_blob", ["stg_customers.contact_blob"], "copy", "contact_blob"),
            col("phone_masked", ["stg_customers.phone_masked"], "copy", "phone_masked"),
            col("profile_note", ["stg_customers.profile_note"], "copy", "profile_note"),
        ],
    },
    "export_marketing_safe": {
        "output": "export_marketing_safe", "kind": "export",
        "compute": compute_export_marketing_safe,
        "columns": [
            col("customer_id", ["stg_customers.customer_id"], "copy", "customer_id"),
            col("full_name", ["stg_customers.full_name"], "copy", "full_name"),
            col("city", ["stg_customers.city"], "copy", "city"),
            col("phone_masked", ["stg_customers.phone_masked"], "copy", "phone_masked"),
        ],
    },
    "export_partner_feed": {
        "output": "export_partner_feed", "kind": "export",
        "compute": compute_export_partner_feed,
        "columns": [
            col("day", ["agg_daily_sales.day"], "copy", "day"),
            col("city", ["agg_daily_sales.city"], "copy", "city"),
            col("orders_count", ["agg_daily_sales.orders_count"], "copy", "orders_count"),
            col("total_revenue", ["agg_daily_sales.total_revenue"], "copy", "total_revenue"),
        ],
    },
}

# 拓扑序
DAG_ORDER = ["clean_customers", "clean_orders", "agg_daily_sales",
             "export_billing", "export_marketing", "export_marketing_safe",
             "export_partner_feed"]
