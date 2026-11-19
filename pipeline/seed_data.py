#!/usr/bin/env python3
"""生成虚构源数据，并在其中埋入可追踪标记（CANARY）。

标记设计：
- 12 个客户的 phone 为 CANARY-PHN-00xx，email 为 canaryxx@example.com
- 扫描分发目录中的 /canary/i 即可核对敏感数据是否外发
- 掩码后 ***-***-0001 不含标记；拼接列 contact_blob 完整携带标记
"""
import os
import random

from common import SRC_DIR, ensure_dirs, write_csv

CITIES = ["上海", "北京", "深圳"]
SURNAMES = ["王", "李", "张", "刘", "陈", "杨", "赵", "黄", "周", "吴"]
GIVEN = ["伟", "芳", "娜", "敏", "静", "磊", "洋", "艳", "勇", "军", "杰", "娟", "涛", "明", "超"]
CHANNELS = ["app", "web", "store"]

N_CUSTOMERS = 30
N_CANARY = 12


def generate():
    ensure_dirs()
    rng = random.Random(20261120)

    customers = []
    for i in range(1, N_CUSTOMERS + 1):
        cid = f"C{i:04d}"
        canary = i <= N_CANARY
        first = rng.choice(GIVEN)
        last = rng.choice(SURNAMES)
        if canary:
            phone = f"CANARY-PHN-{i:04d}"
            email = f"canary{i:02d}@example.com"
        else:
            phone = f"138-{rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}"
            email = f"user{i:02d}@example.com"
        customers.append({
            "customer_id": cid,
            "first_name": first,
            "last_name": last,
            "email": email,
            "phone": phone,
            "city": CITIES[(i - 1) % len(CITIES)],
            "signup_date": f"2025-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}",
        })

    orders = []
    oid = 0
    for c in customers:
        for _ in range(14):  # 每客户 14 单 → 每个 (日,城市) 组约 28 单，k≥10 成立
            oid += 1
            orders.append({
                "order_id": f"O{oid:05d}",
                "customer_id": c["customer_id"],
                "amount": f"{rng.uniform(20, 500):.2f}",
                "order_date": f"2026-11-{rng.randint(1, 5):02d}",
                "channel": rng.choice(CHANNELS),
            })

    write_csv(os.path.join(SRC_DIR, "customers.csv"), customers,
              ["customer_id", "first_name", "last_name", "email", "phone", "city", "signup_date"])
    write_csv(os.path.join(SRC_DIR, "orders.csv"), orders,
              ["order_id", "customer_id", "amount", "order_date", "channel"])
    return {"customers": len(customers), "orders": len(orders), "canary_customers": N_CANARY}


if __name__ == "__main__":
    print(generate())
