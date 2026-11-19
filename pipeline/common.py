#!/usr/bin/env python3
"""公共模块：路径、HTTP JSON 调用、微型 JSON 服务框架。仅使用标准库。"""
import csv
import json
import os
import urllib.request
import urllib.error
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.environ.get("PIPELINE_DATA", os.path.join(BASE_DIR, "data"))
SRC_DIR = os.path.join(DATA_DIR, "source")          # 源数据（虚构，含可追踪标记）
WAREHOUSE_DIR = os.path.join(DATA_DIR, "warehouse")  # 清洗表 / 聚合表
EXPORT_DIR = os.path.join(DATA_DIR, "exports")       # 待发布的导出（ staging ）
DIST_DIR = os.path.join(DATA_DIR, "distribution")    # 已分发（模拟外部伙伴可取的目录）
QUAR_DIR = os.path.join(DATA_DIR, "quarantine")      # 被隔离的产物

REGISTRY_URL = os.environ.get("REGISTRY_URL", "http://127.0.0.1:7801")
SCHEDULER_URL = os.environ.get("SCHEDULER_URL", "http://127.0.0.1:7802")
PUBLISHER_URL = os.environ.get("PUBLISHER_URL", "http://127.0.0.1:7803")

# 可追踪标记：虚构数据中埋入的“金丝雀”值，扫描分发目录即可核对敏感数据是否外发
MARKER = "CANARY"


def ensure_dirs():
    for d in (DATA_DIR, SRC_DIR, WAREHOUSE_DIR, EXPORT_DIR, DIST_DIR, QUAR_DIR):
        os.makedirs(d, exist_ok=True)


def now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, header):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        w.writerows(rows)


def http_json(method, url, payload=None, timeout=15):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"error": raw}
    except urllib.error.URLError as e:
        return 0, {"error": f"无法连接 {url}: {e.reason}"}


def get(url, **kw):
    return http_json("GET", url, **kw)


def post(url, payload=None, **kw):
    return http_json("POST", url, payload, **kw)


class Service:
    """微型 JSON HTTP 服务框架：svc.route(method, path) 装饰器注册处理函数。"""

    def __init__(self, name):
        self.name = name
        self.routes = {}

    def route(self, method, path):
        def deco(fn):
            self.routes[(method, path)] = fn
            return fn
        return deco

    def run(self, port):
        routes = self.routes
        name = self.name

        class Handler(BaseHTTPRequestHandler):
            def _handle(self, method):
                u = urlparse(self.path)
                fn = routes.get((method, u.path))
                if not fn:
                    self._send(404, {"error": f"无路由 {method} {u.path}"})
                    return
                try:
                    body = None
                    if method in ("POST", "PUT", "DELETE"):
                        n = int(self.headers.get("Content-Length") or 0)
                        raw = self.rfile.read(n) if n else b""
                        body = json.loads(raw.decode()) if raw else {}
                    q = {k: v[0] for k, v in parse_qs(u.query).items()}
                    res = fn(q, body)
                    code, payload = res if isinstance(res, tuple) else (200, res)
                    self._send(code, payload)
                except Exception as e:  # noqa: BLE001
                    import traceback
                    traceback.print_exc()
                    self._send(500, {"error": f"{type(e).__name__}: {e}"})

            def _send(self, code, payload):
                if isinstance(payload, (dict, list)):
                    raw = json.dumps(payload, ensure_ascii=False, indent=1).encode()
                    ctype = "application/json; charset=utf-8"
                else:
                    raw = payload.encode() if isinstance(payload, str) else payload
                    ctype = "text/html; charset=utf-8"
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(raw)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self):
                self._handle("GET")

            def do_POST(self):
                self._handle("POST")

            def do_OPTIONS(self):
                self.send_response(204)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.end_headers()

            def log_message(self, *args):
                pass

        srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        print(f"[{name}] 监听 http://127.0.0.1:{port}", flush=True)
        srv.serve_forever()
