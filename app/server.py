"""HTTP 服务：标准库实现，无第三方 Web 框架依赖。"""
from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from .filter import ConflictError, StaleGenerationError, StoreError
from .store import EventStore

WEB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "web")


class _Handler(BaseHTTPRequestHandler):
    server_version = "BitmapEventStore/1.0"

    def log_message(self, fmt, *args):  # 安静一点
        pass

    @property
    def store(self) -> EventStore:
        return self.server.store  # type: ignore[attr-defined]

    # ---- 工具 -----------------------------------------------------------
    def _send_json(self, obj, status: int = 200) -> None:
        body = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except ValueError as exc:
            raise StoreError(f"请求体不是合法 JSON: {exc}")
        if not isinstance(data, dict):
            raise StoreError("请求体必须是 JSON 对象")
        return data

    def _error_status(self, exc: Exception) -> int:
        if isinstance(exc, StaleGenerationError):
            return 409
        if isinstance(exc, ConflictError):
            return 409
        if isinstance(exc, StoreError):
            return 400
        return 500

    # ---- 路由 -----------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            if parsed.path.startswith("/api/"):
                self._route_get(parsed.path, parse_qs(parsed.query))
            else:
                self._serve_static(parsed.path)
        except Exception as exc:  # noqa: BLE001
            self._send_json({"error": str(exc), "type": type(exc).__name__}, self._error_status(exc))

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            if not parsed.path.startswith("/api/"):
                raise StoreError("仅 /api/* 支持 POST")
            self._route_post(parsed.path)
        except Exception as exc:  # noqa: BLE001
            self._send_json({"error": str(exc), "type": type(exc).__name__}, self._error_status(exc))

    def _route_get(self, path: str, query: dict) -> None:
        store = self.store
        if path == "/api/overview":
            self._send_json(store.overview())
        elif path == "/api/queries":
            self._send_json({"queries": store.list_queries()})
        elif path == "/api/compaction":
            self._send_json(store.compaction_status())
        elif path.startswith("/api/query/") and path.endswith("/preview"):
            qid = path.split("/")[3]
            offset = int(query.get("offset", ["0"])[0])
            limit = min(int(query.get("limit", ["20"])[0]), 200)
            self._send_json(store.preview(qid, offset, limit))
        else:
            self._send_json({"error": f"未知接口 {path}"}, 404)

    def _route_post(self, path: str) -> None:
        store = self.store
        body = self._read_json()
        if path == "/api/events":
            self._send_json(store.put_event(body), 201)
        elif path.endswith("/update"):
            event_id = path.split("/")[3]
            self._send_json(store.update_event(event_id, body.get("changes", body)))
        elif path.endswith("/delete"):
            event_id = path.split("/")[3]
            self._send_json(store.delete_event(event_id))
        elif path == "/api/query":
            handle = store.create_query(body.get("filter", {}), bool(body.get("index_enabled", True)))
            self._send_json(
                {
                    "query_id": handle.query_id,
                    "gen_id": handle.gen_id,
                    "combinator": handle.combinator,
                    "hits": len(handle.ordinals),
                    "read_stat": handle.read_stat,
                    "created_lsn": handle.created_lsn,
                    "delete_snapshot": handle.delete_snapshot,
                },
                201,
            )
        elif path == "/api/query/close":
            self._send_json(store.close_query(body["query_id"]))
        elif path == "/api/compaction/start":
            self._send_json(store.start_compaction(), 202)
        elif path == "/api/compaction/control":
            self._send_json(store.request_compaction_control(body.get("action", "")))
        else:
            self._send_json({"error": f"未知接口 {path}"}, 404)

    # ---- 静态资源 -------------------------------------------------------
    def _serve_static(self, path: str) -> None:
        if path in ("", "/"):
            path = "/index.html"
        safe = os.path.normpath(path).lstrip("/")
        full = os.path.join(WEB_DIR, safe)
        if not os.path.isfile(full) or not os.path.abspath(full).startswith(os.path.abspath(WEB_DIR)):
            self._send_json({"error": "页面不存在"}, 404)
            return
        ctype = "text/html; charset=utf-8" if full.endswith(".html") else "application/javascript; charset=utf-8"
        with open(full, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def build_server(data_dir: str, host: str = "127.0.0.1", port: int = 8000) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), _Handler)
    server.store = EventStore(data_dir)  # type: ignore[attr-defined]
    return server


def serve(data_dir: str, host: str = "127.0.0.1", port: int = 8000) -> None:
    server = build_server(data_dir, host, port)
    print(f"位图事件仓服务已启动: http://{host}:{port}/  数据目录: {data_dir}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
