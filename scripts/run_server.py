"""服务启动入口：python -m scripts.run_server [--data data/store] [--port 8000]"""
from __future__ import annotations

import argparse

from app.server import serve

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data/store")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args()
    serve(args.data, args.host, args.port)
