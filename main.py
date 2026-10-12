import base64
import math
import threading
import time
import uuid
from collections import defaultdict, deque

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

TOTAL = 50          # T: catalog is IDs 1..50
RATE_LIMIT = 20     # R requests per window
WINDOW = 60.0       # seconds

app = FastAPI()

# ---------- per-client rate limiting ----------
_hits = defaultdict(deque)
_rl_lock = threading.Lock()


@app.middleware("http")
async def rate_limit(request: Request, call_next):
    client_id = request.headers.get("x-client-id")
    if client_id and request.method != "OPTIONS":
        now = time.monotonic()
        with _rl_lock:
            q = _hits[client_id]
            while q and now - q[0] >= WINDOW:
                q.popleft()
            if len(q) >= RATE_LIMIT:
                retry = max(1, math.ceil(WINDOW - (now - q[0])))
                return JSONResponse(
                    {"detail": "rate limit exceeded"},
                    status_code=429,
                    headers={"Retry-After": str(retry)},
                )
            q.append(now)
    return await call_next(request)


# Added last, so CORS is outermost and also covers 429 responses and preflights.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After", "Idempotency-Key"],
)

# ---------- idempotent order creation ----------
_orders_by_key = {}
_orders_lock = threading.Lock()


@app.post("/orders", status_code=201)
async def create_order(request: Request):
    key = request.headers.get("idempotency-key")
    if not key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header required")
    try:
        body = await request.json()
    except Exception:
        body = {}
    with _orders_lock:
        order = _orders_by_key.get(key)
        if order is None:
            order = {
                "id": str(uuid.uuid4()),
                "status": "created",
                "data": body if isinstance(body, dict) else {},
            }
            _orders_by_key[key] = order
    return order


# ---------- cursor pagination ----------
def encode_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(f"o:{offset}".encode()).decode()


def decode_cursor(cursor: str) -> int:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        prefix, num = raw.split(":")
        offset = int(num)
        if prefix != "o" or offset < 0:
            raise ValueError
        return offset
    except Exception:
        raise HTTPException(status_code=400, detail="invalid cursor")


@app.get("/orders")
def list_orders(limit: int = Query(10), cursor: str | None = Query(None)):
    limit = max(1, min(limit, 100))
    start = decode_cursor(cursor) if cursor else 0
    end = min(start + limit, TOTAL)
    items = [{"id": i, "name": f"Order {i}"} for i in range(start + 1, end + 1)]
    next_cursor = encode_cursor(end) if end < TOTAL else None
    return {"items": items, "next_cursor": next_cursor}

import os

@app.get("/debug")
def debug():
    return {"pid": os.getpid(), "window": WINDOW, "workers_env": os.environ.get("WEB_CONCURRENCY")}    