import os
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
import psycopg2
import psycopg2.extras
import redis
from dotenv import load_dotenv
import json
import redis.asyncio as aioredis
from fastapi import WebSocket, WebSocketDisconnect

load_dotenv()

app = FastAPI(title="Stock Pipeline API")

SYMBOLS = ["AAPL", "TSLA", "NVDA", "MSFT"]

pg_conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
pg_conn.autocommit = True

r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    decode_responses=True,
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/stocks/live")
def get_live_stocks():
    """Fast path: read current snapshot from Redis. Falls back to Postgres
    (flagged as possibly stale) for any symbol whose Redis key has expired —
    the cache-aside pattern."""
    results = {}
    for symbol in SYMBOLS:
        key = f"stock:{symbol}"
        data = r.hgetall(key)
        if data:
            results[symbol] = {
                "price": float(data["price"]),
                "pct_change_5m": float(data["pct_change_5m"]),
                "rolling_avg": float(data["rolling_avg"]),
                "last_updated": float(data["last_updated"]),
                "is_stale": False,
            }
        else:
            # Cache miss / expired — fall back to the most recent Postgres row
            with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT price, tick_timestamp FROM ticks "
                    "WHERE symbol = %s ORDER BY id DESC LIMIT 1",
                    (symbol,),
                )
                row = cur.fetchone()
            if row:
                results[symbol] = {
                    "price": float(row["price"]),
                    "pct_change_5m": None,
                    "rolling_avg": None,
                    "last_updated": row["tick_timestamp"].isoformat(),
                    "is_stale": True,
                }
            else:
                results[symbol] = {"error": "no data available"}
    return results


@app.get("/stocks/{symbol}/history")
def get_stock_history(symbol: str, limit: int = 50):
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        raise HTTPException(status_code=404, detail=f"Unknown symbol: {symbol}")

    with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT symbol, price, tick_timestamp FROM ticks "
            "WHERE symbol = %s ORDER BY id DESC LIMIT %s",
            (symbol, limit),
        )
        rows = cur.fetchall()

    return {
        "symbol": symbol,
        "count": len(rows),
        "history": [
            {"price": float(row["price"]), "timestamp": row["tick_timestamp"].isoformat()}
            for row in rows
        ],
    }


@app.get("/alerts/active")
def get_active_alerts(limit: int = 20):
    with pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT symbol, alert_type, pct_change, triggered_at, details "
            "FROM alerts ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()

    return {
        "count": len(rows),
        "alerts": [
            {
                "symbol": row["symbol"],
                "alert_type": row["alert_type"],
                "pct_change": float(row["pct_change"]) if row["pct_change"] is not None else None,
                "triggered_at": row["triggered_at"].isoformat(),
                "details": row["details"],
            }
            for row in rows
        ],
    }
    
@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    await websocket.accept()
    redis_client = aioredis.Redis(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        decode_responses=True,
    )
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("alerts")

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                await websocket.send_text(message["data"])
    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe("alerts")
        await redis_client.close()