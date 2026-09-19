import json
import os
import time
from collections import deque
from dotenv import load_dotenv
from kafka import KafkaConsumer
import psycopg2
import redis
from datetime import datetime, timezone

load_dotenv()

SYMBOLS = ["AAPL", "TSLA", "NVDA", "MSFT"]
ALERT_THRESHOLD_PCT = 2.0
WINDOW_SECONDS = 5 * 60
BATCH_SIZE = 20          # write to Postgres every N ticks, not every single one
REDIS_TTL_SECONDS = 60   # if no update in 60s, key expires (signals staleness)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

tick_history = {s: deque() for s in SYMBOLS}
running_sum = {s: 0.0 for s in SYMBOLS}
pending_batch = []  # ticks waiting to be flushed to Postgres

# --- Postgres connection ---
pg_conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
pg_conn.autocommit = True
pg_cursor = pg_conn.cursor()

# --- Redis connection ---
r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    decode_responses=True,
)


def flush_batch_to_postgres():
    """Write all pending ticks to Postgres in one batched INSERT, then clear the batch."""
    if not pending_batch:
        return
    pg_cursor.executemany(
        "INSERT INTO ticks (symbol, price, tick_timestamp) VALUES (%s, %s, %s)",
        pending_batch,
    )
    pending_batch.clear()


def write_alert_to_postgres(symbol, pct_change):
    pg_cursor.execute(
        "INSERT INTO alerts (symbol, alert_type, pct_change, details) VALUES (%s, %s, %s, %s)",
        (symbol, "price_move", pct_change,
         f"{symbol} moved {pct_change:+.2f}% in {WINDOW_SECONDS // 60} minutes"),
    )


def update_redis_snapshot(symbol, price, pct_change, rolling_avg):
    key = f"stock:{symbol}"
    r.hset(key, mapping={
        "price": round(price, 2),
        "pct_change_5m": round(pct_change, 2),
        "rolling_avg": round(rolling_avg, 2),
        "last_updated": round(time.time(), 3),
    })
    r.expire(key, REDIS_TTL_SECONDS)


def process_tick(symbol, price, timestamp):
    history = tick_history[symbol]
    history.append((timestamp, price))
    running_sum[symbol] += price
    pending_batch.append((symbol, round(price, 2), datetime.fromtimestamp(round(timestamp, 3), tz=timezone.utc)))

    cutoff = timestamp - WINDOW_SECONDS
    while history and history[0][0] < cutoff:
        _, old_price = history.popleft()
        running_sum[symbol] -= old_price

    if len(history) >= BATCH_SIZE:
        pass  # batch flush is handled by count below, not window size

    if len(pending_batch) >= BATCH_SIZE:
        flush_batch_to_postgres()

    if len(history) < 2:
        return

    oldest_price = history[0][1]
    pct_change = ((price - oldest_price) / oldest_price) * 100
    rolling_avg = running_sum[symbol] / len(history)

    print(f"[{symbol}] price={price:.2f}  5m_change={pct_change:+.2f}%  "
          f"rolling_avg={rolling_avg:.2f}  window_size={len(history)}")

    update_redis_snapshot(symbol, price, pct_change, rolling_avg)

    if abs(pct_change) >= ALERT_THRESHOLD_PCT:
        print(f"  🚨 ALERT: {symbol} moved {pct_change:+.2f}% "
              f"in the last {WINDOW_SECONDS // 60} minutes!")
        write_alert_to_postgres(symbol, pct_change)

        alert_payload = {
            "symbol": symbol,
            "pct_change": round(pct_change, 2),
            "details": f"{symbol} moved {pct_change:+.2f}% in {WINDOW_SECONDS // 60} minutes",
        }
        r.publish("alerts", json.dumps(alert_payload))


if __name__ == "__main__":
    group_id = "stock-workers"

    consumer = KafkaConsumer(
        "stock-ticks",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=group_id,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
    )

    print(f"Consumer started (group_id={group_id}). Waiting for ticks...\n")
    message_count = 0
    for message in consumer:
        tick = message.value
        process_tick(tick["symbol"], tick["price"], tick["timestamp"])
        message_count += 1
        if message_count % 10 == 0:
            assigned = sorted(p.partition for p in consumer.assignment())
            print(f"[assignment check] partitions: {assigned}")