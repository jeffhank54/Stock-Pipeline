import json
import sys
from collections import deque
from kafka import KafkaConsumer

SYMBOLS = ["AAPL", "TSLA", "NVDA", "MSFT"]
ALERT_THRESHOLD_PCT = 2.0
WINDOW_SECONDS = 5 * 60

tick_history = {s: deque() for s in SYMBOLS}
running_sum = {s: 0.0 for s in SYMBOLS}


def process_tick(symbol, price, timestamp):
    history = tick_history[symbol]
    history.append((timestamp, price))
    running_sum[symbol] += price

    cutoff = timestamp - WINDOW_SECONDS
    while history and history[0][0] < cutoff:
        _, old_price = history.popleft()
        running_sum[symbol] -= old_price

    if len(history) < 2:
        return

    oldest_price = history[0][1]
    pct_change = ((price - oldest_price) / oldest_price) * 100
    rolling_avg = running_sum[symbol] / len(history)

    print(f"[{symbol}] price={price:.2f}  5m_change={pct_change:+.2f}%  "
          f"rolling_avg={rolling_avg:.2f}  window_size={len(history)}")

    if abs(pct_change) >= ALERT_THRESHOLD_PCT:
        print(f"  🚨 ALERT: {symbol} moved {pct_change:+.2f}% "
              f"in the last {WINDOW_SECONDS // 60} minutes!")


if __name__ == "__main__":
    # group_id is what makes this a "consumer group" — run multiple copies
    # of this script with the SAME group_id, and Kafka will split the 4
    # partitions between them automatically instead of duplicating work.
    group_id = "stock-workers"

    consumer = KafkaConsumer(
        "stock-ticks",
        bootstrap_servers="localhost:9092",
        group_id=group_id,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",  # only process new messages, ignore old ones on startup
    )

    print(f"Consumer started (group_id={group_id}). Waiting for ticks...\n")
    for message in consumer:
        tick = message.value
        process_tick(tick["symbol"], tick["price"], tick["timestamp"])