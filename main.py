import os
import json
import time
import random
import threading
from collections import deque
from dotenv import load_dotenv
import websocket

load_dotenv()

API_KEY = os.getenv("FINNHUB_API_KEY")
MOCK_MODE = os.getenv("MOCK_MODE", "false").lower() == "true"

SYMBOLS = ["AAPL", "TSLA", "NVDA", "MSFT"]
ALERT_THRESHOLD_PCT = 2.0      # alert if price moves >2%
WINDOW_SECONDS = 5 * 60        # 5-minute rolling window

tick_history = {}
running_sum = {}

for s in SYMBOLS:
    tick_history[s] = deque()
    running_sum[s] = 0.0

def process_tick(symbol, price, timestamp):
    """Core logic: store the tick, compute rolling stats, check for alerts.
    This function is identical whether the tick came from a real Finnhub
    message or a mock generator — that's deliberate, so today's mock testing
    exercises the same code Monday's real data will run through."""

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

def run_mock_mode():
    """Generates fake ticks so you can test the logic above without waiting
    for real market hours."""
    print("Running in MOCK MODE — generating fake ticks, no real data.\n")
    prices = {symbol: 100.0 + random.uniform(0, 400) for symbol in SYMBOLS}

    while True:
        for symbol in SYMBOLS:
            # random walk: nudge price slightly, occasionally with a bigger jump
            move_pct = random.gauss(0, 0.3)
            if random.random() < 0.05:  # occasional bigger move to trigger alerts
                move_pct += random.choice([-1, 1]) * random.uniform(2, 4)
            prices[symbol] *= (1 + move_pct / 100)
            process_tick(symbol, prices[symbol], time.time())
        time.sleep(1)

def run_real_mode():
    """Connects to Finnhub's real WebSocket feed."""
    def on_message(ws, message):
        data = json.loads(message)
        if data.get("type") != "trade":
            return
        for trade in data.get("data", []):
            symbol = trade["s"]
            price = trade["p"]
            timestamp = trade["t"] / 1000  # Finnhub sends milliseconds
            if symbol in tick_history:
                process_tick(symbol, price, timestamp)

    def on_open(ws):
        print("Connected to Finnhub. Subscribing to symbols...")
        for symbol in SYMBOLS:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))

    def on_error(ws, error):
        print(f"WebSocket error: {error}")

    def on_close(ws, close_status_code, close_msg):
        print("Connection closed. Reconnecting in 5s...")
        time.sleep(5)
        run_real_mode()  # basic reconnect logic

    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={API_KEY}",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()


if __name__ == "__main__":
    if MOCK_MODE or not API_KEY:
        run_mock_mode()
    else:
        run_real_mode()