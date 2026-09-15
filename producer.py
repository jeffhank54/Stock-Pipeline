import os
import json
import time
import random
from dotenv import load_dotenv
from kafka import KafkaProducer
import websocket

load_dotenv()

API_KEY = os.getenv("FINNHUB_API_KEY")
MOCK_MODE = os.getenv("MOCK_MODE", "false").lower() == "true"
SYMBOLS = ["AAPL", "TSLA", "NVDA", "MSFT"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    # key_serializer/value_serializer control how Python objects become bytes
    # on the wire — Kafka only ever moves raw bytes, it doesn't know about JSON.
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def publish_tick(symbol: str, price: float, timestamp: float):
    """Send one tick to the 'stock-ticks' topic.
    Keying by symbol ensures every tick for a given symbol always lands on
    the same partition — important later so a single consumer instance
    handles all of one symbol's data consistently."""
    producer.send(
        "stock-ticks",
        key=symbol,
        value={"symbol": symbol, "price": price, "timestamp": timestamp},
    )

def run_mock_mode():
    print("Producer running in MOCK MODE — generating fake ticks.\n")
    prices = {symbol: 100.0 + random.uniform(0, 400) for symbol in SYMBOLS}

    while True:
        for symbol in SYMBOLS:
            move_pct = random.gauss(0, 0.3)
            if random.random() < 0.05:
                move_pct += random.choice([-1, 1]) * random.uniform(2, 4)
            prices[symbol] *= (1 + move_pct / 100)
            publish_tick(symbol, prices[symbol], time.time())
            print(f"Published: {symbol} @ {prices[symbol]:.2f}")
        producer.flush()  # ensure messages are actually sent, not just buffered
        time.sleep(1)


def run_real_mode():
    def on_message(ws, message):
        data = json.loads(message)
        if data.get("type") != "trade":
            return
        for trade in data.get("data", []):
            symbol = trade["s"]
            if symbol in SYMBOLS:
                publish_tick(symbol, trade["p"], trade["t"] / 1000)
                print(f"Published: {symbol} @ {trade['p']:.2f}")

    def on_open(ws):
        print("Connected to Finnhub. Subscribing to symbols...")
        for symbol in SYMBOLS:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))

    def on_error(ws, error):
        print(f"WebSocket error: {error}")

    def on_close(ws, close_status_code, close_msg):
        print("Connection closed. Reconnecting in 5s...")
        time.sleep(5)
        run_real_mode()

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