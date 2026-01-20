import json
import csv
import time
from collections import deque, defaultdict
from datetime import datetime, timezone
import requests
import websocket
from lamda_architecture.publish_to_queue import init_rabbitmq, publish_to_rabbitmq

# =========================
# CONFIG
# =========================
SYMBOLS = ["btcusdt", "ethusdt"]  # lowercase
INTERVAL = "1m"

# Threshold rules
PRICE_THRESHOLDS = {
    # symbol: (below, above)  # put None if you don't care
    "btcusdt": (93099.18, 93100.18),
    "ethusdt": (3317.88, 3320.88),
}

PCT_MOVE_THRESHOLD = 0.7  # % change in a 1m candle body (open->close), e.g. 0.7%
VOLUME_SPIKE_MULT = 3.0   # current candle volume > 3x rolling average
VOLUME_ROLLING_WINDOW = 30  # last 30 closed candles


vol_hist = defaultdict(lambda: deque(maxlen=VOLUME_ROLLING_WINDOW))

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def ms_to_iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()

# def fire_webhook(payload):
#     if not WEBHOOK_URL:
#         return
#     try:
#         requests.post(WEBHOOK_URL, json=payload, timeout=WEBHOOK_TIMEOUT_SEC)
#     except Exception as e:
#         # don't crash on webhook issues
#         print("[WARN] webhook failed:", repr(e))

def make_alert(symbol, alert_type, message, price=None, pct_move=None, volume=None, vol_avg=None):
    return  {
        "event_type": alert_type,
        "symbol": symbol,
        "timestamp_utc": utc_now_iso(),
        "message": message,
        "metrics": {
            "price": price,
            "pct_move": pct_move,
            "volume": volume,
            "volume_avg": vol_avg
        },
        "source": "binance-websocket",
        "interval": INTERVAL
    }

# =========================
# RULES
# =========================
def check_rules(symbol, o, c, v):
    alerts = []
    # price threshold
    below, above = PRICE_THRESHOLDS.get(symbol, (None, None))
    if above is not None and c > above:
        alerts.append(make_alert(symbol, "PRICE_ABOVE", f"Close {c:.4f} > {above}", price=c))
    if below is not None and c < below:
        alerts.append(make_alert(symbol, "PRICE_BELOW", f"Close {c:.4f} < {below}", price=c))

    # % move in candle (open->close)
    if o > 0:
        pct_move = abs((c - o) / o) * 100.0
        if pct_move >= PCT_MOVE_THRESHOLD:
            alerts.append(make_alert(symbol, "PCT_MOVE", f"1m move {pct_move:.2f}% (open {o:.4f} -> close {c:.4f})",price=c, pct_move=pct_move, volume=v))

    # volume spike (needs history)
    hist = vol_hist[symbol]
    if len(hist) >= 10:  # wait until you have some baseline
        vol_avg = sum(hist) / len(hist)
        if vol_avg > 0 and v > VOLUME_SPIKE_MULT * vol_avg:
            alerts.append(make_alert(symbol, "VOLUME_SPIKE",f"Volume {v:.2f} > {VOLUME_SPIKE_MULT:.1f}x avg {vol_avg:.2f}",price=c, volume=v, vol_avg=vol_avg))
    for alert in alerts:
        publish_to_rabbitmq(alert)
        print(f"[ALERT] {symbol} {alert['event_type']}: {alert['message']}")

# =========================
# WEBSOCKET HANDLERS
# =========================
def on_message(ws, message):
    try:
        msg = json.loads(message)
        data = msg.get("data", msg)  # multi-stream wraps in {"stream": "...", "data": {...}}

        if data.get("e") != "kline":
            return

        k = data["k"]
        symbol = k["s"].lower()
        is_closed = bool(k["x"])

        o = float(k["o"])
        h = float(k["h"])
        l = float(k["l"])
        c = float(k["c"])
        v = float(k["v"])
        kline_start = int(k["t"])

        # Only run rules + update volume baseline on CLOSED candles
        if is_closed:
            check_rules(symbol, o, c, v)
            vol_hist[symbol].append(v)

    except Exception as e:
        print("[WARN] failed to parse message:", repr(e))

def on_error(ws, error):
    print("[ERROR]", error)

def on_close(ws, close_status_code, close_msg):
    print("[CLOSE]", close_status_code, close_msg)

def on_open(ws):
    print("[OPEN] Connected to Binance WebSocket")

# =========================
# MAIN
# =========================
def main():

    streams = "/".join([f"{s}@kline_{INTERVAL}" for s in SYMBOLS])
    ws_url = f"wss://stream.binance.com:9443/stream?streams={streams}"

    print("Streaming URL:", ws_url)
    # print("Webhook:", WEBHOOK_URL if WEBHOOK_URL else "(disabled)")

    while True:
        try:
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except KeyboardInterrupt:
            print("\nStopped by user.")
            break
        except Exception as e:
            print("[WARN] reconnecting after error:", repr(e))
            time.sleep(3)

if __name__ == "__main__":
    init_rabbitmq()
    main()