#!/usr/bin/env python3
"""Quick test: verify KiteTicker WebSocket connectivity.

Usage:
    doppler run -- uv run python scripts/test_kite_websocket.py

Tests:
  1. REST API health (kite.ltp for SBIN)
  2. WebSocket connection + tick receipt (KiteTicker, 15s window)
  3. Instrument token lookup

Requires: KITE_API_KEY + KITE_ACCESS_TOKEN in env (via Doppler).
"""

from __future__ import annotations

import os
import sys
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

# Symbols to test
TEST_SYMBOLS = ["SBIN", "RELIANCE", "INFY"]
EXCHANGE = "NSE"
WEBSOCKET_TIMEOUT = 15  # seconds to wait for ticks


def _get_kite():
    from kiteconnect import KiteConnect

    api_key = os.environ.get("KITE_API_KEY")
    access_token = os.environ.get("KITE_ACCESS_TOKEN")
    if not api_key or not access_token:
        print("ERROR: KITE_API_KEY and KITE_ACCESS_TOKEN must be set.")
        print("Run with: doppler run -- uv run python scripts/test_kite_websocket.py")
        sys.exit(1)

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite, api_key, access_token


def test_rest_api(kite):
    """Test 1: REST API health check."""
    print("\n--- Test 1: REST API (kite.ltp) ---")
    try:
        keys = [f"{EXCHANGE}:{s}" for s in TEST_SYMBOLS]
        data = kite.ltp(keys)
        for key, info in data.items():
            sym = key.split(":")[1]
            ltp = info.get("last_price", "N/A")
            token = info.get("instrument_token", "N/A")
            print(f"  OK {sym:12s}  LTP={ltp:>10}  token={token}")
        print(f"  REST API OK - {len(data)} symbols responded")
        return data
    except Exception as e:
        print(f"  FAIL REST API: {e}")
        return None


def test_instrument_lookup(kite):
    """Test 2: Instrument token lookup."""
    print("\n--- Test 2: Instrument Token Lookup ---")
    try:
        instruments = kite.instruments(EXCHANGE)
        token_map = {}
        for inst in instruments:
            if inst["tradingsymbol"] in TEST_SYMBOLS and inst["exchange"] == EXCHANGE:
                token_map[inst["tradingsymbol"]] = inst["instrument_token"]

        for sym in TEST_SYMBOLS:
            token = token_map.get(sym)
            if token:
                print(f"  OK {sym:12s}  instrument_token={token}")
            else:
                print(f"  FAIL {sym:12s}  NOT FOUND in instruments list")

        print(f"  Instrument lookup OK - {len(token_map)} tokens resolved")
        return token_map
    except Exception as e:
        print(f"  FAIL Instrument lookup: {e}")
        return None


def test_websocket(api_key, access_token, token_map):
    """Test 3: KiteTicker WebSocket connectivity."""
    print("\n--- Test 3: KiteTicker WebSocket ---")

    try:
        from kiteconnect import KiteTicker
    except ImportError:
        print("  FAIL KiteTicker not available in kiteconnect package")
        return False

    if not token_map:
        print("  FAIL Skipped - no instrument tokens available")
        return False

    tokens = list(token_map.values())
    reverse_map = {v: k for k, v in token_map.items()}

    ticks_received: list[dict] = []
    connected = threading.Event()
    error_msgs: list[str] = []

    def on_ticks(ws, ticks):
        for tick in ticks:
            sym = reverse_map.get(tick.get("instrument_token"), "?")
            ltp = tick.get("last_price", "N/A")
            mode = tick.get("mode", "?")
            ts = tick.get("exchange_timestamp", "")
            vol = tick.get("volume_traded", "N/A")
            ohlc = tick.get("ohlc", {})
            print(
                f"  TICK  {sym:12s}  LTP={ltp:>10}  "
                f"vol={vol}  ohlc_high={ohlc.get('high', 'N/A')}  "
                f"mode={mode}  ts={ts}"
            )
            ticks_received.append(tick)

    def on_connect(ws, response):
        print(f"  OK WebSocket CONNECTED  (response={response})")
        connected.set()
        # Subscribe in MODE_QUOTE to get LTP + day OHLC + volume
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_QUOTE, tokens)
        print(f"  OK Subscribed to {len(tokens)} tokens in MODE_QUOTE")

    def on_close(ws, code, reason):
        print(f"  WebSocket closed  code={code}  reason={reason}")

    def on_error(ws, code, reason):
        msg = f"WebSocket error  code={code}  reason={reason}"
        print(f"  FAIL {msg}")
        error_msgs.append(msg)

    def on_reconnect(ws, attempts):
        print(f"  Reconnecting... attempt={attempts}")

    def on_noreconnect(ws):
        print("  FAIL Max reconnection attempts reached")

    ticker = KiteTicker(api_key, access_token)
    ticker.on_ticks = on_ticks
    ticker.on_connect = on_connect
    ticker.on_close = on_close
    ticker.on_error = on_error
    ticker.on_reconnect = on_reconnect
    ticker.on_noreconnect = on_noreconnect

    # Run ticker in a background thread (it blocks)
    ws_thread = threading.Thread(
        target=lambda: ticker.connect(threaded=False),
        daemon=True,
    )
    ws_thread.start()

    # Wait for connection
    print(f"  Waiting up to {WEBSOCKET_TIMEOUT}s for ticks...")
    start = time.time()
    connected.wait(timeout=10)

    if not connected.is_set():
        print("  FAIL WebSocket did NOT connect within 10s")
        ticker.close()
        return False

    # Wait for ticks
    while time.time() - start < WEBSOCKET_TIMEOUT and len(ticks_received) < 5:
        time.sleep(0.5)

    elapsed = time.time() - start
    ticker.close()

    if ticks_received:
        unique_syms = {reverse_map.get(t.get("instrument_token"), "?") for t in ticks_received}
        print(
            f"\n  OK WebSocket - received {len(ticks_received)} ticks "
            f"for {len(unique_syms)} symbols in {elapsed:.1f}s"
        )
        # Check what data is available in ticks
        sample = ticks_received[0]
        print(f"  Tick fields: {sorted(sample.keys())}")
        return True
    else:
        print(f"\n  FAIL No ticks received in {elapsed:.1f}s")
        if error_msgs:
            print(f"  Errors: {error_msgs}")
        print("  Note: Market may be closed. Try during trading hours (09:15-15:30 IST)")
        return False


def main():
    now = datetime.now(IST)
    print(f"Kite Connectivity Test - {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("Market hours: 09:15-15:30 IST")
    if now.hour < 9 or (now.hour == 9 and now.minute < 15) or now.hour >= 16:
        print("WARN Market is likely CLOSED - WebSocket test may not receive ticks")

    kite, api_key, access_token = _get_kite()

    # Test 1: REST
    rest_data = test_rest_api(kite)

    # Test 2: Instrument tokens
    token_map = test_instrument_lookup(kite)

    # Test 3: WebSocket
    ws_ok = test_websocket(api_key, access_token, token_map)

    # Summary
    print("\n--- Summary ---")
    print(f"  REST API:    {'OK' if rest_data else 'FAILED'}")
    print(f"  Instruments: {'OK' if token_map else 'FAILED'}")
    print(f"  WebSocket:   {'OK' if ws_ok else 'FAILED / No ticks (market closed?)'}")

    if rest_data and not ws_ok:
        print("\n  REST works but WebSocket didn't receive ticks.")
        print(
            "  If market is closed, this is expected - WebSocket only pushes during market hours."
        )
        print("  Re-run during 09:15-15:30 IST to confirm WebSocket works.")


if __name__ == "__main__":
    main()
