# spark_worker.py — Production HFT Streaming Engine v3
# =============================================================================
# CHANGES FROM v2:
#   Phase 3 - Historical Replay Engine:
#     - SYMBOLS and BASE_PRICES now loaded from config.json on boot
#     - max_files_per_trigger, sma_window, rsi_window all read from config.json
#     - generate_market_data() is now dual-mode:
#         data_mode="synthetic" → existing random generator (unchanged)
#         data_mode="replay"    → reads CSV row-by-row, writes each row to
#                                 stream_input/ atomically, sleeps tick_interval_seconds
#                                 CSV format: Timestamp,Symbol,Open,High,Low,Close,Volume
#     - auto_trade defaults to False (loaded from config); suggestion-only
#   Phase 1 Bug fixes preserved:
#     - True incremental EMA for MACD (O(1) per tick)
#     - Wilder's RSI smoothing
#     - Lasso feature slice [:7]
#     - Confidence clamped [0, 100]
#   All other v2 features preserved:
#     - Live config polling from API every 5s
#     - Market condition math
#     - Dynamic position sizing
#     - Confidence threshold gating
#     - Feature toggles
# =============================================================================

import os
import sys
import time
import csv
import json
import threading
import requests
import random
import glob
import numpy as np
from collections import deque
from datetime import datetime
from datetime import datetime

# ── Environment ────────────────────────────────────────────────────────────────
os.environ['PYSPARK_PYTHON']        = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
if sys.platform == 'win32':
    hadoop_home = os.environ.get('HADOOP_HOME', r'C:\hadoop')
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['PATH']        = hadoop_home + r'\bin;' + os.environ.get('PATH', '')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev
from pyspark.sql.types import (
    StructType, StructField, DoubleType, TimestampType, StringType
)
from pyspark.ml.regression import LinearRegressionModel

# ── Load config.json on boot ───────────────────────────────────────────────────
CONFIG_FILE = "config.json"
_CONFIG_DEFAULTS = {
    "data_mode":             "synthetic",
    "replay_filepath":       None,
    "tick_interval_seconds": 2.0,
    "active_symbols": [
        {"symbol": "AAPL",  "base_price": 150.0},
        {"symbol": "TSLA",  "base_price": 200.0},
        {"symbol": "NVDA",  "base_price": 400.0},
        {"symbol": "MSFT",  "base_price": 330.0},
        {"symbol": "GOOGL", "base_price": 130.0},
        {"symbol": "AMZN",  "base_price": 135.0},
        {"symbol": "META",  "base_price": 300.0},
        {"symbol": "BRK.B", "base_price": 350.0},
        {"symbol": "LLY",   "base_price": 550.0},
        {"symbol": "AVGO",  "base_price": 850.0},
    ],
    "max_files_per_trigger": 5,
    "sma_window":            20,
    "rsi_window":            14,
    "starting_cash":         100_000.0,
    "min_ai_confidence":     0,
    "trade_size_shares":     1,
    "auto_trade":            False,
    "position_sizing":       "fixed",
    "risk_pct":              0.05,
    "xgb_weight":            0.5,
    "lasso_weight":          0.5,
    "use_rsi":               True,
    "use_macd":              True,
    "use_volume":            True,
    "use_sma":               True,
    "market_condition":      "normal",
}

def _load_file_config() -> dict:
    cfg = dict(_CONFIG_DEFAULTS)
    try:
        with open(CONFIG_FILE, "r") as f:
            cfg.update(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"⚠️  config.json not found or invalid ({e}); using defaults")
    return cfg

def _auto_discover_symbols(limit=500):
    """Scans the parquet directory and returns all available symbols."""
    parquet_dir = "data/processed/historical_parquet"
    symbols = []
    if os.path.exists(parquet_dir):
        folders = glob.glob(f"{parquet_dir}/symbol=*")
        for folder in folders:
            sym = folder.split("symbol=")[-1].replace("\\", "").replace("/", "")
            symbols.append({"symbol": sym, "base_price": 100.0})
            if len(symbols) >= limit: 
                break
    
    if not symbols:  # Fallback
        symbols = [{"symbol": "AAPL", "base_price": 150.0}, {"symbol": "TSLA", "base_price": 200.0}]
    return symbols

_file_cfg = _load_file_config()

# Derive SYMBOLS and BASE_PRICES from auto-discovered parquet symbols (fallback to config)
_discovered_symbols = _auto_discover_symbols(500)
SYMBOLS     = [e["symbol"]               for e in _discovered_symbols]
BASE_PRICES = {e["symbol"]: float(e["base_price"]) for e in _discovered_symbols}
MAX_FILES   = int(_file_cfg.get("max_files_per_trigger", 1))

print(f"Config loaded | discovered {len(SYMBOLS)} symbols | data_mode={_file_cfg['data_mode']}")

# ── Spark Session ──────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("MultiStock_HFT_Pipeline_v3")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.default.parallelism", "4")
    .config("spark.network.timeout", "800s")
    .config("spark.executor.heartbeatInterval", "60s")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ── API / directory constants ──────────────────────────────────────────────────
API_URL    = "http://localhost:8000/update"
CONFIG_URL = "http://localhost:8000/config"
SESSION_ID = int(time.time())
STREAM_DIR = "data/stream_input"
TEMP_DIR   = "data/temp_generator"
CHECKPOINT = f"data/fresh_checkpoints_{SESSION_ID}"
# ── Persistent HTTP Pipeline for Ultra-Low Latency ──
http_session = requests.Session()

# ── Live config (polls API every 5s; overrides file config for runtime params) ─
_cfg_lock = threading.Lock()
_live_cfg = {
    "tick_interval_seconds": _file_cfg.get("tick_interval_seconds", 2.0),
    "active_symbols":        _file_cfg.get("active_symbols", []), # <--- NEW
    "sma_window":            _file_cfg.get("sma_window", 20),
    "rsi_window":            _file_cfg.get("rsi_window", 14),
    "market_condition":      _file_cfg.get("market_condition", "normal"),
    "data_mode":             _file_cfg.get("data_mode", "synthetic"),
    "replay_filepath":       _file_cfg.get("replay_filepath", None),
    "auto_trade":            _file_cfg.get("auto_trade", False),
    "min_ai_confidence":     _file_cfg.get("min_ai_confidence", 0),
    "position_sizing":       _file_cfg.get("position_sizing", "fixed"),
    "trade_size_shares":     _file_cfg.get("trade_size_shares", 1),
    "risk_pct":              _file_cfg.get("risk_pct", 0.05),
    "xgb_weight":            _file_cfg.get("xgb_weight", 0.5),
    "lasso_weight":          _file_cfg.get("lasso_weight", 0.5),
    "use_rsi":               _file_cfg.get("use_rsi", True),
    "use_macd":              _file_cfg.get("use_macd", True),
    "use_volume":            _file_cfg.get("use_volume", True),
    "use_sma":               _file_cfg.get("use_sma", True),
}

def _cfg(key, default=None):
    with _cfg_lock:
        return _live_cfg.get(key, default)

def _poll_config():
    while True:
        time.sleep(1)
        try:
            r = requests.get(CONFIG_URL, timeout=2)
            if r.status_code == 200:
                remote = r.json()
                with _cfg_lock:
                    for k in _live_cfg:
                        if k in remote:
                            _live_cfg[k] = remote[k]
        except Exception:
            pass

threading.Thread(target=_poll_config, daemon=True).start()

# ── Load Lasso model coefficients once ────────────────────────────────────────
print("Loading Lasso model coefficients…")
_lasso_spark    = LinearRegressionModel.load("data/models/lasso_model")
LASSO_COEFFS    = np.array(_lasso_spark.coefficients[:7])
LASSO_INTERCEPT = float(_lasso_spark.intercept)
del _lasso_spark
print("Lasso ready. XGBoost: using weighted heuristic.")

# ── Pure-Python ML ─────────────────────────────────────────────────────────────
def lasso_predict(features: np.ndarray) -> float:
    return float(np.dot(LASSO_COEFFS, features[:7]) + LASSO_INTERCEPT)

def xgb_direction_heuristic(current_price: float, lasso_target: float,
                              momentum: float, rsi: float, use_rsi: bool) -> float:
    xgb_w   = _cfg("xgb_weight", 0.5)
    lasso_w = _cfg("lasso_weight", 0.5)
    score   = (lasso_target - current_price) * lasso_w + momentum * xgb_w * 0.4
    if use_rsi:
        if rsi < 30:
            score += 0.5
        elif rsi > 70:
            score -= 0.5
    return 1.0 if score > 0 else 0.0

# ── True incremental EMA state per symbol ────────────────────────────────────
_ema_state: dict[str, dict] = {
    s: {"ema_fast": None, "ema_slow": None, "ema_signal": None}
    for s in SYMBOLS
}

# ── Replay backtesting state ───────────────────────────────────────────────────
# Stores what the model predicted last tick so we can score it this tick
_prev_predictions: dict[str, dict] = {}   # sym -> {lasso_target, xgb_direction, price_at_prediction}
_accuracy_state: dict[str, dict] = {      # sym -> running accuracy metrics
    s: {
        "lasso_errors":       [],   # rolling list of abs errors (last 50)
        "xgb_correct":        0,
        "xgb_total":          0,
        "lasso_mae":          0.0,
        "xgb_accuracy":       0.0,
    }
    for s in SYMBOLS
}

def _update_ema(prev, price: float, period: int) -> float:
    if prev is None:
        return price
    k = 2.0 / (period + 1)
    return price * k + prev * (1 - k)

def calculate_rsi(prices: deque, period: int = 14) -> float:
    if len(prices) < period + 1:
        return 50.0
    arr    = np.array(list(prices)[-period - 1:])
    deltas = np.diff(arr)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = gains[0]
    avg_loss = losses[0]
    for g, l in zip(gains[1:], losses[1:]):
        avg_gain = (avg_gain * (period - 1) + g) / period
        avg_loss = (avg_loss * (period - 1) + l) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    return float(100 - 100 / (1 + avg_gain / avg_loss))

def calculate_macd(sym: str, price: float,
                   fast: int = 12, slow: int = 26, signal: int = 9):
    st = _ema_state[sym]
    st["ema_fast"]   = _update_ema(st["ema_fast"],   price, fast)
    st["ema_slow"]   = _update_ema(st["ema_slow"],   price, slow)
    macd_line        = st["ema_fast"] - st["ema_slow"]
    st["ema_signal"] = _update_ema(st["ema_signal"], macd_line, signal)
    signal_line      = st["ema_signal"]
    return float(macd_line), float(signal_line), float(macd_line - signal_line)

# ── Per-symbol rolling state ───────────────────────────────────────────────────
_state_lock   = threading.Lock()
price_history = {s: deque(maxlen=500) for s in SYMBOLS}
ma10_history  = {s: deque(maxlen=50)  for s in SYMBOLS}
last_prices   = dict(BASE_PRICES)

trading_state = {
    "cash":      _file_cfg.get("starting_cash", 100_000.0),
    "positions": {s: 0 for s in SYMBOLS},
}

# ── Streaming schema ───────────────────────────────────────────────────────────
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("Symbol",    StringType(),    True),
    StructField("Open",      DoubleType(),    True),
    StructField("High",      DoubleType(),    True),
    StructField("Low",       DoubleType(),    True),
    StructField("Close",     DoubleType(),    True),
    StructField("Volume",    DoubleType(),    True),
    StructField("gen_ts",    DoubleType(),    True),  # Unix timestamp when tick was born
])

os.makedirs(STREAM_DIR, exist_ok=True)
os.makedirs(TEMP_DIR,   exist_ok=True)

# Clear leftover CSV files from previous session to prevent checkpoint confusion
try:
    for old_file in os.listdir(STREAM_DIR):
        if old_file.endswith(".csv"):
            os.remove(os.path.join(STREAM_DIR, old_file))
    print(f"Cleared stale tick files from {STREAM_DIR}")
except Exception as e:
    print(f"⚠️  Could not clear stream_input: {e}")

raw_stream = (
    spark.readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 1)
    .option("latestFirst", "true")
    .option("cleanSource", "delete")
    .csv(STREAM_DIR)
)

# ── foreachBatch ───────────────────────────────────────────────────────────────
def process_batch(df, epoch_id):
    print(f"[{time.time():.3f}] Epoch {epoch_id} started")

    agg_df = (
        df.groupBy("Symbol")
        .agg(
            avg("Close").alias("Close"),
            avg("Open").alias("Open"),
            avg("High").alias("High"),
            avg("Low").alias("Low"),
            avg("Volume").alias("Volume"),
            stddev("Close").alias("Close_std"),
        )
        .fillna({"Close_std": 1.0})
    )

    rows = agg_df.collect()
    if not rows:
        print(f"⏳ Epoch {epoch_id}: No data")
        return

    # Snapshot config once per batch (one lock acquisition)
    with _cfg_lock:
        active_subscriptions = [s["symbol"] for s in _live_cfg.get("active_symbols", [])]
        sma_w      = _live_cfg["sma_window"]
        rsi_w      = _live_cfg["rsi_window"]
        auto_trade = _live_cfg["auto_trade"]
        conf_thr   = _live_cfg["min_ai_confidence"]
        pos_mode   = _live_cfg["position_sizing"]
        fixed_sh   = _live_cfg["trade_size_shares"]
        risk_pct   = _live_cfg["risk_pct"]
        use_rsi    = _live_cfg["use_rsi"]
        use_macd   = _live_cfg["use_macd"]
        use_vol    = _live_cfg["use_volume"]
    
    if len(active_subscriptions) == 0:
        print(f"⏳ Epoch {epoch_id}: No active subscriptions - skipping batch")
        return
    
    if epoch_id == 0 or epoch_id % 10 == 0:
        print(f"Watching symbols: {active_subscriptions}")

    # BATCHING FIX: Collect all payloads instead of POSTing 10x
    batch_payloads = []

    for row in rows:
        sym           = row["Symbol"]
        
        # LAZY EVALUATION: If React isn't watching it, skip it!
        if sym not in active_subscriptions:
            continue
        
        # Per-symbol latency timing starts here
        tick_start = time.perf_counter()
        
        current_price = float(row["Close"])
        open_price    = float(row["Open"])
        high_price    = float(row["High"])
        low_price     = float(row["Low"])
        volume        = float(row["Volume"])
        volatility    = float(row["Close_std"]) if row["Close_std"] else 1.0

        if sym not in SYMBOLS:
            continue

        # Update rolling state
        with _state_lock:
            price_history[sym].append(current_price)
            ma10_history[sym].append(current_price)

            hist_len  = len(price_history[sym])
            sma_slice = list(price_history[sym])[-min(sma_w, hist_len):]
            sma_val   = float(np.mean(sma_slice)) if sma_slice else current_price
            ma_10     = float(np.mean(list(ma10_history[sym])[-10:])) if len(ma10_history[sym]) >= 2 else current_price

            rsi = calculate_rsi(price_history[sym], period=rsi_w) if use_rsi else 50.0
            macd_line, signal_line, histogram = (
                calculate_macd(sym, current_price) if use_macd else (0.0, 0.0, 0.0)
            )

            prev_price       = last_prices.get(sym, current_price)
            last_prices[sym] = current_price

        # ── Score last tick's prediction against this tick's actual price (replay only) ──
        lasso_error  = None
        xgb_correct  = None
        data_mode    = _cfg("data_mode", "synthetic")

        if data_mode == "replay" and sym in _prev_predictions:
            prev = _prev_predictions[sym]
            actual_move_up = current_price > prev["price_at_prediction"]

            # Lasso MAE contribution
            lasso_error = abs(prev["lasso_target"] - current_price)
            acc = _accuracy_state.get(sym)
            if acc is not None:
                acc["lasso_errors"].append(lasso_error)
                if len(acc["lasso_errors"]) > 50:
                    acc["lasso_errors"].pop(0)
                acc["lasso_mae"] = round(float(np.mean(acc["lasso_errors"])), 4)

                # XGBoost direction accuracy
                predicted_up = prev["xgb_direction"] == "UP"
                acc["xgb_total"]   += 1
                acc["xgb_correct"] += 1 if (predicted_up == actual_move_up) else 0
                acc["xgb_accuracy"] = round(acc["xgb_correct"] / acc["xgb_total"] * 100, 1)

        # ML inference
        features     = np.array([open_price, high_price, low_price, current_price,
                                  volume if use_vol else 0.0, ma_10, volatility])
        lasso_target = lasso_predict(features)
        momentum     = (current_price - open_price) / open_price * 100 if open_price else 0.0
        xgb_dir      = xgb_direction_heuristic(current_price, lasso_target, momentum, rsi, use_rsi)

        # Normalize confidence calculation using percentage deviation
        percent_diff = abs(lasso_target - current_price) / current_price
        # If the target is 5% away from current price, confidence drops to 50%. If 10% away, 0%.
        confidence = max(0, min(100, int(100 - (percent_diff * 1000))))

        # Store this tick's prediction to be scored next tick
        if data_mode == "replay":
            _prev_predictions[sym] = {
                "lasso_target":       lasso_target,
                "xgb_direction":      "UP" if xgb_dir == 1.0 else "DOWN",
                "price_at_prediction": current_price,
            }

        # ── 1. Calculate Signal & Reasoning FIRST ─────────────────────────────
        if confidence < conf_thr:
            signal = "HOLD"
            reason = f"confidence ({confidence}%) is below the strict {conf_thr}% safety threshold."
        elif xgb_dir == 1.0 and lasso_target > current_price + 0.1:
            signal = "BUY"
            reason = "bullish AI predictions align with upward momentum."
        elif xgb_dir == 0.0 or lasso_target < current_price - 0.1:
            signal = "SELL"
            reason = "bearish AI predictions align with downward pressure."
        else:
            signal = "HOLD"
            reason = "the projected price movement lacks sufficient profit margin."

        # ── 2. Build the AI Consensus Narrative ───────────────────────────────
        if xgb_dir == 1.0 and lasso_target > current_price:
            ml_view = f"Models project an upward trend towards ${lasso_target:.2f}."
        elif xgb_dir == 0.0 and lasso_target < current_price:
            ml_view = f"Models project a downward trend towards ${lasso_target:.2f}."
        else:
            ml_view = f"Models are conflicted (XGB predicts {'UP' if xgb_dir == 1.0 else 'DOWN'}, but Lasso targets ${lasso_target:.2f})."

        # ── 3. Build the Technical Context Narrative ──────────────────────────
        tech_notes = []
        if use_rsi:
            if rsi > 70: tech_notes.append(f"heavily overbought (RSI {rsi:.0f})")
            elif rsi < 30: tech_notes.append(f"heavily oversold (RSI {rsi:.0f})")
        if use_macd:
            if histogram > 0.1: tech_notes.append("accelerating upward")
            elif histogram < -0.1: tech_notes.append("accelerating downward")
            
        tech_str = f" However, the market is currently {' and '.join(tech_notes)}." if tech_notes else ""

        # ── 4. Combine into the Final Human-Readable Rationale ────────────────
        explanation = f"{ml_view}{tech_str} Action: {signal} because {reason}"

        # Anomaly detection
        anomaly = "None"
        if volume > 15_000_000 and use_vol:
            anomaly = "VOLUME SPIKE"
        elif abs(current_price - prev_price) > 2.0:
            anomaly = f"PRICE SHOCK (${abs(current_price - prev_price):.2f})"

        # Paper trading — only executes if auto_trade is True
        with _state_lock:
            trade_executed = "None"
            shares_traded  = 0

            if auto_trade:
                pos = trading_state["positions"].setdefault(sym, 0)
                num_shares = max(1, int(trading_state["cash"] * risk_pct / max(current_price, 0.01))) \
                    if pos_mode == "percentage" else max(1, fixed_sh)

                if signal == "BUY" and trading_state["cash"] >= current_price * num_shares:
                    trading_state["positions"][sym] += num_shares
                    trading_state["cash"]           -= current_price * num_shares
                    trade_executed = f"BOUGHT {num_shares} @ ${current_price:.2f}"
                    shares_traded  = num_shares
                elif signal == "SELL" and trading_state["positions"].get(sym, 0) >= num_shares:
                    trading_state["positions"][sym] -= num_shares
                    trading_state["cash"]           += current_price * num_shares
                    trade_executed = f"SOLD {num_shares} @ ${current_price:.2f}"
                    shares_traded  = num_shares

            port_val       = trading_state["cash"] + sum(
                trading_state["positions"].get(s, 0) * last_prices.get(s, BASE_PRICES.get(s, 100.0))
                for s in SYMBOLS
            )
            cash_snap      = trading_state["cash"]
            positions_snap = dict(trading_state["positions"])

        # Per-symbol latency: time spent in ML inference for this tick
        latency_ms = (time.perf_counter() - tick_start) * 1000
        
        payload = {
            "symbol":          sym,
            "price":           round(current_price, 2),
            "sma_20":          round(sma_val, 2),
            "momentum":        round(momentum, 2),
            "signal":          signal,
            "lasso_target":    round(lasso_target, 2),
            "xgb_direction":   "UP" if xgb_dir == 1.0 else "DOWN",
            "confidence":      confidence,
            "explanation":     explanation,
            "latency_ms":      round(latency_ms, 3),
            "throughput":      len(rows),
            "portfolio_value": round(port_val, 2),
            "cash":            round(cash_snap, 2),
            "positions":       positions_snap,
            "anomaly":         anomaly,
            "trade_executed":  trade_executed,
            "shares_traded":   shares_traded,
            "rsi":             round(rsi, 2),
            "macd_line":       round(macd_line, 4),
            "macd_signal":     round(signal_line, 4),
            "macd_histogram":  round(histogram, 4),
            "lasso_mae":       round(_accuracy_state[sym]["lasso_mae"], 4)    if sym in _accuracy_state else 0.0,
            "xgb_accuracy":    round(_accuracy_state[sym]["xgb_accuracy"], 1) if sym in _accuracy_state else 0.0,
            "lasso_error":     round(lasso_error, 4) if lasso_error is not None else None,
        }

        # 🔥 BATCHING FIX: Append to batch instead of POSTing immediately
        batch_payloads.append(payload)

    if batch_payloads:
        try:
            # Grab the number of distributed partitions Spark used for this batch
            num_partitions = df.rdd.getNumPartitions()
            print(f"[SPARK DAG] Epoch {epoch_id} | Processed {len(rows)} active symbols across {num_partitions} parallel partitions in {latency_ms:.1f}ms")
            
            r = http_session.post(API_URL, json=batch_payloads, timeout=10) # <--- CHANGED HERE
            if r.status_code == 200:
                pass # Removed print statement to save I/O time!
            else:
                print(f"  ⚠️ API returned {r.status_code}")
        except requests.exceptions.ConnectionError:
            print(f"  API unreachable — is api_engine.py running on port 8000?")
        except Exception as e:
            print(f"  Batch POST error: {e}")

    # Cleanup old stream files (cutoff extended to 600s as Spark now manages deletion via cleanSource)
    try:
        cutoff = time.time() - 600
        for fname in os.listdir(STREAM_DIR):
            fpath = os.path.join(STREAM_DIR, fname)
            if os.path.isfile(fpath) and os.path.getmtime(fpath) < cutoff:
                os.remove(fpath)
    except Exception:
        pass


# ── Streaming query ────────────────────────────────────────────────────────────
query = (
    raw_stream.writeStream
    .outputMode("append")
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="100 milliseconds")
    .start()
)
print("Streaming engine live.")

# ── Market data generator — dual mode ─────────────────────────────────────────
_CONDITION_PARAMS = {
    "normal":   {"drift":  0.0,  "vol_mult": 1.0, "vol_range": (-1.5,  1.5)},
    "bull":     {"drift":  0.3,  "vol_mult": 0.8, "vol_range": (-0.5,  2.0)},
    "bear":     {"drift": -0.3,  "vol_mult": 0.8, "vol_range": (-2.0,  0.5)},
    "crash":    {"drift": -2.0,  "vol_mult": 3.0, "vol_range": (-5.0,  1.0)},
    "volatile": {"drift":  0.0,  "vol_mult": 3.0, "vol_range": (-4.0,  4.0)},
}

def _write_tick_rows(rows_data: list, tick_count: int) -> None:
    """Atomically write a list of [ts_str, sym, open, high, low, close, vol] rows."""
    ts   = int(time.time())
    temp = os.path.join(TEMP_DIR,   f"tick_{ts}_{tick_count}.csv")
    dest = os.path.join(STREAM_DIR, f"tick_{ts}_{tick_count}.csv")
    try:
        with open(temp, 'w', newline='') as f:
            writer = csv.writer(f)
            for row in rows_data:
                writer.writerow(row)
        os.rename(temp, dest)
    except Exception as e:
        print(f"❌ Write tick error: {e}")
        try:
            os.remove(temp)
        except Exception:
            pass

def _synthetic_loop():
    tick_count = 0
    while True:
        interval_s = max(0.1, float(_cfg("tick_interval_seconds", 2.0)))
        time.sleep(interval_s)

        condition = _cfg("market_condition", "normal")
        params    = _CONDITION_PARAMS.get(condition, _CONDITION_PARAMS["normal"])
        lo, hi    = params["vol_range"]
        drift     = params["drift"]
        vol_mult  = params["vol_mult"]

        rows_data = []
        for sym in SYMBOLS:
            with _cfg_lock:
                active_subscriptions = [s["symbol"] for s in _live_cfg.get("active_symbols", [])]
                
            if sym not in active_subscriptions:
                continue
            
            with _state_lock:
                base = last_prices.get(sym, BASE_PRICES.get(sym, 100.0))

            noise = random.uniform(lo, hi) * vol_mult + drift
            price = max(0.01, base + noise)

            with _state_lock:
                last_prices[sym] = price

            ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            gen_ts = time.time()  # Birth timestamp for transport lag calculation
            open_p = price - random.uniform(0.0, 0.5)
            high_p = price + random.uniform(0.1, 0.8)
            low_p  = price - random.uniform(0.1, 0.8)
            vol    = random.randint(1_000_000, 20_000_000)
            rows_data.append([ts_str, sym, round(open_p, 4), round(high_p, 4),
                               round(low_p, 4), round(price, 4), vol, gen_ts])

        _write_tick_rows(rows_data, tick_count)
        tick_count += 1
        if tick_count % 20 == 0:
            print(f"Synthetic tick #{tick_count} @ {interval_s:.2f}s | {condition}")

def _replay_loop():
    """
    Read the CSV at replay_filepath row by row.
    Expected columns: Timestamp, Symbol, Open, High, Low, Close, Volume
    Loops back to the beginning when the file is exhausted.
    """
    tick_count = 0
    while True:
        filepath = _cfg("replay_filepath", None)
        if not filepath or not os.path.isfile(filepath):
            print("⚠️  Replay mode: no valid replay_filepath set; waiting 5s…")
            time.sleep(5)
            continue

        print(f"▶️  Replay starting: {filepath}")
        try:
            with open(filepath, "r", newline='') as f:
                reader = csv.reader(f)
                header = next(reader, None)  # skip header row

                for csv_row in reader:
                    if len(csv_row) < 7:
                        continue
                    interval_s = max(0.05, float(_cfg("tick_interval_seconds", 2.0)))
                    time.sleep(interval_s)

                    # Repackage into our standard format
                    # Input: Timestamp, Symbol, Open, High, Low, Close, Volume
                    ts_str = csv_row[0].strip()
                    sym    = csv_row[1].strip().upper()
                    
                    with _cfg_lock:
                        active_subscriptions = [s["symbol"] for s in _live_cfg.get("active_symbols", [])]
                        
                    if sym not in active_subscriptions:
                        continue
                    
                    try:
                        open_p = float(csv_row[2])
                        high_p = float(csv_row[3])
                        low_p  = float(csv_row[4])
                        close  = float(csv_row[5])
                        vol    = float(csv_row[6])
                    except ValueError:
                        continue

                    # Update last_prices for portfolio valuation
                    with _state_lock:
                        last_prices[sym] = close

                    _write_tick_rows(
                        [[ts_str, sym,
                          round(open_p, 4), round(high_p, 4),
                          round(low_p, 4),  round(close, 4),
                          int(vol)]],
                        tick_count
                    )
                    tick_count += 1
                    if tick_count % 50 == 0:
                        print(f"▶️  Replay tick #{tick_count} | {sym} @ ${close:.2f}")

            print("🔁 Replay file exhausted — looping from start")
        except Exception as e:
            print(f"Replay error: {e}")
            time.sleep(5)

def generate_market_data():
    """Entry point: routes to synthetic or replay based on live config."""
    while True:
        mode = _cfg("data_mode", "synthetic")
        if mode == "replay":
            _replay_loop()
        else:
            _synthetic_loop()
        # If _synthetic_loop returns (it won't normally), re-check mode
        time.sleep(1)

gen_thread = threading.Thread(target=generate_market_data, daemon=True)
gen_thread.start()
print(f"🧵 Generator started | mode={_cfg('data_mode')} | symbols={', '.join(SYMBOLS)}")

# ── Keep alive ─────────────────────────────────────────────────────────────────
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n🛑 Shutting down…")
    query.stop()
    spark.stop()
