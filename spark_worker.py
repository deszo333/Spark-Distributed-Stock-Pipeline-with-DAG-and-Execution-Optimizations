# spark_worker.py — Production HFT Streaming Engine v4
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

# ── Environment ────────────────────────────────────────────────────────────────
os.environ['PYSPARK_PYTHON']        = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
if sys.platform == 'win32':
    # Search common Hadoop locations — don't hardcode
    for _hpath in [r'C:\hadoop', r'C:\winutils',
                   os.path.join(os.path.expanduser('~'), 'winutils', 'hadoop-3.3.5')]:
        if os.path.exists(os.path.join(_hpath, 'bin', 'winutils.exe')):
            os.environ['HADOOP_HOME'] = _hpath
            os.environ['PATH'] = os.path.join(_hpath, 'bin') + ';' + os.environ.get('PATH', '')
            break

# ── CLI toggle: python spark_worker.py serial → local[1]  [FIX-7] ─────────────
is_serial_mode = len(sys.argv) > 1 and sys.argv[1].lower() == "serial"
master_config  = "local[1]" if is_serial_mode else "local[*]"
print(f"\n{'='*70}")
print(f"  STREAMING ENGINE — "
      f"{'SERIAL (1 Core)' if is_serial_mode else 'PARALLEL (All Cores)'} MODE")
print(f"  Spark master: {master_config}")
print(f"{'='*70}\n")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev
from pyspark.sql.types import (
    StructType, StructField, DoubleType, TimestampType, StringType
)
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml.classification import GBTClassificationModel
from pyspark.ml.feature import VectorAssembler

# ── Config ─────────────────────────────────────────────────────────────────────
CONFIG_FILE = "config.json"
_CONFIG_DEFAULTS = {
    "data_mode":             "synthetic",
    "stream_workload":       100,          # [FIX-6] Dial of Doom
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
    "use_rsi":               True,
    "use_macd":              True,
    "use_volume":            True,
    "use_sma":               True,
    "market_condition":      "normal",
    "gbt_inference_interval_seconds": 5.0,
}

MAX_LIVE_LASSO_RETURN = 0.10
MAX_STREAM_BACKLOG_FILES = 20
MAX_GBT_CACHE_AGE_SECONDS = 3.0


def _load_file_config() -> dict:
    cfg = dict(_CONFIG_DEFAULTS)
    try:
        with open(CONFIG_FILE, "r") as f:
            cfg.update(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"  config.json not found or invalid ({e}); using defaults")
    return cfg


def _auto_discover_symbols(limit=500):
    parquet_dir = "data/processed/historical_parquet"
    symbols = []
    if os.path.exists(parquet_dir):
        folders = glob.glob(f"{parquet_dir}/symbol=*")
        for folder in folders:
            sym = folder.split("symbol=")[-1].replace("\\","").replace("/","")
            symbols.append({"symbol": sym, "base_price": 100.0})
            if len(symbols) >= limit:
                break
    if not symbols:
        symbols = [{"symbol":"AAPL","base_price":150.0},
                   {"symbol":"TSLA","base_price":200.0}]
    return symbols


_file_cfg          = _load_file_config()
_discovered_symbols = _auto_discover_symbols(500)
SYMBOLS     = [e["symbol"]               for e in _discovered_symbols]
BASE_PRICES = {e["symbol"]: float(e["base_price"]) for e in _discovered_symbols}
MAX_FILES_PER_TRIGGER = max(1, int(_file_cfg.get("max_files_per_trigger", 5)))

print(f"Config loaded | discovered {len(SYMBOLS)} symbols | data_mode={_file_cfg['data_mode']}")

# ── Spark Session ──────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("MultiStock_HFT_Pipeline_v4")
    .master(master_config)                    # [FIX-7] dynamic from CLI
    .config("spark.driver.memory", "4g")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.default.parallelism", "4")
    .config("spark.network.timeout", "800s")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.ui.port", "0")
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
http_session = requests.Session()

# ── Live config ────────────────────────────────────────────────────────────────
_cfg_lock = threading.Lock()
_live_cfg = {
    "tick_interval_seconds": _file_cfg.get("tick_interval_seconds", 2.0),
    "stream_workload":       _file_cfg.get("stream_workload", 100),    # [FIX-6]
    "active_symbols":        _file_cfg.get("active_symbols", []),
    "sma_window":            _file_cfg.get("sma_window", 20),
    "rsi_window":            _file_cfg.get("rsi_window", 14),
    "market_condition":      _file_cfg.get("market_condition", "normal"),
    "gbt_inference_interval_seconds": _file_cfg.get("gbt_inference_interval_seconds", 5.0),
    "data_mode":             _file_cfg.get("data_mode", "synthetic"),
    "replay_filepath":       _file_cfg.get("replay_filepath", None),
    "auto_trade":            _file_cfg.get("auto_trade", False),
    "min_ai_confidence":     _file_cfg.get("min_ai_confidence", 0),
    "position_sizing":       _file_cfg.get("position_sizing", "fixed"),
    "trade_size_shares":     _file_cfg.get("trade_size_shares", 1),
    "risk_pct":              _file_cfg.get("risk_pct", 0.05),
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

# =============================================================================
# MODEL LOADING
# =============================================================================

# ── Load Lasso model (baseline — predicts % return) ────────────────────────────
print("Loading Lasso model coefficients...")
try:
    _lasso_spark    = LinearRegressionModel.load("data/models/lasso_model")
    # [FIX-5] DenseVector does not support slice — convert to list first
    LASSO_COEFFS    = np.array(list(_lasso_spark.coefficients)[:7])
    LASSO_INTERCEPT = float(_lasso_spark.intercept)
    del _lasso_spark
    LASSO_READY = True
    print("  Lasso model ready.")
except Exception as e:
    print(f"  WARNING: Lasso model not found ({e}). Run ml_forecaster.py first.")
    LASSO_COEFFS    = np.zeros(7)
    LASSO_INTERCEPT = 0.0
    LASSO_READY     = False

# ── Load GBT model (main — predicts direction UP/DOWN)  [FIX-1, FIX-2] ────────
# GBT inference uses a Spark DataFrame per batch — we build it from collected rows
print("Loading GBT model (main classifier)...")
try:
    GBT_MODEL = GBTClassificationModel.load("data/models/gbt_model")
    GBT_READY = True
    print("  GBT model ready.")
except Exception as e:
    print(f"  WARNING: GBT model not found ({e}). Run ml_forecaster.py first.")
    GBT_MODEL = None
    GBT_READY = False

# GBT feature set — must match GBT_FEATURE_COLS in ml_forecaster.py
GBT_INFERENCE_COLS = [
    "Open", "High", "Low", "Close", "Volume",
    "MA_10", "MA_30", "Volatility", "Vol_MA_10",
    "Momentum_1", "Momentum_5",
    "Close_norm", "Volume_norm", "MA_ratio_10_30",
]

# VectorAssembler for GBT inference (initialized once, reused per batch)
_gbt_assembler = VectorAssembler(
    inputCols=GBT_INFERENCE_COLS,
    outputCol="gbt_features",
    handleInvalid="skip"
)


def lasso_predict(features: np.ndarray) -> float:
    """
    Lasso baseline prediction — returns % return estimate.
    Pure NumPy dot product using the 7 normalized/scale-invariant features.
    NOTE: result is a % return (e.g. 0.015 = +1.5%), not an absolute price.
    """
    return float(np.dot(LASSO_COEFFS, features[:7]) + LASSO_INTERCEPT)


def gbt_predict_batch(rows_data: list) -> dict:
    """
    [FIX-1] Real GBT inference using the trained GBTClassifierModel.
    Converts collected rows to a Spark DataFrame and calls .transform().
    Returns dict: symbol -> {direction, probability}
    Falls back to Lasso-based direction if GBT model unavailable.
    """
    if not GBT_READY or not rows_data:
        return {}

    from pyspark.sql import Row
    from pyspark.sql.types import (StructType, StructField, StringType,
                                   DoubleType, LongType)

    schema = StructType([
        StructField("symbol",     StringType(), True),
        StructField("Open",       DoubleType(), True),
        StructField("High",       DoubleType(), True),
        StructField("Low",        DoubleType(), True),
        StructField("Close",      DoubleType(), True),
        StructField("Volume",     DoubleType(), True),
        StructField("MA_10",      DoubleType(), True),
        StructField("MA_30",      DoubleType(), True),
        StructField("Volatility", DoubleType(), True),
        StructField("Vol_MA_10",  DoubleType(), True),
        StructField("Momentum_1", DoubleType(), True),
        StructField("Momentum_5", DoubleType(), True),
        StructField("Close_norm", DoubleType(), True),
        StructField("Volume_norm",DoubleType(), True),
        StructField("MA_ratio_10_30", DoubleType(), True),
    ])

    try:
        df_inf = spark.createDataFrame(rows_data, schema=schema)
        df_vec = _gbt_assembler.transform(df_inf)
        df_pred = GBT_MODEL.transform(df_vec)
        results = {}
        for row in df_pred.select("symbol", "prediction", "probability").collect():
            prediction = float(row["prediction"])
            probs = row["probability"]
            predicted_prob = float(probs[int(prediction)]) if probs is not None else None
            results[row["symbol"]] = {
                "direction": prediction,
                "probability": predicted_prob,
                "ts": time.time(),
            }
        return results
    except Exception as e:
        print(f"  GBT inference error: {e}")
        return {}


# ── EMA state ──────────────────────────────────────────────────────────────────
_ema_state: dict = {
    s: {"ema_fast": None, "ema_slow": None, "ema_signal": None}
    for s in SYMBOLS
}

# ── Replay accuracy state — renamed xgb→gbt  [FIX-3] ─────────────────────────
_prev_predictions: dict = {}
_accuracy_state: dict = {
    s: {
        "lasso_errors": [],
        "gbt_correct":  0,        # [FIX-3] was xgb_correct
        "gbt_total":    0,        # [FIX-3] was xgb_total
        "lasso_mae":    0.0,
        "gbt_accuracy": 0.0,      # [FIX-3] was xgb_accuracy
    }
    for s in SYMBOLS
}
_last_gbt_predictions: dict = {}
_last_gbt_inference_ts = 0.0
_gbt_inference_inflight = False
_gbt_cache_lock = threading.Lock()


def _update_ema(prev, price: float, period: int) -> float:
    if prev is None: return price
    k = 2.0 / (period + 1)
    return price * k + prev * (1 - k)


def calculate_rsi(prices: deque, period: int = 14) -> float:
    if len(prices) < period + 1: return 50.0
    arr    = np.array(list(prices)[-period - 1:])
    deltas = np.diff(arr)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain, avg_loss = gains[0], losses[0]
    for g, l in zip(gains[1:], losses[1:]):
        avg_gain = (avg_gain*(period-1)+g)/period
        avg_loss = (avg_loss*(period-1)+l)/period
    if avg_loss == 0: return 100.0 if avg_gain > 0 else 50.0
    return float(100 - 100/(1 + avg_gain/avg_loss))


def calculate_macd(sym: str, price: float, fast=12, slow=26, signal=9):
    st = _ema_state[sym]
    st["ema_fast"]   = _update_ema(st["ema_fast"],   price, fast)
    st["ema_slow"]   = _update_ema(st["ema_slow"],   price, slow)
    macd_line        = st["ema_fast"] - st["ema_slow"]
    st["ema_signal"] = _update_ema(st["ema_signal"], macd_line, signal)
    return float(macd_line), float(st["ema_signal"]), float(macd_line - st["ema_signal"])


def _trim_stream_backlog(max_files: int = MAX_STREAM_BACKLOG_FILES) -> None:
    try:
        files = [
            os.path.join(STREAM_DIR, f)
            for f in os.listdir(STREAM_DIR)
            if f.endswith(".csv")
        ]
        if len(files) <= max_files:
            return
        files.sort(key=os.path.getmtime, reverse=True)
        for path in files[max_files:]:
            try:
                os.remove(path)
            except OSError:
                pass
    except Exception:
        pass


def _launch_gbt_inference(rows_snapshot: list) -> None:
    global _gbt_inference_inflight, _last_gbt_predictions
    try:
        fresh_gbt = gbt_predict_batch(rows_snapshot)
        if fresh_gbt:
            with _gbt_cache_lock:
                _last_gbt_predictions.update(fresh_gbt)
    finally:
        _gbt_inference_inflight = False


# ── Per-symbol rolling state ───────────────────────────────────────────────────
_state_lock   = threading.Lock()
price_history = {s: deque(maxlen=500) for s in SYMBOLS}
ma10_history  = {s: deque(maxlen=50)  for s in SYMBOLS}
ma30_history  = {s: deque(maxlen=50)  for s in SYMBOLS}
vol_history   = {s: deque(maxlen=50)  for s in SYMBOLS}
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
    StructField("gen_ts",    DoubleType(),    True),
])

os.makedirs(STREAM_DIR, exist_ok=True)
os.makedirs(TEMP_DIR,   exist_ok=True)

# Clear leftover files
try:
    for f in os.listdir(STREAM_DIR):
        if f.endswith(".csv"):
            os.remove(os.path.join(STREAM_DIR, f))
    print(f"Cleared stale tick files from {STREAM_DIR}")
except Exception as e:
    print(f"  Could not clear stream_input: {e}")

raw_stream = (
    spark.readStream
    .schema(schema)
    .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
    .option("latestFirst", "true")
    .option("cleanSource", "delete")
    .csv(STREAM_DIR)
)


# =============================================================================
# PROCESS BATCH — core streaming logic
# =============================================================================
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
        print(f"  Epoch {epoch_id}: No data"); return

    # Snapshot config once per batch
    with _cfg_lock:
        workload   = _live_cfg.get("stream_workload", 100)
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
        data_mode  = _live_cfg["data_mode"]
        gbt_interval = float(_live_cfg.get("gbt_inference_interval_seconds", 5.0))

    # Workload dial controls how many discovered symbols are analyzed.
    # active_symbols is only the UI watchlist and should not limit analysis.
    allowed_symbols = set(SYMBOLS[:workload])
    active_rows     = [r for r in rows if r["Symbol"] in allowed_symbols and r["Symbol"] in SYMBOLS]

    if not active_rows:
        print(f"  Epoch {epoch_id}: No symbols in workload"); return

    if epoch_id == 0 or epoch_id % 10 == 0:
        print(f"  Workload: {workload} symbols | Active this batch: {len(active_rows)}")

    # ── Step 1: compute per-symbol rolling features ────────────────────────────
    # We need these to build the GBT inference DataFrame
    symbol_features = {}  # sym -> feature dict for GBT

    for row in active_rows:
        sym           = row["Symbol"]
        current_price = float(row["Close"])
        open_price    = float(row["Open"])
        high_price    = float(row["High"])
        low_price     = float(row["Low"])
        volume        = float(row["Volume"])
        volatility    = float(row["Close_std"]) if row["Close_std"] else 1.0

        with _state_lock:
            price_history[sym].append(current_price)
            ma10_history[sym].append(current_price)
            ma30_history[sym].append(current_price)
            vol_history[sym].append(volume)

            hist_len  = len(price_history[sym])
            sma_slice = list(price_history[sym])[-min(sma_w, hist_len):]
            sma_val   = float(np.mean(sma_slice)) if sma_slice else current_price

            ma_10_arr  = list(ma10_history[sym])[-10:]
            ma_30_arr  = list(ma30_history[sym])[-30:]
            ma_10      = float(np.mean(ma_10_arr)) if len(ma_10_arr)>=2 else current_price
            ma_30      = float(np.mean(ma_30_arr)) if len(ma_30_arr)>=2 else current_price
            vol_ma_10  = float(np.mean(list(vol_history[sym])[-10:])) if len(vol_history[sym])>=2 else volume

            prev_price       = last_prices.get(sym, current_price)
            last_prices[sym] = current_price

            rsi = calculate_rsi(price_history[sym], period=rsi_w) if use_rsi else 50.0
            macd_line, signal_line, histogram = (
                calculate_macd(sym, current_price) if use_macd else (0.0, 0.0, 0.0)
            )

        # Compute momentum
        momentum_1 = (current_price - prev_price)/prev_price if prev_price else 0.0
        prices_list = list(price_history[sym])
        momentum_5 = ((current_price - prices_list[-6])/prices_list[-6]
                      if len(prices_list) >= 6 else 0.0)

        # Compute normalized features
        close_hist = list(price_history[sym])
        if len(close_hist) >= 2:
            mean_c = float(np.mean(close_hist[-30:])) if len(close_hist)>=30 else float(np.mean(close_hist))
            std_c  = float(np.std(close_hist[-30:]))  if len(close_hist)>=30 else float(np.std(close_hist))
            close_norm = (current_price - mean_c)/(std_c + 1e-8)
        else:
            close_norm = 0.0

        vol_hist_arr = list(vol_history[sym])
        if len(vol_hist_arr) >= 2:
            mean_v = float(np.mean(vol_hist_arr[-10:]))
            std_v  = float(np.std(vol_hist_arr[-10:]))
            vol_norm = (volume - mean_v)/(std_v + 1e-8)
        else:
            vol_norm = 0.0

        ma_ratio = ma_10/ma_30 if ma_30 != 0 else 1.0

        symbol_features[sym] = {
            "symbol":         sym,
            "open_price":     open_price,
            "high_price":     high_price,
            "low_price":      low_price,
            "current_price":  current_price,
            "volume":         volume,
            "volatility":     volatility,
            "sma_val":        sma_val,
            "ma_10":          ma_10,
            "ma_30":          ma_30,
            "vol_ma_10":      vol_ma_10,
            "momentum_1":     momentum_1,
            "momentum_5":     momentum_5,
            "close_norm":     close_norm,
            "volume_norm":    vol_norm,
            "ma_ratio":       ma_ratio,
            "rsi":            rsi,
            "macd_line":      macd_line,
            "signal_line":    signal_line,
            "histogram":      histogram,
            "prev_price":     prev_price,
        }

    # ── Step 2: GBT batch inference  [FIX-1, FIX-2] ───────────────────────────
    # Build rows for GBT inference DataFrame
    gbt_rows = []
    for sym, f in symbol_features.items():
        gbt_rows.append({
            "symbol":          sym,
            "Open":            f["open_price"],
            "High":            f["high_price"],
            "Low":             f["low_price"],
            "Close":           f["current_price"],
            "Volume":          f["volume"],
            "MA_10":           f["ma_10"],
            "MA_30":           f["ma_30"],
            "Volatility":      f["volatility"],
            "Vol_MA_10":       f["vol_ma_10"],
            "Momentum_1":      f["momentum_1"],
            "Momentum_5":      f["momentum_5"],
            "Close_norm":      f["close_norm"],
            "Volume_norm":     f["volume_norm"],
            "MA_ratio_10_30":  f["ma_ratio"],
        })

    # Force synchronous, blocking inference on every batch
    gbt_predictions = gbt_predict_batch([dict(row) for row in gbt_rows]) if GBT_READY else {}

    # ── Step 3: Lasso inference + signal generation per symbol ────────────────
    batch_payloads = []

    for sym, f in symbol_features.items():
        tick_start    = time.perf_counter()
        current_price = f["current_price"]
        open_price    = f["open_price"]

        # Lasso: predicts % return using 7 normalized features
        lasso_features = np.array([
            f["close_norm"], f["volume_norm"], f["ma_ratio"],
            f["volatility"], f["momentum_1"], f["momentum_5"], f["vol_ma_10"]
        ])
        lasso_return_raw = lasso_predict(lasso_features)
        lasso_return = max(
            -MAX_LIVE_LASSO_RETURN,
            min(MAX_LIVE_LASSO_RETURN, lasso_return_raw)
        )
        # Convert % return to a target price for UI display
        lasso_target_price = round(current_price * (1 + lasso_return), 2)

        # GBT direction: 1.0=UP, 0.0=DOWN
        gbt_result = gbt_predictions.get(sym)
        
        if gbt_result is not None:
            gbt_dir = gbt_result["direction"]
            gbt_probability = gbt_result.get("probability")
            gbt_is_stale = False
        else:
            # Fallback to Lasso if GBT model is missing entirely
            gbt_dir = 1.0 if lasso_return > 0 else 0.0
            gbt_probability = None
            gbt_is_stale = True

        # Fresh GBT confidence is pure model probability; stale/missing GBT falls
        # back to a bounded Lasso strength score and is flagged in the payload.
        confidence = (
            int(gbt_probability * 100)
            if gbt_probability is not None
            else int(min(70, abs(lasso_return) * 1000))
        )

        # ── Replay accuracy scoring  [FIX-3] renamed xgb→gbt ─────────────────
        lasso_error = None
        if data_mode == "replay" and sym in _prev_predictions:
            prev = _prev_predictions[sym]
            actual_move_up = current_price > prev["price_at_prediction"]
            lasso_error    = abs(prev["lasso_target_price"] - current_price)
            acc = _accuracy_state.get(sym)
            if acc is not None:
                acc["lasso_errors"].append(lasso_error)
                if len(acc["lasso_errors"]) > 50:
                    acc["lasso_errors"].pop(0)
                acc["lasso_mae"] = round(float(np.mean(acc["lasso_errors"])), 4)
                predicted_up = prev["gbt_direction"] == "UP"
                acc["gbt_total"]   += 1
                acc["gbt_correct"] += 1 if (predicted_up == actual_move_up) else 0
                acc["gbt_accuracy"] = round(acc["gbt_correct"]/acc["gbt_total"]*100, 1)

        if data_mode == "replay":
            _prev_predictions[sym] = {
                "lasso_target_price":  lasso_target_price,
                "gbt_direction":       "UP" if gbt_dir == 1.0 else "DOWN",
                "price_at_prediction": current_price,
            }

        # ── Signal generation (GBT is the primary signal)  [FIX-1] ───────────
        if confidence < conf_thr:
            signal = "HOLD"
            reason = f"GBT confidence ({confidence}%) below {conf_thr}% threshold."
        elif gbt_dir == 1.0 and lasso_return > 0:
            signal = "BUY"
            reason = "GBT predicts UP and Lasso confirms positive return."
        elif gbt_dir == 0.0 and lasso_return < 0:
            signal = "SELL"
            reason = "GBT predicts DOWN and Lasso confirms negative return."
        elif gbt_dir == 1.0:
            signal = "BUY"
            reason = "GBT main model predicts upward direction."
        elif gbt_dir == 0.0:
            signal = "SELL"
            reason = "GBT main model predicts downward direction."
        else:
            signal = "HOLD"
            reason = "models have insufficient directional agreement."

        # ── Narrative ─────────────────────────────────────────────────────────
        gbt_str   = f"GBT predicts {'UP' if gbt_dir==1.0 else 'DOWN'}."
        lasso_str = f"Lasso baseline: {lasso_return*100:+.2f}% expected return."
        tech_notes = []
        if use_rsi:
            rsi = f["rsi"]
            if rsi > 70: tech_notes.append(f"overbought (RSI {rsi:.0f})")
            elif rsi < 30: tech_notes.append(f"oversold (RSI {rsi:.0f})")
        if use_macd:
            h = f["histogram"]
            if h > 0.1: tech_notes.append("MACD bullish")
            elif h < -0.1: tech_notes.append("MACD bearish")
        tech_str    = f" Technical: {', '.join(tech_notes)}." if tech_notes else ""
        explanation = f"{gbt_str} {lasso_str}{tech_str} Action: {signal} — {reason}"

        anomaly = "None"
        if f["volume"] > 15_000_000 and use_vol:
            anomaly = "VOLUME SPIKE"
        elif abs(current_price - f["prev_price"]) > 2.0:
            anomaly = f"PRICE SHOCK (${abs(current_price - f['prev_price']):.2f})"

        # ── Paper trading ──────────────────────────────────────────────────────
        with _state_lock:
            trade_executed = "None"
            shares_traded  = 0
            if auto_trade:
                pos = trading_state["positions"].setdefault(sym, 0)
                num_shares = max(1, int(trading_state["cash"]*risk_pct/max(current_price,0.01))) \
                    if pos_mode == "percentage" else max(1, fixed_sh)
                if signal=="BUY" and trading_state["cash"] >= current_price*num_shares:
                    trading_state["positions"][sym] += num_shares
                    trading_state["cash"]           -= current_price*num_shares
                    trade_executed = f"BOUGHT {num_shares} @ ${current_price:.2f}"
                    shares_traded  = num_shares
                elif signal=="SELL" and trading_state["positions"].get(sym,0) >= num_shares:
                    trading_state["positions"][sym] -= num_shares
                    trading_state["cash"]           += current_price*num_shares
                    trade_executed = f"SOLD {num_shares} @ ${current_price:.2f}"
                    shares_traded  = num_shares

            port_val = trading_state["cash"] + sum(
                trading_state["positions"].get(s,0) * last_prices.get(s, BASE_PRICES.get(s,100.0))
                for s in SYMBOLS
            )
            cash_snap      = trading_state["cash"]
            positions_snap = dict(trading_state["positions"])

        latency_ms = (time.perf_counter() - tick_start) * 1000

        # [FIX-3] field names: xgb_direction kept for UI backward-compat,
        # xgb_accuracy renamed to gbt_accuracy. Both sent.
        payload = {
            "symbol":          sym,
            "price":           round(current_price, 2),
            "sma_20":          round(f["sma_val"], 2),
            "momentum":        round(f["momentum_1"]*100, 2),
            "signal":          signal,
            "lasso_target":    lasso_target_price,       # price form for UI
            "lasso_return":    round(lasso_return*100, 4), # % form
            "lasso_return_raw": round(lasso_return_raw*100, 4), # unclamped diagnostic
            "xgb_direction":   "UP" if gbt_dir==1.0 else "DOWN",  # UI compat
            "gbt_direction":   "UP" if gbt_dir==1.0 else "DOWN",  # new name
            "gbt_probability": round(gbt_probability * 100, 2) if gbt_probability is not None else None,
            "gbt_stale":       gbt_is_stale,
            "confidence":      confidence,
            "explanation":     explanation,
            "latency_ms":      round(latency_ms, 3),
            "throughput":      len(active_rows),
            "portfolio_value": round(port_val, 2),
            "cash":            round(cash_snap, 2),
            "positions":       positions_snap,
            "anomaly":         anomaly,
            "trade_executed":  trade_executed,
            "shares_traded":   shares_traded,
            "rsi":             round(f["rsi"], 2),
            "macd_line":       round(f["macd_line"], 4),
            "macd_signal":     round(f["signal_line"], 4),
            "macd_histogram":  round(f["histogram"], 4),
            "lasso_mae":       round(_accuracy_state[sym]["lasso_mae"], 4)    if sym in _accuracy_state else 0.0,
            "xgb_accuracy":    round(_accuracy_state[sym]["gbt_accuracy"], 1) if sym in _accuracy_state else 0.0,
            "gbt_accuracy":    round(_accuracy_state[sym]["gbt_accuracy"], 1) if sym in _accuracy_state else 0.0,
            "lasso_error":     round(lasso_error, 4) if lasso_error is not None else None,
        }
        batch_payloads.append(payload)

    if batch_payloads:
        try:
            num_partitions = df.rdd.getNumPartitions()
            print(f"  [DAG] Epoch {epoch_id} | {len(batch_payloads)} symbols | "
                  f"{num_partitions} partitions | {latency_ms:.1f}ms | "
                  f"GBT={'ready' if GBT_READY else 'fallback'}")
            r = http_session.post(API_URL, json=batch_payloads, timeout=10)
            if r.status_code != 200:
                print(f"  API returned {r.status_code}")
        except requests.exceptions.ConnectionError:
            print("  API unreachable — is api_engine.py running on port 8000?")
        except Exception as e:
            print(f"  Batch POST error: {e}")

    # Cleanup stale stream files
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

# ── Market data generator ──────────────────────────────────────────────────────
_CONDITION_PARAMS = {
    "normal":   {"drift":  0.0,  "vol_mult": 1.0, "vol_range": (-1.5,  1.5)},
    "bull":     {"drift":  0.3,  "vol_mult": 0.8, "vol_range": (-0.5,  2.0)},
    "bear":     {"drift": -0.3,  "vol_mult": 0.8, "vol_range": (-2.0,  0.5)},
    "crash":    {"drift": -2.0,  "vol_mult": 3.0, "vol_range": (-5.0,  1.0)},
    "volatile": {"drift":  0.0,  "vol_mult": 3.0, "vol_range": (-4.0,  4.0)},
}


def _write_tick_rows(rows_data: list, tick_count: int) -> None:
    ts   = int(time.time())
    temp = os.path.join(TEMP_DIR,   f"tick_{ts}_{tick_count}.csv")
    dest = os.path.join(STREAM_DIR, f"tick_{ts}_{tick_count}.csv")
    try:
        _trim_stream_backlog()
        with open(temp, 'w', newline='') as f:
            csv.writer(f).writerows(rows_data)
        os.rename(temp, dest)
    except Exception as e:
        print(f"  Write tick error: {e}")
        try: os.remove(temp)
        except: pass


def _synthetic_loop():
    tick_count = 0
    while True:
        interval_s = max(0.1, float(_cfg("tick_interval_seconds", 2.0)))
        time.sleep(interval_s)
        condition = _cfg("market_condition", "normal")
        params    = _CONDITION_PARAMS.get(condition, _CONDITION_PARAMS["normal"])
        lo, hi    = params["vol_range"]
        drift, vol_mult = params["drift"], params["vol_mult"]

        workload = int(_cfg("stream_workload", 100))
        current_symbols = SYMBOLS[:workload]

        rows_data = []
        for sym in current_symbols:
            with _state_lock:
                base = last_prices.get(sym, BASE_PRICES.get(sym, 100.0))
            noise = random.uniform(lo, hi)*vol_mult + drift
            price = max(0.01, base + noise)
            with _state_lock:
                last_prices[sym] = price
            ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            gen_ts = time.time()
            open_p = price - random.uniform(0.0, 0.5)
            high_p = price + random.uniform(0.1, 0.8)
            low_p  = price - random.uniform(0.1, 0.8)
            vol    = random.randint(1_000_000, 20_000_000)
            rows_data.append([ts_str, sym, round(open_p,4), round(high_p,4),
                               round(low_p,4), round(price,4), vol, gen_ts])

        _write_tick_rows(rows_data, tick_count)
        tick_count += 1
        if tick_count % 20 == 0:
            print(f"  Synthetic tick #{tick_count} | {len(current_symbols)} symbols "
                  f"| {interval_s:.2f}s | {condition} | "
                  f"{'SERIAL' if is_serial_mode else 'PARALLEL'}")


def _replay_loop():
    tick_count = 0
    while True:
        filepath = _cfg("replay_filepath", None)
        if not filepath or not os.path.isfile(filepath):
            print("  Replay mode: no valid filepath set; waiting 5s...")
            time.sleep(5); continue
        print(f"  Replay starting: {filepath}")
        try:
            with open(filepath, "r", newline='') as f:
                reader = csv.reader(f)
                next(reader, None)
                for csv_row in reader:
                    if len(csv_row) < 7: continue
                    interval_s = max(0.05, float(_cfg("tick_interval_seconds", 2.0)))
                    time.sleep(interval_s)
                    ts_str = csv_row[0].strip()
                    sym    = csv_row[1].strip().upper()
                    workload = int(_cfg("stream_workload", 100))
                    allowed_symbols = set(SYMBOLS[:workload])
                    if sym not in allowed_symbols: continue
                    try:
                        op,hp,lp,cl,vl = (float(csv_row[i]) for i in range(2,7))
                    except ValueError: continue
                    with _state_lock: last_prices[sym] = cl
                    _write_tick_rows([[ts_str,sym,round(op,4),round(hp,4),
                                       round(lp,4),round(cl,4),int(vl)]], tick_count)
                    tick_count += 1
                    if tick_count%50==0:
                        print(f"  Replay tick #{tick_count} | {sym} @ ${cl:.2f}")
            print("  Replay exhausted — looping")
        except Exception as e:
            print(f"  Replay error: {e}"); time.sleep(5)


def generate_market_data():
    while True:
        mode = _cfg("data_mode", "synthetic")
        if mode == "replay": _replay_loop()
        else: _synthetic_loop()
        time.sleep(1)


gen_thread = threading.Thread(target=generate_market_data, daemon=True)
gen_thread.start()
print(f"  Generator started | mode={_cfg('data_mode')} | symbols={len(SYMBOLS)}")

# ── Keep alive ─────────────────────────────────────────────────────────────────
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n  Shutting down...")
    query.stop()
    spark.stop()
