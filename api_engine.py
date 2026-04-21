# api_engine.py — Production HFT API v3
# =============================================================================
# CHANGES FROM v2:
#   Phase 1 Bug Fixes:
#     - benchmark workload cap raised to 10000 (was 1000)
#     - top_movers recomputed on every /update and broadcast in SYSTEM_STATE
#       so React sidebar "TOP MOVERS" renders correctly
#   Phase 2 - config.json:
#     - CONFIG loaded from config.json on boot; missing keys filled from defaults
#     - POST /config persists changes atomically (rename-swap) to config.json
#     - POST /upload saves CSV to data/user_uploads/ and auto-sets replay_filepath
#     - GET /uploads lists available CSV files
#     - auto_trade defaults to False (suggestion-only mode)
#   Preserved: WebSockets, SQLite pub/sub, O(1) history trim, reset-portfolio,
#              all prior config keys
# =============================================================================

import json
import sqlite3
import threading
import asyncio
import multiprocessing
import shutil
import glob
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
import time
from mpi_benchmark import run_serial as mpi_serial, run_parallel as mpi_parallel
from historical_benchmark import (
    run_bigdata_ml_benchmark, 
    warmup_benchmark_1,
    run_streaming_latency_benchmark
)

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from mpi_benchmark import run_serial, run_parallel

import os
try:
    import orjson
except ImportError:
    orjson = None


def _json_dumps(payload: dict) -> str:
    if orjson is not None:
        return orjson.dumps(payload, default=str).decode("utf-8")
    return json.dumps(payload, default=str)

os.makedirs("data", exist_ok=True)
os.makedirs("data/user_uploads", exist_ok=True)

app = FastAPI(title="HFT State Store API", version="3.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    def delayed_warmup():
        time.sleep(20)
        warmup_benchmark_1()

    threading.Thread(target=delayed_warmup, daemon=True).start()

# ── Config helpers ─────────────────────────────────────────────────────────────
CONFIG_FILE = "config.json"

def _auto_discover_symbols(limit=500):
    """Scans the parquet directory and returns all available symbols."""
    parquet_dir = "data/processed/historical_parquet"
    symbols = []
    if os.path.exists(parquet_dir):
        folders = glob.glob(f"{parquet_dir}/symbol=*")
        print(f"🔍 Found {len(folders)} parquet folders in {parquet_dir}")
        for folder in folders:
            sym = folder.split("symbol=")[-1].replace("\\", "").replace("/", "")
            symbols.append({"symbol": sym, "base_price": 100.0})
            if len(symbols) >= limit: break
        print(f"Discovered {len(symbols)} symbols")
    else:
        print(f"⚠️ Parquet directory not found: {parquet_dir}")
    
    if not symbols: # Fallback
        print(f"ℹ️ Using fallback symbols (should not happen if parquet exists)")
        symbols = [{"symbol": "AAPL", "base_price": 150.0}, {"symbol": "TSLA", "base_price": 200.0}]
    return symbols

_CONFIG_DEFAULTS: Dict[str, Any] = {
    "data_mode":             "synthetic",
    "stream_workload":       100,           
    "replay_filepath":       None,
    "tick_interval_seconds": 2.0,
    "active_symbols": _auto_discover_symbols(50), # Start with 50 default active
    "max_files_per_trigger": 5,
    "sma_window":            20,
    "rsi_window":            14,
    "starting_cash":         100_000.0,
    "min_ai_confidence":     0,
    "trade_size_shares":     1,
    "max_chart_points":      100,
    "benchmark_workload":    5000,
    "market_condition":      "normal",
    "auto_trade":            False,   # DEFAULT = suggestion-only
    "position_sizing":       "fixed",
    "risk_pct":              0.05,
    "use_rsi":               True,
    "use_macd":              True,
    "use_volume":            True,
    "use_sma":               True,
}

_config_file_lock = threading.Lock()

def _load_config() -> Dict[str, Any]:
    cfg = dict(_CONFIG_DEFAULTS)
    try:
        with open(CONFIG_FILE, "r") as f:
            on_disk = json.load(f)
        cfg.update(on_disk)
    except (FileNotFoundError, json.JSONDecodeError):
        _save_config(cfg)
    return cfg

def _save_config(cfg: Dict[str, Any]) -> None:
    with _config_file_lock:
        tmp = CONFIG_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(cfg, f, indent=2)
        os.replace(tmp, CONFIG_FILE)

CONFIG: Dict[str, Any] = _load_config()
print(f"Config loaded | data_mode={CONFIG['data_mode']} | auto_trade={CONFIG['auto_trade']}")

def _derive_symbols(cfg: Dict[str, Any]):
    syms   = [e["symbol"]               for e in cfg.get("active_symbols", [])]
    prices = {e["symbol"]: float(e["base_price"]) for e in cfg.get("active_symbols", [])}
    return syms, prices

TICKERS, BASE_PRICES = _derive_symbols(CONFIG)

# ── SQLite ─────────────────────────────────────────────────────────────────────
db_lock = threading.Lock()
db_path = "data/trades.db"

def init_db():
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol    TEXT    NOT NULL,
                action    TEXT    NOT NULL,
                price     REAL    NOT NULL,
                shares    INTEGER NOT NULL DEFAULT 1,
                timestamp TEXT    NOT NULL,
                pnl       REAL    DEFAULT 0.0
            )
        """)
        conn.commit()

init_db()
print("SQLite trade database initialized")

# ── WebSocket Manager ──────────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self.active.append(ws)

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            if ws in self.active:
                self.active.remove(ws)

    async def broadcast(self, payload: dict):
        dead = []
        msg  = _json_dumps(payload)
        async with self._lock:
            targets = list(self.active)
        for ws in targets:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            await self.disconnect(ws)

manager = ConnectionManager()

# ── MPI seed data ───────────────────────────────────────────────────────────────
try:
    with open("data/mpi_results.json", "r") as f:
        real_mpi_data = json.load(f)
except (FileNotFoundError, json.JSONDecodeError):
    real_mpi_data = {"mpi_speedup": 0.0, "serial_time": 0.0, "parallel_time": 0.0, "cores_used": 1}

# ── System State ───────────────────────────────────────────────────────────────
def _default_ticker_state(price: float = 150.0) -> dict:
    return {
        "market":     {"price": price, "history": []},
        "indicators": {"sma_20": price, "momentum": 0.0, "rsi": 50.0,
                       "macd": {"line": 0.0, "signal": 0.0, "histogram": 0.0}},
        "signals":    {"current": "HOLD", "lasso_target": price,
                       "xgb_direction": "FLAT", "confidence": 0,
                       "explanation": "Awaiting data…"},
        "metrics":    {"latency_ms": 0.0, "throughput": 0},
        "anomalies":  [],
    }

SYSTEM_STATE: Dict[str, Any] = {
    t: _default_ticker_state(BASE_PRICES.get(t, 100.0)) for t in TICKERS
}
SYSTEM_STATE["global_trading"] = {
    "portfolio_value":  CONFIG["starting_cash"],
    "cash":             CONFIG["starting_cash"],
    "positions":        {t: 0 for t in TICKERS},
    "trade_log":        [],
    "starting_capital": CONFIG["starting_cash"],
}
SYSTEM_STATE["benchmark"]  = real_mpi_data
SYSTEM_STATE["config"]     = dict(CONFIG)
SYSTEM_STATE["top_movers"] = []

# ── Top Movers ─────────────────────────────────────────────────────────────────
def _recompute_top_movers() -> List[dict]:
    """Safely scan all symbols in the state, ignoring global keys and handling missing data."""
    movers = []
    # Safely scan all symbols in the state, ignoring global keys
    for sym, st in SYSTEM_STATE.items():
        if sym in ["global_trading", "benchmark", "config", "top_movers"]:
            continue
        try:
            price    = float(st["market"].get("price", 0.0) or 0.0)
            momentum = float(st["indicators"].get("momentum", 0.0) or 0.0)
            if price > 0:
                movers.append({"symbol": sym, "price": price, "momentum": momentum})
        except Exception:
            continue
            
    movers.sort(key=lambda x: abs(x["momentum"]), reverse=True)
    return movers[:5]

# ── Pydantic Models ────────────────────────────────────────────────────────────
class StateUpdate(BaseModel):
    symbol:          str
    price:           float
    sma_20:          float
    momentum:        float
    signal:          str
    lasso_target:    float
    xgb_direction:   str            # kept for UI backward compat
    confidence:      int
    explanation:     str
    latency_ms:      float
    throughput:      int
    portfolio_value: float
    cash:            float
    positions:       dict
    anomaly:         str
    trade_executed:  str
    shares_traded:   int   = 1
    rsi:             float = 50.0
    macd_line:       float = 0.0
    macd_signal:     float = 0.0
    macd_histogram:  float = 0.0
    lasso_mae:       float = 0.0
    xgb_accuracy:    float = 0.0    # kept for UI backward compat
    gbt_accuracy:    float = 0.0    # NEW — real GBT accuracy
    lasso_return:    float = 0.0    # NEW — % return from Lasso
    lasso_return_raw: float = 0.0
    lasso_error:     Optional[float] = None
    gbt_probability: Optional[float] = None
    gbt_stale:       bool = False
    gbt_direction:   str  = "HOLD"  # NEW — same as xgb_direction, cleaner name

class BenchmarkRequest(BaseModel):
    type: str
    mode:     str
    workload: int = 5000
    optimized: bool = False

class SymbolEntry(BaseModel):
    symbol:     str
    base_price: float

class ConfigUpdate(BaseModel):
    data_mode:             Optional[str]               = None
    stream_workload:       Optional[int]               = None
    replay_filepath:       Optional[str]               = None
    tick_interval_seconds: Optional[float]             = None
    active_symbols:        Optional[List[SymbolEntry]] = None
    max_files_per_trigger: Optional[int]               = None
    sma_window:            Optional[int]               = None
    rsi_window:            Optional[int]               = None
    starting_cash:         Optional[float]             = None
    min_ai_confidence:     Optional[int]               = None
    trade_size_shares:     Optional[int]               = None
    max_chart_points:      Optional[int]               = None
    benchmark_workload:    Optional[int]               = None
    market_condition:      Optional[str]               = None
    auto_trade:            Optional[bool]              = None
    position_sizing:       Optional[str]               = None
    risk_pct:              Optional[float]             = None
    use_rsi:               Optional[bool]              = None
    use_macd:              Optional[bool]              = None
    use_volume:            Optional[bool]              = None
    use_sma:               Optional[bool]              = None

# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/state")
def get_state():
    return SYSTEM_STATE

@app.get("/config")
def get_config():
    return CONFIG

@app.get("/symbols")
def get_all_symbols():
    """Lets the frontend know every single stock available in the DB."""
    discovered = _auto_discover_symbols(500)
    result = {"symbols": [s["symbol"] for s in discovered]}
    print(f"📡 /symbols endpoint returning {len(result['symbols'])} symbols: {result['symbols'][:10]}...")
    return result

@app.post("/config")
async def update_config(data: ConfigUpdate):
    updated = data.model_dump(exclude_none=True)
    
    if "active_symbols" in updated:
        syms = [e.get("symbol") if isinstance(e, dict) else e for e in updated["active_symbols"]]
        print(f"📋 /config POST: Watchlist updated to {syms}")
        updated["active_symbols"] = [
            {"symbol": e["symbol"], "base_price": e["base_price"]}
            for e in updated["active_symbols"]
        ]

    CONFIG.update(updated)

    _save_config(CONFIG)
    SYSTEM_STATE["config"] = dict(CONFIG)

    await manager.broadcast({"type": "config_update", "data": CONFIG})
    return {"status": "ok", "config": CONFIG}

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not (file.filename or "").endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are accepted.")
    dest = Path("data/user_uploads") / (file.filename or "upload.csv")
    try:
        with open(dest, "wb") as out:
            shutil.copyfileobj(file.file, out)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {e}")
    finally:
        await file.close()

    CONFIG["replay_filepath"] = str(dest)
    _save_config(CONFIG)
    SYSTEM_STATE["config"] = dict(CONFIG)
    await manager.broadcast({"type": "config_update", "data": CONFIG})
    return {"status": "uploaded", "path": str(dest), "filename": file.filename}

@app.get("/uploads")
def list_uploads():
    upload_dir = Path("data/user_uploads")
    files = [f.name for f in upload_dir.glob("*.csv")]
    return {"files": files}

@app.post("/reset-portfolio")
async def reset_portfolio():
    capital = CONFIG["starting_cash"]
    SYSTEM_STATE["global_trading"].update({
        "portfolio_value":  capital,
        "cash":             capital,
        "positions":        {t: 0 for t in TICKERS},
        "trade_log":        [],
        "starting_capital": capital,
    })
    with db_lock:
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("DELETE FROM trades")
            conn.commit()
        finally:
            conn.close()
    await manager.broadcast({"type": "state_update", "data": SYSTEM_STATE})
    return {"status": "reset", "capital": capital}


def background_db_writer(trades_to_insert):
    """Writes to SQLite in the background so the UI doesn't have to wait for the hard drive."""
    if not trades_to_insert:
        return
    with db_lock:
        try:
            conn = sqlite3.connect(db_path, timeout=10.0)
            for trade in trades_to_insert:
                conn.execute(
                    "INSERT INTO trades (symbol, action, price, shares, timestamp) VALUES (?,?,?,?,?)",
                    trade
                )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"⚠️ BG DB Error: {e}")


@app.post("/update")
async def update_state(data_list: List[StateUpdate], bg_tasks: BackgroundTasks):
    """Ultra-Low Latency State Update."""
    
    trades_to_insert = []
    tick_deltas = {}
    
    for data in data_list:
        try:
            sym = data.symbol
            if sym not in SYSTEM_STATE:
                SYSTEM_STATE[sym] = _default_ticker_state(data.price)

            # In-memory dictionary updates (Lightning Fast)
            SYSTEM_STATE[sym]["market"]["price"] = data.price
            SYSTEM_STATE[sym]["indicators"].update({
                "sma_20": data.sma_20, "momentum": data.momentum, "rsi": data.rsi,
                "macd": {"line": data.macd_line, "signal": data.macd_signal, "histogram": data.macd_histogram}
            })
            SYSTEM_STATE[sym]["signals"].update({
                "current":       data.signal,
                "lasso_target":  data.lasso_target,
                "lasso_return":  data.lasso_return,    # NEW — % return
                "lasso_return_raw": data.lasso_return_raw,
                "xgb_direction": data.xgb_direction,   # kept for UI compat
                "gbt_direction": data.gbt_direction,   # NEW — cleaner name
                "gbt_probability": data.gbt_probability,
                "gbt_stale":     data.gbt_stale,
                "confidence":    data.confidence,
                "explanation":   data.explanation,
                "lasso_mae":     data.lasso_mae,
                "xgb_accuracy":  data.xgb_accuracy,    # kept for UI compat
                "gbt_accuracy":  data.gbt_accuracy,    # NEW
                "lasso_error":   data.lasso_error
            })
            SYSTEM_STATE[sym]["metrics"].update({
                "latency_ms": data.latency_ms, "throughput": data.throughput
            })

            # Append to fast rolling arrays
            history_point = {
                "price": data.price, "target": data.lasso_target, "sma": data.sma_20,
                "rsi": data.rsi, "macd": data.macd_line, "time": datetime.now().isoformat()
            }
            SYSTEM_STATE[sym]["market"]["history"].append(history_point)
            if len(SYSTEM_STATE[sym]["market"]["history"]) > 100:
                SYSTEM_STATE[sym]["market"]["history"].pop(0)

            SYSTEM_STATE["global_trading"]["portfolio_value"] = data.portfolio_value
            SYSTEM_STATE["global_trading"]["cash"] = data.cash
            SYSTEM_STATE["global_trading"]["positions"] = data.positions

            # Queue trade for background writing (DO NOT WRITE TO DISK YET)
            if data.trade_executed != "None":
                trades_to_insert.append((sym, data.trade_executed, data.price, data.shares_traded, datetime.now().isoformat()))
                log = SYSTEM_STATE["global_trading"]["trade_log"]
                log.insert(0, f"[{sym}] {data.trade_executed}")
                if len(log) > 10: log.pop()

            tick_deltas[sym] = {
                "market": {
                    "price": SYSTEM_STATE[sym]["market"]["price"],
                    "history_point": history_point,
                },
                "indicators": SYSTEM_STATE[sym]["indicators"],
                "signals": SYSTEM_STATE[sym]["signals"],
                "metrics": SYSTEM_STATE[sym]["metrics"],
                "anomalies": SYSTEM_STATE[sym].get("anomalies", []),
            }
                
        except Exception as e:
            print(f"⚠️ [/update] Error updating {sym}: {type(e).__name__}: {e}")
            continue

    SYSTEM_STATE["top_movers"] = _recompute_top_movers()

    # Aggregate replay accuracy across all symbols (if in replay mode)
    if CONFIG.get("data_mode") == "replay":
        all_mae = [
            SYSTEM_STATE[s]["signals"].get("lasso_mae", 0)
            for s in SYSTEM_STATE
            if s not in ["global_trading", "benchmark", "config", "top_movers", "replay_accuracy"]
            and SYSTEM_STATE[s]["signals"].get("lasso_mae", 0) > 0
        ]
        all_xgb = [
            SYSTEM_STATE[s]["signals"].get("xgb_accuracy", 0)
            for s in SYSTEM_STATE
            if s not in ["global_trading", "benchmark", "config", "top_movers", "replay_accuracy"]
            and SYSTEM_STATE[s]["signals"].get("xgb_accuracy", 0) > 0
        ]
        SYSTEM_STATE["replay_accuracy"] = {
            "avg_lasso_mae":    round(float(np.mean(all_mae)), 4)  if all_mae else 0.0,
            "avg_xgb_accuracy": round(float(np.mean(all_xgb)), 1) if all_xgb else 0.0,
            "symbols_scored":   len(all_mae),
        }

    # 1. BROADCAST COMPACT DELTAS TO REACT UI IMMEDIATELY
    payload = {
        "symbols": tick_deltas,
        "global_trading": SYSTEM_STATE["global_trading"],
        "top_movers": SYSTEM_STATE["top_movers"],
        "config": SYSTEM_STATE["config"],
    }
    if "replay_accuracy" in SYSTEM_STATE:
        payload["replay_accuracy"] = SYSTEM_STATE["replay_accuracy"]
    await manager.broadcast({"type": "tick_update", "data": payload})

    # 2. DISPATCH HARD DRIVE WRITE TO BACKGROUND THREAD
    if trades_to_insert:
        bg_tasks.add_task(background_db_writer, trades_to_insert)
        
    return {"status": "fast"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        await websocket.send_text(
            _json_dumps({"type": "state_update", "data": SYSTEM_STATE})
        )
    except Exception:
        pass
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                payload = json.loads(raw)
                if payload.get("action") == "ping":
                    await websocket.send_text(_json_dumps({"type": "pong"}))
            except json.JSONDecodeError:
                pass
    except (WebSocketDisconnect, Exception):
        await manager.disconnect(websocket)

@app.get("/trades")
def get_trades(symbol: str = None, limit: int = 100):
    limit = max(1, min(limit, 1000))
    with db_lock:
        conn = sqlite3.connect(db_path)
        try:
            if symbol:
                rows = conn.execute(
                    "SELECT * FROM trades WHERE symbol=? ORDER BY id DESC LIMIT ?",
                    (symbol, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
                ).fetchall()
        finally:
            conn.close()
    return [
        {"id": r[0], "symbol": r[1], "action": r[2],
         "price": r[3], "shares": r[4], "timestamp": r[5], "pnl": r[6]}
        for r in rows
    ]

@app.post("/benchmark")
async def execute_benchmark(req: BenchmarkRequest):
    """Executes specific benchmarks on-demand for better presentation pacing."""
    if "benchmark_suite" not in SYSTEM_STATE:
        SYSTEM_STATE["benchmark_suite"] = {}

    try:
        if req.type == "ml":
            res = await asyncio.to_thread(run_bigdata_ml_benchmark, 5_000_000)
            SYSTEM_STATE["benchmark_suite"]["big_data_ml"] = res
            
        elif req.type == "streaming":
            # Pass 50,000 ticks and the toggle state
            res = await asyncio.to_thread(run_streaming_latency_benchmark, 50_000, req.optimized)
            SYSTEM_STATE["benchmark_suite"]["streaming_latency"] = res
            
        elif req.type == "compute":
            # Execute heavy CPU stress testing
            workload = max(1, min(req.workload, 10_000))
            iterations = 100_000
            
            s_time = await asyncio.to_thread(mpi_serial, workload, iterations)
            p_time = await asyncio.to_thread(mpi_parallel, workload, iterations)
            
            n_cores = multiprocessing.cpu_count()
            speedup = s_time / p_time if p_time > 0 else 1.0
            
            # [FIX] Update the payload to reflect a PySpark benchmark for the UI
            SYSTEM_STATE["benchmark_suite"]["monte_carlo"] = {
                "data_points": workload * iterations,
                "cores_used": n_cores,
                "serial_time": round(s_time, 3),
                "parallel_time": round(p_time, 3),
                "speedup": round(speedup, 2),
                "task": "DataFrame GroupBy & Aggregation",
                "model": "PySpark Engine (local[1] vs local[*])"
            }
    except Exception as e:
        print(f"⚠️ Benchmark Error ({req.type}): {e}")

    await manager.broadcast({"type": "state_update", "data": SYSTEM_STATE})
    return {"status": "ok"}
