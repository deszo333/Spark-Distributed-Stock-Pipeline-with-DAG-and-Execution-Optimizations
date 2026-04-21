# =============================================================================
# lgbm_native_forecaster.py — Native LightGBM benchmark (1, 2, 4, 8 Cores)
# =============================================================================
import os
import sys
import csv
import json
import time
import subprocess
import multiprocessing
from pathlib import Path

def _pip(package: str):
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", package, "-q"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

print("\n  Checking dependencies...")
REQUIRED = {
    "numpy": "numpy",
    "pandas": "pandas",
    "pyarrow": "pyarrow",
    "psutil": "psutil",
    "lightgbm": "lightgbm",
    "sklearn": "scikit-learn",
    "matplotlib": "matplotlib",
}
for module, package in REQUIRED.items():
    try:
        __import__(module)
    except Exception:
        print(f"  Installing {package}...")
        _pip(package)
        print(f"  {package} installed.")
print("  All dependencies ready.")

# =============================================================================
# ENVIRONMENT
# =============================================================================
import numpy as np
import pandas as pd
import psutil
from sklearn.metrics import accuracy_score, roc_auc_score, log_loss
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

TOTAL_GB = psutil.virtual_memory().total / (1024 ** 3)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
PARQUET_PATH = os.path.join(DATA_DIR, "processed", "historical_parquet")
MODEL_PATH = os.path.join(DATA_DIR, "models", "lightgbm_native_model.txt")
CHARTS_DIR = os.path.join(DATA_DIR, "charts")
CSV_OUTPUT_PATH = os.path.join(DATA_DIR, "lgbm_core_scaling_results.csv")

for d in [CHARTS_DIR, os.path.join(DATA_DIR, "models")]:
    os.makedirs(d, exist_ok=True)

FEATURE_COLS = [
    "Open", "High", "Low", "Close", "Volume",
    "MA_10", "MA_30", "Volatility", "Vol_MA_10",
    "Momentum_1", "Momentum_5",
    "Close_norm", "Volume_norm", "MA_ratio_10_30",
]
LABEL_COL = "Target_Direction"

MIN_VALID_PRICE = 1.0
MAX_ABS_TARGET_RETURN = 0.25

def load_parquet() -> pd.DataFrame:
    cols = ["symbol", "timestamp", "Open", "High", "Low", "Close", "Volume"]
    try:
        df = pd.read_parquet(PARQUET_PATH, columns=cols, engine="pyarrow")
        return df
    except Exception:
        import pyarrow.dataset as ds
        dataset = ds.dataset(PARQUET_PATH, format="parquet", partitioning="hive")
        table = dataset.to_table(columns=cols)
        return table.to_pandas()

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["symbol", "timestamp", "Open", "High", "Low", "Close", "Volume"])
    df = df.sort_values(["symbol", "timestamp"], kind="mergesort").reset_index(drop=True)

    g = df.groupby("symbol", sort=False, group_keys=False)

    df["MA_10"] = g["Close"].transform(lambda s: s.rolling(11, min_periods=1).mean())
    df["MA_30"] = g["Close"].transform(lambda s: s.rolling(31, min_periods=1).mean())
    df["Volatility"] = g["Close"].transform(lambda s: s.rolling(11, min_periods=1).std())
    df["Vol_MA_10"] = g["Volume"].transform(lambda s: s.rolling(11, min_periods=1).mean())
    df["Momentum_1"] = g["Close"].transform(lambda s: s.pct_change(1).fillna(0.0))
    df["Momentum_5"] = g["Close"].transform(lambda s: s.pct_change(5).fillna(0.0))

    close_mean_30 = g["Close"].transform(lambda s: s.rolling(31, min_periods=1).mean())
    close_std_30 = g["Close"].transform(lambda s: s.rolling(31, min_periods=1).std())
    vol_mean_10 = g["Volume"].transform(lambda s: s.rolling(11, min_periods=1).mean())
    vol_std_10 = g["Volume"].transform(lambda s: s.rolling(11, min_periods=1).std())

    df["Close_norm"] = np.where(
        close_std_30 > 1e-8,
        (df["Close"] - close_mean_30) / close_std_30,
        0.0,
    )
    df["Volume_norm"] = np.where(
        vol_std_10 > 1e-8,
        (df["Volume"].astype("float64") - vol_mean_10) / vol_std_10,
        0.0,
    )
    df["MA_ratio_10_30"] = np.where(df["MA_30"] != 0, df["MA_10"] / df["MA_30"], 1.0)

    df["Next_Close"] = g["Close"].shift(-1)
    df = df[(df["Close"] > MIN_VALID_PRICE) & (df["Next_Close"] > MIN_VALID_PRICE)].copy()
    df["Target_Return"] = (df["Next_Close"] - df["Close"]) / df["Close"]
    df = df[df["Target_Return"].abs() <= MAX_ABS_TARGET_RETURN].copy()
    df["Target_Direction"] = (df["Next_Close"] > df["Close"]).astype(np.int32)

    group_size = df.groupby("symbol", sort=False)["symbol"].transform("size")
    denom = (group_size - 1).replace(0, 1)
    df["Split_Rank"] = df.groupby("symbol", sort=False).cumcount() / denom

    df = df.replace([np.inf, -np.inf], np.nan).dropna().reset_index(drop=True)
    return df

def train_lgbm(train_df: pd.DataFrame, test_df: pd.DataFrame, cores: int) -> tuple:
    import lightgbm as lgb
    
    X_train = train_df[FEATURE_COLS].to_numpy(dtype=np.float32, copy=False)
    y_train = train_df[LABEL_COL].to_numpy(dtype=np.int32, copy=False)
    X_test = test_df[FEATURE_COLS].to_numpy(dtype=np.float32, copy=False)
    y_test = test_df[LABEL_COL].to_numpy(dtype=np.int32, copy=False)

    model = lgb.LGBMClassifier(
        objective="binary",
        n_estimators=200,
        learning_rate=0.05,
        num_leaves=31,
        max_depth=-1,
        min_child_samples=20,
        subsample=0.8,
        subsample_freq=1,
        colsample_bytree=0.8,
        reg_lambda=1.0,
        n_jobs=cores,         # Sets the core count
        random_state=42,
        verbose=-1,
    )

    # Strictly time the algorithm math to prove scalability
    t_start = time.perf_counter()
    model.fit(X_train, y_train)
    t_end = time.perf_counter()
    duration = t_end - t_start
    
    # Calculate all metrics
    pred = model.predict(X_test)
    proba = model.predict_proba(X_test)[:, 1]

    acc = accuracy_score(y_test, pred)
    auc = roc_auc_score(y_test, proba) if len(np.unique(y_test)) > 1 else float("nan")
    ll = log_loss(y_test, proba, labels=[0, 1]) if len(np.unique(y_test)) > 1 else float("nan")

    return duration, acc, auc, ll

def generate_scaling_charts(results):
    print("  Generating Speedup and Efficiency charts...")
    cores = [r["cores"] for r in results]
    speedups = [r["speedup"] for r in results]
    efficiencies = [r["efficiency"] for r in results]

    # Chart 1: Speedup
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(cores, speedups, 'o-', color='#2563eb', linewidth=2.5, markersize=8, label="Measured Speedup")
    ax.plot(cores, cores, 'k--', alpha=0.5, label="Ideal (Linear) Speedup")
    ax.set_xlabel("Number of CPU Cores", fontsize=11)
    ax.set_ylabel("Speedup Multiplier (T1 / Tp)", fontsize=11)
    ax.set_title("Native LightGBM Parallel Speedup", fontsize=14, fontweight='bold')
    ax.set_xticks(cores)
    ax.legend()
    ax.grid(True, alpha=0.3)
    for x, y in zip(cores, speedups):
        ax.annotate(f"{y:.2f}x", (x, y), textcoords="offset points", xytext=(0, 10), ha='center', fontsize=10, fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(CHARTS_DIR, "lgbm_speedup_curve.png"), dpi=150)
    plt.close()

    # Chart 2: Efficiency
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(cores, efficiencies, 's-', color='#10b981', linewidth=2.5, markersize=8, label="Efficiency (Speedup / Cores)")
    ax.axhline(y=1.0, color='gray', linestyle=':', label="100% Efficiency")
    ax.set_xlabel("Number of CPU Cores", fontsize=11)
    ax.set_ylabel("Parallel Efficiency", fontsize=11)
    ax.set_title("Native LightGBM Parallel Efficiency", fontsize=14, fontweight='bold')
    ax.set_xticks(cores)
    ax.legend()
    ax.grid(True, alpha=0.3)
    for x, y in zip(cores, efficiencies):
        ax.annotate(f"{y:.2f}", (x, y), textcoords="offset points", xytext=(0, 10), ha='center', fontsize=10, fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(CHARTS_DIR, "lgbm_efficiency_curve.png"), dpi=150)
    plt.close()

def run_scaling_benchmark():
    print("\n" + "=" * 70)
    print("  NATIVE LIGHTGBM CORE SCALING BENCHMARK (1, 2, 4, 8 Cores)")
    print("=" * 70)

    print("  Loading parquet...")
    df = load_parquet()
    print("  Engineering features in pandas...")
    df_feat = build_features(df)

    train_df = df_feat[df_feat["Split_Rank"] < 0.8].copy().reset_index(drop=True)
    test_df = df_feat[df_feat["Split_Rank"] >= 0.8].copy().reset_index(drop=True)

    print(f"  Train rows: {len(train_df):,} | Test rows: {len(test_df):,}\n")

    core_counts = [1, 2, 4, 8]
    results = []
    serial_time = None

    for c in core_counts:
        print(f"  [BENCHMARK] Running Training on {c} Core(s)...")
        dur, acc, auc, ll = train_lgbm(train_df, test_df, cores=c)
        
        if c == 1:
            serial_time = dur
            
        speedup = serial_time / dur
        efficiency = speedup / c
        
        print(f"  -> Time: {dur:.2f}s | Speedup: {speedup:.2f}x | Acc: {acc*100:.2f}%\n")
        
        results.append({
            "cores": c,
            "time_s": round(dur, 3),
            "speedup": round(speedup, 3),
            "efficiency": round(efficiency, 3),
            "accuracy": round(acc * 100, 2),
            "auc": round(auc, 4),
            "logloss": round(ll, 4)
        })

    # Save to CSV
    with open(CSV_OUTPUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    # Generate Charts
    generate_scaling_charts(results)

    print("=" * 70)
    print(f"  SCALING BENCHMARK COMPLETE")
    print(f"  Data saved to   : {CSV_OUTPUT_PATH}")
    print(f"  Charts saved to : {CHARTS_DIR}")
    print("=" * 70)

if __name__ == "__main__":
    run_scaling_benchmark()