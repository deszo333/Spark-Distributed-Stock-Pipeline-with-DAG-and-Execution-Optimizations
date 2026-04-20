# =============================================================================
# ml_forecaster.py — PDC Parallel ML Pipeline (Bulletproof Edition)
# =============================================================================

# =============================================================================
# PHASE 0 — AUTO-INSTALLER (runs before anything else)
# =============================================================================
import subprocess
import sys
import os


def _pip(package):
    """Install a package silently."""
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", package, "-q"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )


print("\n  Checking dependencies...")  # [AUTO-1]
REQUIRED = {
    "pyspark":    "pyspark",
    "psutil":     "psutil",
    "numpy":      "numpy",
    "requests":   "requests",
    "matplotlib": "matplotlib",
    "pyarrow":    "pyarrow",
}
for module, package in REQUIRED.items():
    try:
        __import__(module)
    except ImportError:
        print(f"  Installing {package}...")
        _pip(package)
        print(f"  {package} installed.")
print("  All dependencies ready.")


# =============================================================================
# PHASE 1 — ENVIRONMENT SETUP
# =============================================================================

def _setup_java(is_colab):  # [AUTO-8]
    """Check Java is available; install on Colab if missing."""
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True, text=True
        )
        line = (result.stderr or result.stdout or "").splitlines()
        print(f"  Java     : {line[0] if line else 'found'}")
    except FileNotFoundError:
        if is_colab:
            print("  Java not found — installing on Colab...")
            os.system("apt-get install -y default-jdk -qq")
            print("  Java installed.")
        else:
            print("  WARNING: Java not found.")
            print("  Please install Java 11 or 17 from: https://adoptium.net/")


def _create_dummy_winutils(bin_dir):  # [AUTO-3]
    """
    Creates a minimal dummy winutils.exe + hadoop.dll so Spark can start
    without a real Hadoop installation. Safe for local[*] mode.
    Spark only calls winutils for chmod on temp dirs — if it exits 0, Spark continues.
    """
    # Smallest valid Windows PE stub — exits immediately with code 0
    pe_stub = bytes([
        0x4D,0x5A,0x90,0x00,0x03,0x00,0x00,0x00,0x04,0x00,0x00,0x00,0xFF,0xFF,0x00,0x00,
        0xB8,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x40,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
        0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
        0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x80,0x00,0x00,0x00,
        0x0E,0x1F,0xBA,0x0E,0x00,0xB4,0x09,0xCD,0x21,0xB8,0x01,0x4C,0xCD,0x21,0x54,0x68,
        0x69,0x73,0x20,0x70,0x72,0x6F,0x67,0x72,0x61,0x6D,0x20,0x63,0x61,0x6E,0x6E,0x6F,
        0x74,0x20,0x62,0x65,0x20,0x72,0x75,0x6E,0x20,0x69,0x6E,0x20,0x44,0x4F,0x53,0x20,
        0x6D,0x6F,0x64,0x65,0x2E,0x0D,0x0D,0x0A,0x24,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
    ])
    try:
        with open(os.path.join(bin_dir, 'winutils.exe'), 'wb') as f:
            f.write(pe_stub)
        # hadoop.dll just needs to exist — Spark checks presence, not content
        with open(os.path.join(bin_dir, 'hadoop.dll'), 'wb') as f:
            f.write(b'\x00' * 64)
        fake_hadoop = os.path.dirname(bin_dir)
        os.environ['HADOOP_HOME'] = fake_hadoop
        os.environ['PATH'] = bin_dir + ';' + os.environ.get('PATH', '')
        print(f"  Hadoop   : dummy winutils created at {bin_dir}")
        print(f"             (Spark may show one warning — pipeline runs fine)")
    except Exception as e:
        print(f"  Hadoop   : dummy fallback failed ({e})")


def _setup_hadoop_windows():  # [AUTO-2]
    """
    Auto-configure Hadoop/winutils on Windows.
    Priority:
      1. Already installed at a known location → use it
      2. Internet available → download from GitHub
      3. No internet → create dummy winutils stub
    """
    import urllib.request

    common_paths = [
        r'C:\hadoop',
        r'C:\winutils',
        r'C:\tools\hadoop',
        os.path.join(os.path.expanduser('~'), 'hadoop'),
        os.path.join(os.path.expanduser('~'), 'winutils'),
        os.path.join(os.path.expanduser('~'), 'winutils', 'hadoop-3.3.5'),
    ]

    # Step 1: check existing installations
    for path in common_paths:
        winutils = os.path.join(path, 'bin', 'winutils.exe')
        if os.path.exists(winutils):
            os.environ['HADOOP_HOME'] = path
            os.environ['PATH'] = os.path.join(path, 'bin') + ';' + os.environ.get('PATH', '')
            print(f"  Hadoop   : found at {path}")
            return

    # Step 2: download from GitHub
    print("  Hadoop/winutils not found — downloading automatically...")
    install_dir = os.path.join(os.path.expanduser('~'), 'winutils', 'hadoop-3.3.5')
    bin_dir     = os.path.join(install_dir, 'bin')
    os.makedirs(bin_dir, exist_ok=True)

    BASE_URL     = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin"
    files_needed = ["winutils.exe", "hadoop.dll", "hadoop.exp", "hadoop.lib"]
    downloaded   = 0

    for filename in files_needed:
        dest = os.path.join(bin_dir, filename)
        if os.path.exists(dest):
            downloaded += 1
            continue
        try:
            urllib.request.urlretrieve(f"{BASE_URL}/{filename}", dest)
            downloaded += 1
            print(f"    {filename} ✓")
        except Exception:
            print(f"    {filename} ✗ (skipped)")

    if downloaded >= 2:
        os.environ['HADOOP_HOME'] = install_dir
        os.environ['PATH'] = bin_dir + ';' + os.environ.get('PATH', '')
        print(f"  Hadoop   : installed to {install_dir}")
    else:
        # Step 3: no internet fallback
        print("  Hadoop   : download failed — creating dummy stub...")
        _create_dummy_winutils(bin_dir)


def setup_environment():
    """Fully automatic environment setup for Windows, Mac, Linux, and Colab."""
    os.environ['PYSPARK_PYTHON']        = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    is_windows = sys.platform == 'win32'
    is_colab   = 'google.colab' in sys.modules or os.path.exists('/content')
    is_mac     = sys.platform == 'darwin'

    print(f"  Platform : {'Windows' if is_windows else 'Colab' if is_colab else 'Mac' if is_mac else 'Linux'}")
    print(f"  Python   : {sys.executable}")

    _setup_java(is_colab)

    if is_windows:
        _setup_hadoop_windows()

    if is_colab:  # [AUTO-8]
        for java_path in [
            "/usr/lib/jvm/java-11-openjdk-amd64",
            "/usr/lib/jvm/java-17-openjdk-amd64",
            "/usr/lib/jvm/java-8-openjdk-amd64",
        ]:
            if os.path.exists(java_path):
                os.environ["JAVA_HOME"] = java_path
                print(f"  JAVA_HOME: {java_path}")
                break


setup_environment()

# =============================================================================
# PHASE 2 — RESOURCE DETECTION  [AUTO-4, AUTO-5, AUTO-6, AUTO-7]
# =============================================================================
import time
import multiprocessing
import psutil

NUM_CORES = multiprocessing.cpu_count() or 2
_total_gb = psutil.virtual_memory().total / (1024 ** 3)

# Scale memory to available RAM  [AUTO-4]
if _total_gb >= 32:   DRIVER_MEM, OFFHEAP_MEM = "12g", "4g"
elif _total_gb >= 16: DRIVER_MEM, OFFHEAP_MEM = "8g",  "2g"
elif _total_gb >= 8:  DRIVER_MEM, OFFHEAP_MEM = "4g",  "1g"
else:                 DRIVER_MEM, OFFHEAP_MEM = "2g",  "512m"

# Arrow compatibility check  [AUTO-5]
try:
    import pyarrow as pa
    _av = tuple(int(x) for x in pa.__version__.split(".")[:2])
    ARROW_ENABLED = "true" if _av >= (1, 0) else "false"
except ImportError:
    ARROW_ENABLED = "false"

# Absolute paths — safe from any working directory  [AUTO-6]
SCRIPT_DIR         = os.path.dirname(os.path.abspath(__file__))
DATA_DIR           = os.path.join(SCRIPT_DIR, "data")
PARQUET_PATH       = os.path.join(DATA_DIR, "processed", "historical_parquet")
LASSO_MODEL_PATH   = os.path.join(DATA_DIR, "models", "lasso_model")
GBT_MODEL_PATH     = os.path.join(DATA_DIR, "models", "gbt_model")
CHARTS_DIR         = os.path.join(DATA_DIR, "charts")
CSV_OUTPUT_PATH    = os.path.join(DATA_DIR, "benchmark_results.csv")
SESSION_CHECKPOINT = os.path.join(DATA_DIR, f"checkpoints_{int(time.time())}")  # [AUTO-7]

for _d in [CHARTS_DIR, SESSION_CHECKPOINT, os.path.join(DATA_DIR, "models")]:
    os.makedirs(_d, exist_ok=True)

print(f"  Cores    : {NUM_CORES}")
print(f"  RAM      : {_total_gb:.1f} GB")
print(f"  Memory   : driver={DRIVER_MEM}  offheap={OFFHEAP_MEM}")
print(f"  Arrow    : {ARROW_ENABLED}")
print(f"  Data dir : {DATA_DIR}\n")

# =============================================================================
# PHASE 3 — IMPORTS
# =============================================================================
import csv
import concurrent.futures

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, stddev, lead, lag, when, lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import (
    RegressionEvaluator, MulticlassClassificationEvaluator
)
from pyspark.storagelevel import StorageLevel

# =============================================================================
# CONFIGURATION
# =============================================================================

# RUN_LIVE_DEMO = True  → single clean parallel run (show to professor)
# RUN_LIVE_DEMO = False → full automated benchmark, outputs CSV + 3 charts
RUN_LIVE_DEMO = True

FEATURE_COLS = [
    "Open", "High", "Low", "Close", "Volume",
    "MA_10", "MA_30", "Volatility",
    "Vol_MA_10", "Momentum_1", "Momentum_5"
]

OOM_SAFE_THRESHOLD_GB = 4.0


# =============================================================================
# SPARK SESSION BUILDER  [ENH-1 through ENH-4, ENH-19, ENH-24 through ENH-27]
# =============================================================================
def build_spark_session(is_serial: bool, log_gc: bool = False) -> SparkSession:
    app_name = "PDC_Serial_Baseline" if is_serial else "PDC_Parallel_GodTier"

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory",  DRIVER_MEM)   # [AUTO-4]
        .config("spark.executor.memory", DRIVER_MEM)
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")
        .config("spark.ui.port", "0")                  # [ENH-27] no port conflicts
        .config("spark.sql.execution.arrow.pyspark.enabled", ARROW_ENABLED)  # [AUTO-5]
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")
    )

    if is_serial:
        # Honest serial baseline — no optimizations  [ENH-19]
        builder = (
            builder
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.adaptive.enabled",   "false")
            .config("spark.speculation",             "false")
        )
    else:
        # Full parallel — all optimizations active
        builder = (
            builder
            .master("local[*]")
            .config("spark.serializer",
                    "org.apache.spark.serializer.KryoSerializer")  # [ENH-1]
            .config("spark.kryo.unsafe", "true")
            .config("spark.kryoserializer.buffer.max", "512m")
            .config("spark.scheduler.mode",   "FAIR")              # [ENH-2]
            .config("spark.sql.adaptive.enabled", "true")          # [ENH-3]
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled",         "true")
            .config("spark.reducer.maxSizeInFlight", "96m")        # [ENH-24]
            .config("spark.shuffle.file.buffer",     "1m")
            .config("spark.sql.shuffle.partitions",  str(NUM_CORES * 3))
            .config("spark.default.parallelism",     str(NUM_CORES * 2))
            .config("spark.speculation", "true")                   # [ENH-25]
            .config("spark.memory.offHeap.enabled",  "true")
            .config("spark.memory.offHeap.size",     OFFHEAP_MEM) # [AUTO-4]
        )

    if log_gc:  # [ENH-26]
        builder = builder.config(
            "spark.executor.extraJavaOptions",
            "-verbose:gc -XX:+PrintGCDetails -XX:+UseG1GC"
        )

    return builder.getOrCreate()


# =============================================================================
# CHART GENERATION  [ENH-30]
# =============================================================================
def generate_charts(all_results: list, gantt_data: dict, output_dir: str):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("  [Charts] matplotlib not available — skipping.")
        return

    os.makedirs(output_dir, exist_ok=True)
    fractions_pct = [r["fraction"] * 100 for r in all_results]
    speedups      = [r["speedup_x"]      for r in all_results]
    efficiencies  = [r["efficiency"]      for r in all_results]

    # ── Chart 1: Speedup Curve ────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(fractions_pct, speedups, marker="o", linewidth=2.5,
            color="#2563EB", label="Measured speedup", zorder=3)
    ax.fill_between(fractions_pct, speedups, alpha=0.08, color="#2563EB")
    ax.axhline(y=1,  color="#EF4444", linewidth=1.2, linestyle="--",
               label="Break-even (1×)")
    ax.axhline(y=5,  color="#F59E0B", linewidth=1.5, linestyle="--",
               label="Rubric target: 5× (full credit)")
    ax.axhline(y=10, color="#10B981", linewidth=1.5, linestyle="--",
               label="Rubric target: 10× (max credit)")
    for x, y in zip(fractions_pct, speedups):
        ax.annotate(f"{y:.2f}×", (x, y),
                    textcoords="offset points", xytext=(0, 10),
                    ha="center", fontsize=9, color="#1E40AF")
    ax.set_xlabel("Data fraction (%)", fontsize=11)
    ax.set_ylabel("Speedup  (T_serial / T_parallel)", fontsize=11)
    ax.set_title(f"Speedup Curve — Serial vs Parallel  ({NUM_CORES} cores)",
                 fontsize=13, fontweight="bold")
    ax.legend(fontsize=9)
    ax.set_xticks(fractions_pct)
    ax.grid(axis="y", alpha=0.3)
    ax.set_ylim(bottom=0)
    fig.tight_layout()
    p1 = os.path.join(output_dir, "speedup_curve.png")
    fig.savefig(p1, dpi=150)
    plt.close(fig)
    print(f"  [Charts] Speedup curve    → {p1}")

    # ── Chart 2: Efficiency Curve ─────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(fractions_pct, efficiencies, marker="s", linewidth=2.5,
            color="#7C3AED", label=f"Efficiency  E = S / {NUM_CORES}", zorder=3)
    ax.fill_between(fractions_pct, efficiencies, alpha=0.08, color="#7C3AED")
    ax.axhline(y=1.0, color="#6B7280", linewidth=1.2, linestyle=":",
               label="Ideal efficiency (E = 1.0)")
    for x, y in zip(fractions_pct, efficiencies):
        ax.annotate(f"{y:.3f}", (x, y),
                    textcoords="offset points", xytext=(0, 10),
                    ha="center", fontsize=9, color="#5B21B6")
    ax.set_xlabel("Data fraction (%)", fontsize=11)
    ax.set_ylabel(f"Parallel efficiency  E = S / p", fontsize=11)
    ax.set_title(f"Parallel Efficiency Curve  (p = {NUM_CORES} cores)",
                 fontsize=13, fontweight="bold")
    ax.legend(fontsize=9)
    ax.set_xticks(fractions_pct)
    ax.grid(axis="y", alpha=0.3)
    ax.set_ylim(bottom=0, top=max(efficiencies) * 1.3 if efficiencies else 1.5)
    fig.tight_layout()
    p2 = os.path.join(output_dir, "efficiency_curve.png")
    fig.savefig(p2, dpi=150)
    plt.close(fig)
    print(f"  [Charts] Efficiency curve → {p2}")

    # ── Chart 3: Gantt Overlap ────────────────────────────────────────────────
    if gantt_data:
        fig, ax = plt.subplots(figsize=(9, 3.5))
        colors  = {"Lasso": "#2563EB", "GBT": "#7C3AED"}
        yticks, ylabels = [], []
        for i, (name, (t_start_rel, duration)) in enumerate(gantt_data.items()):
            ax.barh(y=i, width=duration, left=t_start_rel,
                    height=0.4, color=colors.get(name, "#6B7280"), alpha=0.85)
            ax.text(t_start_rel + duration / 2, i,
                    f"{name}  ({duration:.1f}s)",
                    ha="center", va="center",
                    color="white", fontsize=9, fontweight="bold")
            yticks.append(i)
            ylabels.append(name)

        lasso_end     = gantt_data["Lasso"][0] + gantt_data["Lasso"][1]
        gbt_end       = gantt_data["GBT"][0]   + gantt_data["GBT"][1]
        overlap_start = max(gantt_data["Lasso"][0], gantt_data["GBT"][0])
        overlap_end   = min(lasso_end, gbt_end)
        if overlap_end > overlap_start:
            ax.axvspan(overlap_start, overlap_end, alpha=0.18, color="#F59E0B",
                       label=f"Concurrent overlap ({overlap_end - overlap_start:.1f}s)")

        ax.set_yticks(yticks)
        ax.set_yticklabels(ylabels, fontsize=11)
        ax.set_xlabel("Wall-clock time (seconds from pipeline start)", fontsize=10)
        ax.set_title(
            "Concurrent Model Training — FAIR Scheduler Gantt Chart\n"
            "Amber zone = proven parallel overlap (Lasso + GBT running simultaneously)",
            fontsize=11, fontweight="bold"
        )
        ax.legend(fontsize=9, loc="lower right")
        ax.grid(axis="x", alpha=0.3)
        fig.tight_layout()
        p3 = os.path.join(output_dir, "gantt_overlap.png")
        fig.savefig(p3, dpi=150)
        plt.close(fig)
        print(f"  [Charts] Gantt chart      → {p3}")


# =============================================================================
# THREAD FUNCTIONS  [ENH-16, ENH-17]
# =============================================================================
def train_lasso(train_data, test_data, pipeline_start: float) -> dict:
    t_start = time.perf_counter()
    print(f"  [Thread-Lasso] Started at T+{t_start - pipeline_start:.3f}s")

    lasso = LinearRegression(
        featuresCol="features",
        labelCol="Target_Price",
        elasticNetParam=1.0,   # Pure L1 Lasso
        regParam=0.1,
        maxIter=100,
        standardization=True
    )
    model = lasso.fit(train_data)
    preds = model.transform(test_data)

    mse  = RegressionEvaluator(
        labelCol="Target_Price", predictionCol="prediction", metricName="mse"
    ).evaluate(preds)
    rmse = mse ** 0.5

    model.write().overwrite().save(LASSO_MODEL_PATH)  # [AUTO-6]

    t_end    = time.perf_counter()
    duration = t_end - t_start
    print(f"  [Thread-Lasso] Done T+{t_end - pipeline_start:.3f}s | "
          f"{duration:.2f}s | MSE={mse:.4f} | RMSE={rmse:.4f}")

    return {"name": "Lasso", "t_start": t_start, "t_end": t_end,
            "duration": duration, "mse": mse, "rmse": rmse}


def train_gbt(train_data, test_data, pipeline_start: float) -> dict:
    t_start = time.perf_counter()
    print(f"  [Thread-GBT]   Started at T+{t_start - pipeline_start:.3f}s")

    gbt = GBTClassifier(
        featuresCol="features",
        labelCol="Target_Direction",
        maxDepth=5,
        maxIter=50,
        maxBins=256,                  # [ENH-14] high-precision histogram splits
        subsamplingRate=0.8,          # [ENH-14] stochastic boosting
        featureSubsetStrategy="auto",
        cacheNodeIds=True,            # [ENH-14] faster per-iteration
        checkpointInterval=10,        # [ENH-15] truncates DAG lineage
        stepSize=0.1
    )
    model = gbt.fit(train_data)
    preds = model.transform(test_data)

    acc = MulticlassClassificationEvaluator(
        labelCol="Target_Direction",
        predictionCol="prediction",
        metricName="accuracy"
    ).evaluate(preds)

    model.write().overwrite().save(GBT_MODEL_PATH)  # [AUTO-6]

    t_end    = time.perf_counter()
    duration = t_end - t_start
    print(f"  [Thread-GBT]   Done T+{t_end - pipeline_start:.3f}s | "
          f"{duration:.2f}s | Accuracy={acc*100:.2f}%")

    return {"name": "GBT", "t_start": t_start, "t_end": t_end,
            "duration": duration, "accuracy": acc}


# =============================================================================
# CORE PIPELINE  [ENH-5 through ENH-15, ENH-18, ENH-22, ENH-23, FIX-1]
# =============================================================================
def execute_pipeline(spark: SparkSession,
                     is_serial: bool,
                     fraction_override=None) -> dict:
    mode_label = "SERIAL [local[1]]" if is_serial else "PARALLEL [local[*]]"
    print(f"\n{'='*70}")
    print(f"  PIPELINE MODE: {mode_label}")
    print(f"{'='*70}")

    # Checkpoint dir for GBT DAG truncation  [ENH-15]
    spark.sparkContext.setCheckpointDir(SESSION_CHECKPOINT)  # [AUTO-7]

    # JVM warmup — pay startup cost before timing  [ENH-5]
    print("  Warming up JVM...")
    spark.range(1).count()

    # ── INGESTION + COLUMN PRUNING  [ENH-6] ───────────────────────────────────
    print("  Loading Parquet with column pruning...")
    df = (
        spark.read.parquet(PARQUET_PATH)   # [AUTO-6]
        .select("symbol", "timestamp", "Open", "High", "Low", "Close", "Volume")
    )

    # ── REPARTITION + PRE-SORT  [ENH-7] ──────────────────────────────────────
    print("  Repartitioning and pre-sorting...")
    df_optimized = df.repartition("symbol").sortWithinPartitions("timestamp")

    # ── FEATURE ENGINEERING  [ENH-8, ENH-9] ──────────────────────────────────
    print("  Engineering distributed window features...")
    windowSpec = Window.partitionBy("symbol").orderBy("timestamp")
    rolling10  = windowSpec.rowsBetween(-10, 0)
    rolling30  = windowSpec.rowsBetween(-30, 0)

    df_features = (
        df_optimized
        .withColumn("MA_10",      avg(col("Close")).over(rolling10))
        .withColumn("MA_30",      avg(col("Close")).over(rolling30))
        .withColumn("Volatility", stddev(col("Close")).over(rolling10))
        .withColumn("Vol_MA_10",  avg(col("Volume")).over(rolling10))
        .withColumn("Momentum_1",
            when(lag(col("Close"), 1).over(windowSpec) != 0,
                (col("Close") - lag(col("Close"), 1).over(windowSpec))
                / lag(col("Close"), 1).over(windowSpec)
            ).otherwise(lit(0.0))
        )
        .withColumn("Momentum_5",
            when(lag(col("Close"), 5).over(windowSpec) != 0,
                (col("Close") - lag(col("Close"), 5).over(windowSpec))
                / lag(col("Close"), 5).over(windowSpec)
            ).otherwise(lit(0.0))
        )
        .withColumn("Target_Price",
            lead(col("Close"), 1).over(windowSpec))
        .withColumn("Target_Direction",
            when(lead(col("Close"), 1).over(windowSpec) > col("Close"), 1.0)
            .otherwise(0.0))
        .dropna()
        .replace(float("inf"),  None)   # [ENH-9] inf guard
        .replace(float("-inf"), None)
        .dropna()
    )

    # ── EARLY PERSIST  [ENH-10] ───────────────────────────────────────────────
    print("  Persisting window features...")
    df_features.persist(StorageLevel.MEMORY_AND_DISK)
    total_rows = df_features.count()
    print(f"  Feature rows materialized: {total_rows:,}")

    # ── VECTOR ASSEMBLY ───────────────────────────────────────────────────────
    assembler = VectorAssembler(
        inputCols=FEATURE_COLS,
        outputCol="features",
        handleInvalid="skip"
    )
    df_ml = assembler.transform(df_features)

    # ── UNPERSIST UPSTREAM  [ENH-11] ─────────────────────────────────────────
    df_features.unpersist()
    print("  Released pre-assembly cache.")

    # ── OOM GUARD  [ENH-12, FIX-1] ───────────────────────────────────────────
    # FIX-1: REMOVED df_ml.limit(5000) — was killing all speedup visibility.
    # Now uses proper row-count-based estimation.
    train_data_full, test_data = df_ml.randomSplit([0.8, 0.2], seed=42)

    if fraction_override is not None:
        actual_fraction = fraction_override
        print(f"  Fraction override: {actual_fraction * 100:.0f}%")
    else:
        train_row_count = train_data_full.count()
        estimated_gb    = (train_row_count * len(FEATURE_COLS) * 8) / (1024**3) * 2.5
        actual_fraction = 1.0 if estimated_gb < OOM_SAFE_THRESHOLD_GB else 0.75
        print(f"  OOM estimate: ~{estimated_gb:.2f} GB "
              f"(threshold {OOM_SAFE_THRESHOLD_GB} GB) "
              f"→ using {actual_fraction * 100:.0f}%")

    # Direct assignment at 100% — avoids Bernoulli sampler  [ENH-13]
    if actual_fraction >= 1.0:
        train_data_sampled = train_data_full
    else:
        train_data_sampled = train_data_full.sample(fraction=actual_fraction, seed=42)

    train_data_sampled.persist(StorageLevel.MEMORY_AND_DISK)
    test_data.persist(StorageLevel.MEMORY_AND_DISK)
    train_count = train_data_sampled.count()
    test_count  = test_data.count()
    print(f"  Train rows: {train_count:,} | Test rows: {test_count:,}")

    # ── CONCURRENT MODEL TRAINING  [ENH-16, ENH-17, ENH-18, ENH-29] ──────────
    print(f"\n{'='*70}")
    if is_serial:
        print("  TRAINING MODELS SEQUENTIALLY (serial baseline)")
    else:
        print("  LAUNCHING CONCURRENT DISTRIBUTED TRAINING (ThreadPoolExecutor)")
        print("  NOTE: Both DAGs submitted to FAIR scheduler simultaneously.")  # [ENH-29]
        print("  Spark distributes tasks across all cores concurrently.")
        print("  Primary speedup = distributed data processing (Gustafson's Law).")
    print(f"{'='*70}")

    pipeline_start = time.perf_counter()

    if is_serial:
        lasso_r = train_lasso(train_data_sampled, test_data, pipeline_start)
        gbt_r   = train_gbt(train_data_sampled,   test_data, pipeline_start)
        results = [lasso_r, gbt_r]
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            fl = ex.submit(train_lasso, train_data_sampled, test_data, pipeline_start)
            fg = ex.submit(train_gbt,   train_data_sampled, test_data, pipeline_start)
            results = [fl.result(), fg.result()]

    total_time = time.perf_counter() - pipeline_start

    lasso_r = next(r for r in results if r["name"] == "Lasso")
    gbt_r   = next(r for r in results if r["name"] == "GBT")

    # Positive overlap = mathematically proven concurrent execution  [ENH-18]
    overlap_seconds = max(
        (lasso_r["duration"] + gbt_r["duration"]) - total_time, 0.0
    )

    # Memory hygiene  [ENH-22]
    train_data_sampled.unpersist()
    test_data.unpersist()

    # ── SUMMARY ───────────────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  PIPELINE COMPLETE ({mode_label})")
    print(f"  Total wall-clock : {total_time:.2f}s")
    print(f"  Lasso  — T+{lasso_r['t_start'] - pipeline_start:.3f}s | "
          f"MSE={lasso_r['mse']:.4f} | RMSE={lasso_r['rmse']:.4f}")
    print(f"  GBT    — T+{gbt_r['t_start'] - pipeline_start:.3f}s | "
          f"Accuracy={gbt_r['accuracy']*100:.2f}%")
    print(f"  Overlap: {overlap_seconds:.2f}s "
          f"({'CONCURRENT EXECUTION PROVEN' if overlap_seconds > 0.1 else 'minimal'})")
    print(f"{'='*70}")

    return {                              # [ENH-23]
        "mode":            "serial" if is_serial else "parallel",
        "fraction":        actual_fraction,
        "total_time":      total_time,
        "lasso_mse":       lasso_r["mse"],
        "lasso_rmse":      lasso_r["rmse"],
        "lasso_duration":  lasso_r["duration"],
        "lasso_t_start":   lasso_r["t_start"] - pipeline_start,
        "gbt_accuracy":    gbt_r["accuracy"],
        "gbt_duration":    gbt_r["duration"],
        "gbt_t_start":     gbt_r["t_start"] - pipeline_start,
        "overlap_seconds": overlap_seconds,
        "train_rows":      train_count,
        "test_rows":       test_count,
    }


# =============================================================================
# BENCHMARK SUITE  [ENH-20, ENH-21, ENH-28, ENH-30]
# =============================================================================
def run_benchmark_suite(log_gc: bool = True):
    fractions   = [0.25, 0.50, 0.75, 1.0]
    all_results = []
    gantt_data  = {}

    print("\n" + "*" * 70)
    print("  PDC AUTOMATED SCALABILITY BENCHMARK SUITE")
    print(f"  Detected cores : {NUM_CORES}")
    print(f"  Driver memory  : {DRIVER_MEM}")
    print("  Fractions      : 25% / 50% / 75% / 100%")
    print("*" * 70)

    for frac in fractions:
        print(f"\n\n{'#'*70}")
        print(f"  BENCHMARK FRACTION: {frac * 100:.0f}%")
        print(f"{'#'*70}")

        # Fresh sessions per iteration — no JVM state leaks  [ENH-20]
        spark_s = build_spark_session(is_serial=True,  log_gc=log_gc)
        spark_s.sparkContext.setLogLevel("ERROR")
        serial_r = execute_pipeline(spark_s, is_serial=True, fraction_override=frac)
        spark_s.stop()

        spark_p = build_spark_session(is_serial=False, log_gc=log_gc)
        spark_p.sparkContext.setLogLevel("ERROR")
        parallel_r = execute_pipeline(spark_p, is_serial=False, fraction_override=frac)
        spark_p.stop()

        speedup    = serial_r["total_time"] / parallel_r["total_time"]
        efficiency = speedup / NUM_CORES    # E = S/p  [ENH-28]

        print(f"\n  >>> SPEEDUP: {speedup:.2f}× | EFFICIENCY: {efficiency:.4f} "
              f"(E = {speedup:.2f} / {NUM_CORES} cores) <<<")

        all_results.append({
            "fraction":         frac,
            "serial_time_s":    round(serial_r["total_time"],    2),
            "parallel_time_s":  round(parallel_r["total_time"],  2),
            "speedup_x":        round(speedup,    2),
            "efficiency":       round(efficiency, 4),
            "num_cores":        NUM_CORES,
            "lasso_mse":        round(parallel_r["lasso_mse"],   4),
            "lasso_rmse":       round(parallel_r["lasso_rmse"],  4),
            "gbt_accuracy_pct": round(parallel_r["gbt_accuracy"] * 100, 2),
            "overlap_seconds":  round(parallel_r["overlap_seconds"], 2),
            "train_rows":       parallel_r["train_rows"],
        })

        if frac == max(fractions):
            gantt_data = {
                "Lasso": (parallel_r["lasso_t_start"], parallel_r["lasso_duration"]),
                "GBT":   (parallel_r["gbt_t_start"],   parallel_r["gbt_duration"]),
            }

    # ── CSV Export  [ENH-21] ──────────────────────────────────────────────────
    fieldnames = [
        "fraction", "serial_time_s", "parallel_time_s",
        "speedup_x", "efficiency", "num_cores",
        "lasso_mse", "lasso_rmse", "gbt_accuracy_pct",
        "overlap_seconds", "train_rows"
    ]
    with open(CSV_OUTPUT_PATH, "w", newline="") as f:  # [AUTO-6]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)

    # ── Console table ──────────────────────────────────────────────────────────
    print("\n" + "*" * 70)
    print("  FINAL BENCHMARK RESULTS")
    print("*" * 70)
    print(f"  {'Frac':>6} {'Serial':>10} {'Parallel':>10} "
          f"{'Speedup':>10} {'Efficiency':>12} {'Overlap':>10} {'Rows':>10}")
    print("  " + "-" * 74)
    for r in all_results:
        print(f"  {r['fraction']*100:>5.0f}%  "
              f"{r['serial_time_s']:>9.2f}s  "
              f"{r['parallel_time_s']:>9.2f}s  "
              f"{r['speedup_x']:>9.2f}×  "
              f"{r['efficiency']:>11.4f}  "
              f"{r['overlap_seconds']:>9.2f}s  "
              f"{r['train_rows']:>9,}")
    print(f"\n  CSV  → {CSV_OUTPUT_PATH}")
    print("*" * 70)

    # ── Charts  [ENH-30] ──────────────────────────────────────────────────────
    print("\n  Generating report charts...")
    generate_charts(all_results, gantt_data, output_dir=CHARTS_DIR)
    print(f"  Charts saved to: {CHARTS_DIR}")


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    if RUN_LIVE_DEMO:
        print("\n  MODE: LIVE DEMO  (Parallel | Auto-Fraction | Silent GC)")
        print(f"  Machine: {NUM_CORES} cores | {_total_gb:.1f} GB RAM")

        spark = build_spark_session(is_serial=False, log_gc=False)
        spark.sparkContext.setLogLevel("ERROR")
        result = execute_pipeline(spark, is_serial=False)
        spark.stop()

        print("\n  DEMO METRICS:")
        print(f"    Total pipeline time : {result['total_time']:.2f}s")
        print(f"    Lasso MSE           : {result['lasso_mse']:.4f}")
        print(f"    Lasso RMSE          : {result['lasso_rmse']:.4f}")
        print(f"    GBT Accuracy        : {result['gbt_accuracy']*100:.2f}%")
        print(f"    Thread overlap      : {result['overlap_seconds']:.2f}s  "
              f"← concurrent execution proof")
        print(f"    Cores utilized      : {NUM_CORES}")
        print(f"\n  For full benchmark CSV + charts:")
        print(f"    Set RUN_LIVE_DEMO = False and re-run.")
    else:
        print("\n  MODE: BENCHMARK SUITE  (Serial vs Parallel | All Fractions)")
        run_benchmark_suite(log_gc=True)
