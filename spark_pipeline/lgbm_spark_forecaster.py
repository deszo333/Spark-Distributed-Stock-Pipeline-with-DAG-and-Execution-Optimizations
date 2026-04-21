# =============================================================================
# lgbm_spark_forecaster.py — Spark-native LightGBM benchmark
# =============================================================================
# Keeps the Spark preprocessing path, but replaces GBT with SynapseML LightGBM.

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
    "pyspark": "pyspark",
    "psutil": "psutil",
    "numpy": "numpy",
    "pyarrow": "pyarrow",
    "matplotlib": "matplotlib",
    "synapse.ml": "synapseml",
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
# ENVIRONMENT SETUP
# =============================================================================
def _setup_java(is_colab: bool):
    try:
        r = subprocess.run(["java", "-version"], capture_output=True, text=True)
        line = (r.stderr or r.stdout or "").splitlines()
        print(f"  Java     : {line[0] if line else 'found'}")
    except FileNotFoundError:
        if is_colab:
            os.system("apt-get install -y default-jdk -qq")
        else:
            print("  WARNING: Java not found. Install: https://adoptium.net/")

def _create_dummy_winutils(bin_dir: str):
    pe = bytes([0x4D,0x5A,0x90,0x00,0x03,0x00,0x00,0x00,0x04,0x00,0x00,0x00,0xFF,0xFF,0x00,0x00,
                0xB8,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x40,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
                0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
                0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x80,0x00,0x00,0x00,
                0x0E,0x1F,0xBA,0x0E,0x00,0xB4,0x09,0xCD,0x21,0xB8,0x01,0x4C,0xCD,0x21,0x54,0x68,
                0x69,0x73,0x20,0x70,0x72,0x6F,0x67,0x72,0x61,0x6D,0x20,0x63,0x61,0x6E,0x6E,0x6F,
                0x74,0x20,0x62,0x65,0x20,0x72,0x75,0x6E,0x20,0x69,0x6E,0x20,0x44,0x4F,0x53,0x20,
                0x6D,0x6F,0x64,0x65,0x2E,0x0D,0x0D,0x0A,0x24,0x00,0x00,0x00,0x00,0x00,0x00,0x00])
    try:
        Path(bin_dir).mkdir(parents=True, exist_ok=True)
        (Path(bin_dir) / "winutils.exe").write_bytes(pe)
        (Path(bin_dir) / "hadoop.dll").write_bytes(b"\x00" * 64)
        os.environ["HADOOP_HOME"] = str(Path(bin_dir).parent)
        os.environ["PATH"] = bin_dir + ";" + os.environ.get("PATH", "")
        print(f"  Hadoop   : dummy winutils at {bin_dir}")
    except Exception as e:
        print(f"  Hadoop   : dummy failed ({e})")

def _setup_hadoop_windows():
    import urllib.request

    for path in [
        r"C:\hadoop",
        r"C:\winutils",
        r"C:\tools\hadoop",
        os.path.join(os.path.expanduser("~"), "hadoop"),
        os.path.join(os.path.expanduser("~"), "winutils"),
        os.path.join(os.path.expanduser("~"), "winutils", "hadoop-3.3.5"),
    ]:
        if os.path.exists(os.path.join(path, "bin", "winutils.exe")):
            os.environ["HADOOP_HOME"] = path
            os.environ["PATH"] = os.path.join(path, "bin") + ";" + os.environ.get("PATH", "")
            print(f"  Hadoop   : found at {path}")
            return

    print("  Hadoop not found — downloading...")
    install_dir = os.path.join(os.path.expanduser("~"), "winutils", "hadoop-3.3.5")
    bin_dir = os.path.join(install_dir, "bin")
    os.makedirs(bin_dir, exist_ok=True)

    base = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin"
    ok = 0
    for fn in ["winutils.exe", "hadoop.dll", "hadoop.exp", "hadoop.lib"]:
        dest = os.path.join(bin_dir, fn)
        if os.path.exists(dest):
            ok += 1
            continue
        try:
            urllib.request.urlretrieve(f"{base}/{fn}", dest)
            ok += 1
            print(f"    {fn} ✓")
        except Exception:
            print(f"    {fn} ✗")

    if ok >= 2:
        os.environ["HADOOP_HOME"] = install_dir
        os.environ["PATH"] = bin_dir + ";" + os.environ.get("PATH", "")
        print(f"  Hadoop   : installed to {install_dir}")
    else:
        _create_dummy_winutils(bin_dir)

def setup_environment():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    is_windows = sys.platform == "win32"
    is_colab = "google.colab" in sys.modules or os.path.exists("/content")
    is_mac = sys.platform == "darwin"

    print(f"  Platform : {'Windows' if is_windows else 'Colab' if is_colab else 'Mac' if is_mac else 'Linux'}")
    print(f"  Python   : {sys.executable}")
    _setup_java(is_colab)
    if is_windows:
        _setup_hadoop_windows()
    if is_colab:
        for jp in [
            "/usr/lib/jvm/java-11-openjdk-amd64",
            "/usr/lib/jvm/java-17-openjdk-amd64",
        ]:
            if os.path.exists(jp):
                os.environ["JAVA_HOME"] = jp
                print(f"  JAVA_HOME: {jp}")
                break

setup_environment()

# =============================================================================
# RESOURCE DETECTION
# =============================================================================
import numpy as np
import pandas as pd
import psutil
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, stddev, lead, lag, when, lit, percent_rank
from pyspark.sql.functions import abs as spark_abs
from pyspark.storagelevel import StorageLevel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

try:
    import pyarrow as pa
    _av = tuple(int(x) for x in pa.__version__.split(".")[:2])
    ARROW_ENABLED = "true" if _av >= (1, 0) else "false"
except Exception:
    ARROW_ENABLED = "false"

try:
    from synapse.ml.lightgbm import LightGBMClassifier
except Exception as e:
    raise ImportError(
        "SynapseML is required for this script. Install it with `pip install synapseml` "
        "and make sure Spark loads the SynapseML jar coordinate."
    ) from e

NUM_CORES = max(1, min(int(os.environ.get("PDC_CORES", multiprocessing.cpu_count() or 2)), multiprocessing.cpu_count() or 2))
TOTAL_GB = psutil.virtual_memory().total / (1024 ** 3)

DRIVER_MEM = os.environ.get("PDC_DRIVER_MEMORY", "8g").strip()
OFFHEAP_MEM = os.environ.get("PDC_OFFHEAP_MEMORY", "2g").strip()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
PARQUET_PATH = os.path.join(DATA_DIR, "processed", "historical_parquet")
MODEL_PATH = os.path.join(DATA_DIR, "models", "lightgbm_spark_model")
CHARTS_DIR = os.path.join(DATA_DIR, "charts")
CSV_OUTPUT_PATH = os.path.join(DATA_DIR, "benchmark_results_lgbm_spark.csv")
SESSION_CHECKPOINT = os.path.join(DATA_DIR, f"checkpoints_lgbm_spark_{int(time.time())}")

for d in [CHARTS_DIR, SESSION_CHECKPOINT, os.path.join(DATA_DIR, "models")]:
    os.makedirs(d, exist_ok=True)

SYNAPSEML_COORDINATE = os.environ.get("SYNAPSEML_COORDINATE", "com.microsoft.azure:synapseml_2.12:1.1.3")
SYNAPSEML_REPO = os.environ.get("SYNAPSEML_REPO", "https://mmlspark.azureedge.net/maven")

print(f"  Cores    : {NUM_CORES}")
print(f"  RAM      : {TOTAL_GB:.1f} GB  |  driver={DRIVER_MEM}  offheap={OFFHEAP_MEM}")
print(f"  Arrow    : {ARROW_ENABLED}")
print(f"  Data dir : {os.path.abspath(DATA_DIR)}")
print(f"  SynapseML: {SYNAPSEML_COORDINATE}")

# =============================================================================
# CONFIG
# =============================================================================
RUN_LIVE_DEMO = os.environ.get("PDC_RUN_MODE", "demo").strip().lower() != "benchmark"
BENCHMARK_FRACTIONS = [
    float(x) for x in os.environ.get("PDC_FRACTIONS", "1.0").split(",")
    if x.strip()
]

FEATURE_COLS = [
    "Open", "High", "Low", "Close", "Volume",
    "MA_10", "MA_30", "Volatility", "Vol_MA_10",
    "Momentum_1", "Momentum_5",
    "Close_norm", "Volume_norm", "MA_ratio_10_30",
]
LABEL_COL = "Target_Direction"

MIN_VALID_PRICE = 1.0
MAX_ABS_TARGET_RETURN = 0.25

def build_spark_session(is_serial: bool) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("PDC_LightGBM_Spark")
        .config("spark.driver.memory", DRIVER_MEM)
        .config("spark.executor.memory", DRIVER_MEM)
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")
        .config("spark.ui.port", "0")
        .config("spark.sql.execution.arrow.pyspark.enabled", ARROW_ENABLED)
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.jars.packages", SYNAPSEML_COORDINATE)
        .config("spark.jars.repositories", SYNAPSEML_REPO)
    )

    if is_serial:
        builder = (
            builder.master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.speculation", "false")
        )
    else:
        builder = (
            builder.master(f"local[{NUM_CORES}]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.unsafe", "true")
            .config("spark.kryoserializer.buffer.max", "512m")
            .config("spark.scheduler.mode", "FAIR")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.shuffle.partitions", str(max(32, NUM_CORES * 24)))
            .config("spark.default.parallelism", str(NUM_CORES * 2))
            .config("spark.speculation", "true")
            .config("spark.memory.offHeap.enabled", "true")
            .config("spark.memory.offHeap.size", OFFHEAP_MEM)
        )
    return builder.getOrCreate()

def build_features(df):
    windowSpec = Window.partitionBy("symbol").orderBy("timestamp")
    rolling10 = windowSpec.rowsBetween(-10, 0)
    rolling30 = windowSpec.rowsBetween(-30, 0)

    out = (
        df
        .withColumn("MA_10", avg(col("Close")).over(rolling10))
        .withColumn("MA_30", avg(col("Close")).over(rolling30))
        .withColumn("Volatility", stddev(col("Close")).over(rolling10))
        .withColumn("Vol_MA_10", avg(col("Volume")).over(rolling10))
        .withColumn(
            "Momentum_1",
            when(
                lag(col("Close"), 1).over(windowSpec) != 0,
                (col("Close") - lag(col("Close"), 1).over(windowSpec)) / lag(col("Close"), 1).over(windowSpec),
            ).otherwise(lit(0.0)),
        )
        .withColumn(
            "Momentum_5",
            when(
                lag(col("Close"), 5).over(windowSpec) != 0,
                (col("Close") - lag(col("Close"), 5).over(windowSpec)) / lag(col("Close"), 5).over(windowSpec),
            ).otherwise(lit(0.0)),
        )
        .withColumn(
            "Close_norm",
            when(
                stddev(col("Close")).over(rolling30) > lit(1e-8),
                (col("Close") - avg(col("Close")).over(rolling30)) / stddev(col("Close")).over(rolling30),
            ).otherwise(lit(0.0)),
        )
        .withColumn(
            "Volume_norm",
            when(
                stddev(col("Volume").cast("double")).over(rolling10) > lit(1e-8),
                (col("Volume") - avg(col("Volume")).over(rolling10)) / stddev(col("Volume").cast("double")).over(rolling10),
            ).otherwise(lit(0.0)),
        )
        .withColumn("MA_ratio_10_30", when(col("MA_30") != 0, col("MA_10") / col("MA_30")).otherwise(lit(1.0)))
        .withColumn("Next_Close", lead(col("Close"), 1).over(windowSpec))
        .filter((col("Close") > lit(MIN_VALID_PRICE)) & (col("Next_Close") > lit(MIN_VALID_PRICE)))
        .withColumn("Target_Return", (col("Next_Close") - col("Close")) / col("Close"))
        .filter(spark_abs(col("Target_Return")) <= lit(MAX_ABS_TARGET_RETURN))
        .withColumn("Target_Direction", when(col("Next_Close") > col("Close"), 1.0).otherwise(0.0))
        .drop("Next_Close")
        .withColumn("Split_Rank", percent_rank().over(windowSpec))
        .dropna()
        .replace(float("inf"), None)
        .replace(float("-inf"), None)
        .dropna()
    )
    return out

def train_lgbm(train_data, test_data, pipeline_start: float) -> dict:
    t_start = time.perf_counter()
    print(f"  [Spark-LGBM] Started at T+{t_start - pipeline_start:.3f}s")

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features", handleInvalid="skip")
    tr = assembler.transform(train_data).select(LABEL_COL, "features")
    te = assembler.transform(test_data).select(LABEL_COL, "features")

    clf = LightGBMClassifier(
        featuresCol="features",
        labelCol=LABEL_COL,
        predictionCol="prediction",
        rawPredictionCol="rawPrediction",
        probabilityCol="probability",
        objective="binary",
        numLeaves=31,
        numIterations=80,
        learningRate=0.05,
        featureFraction=0.8,
        baggingFraction=0.8,
        baggingFreq=1,
        minDataInLeaf=20,
        lambdaL2=1.0,
        verbosity=-1,
        isUnbalance=True,
        defaultListenPort=int(os.environ.get("LGBM_PORT", "12402")),
    )

    model = clf.fit(tr)
    preds = model.transform(te)
    acc = MulticlassClassificationEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="accuracy",
    ).evaluate(preds)

    model.write().overwrite().save(MODEL_PATH)

    t_end = time.perf_counter()
    dur = t_end - t_start
    print(f"  [Spark-LGBM] Done T+{t_end - pipeline_start:.3f}s | {dur:.2f}s | Accuracy={acc*100:.2f}%")
    return {
        "name": "SparkLGBM",
        "t_start": t_start,
        "t_end": t_end,
        "duration": dur,
        "accuracy": acc,
    }

def run_once(fraction_override=None) -> dict:
    spark = build_spark_session(is_serial=False)
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setCheckpointDir(SESSION_CHECKPOINT)

    print("\n" + "=" * 70)
    print("  PIPELINE MODE: SPARK-NATIVE LIGHTGBM")
    print("=" * 70)

    print("  Warming up JVM...")
    spark.range(1).count()

    print("  Loading Parquet with column pruning...")
    df = (
        spark.read.parquet(PARQUET_PATH)
        .select("symbol", "timestamp", "Open", "High", "Low", "Close", "Volume")
    )

    print("  Repartitioning and pre-sorting...")
    df_opt = df.repartition("symbol").sortWithinPartitions("timestamp")

    print("  Engineering distributed window features...")
    df_features = build_features(df_opt)
    df_features.persist(StorageLevel.MEMORY_AND_DISK)

    total_rows = df_features.count()
    print(f"  Feature rows materialized: {total_rows:,}")

    train_full = df_features.filter(col("Split_Rank") < lit(0.8)).drop("Split_Rank")
    test_data = df_features.filter(col("Split_Rank") >= lit(0.8)).drop("Split_Rank")

    if fraction_override is not None:
        actual_fraction = fraction_override
    else:
        actual_fraction = 1.0

    if actual_fraction >= 1.0:
        train_sampled = train_full
    else:
        train_sampled = train_full.sample(fraction=actual_fraction, seed=42)

    train_sampled.persist(StorageLevel.MEMORY_AND_DISK)
    test_data.persist(StorageLevel.MEMORY_AND_DISK)
    train_count = train_sampled.count()
    test_count = test_data.count()
    print(f"  Train rows: {train_count:,} | Test rows: {test_count:,}")

    pipeline_start = time.perf_counter()
    result = train_lgbm(train_sampled, test_data, pipeline_start)
    total_time = time.perf_counter() - pipeline_start

    train_sampled.unpersist()
    test_data.unpersist()
    df_features.unpersist()
    spark.stop()

    summary = {
        "mode": "spark_lgbm",
        "fraction": actual_fraction,
        "total_time": total_time,
        "accuracy": result["accuracy"],
        "duration": result["duration"],
        "train_rows": train_count,
        "test_rows": test_count,
        "cores": NUM_CORES,
    }

    print("\n" + "=" * 70)
    print("  PIPELINE COMPLETE (Spark LightGBM)")
    print(f"  Total wall-clock  : {total_time:.2f}s")
    print(f"  Accuracy          : {result['accuracy']*100:.2f}%")
    print(f"  Cores utilized    : {NUM_CORES}")
    print("=" * 70)
    print("RESULT_JSON=" + json.dumps(summary))
    return summary

if __name__ == "__main__":
    run_once()
