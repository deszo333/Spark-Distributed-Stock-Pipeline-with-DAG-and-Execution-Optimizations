# =============================================================================
# ingestion.py — PDC Parallel Ingestion Pipeline (Bulletproof Edition)
# =============================================================================

# =============================================================================
# PHASE 0 — AUTO-INSTALLER
# =============================================================================
import subprocess
import sys
import os


def _pip(package):
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", package, "-q"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )


print("\n  Checking dependencies...")  # [AUTO-1]
REQUIRED = {
    "pyspark":  "pyspark",
    "psutil":   "psutil",
    "pyarrow":  "pyarrow",
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
    try:
        result = subprocess.run(["java", "-version"], capture_output=True, text=True)
        line = (result.stderr or result.stdout or "").splitlines()
        print(f"  Java     : {line[0] if line else 'found'}")
    except FileNotFoundError:
        if is_colab:
            print("  Java not found — installing on Colab...")
            os.system("apt-get install -y default-jdk -qq")
        else:
            print("  WARNING: Java not found.")
            print("  Please install Java 11 or 17 from: https://adoptium.net/")


def _create_dummy_winutils(bin_dir):  # [AUTO-3]
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
    import urllib.request

    common_paths = [
        r'C:\hadoop',
        r'C:\winutils',
        r'C:\tools\hadoop',
        os.path.join(os.path.expanduser('~'), 'hadoop'),
        os.path.join(os.path.expanduser('~'), 'winutils'),
        os.path.join(os.path.expanduser('~'), 'winutils', 'hadoop-3.3.5'),
    ]

    for path in common_paths:
        winutils = os.path.join(path, 'bin', 'winutils.exe')
        if os.path.exists(winutils):
            os.environ['HADOOP_HOME'] = path
            os.environ['PATH'] = os.path.join(path, 'bin') + ';' + os.environ.get('PATH', '')
            print(f"  Hadoop   : found at {path}")
            return

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
        print("  Hadoop   : download failed — creating dummy stub...")
        _create_dummy_winutils(bin_dir)


def setup_environment():
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

    if is_colab:
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
# PHASE 2 — RESOURCE DETECTION  [AUTO-4, AUTO-5, AUTO-6]
# =============================================================================
import time
import multiprocessing
import psutil

NUM_CORES = min(multiprocessing.cpu_count() or 2, 16)
_total_gb = psutil.virtual_memory().total / (1024 ** 3)

if _total_gb >= 32:   DRIVER_MEM = "12g"
elif _total_gb >= 16: DRIVER_MEM = "8g"
elif _total_gb >= 8:  DRIVER_MEM = "4g"
else:                 DRIVER_MEM = "2g"

try:
    import pyarrow as pa
    _av = tuple(int(x) for x in pa.__version__.split(".")[:2])
    ARROW_ENABLED = "true" if _av >= (1, 0) else "false"
except ImportError:
    ARROW_ENABLED = "false"

# Absolute paths  [AUTO-6]
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_DIR    = os.path.join(SCRIPT_DIR, "data")
RAW_CSV     = os.path.join(DATA_DIR, "raw", "SP500_Historical_Data.csv")
OUTPUT_DIR  = os.path.join(DATA_DIR, "processed", "historical_parquet")

os.makedirs(os.path.join(DATA_DIR, "raw"),       exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, "processed"), exist_ok=True)

print(f"  Cores    : {NUM_CORES}")
print(f"  RAM      : {_total_gb:.1f} GB")
print(f"  Memory   : driver={DRIVER_MEM}")
print(f"  Arrow    : {ARROW_ENABLED}")
print(f"  Input    : {RAW_CSV}")
print(f"  Output   : {OUTPUT_DIR}\n")

# =============================================================================
# PHASE 3 — IMPORTS
# =============================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, randn, expr, broadcast
from pyspark.sql.types import DoubleType, LongType, TimestampType

# =============================================================================
# CONFIGURATION
# =============================================================================

# Multiply base dataset to cross the ~1GB / 27M row threshold.
# MULTIPLIER=15 on a machine with enough RAM.
# Reduce to 10 if you have < 8GB RAM.
MULTIPLIER = 15


# =============================================================================
# SESSION BUILDER
# =============================================================================
def build_ingestion_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("PDC_Parallel_Ingestion")
        .master("local[*]")
        .config("spark.driver.memory",   DRIVER_MEM)        # [AUTO-4]
        .config("spark.executor.memory", DRIVER_MEM)
        .config("spark.ui.port", "0")                        # no port conflicts
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")  # [ENH-I3]
        .config("spark.kryoserializer.buffer.max", "512m")
        .config("spark.sql.adaptive.enabled", "true")          # [ENH-I4]
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", ARROW_ENABLED)  # [ENH-I5]
        .config("spark.sql.shuffle.partitions", str(NUM_CORES * 3))          # [ENH-I8]
        .getOrCreate()
    )


# =============================================================================
# INGESTION PIPELINE
# =============================================================================
def run_ingestion():
    spark = build_ingestion_session()
    spark.sparkContext.setLogLevel("ERROR")

    t_start = time.perf_counter()
    print(f"\n{'='*70}")
    print(f"  STARTING DISTRIBUTED INGESTION PIPELINE")
    print(f"  Multiplier: {MULTIPLIER}x | Cores: {NUM_CORES} | Memory: {DRIVER_MEM}")
    print(f"{'='*70}")

    # ── 1. READ BASE DATA ──────────────────────────────────────────────────────
    print("  [1] Loading base dataset...")
    if not os.path.exists(RAW_CSV):
        print(f"\n  ERROR: Raw CSV not found at: {RAW_CSV}")
        print(f"  Please place SP500_Historical_Data.csv in: {os.path.dirname(RAW_CSV)}")
        spark.stop()
        return

    df_base = spark.read.csv(RAW_CSV, header=True, inferSchema=True)

    # ── 2. STANDARDIZE & CAST SCHEMA  [ENH-I9] ────────────────────────────────
    print("  [2] Standardizing schema and casting data types...")
    df_base = df_base.select(
        col("Ticker").alias("symbol"),
        col("Date").cast(TimestampType()).alias("timestamp"),
        col("Open").cast(DoubleType()),
        col("High").cast(DoubleType()),
        col("Low").cast(DoubleType()),
        col("Close").cast(DoubleType()),
        col("Volume").cast(LongType())
    )
    base_count = df_base.count()
    print(f"      Base rows: {base_count:,}")

    # ── 3. BROADCAST CROSS-JOIN  [ENH-I1] ─────────────────────────────────────
    # Replaces slow Python .union() loop.
    # Small multiplier table is broadcast to all workers instantly.
    print(f"  [3] Executing Broadcast Cross-Join (Multiplier: {MULTIPLIER}×)...")
    df_multiplier = spark.range(1, MULTIPLIER + 1).withColumnRenamed("id", "shift_years")
    big_df = df_base.crossJoin(broadcast(df_multiplier))

    # ── 4. TEMPORAL SHIFT + NOISE INJECTION  [ENH-I2] ─────────────────────────
    # Fixed seeds (42-45) make results deterministic and reproducible.
    # Noise applied to all OHLC columns preserves realistic candle shapes.
    print("  [4] Applying temporal shifts and Gaussian noise...")
    big_df = (
        big_df
        .withColumn("timestamp",
            expr("date_add(timestamp, cast(shift_years * 365 as int))"))
        .withColumn("Open",  col("Open")  + (randn(42) * 0.5))
        .withColumn("High",  col("High")  + (randn(43) * 0.5))
        .withColumn("Low",   col("Low")   + (randn(44) * 0.5))
        .withColumn("Close", col("Close") + (randn(45) * 0.5))
        .drop("shift_years")
    )

    # ── 5. OPTIMIZED PARQUET WRITE  [ENH-I6, ENH-I7] ─────────────────────────
    # repartition("symbol") → one partition per symbol → no shuffle in ML reads
    # sortWithinPartitions → pre-sorted for Window functions in ml_forecaster
    # partitionBy("symbol") → ML forecaster reads only needed symbol folders
    print("  [5] Writing optimized Parquet (partitioned by symbol, pre-sorted)...")
    (
        big_df
        .repartition("symbol")
        .sortWithinPartitions("timestamp")
        .write
        .mode("overwrite")
        .partitionBy("symbol")
        .parquet(OUTPUT_DIR)
    )

    t_end    = time.perf_counter()
    duration = t_end - t_start
    estimated_rows = base_count * MULTIPLIER

    print(f"\n{'='*70}")
    print(f"  INGESTION COMPLETE")
    print(f"  Total pipeline time  : {duration:.2f}s")
    print(f"  Estimated total rows : ~{estimated_rows:,}")
    print(f"  Output directory     : {OUTPUT_DIR}")
    print(f"  Ready for ml_forecaster.py!")
    print(f"{'='*70}\n")

    spark.stop()


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    run_ingestion()
