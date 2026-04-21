# =============================================================================
# spark_pipeline/ml_forecaster.py — PDC Parallel ML Pipeline (Final Edition)
# =============================================================================
# ARCHITECTURE:
#   GBT  (GBTClassifier)    → MAIN MODEL   — predicts direction UP/DOWN
#   Lasso (LinearRegression) → BASELINE     — predicts % return for comparison

import subprocess, sys, os

def _pip(package):
    subprocess.check_call([sys.executable,"-m","pip","install",package,"-q"],
                          stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

print("\n  Checking dependencies...")
for module, package in {"pyspark":"pyspark","psutil":"psutil","numpy":"numpy",
                         "requests":"requests","matplotlib":"matplotlib",
                         "pyarrow":"pyarrow"}.items():
    try: __import__(module)
    except ImportError:
        print(f"  Installing {package}...")
        _pip(package); print(f"  {package} installed.")
print("  All dependencies ready.")

# =============================================================================
# ENVIRONMENT SETUP
# =============================================================================
def _setup_java(is_colab):
    try:
        r = subprocess.run(["java","-version"],capture_output=True,text=True)
        line = (r.stderr or r.stdout or "").splitlines()
        print(f"  Java     : {line[0] if line else 'found'}")
    except FileNotFoundError:
        if is_colab: os.system("apt-get install -y default-jdk -qq")
        else: print("  WARNING: Java not found. Install: https://adoptium.net/")

def _create_dummy_winutils(bin_dir):
    pe = bytes([0x4D,0x5A,0x90,0x00,0x03,0x00,0x00,0x00,0x04,0x00,0x00,0x00,0xFF,0xFF,0x00,0x00,
                0xB8,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x40,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
                0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
                0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x80,0x00,0x00,0x00,
                0x0E,0x1F,0xBA,0x0E,0x00,0xB4,0x09,0xCD,0x21,0xB8,0x01,0x4C,0xCD,0x21,0x54,0x68,
                0x69,0x73,0x20,0x70,0x72,0x6F,0x67,0x72,0x61,0x6D,0x20,0x63,0x61,0x6E,0x6E,0x6F,
                0x74,0x20,0x62,0x65,0x20,0x72,0x75,0x6E,0x20,0x69,0x6E,0x20,0x44,0x4F,0x53,0x20,
                0x6D,0x6F,0x64,0x65,0x2E,0x0D,0x0D,0x0A,0x24,0x00,0x00,0x00,0x00,0x00,0x00,0x00])
    try:
        open(os.path.join(bin_dir,'winutils.exe'),'wb').write(pe)
        open(os.path.join(bin_dir,'hadoop.dll'),'wb').write(b'\x00'*64)
        os.environ['HADOOP_HOME'] = os.path.dirname(bin_dir)
        os.environ['PATH'] = bin_dir+';'+os.environ.get('PATH','')
        print(f"  Hadoop   : dummy winutils at {bin_dir}")
    except Exception as e: print(f"  Hadoop   : dummy failed ({e})")

def _setup_hadoop_windows():
    import urllib.request
    for path in [r'C:\hadoop',r'C:\winutils',r'C:\tools\hadoop',
                 os.path.join(os.path.expanduser('~'),'hadoop'),
                 os.path.join(os.path.expanduser('~'),'winutils'),
                 os.path.join(os.path.expanduser('~'),'winutils','hadoop-3.3.5')]:
        if os.path.exists(os.path.join(path,'bin','winutils.exe')):
            os.environ['HADOOP_HOME'] = path
            os.environ['PATH'] = os.path.join(path,'bin')+';'+os.environ.get('PATH','')
            print(f"  Hadoop   : found at {path}"); return
    print("  Hadoop not found — downloading...")
    install_dir = os.path.join(os.path.expanduser('~'),'winutils','hadoop-3.3.5')
    bin_dir = os.path.join(install_dir,'bin'); os.makedirs(bin_dir,exist_ok=True)
    BASE = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin"
    ok = 0
    for fn in ["winutils.exe","hadoop.dll","hadoop.exp","hadoop.lib"]:
        dest = os.path.join(bin_dir,fn)
        if os.path.exists(dest): ok+=1; continue
        try: urllib.request.urlretrieve(f"{BASE}/{fn}",dest); ok+=1; print(f"    {fn} ✓")
        except: print(f"    {fn} ✗")
    if ok>=2:
        os.environ['HADOOP_HOME'] = install_dir
        os.environ['PATH'] = bin_dir+';'+os.environ.get('PATH','')
        print(f"  Hadoop   : installed to {install_dir}")
    else: _create_dummy_winutils(bin_dir)

def setup_environment():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    is_windows = sys.platform=='win32'
    is_colab   = 'google.colab' in sys.modules or os.path.exists('/content')
    is_mac     = sys.platform=='darwin'
    print(f"  Platform : {'Windows' if is_windows else 'Colab' if is_colab else 'Mac' if is_mac else 'Linux'}")
    print(f"  Python   : {sys.executable}")
    _setup_java(is_colab)
    if is_windows: _setup_hadoop_windows()
    if is_colab:
        for jp in ["/usr/lib/jvm/java-11-openjdk-amd64","/usr/lib/jvm/java-17-openjdk-amd64"]:
            if os.path.exists(jp): os.environ["JAVA_HOME"]=jp; print(f"  JAVA_HOME: {jp}"); break

setup_environment()

# =============================================================================
# RESOURCE DETECTION
# =============================================================================
import time, multiprocessing, psutil, csv, concurrent.futures

_machine_cores = multiprocessing.cpu_count() or 2
try:
    _requested_cores = int(os.environ.get("PDC_CORES", _machine_cores))
except ValueError:
    _requested_cores = _machine_cores
NUM_CORES = max(1, min(_requested_cores, _machine_cores))
_total_gb = psutil.virtual_memory().total/(1024**3)

if   _total_gb>=32: DRIVER_MEM,OFFHEAP_MEM = "12g","4g"
elif _total_gb>=16: DRIVER_MEM,OFFHEAP_MEM = "8g", "2g"
elif _total_gb>=8:  DRIVER_MEM,OFFHEAP_MEM = "4g", "1g"
else:               DRIVER_MEM,OFFHEAP_MEM = "2g", "512m"

DRIVER_MEM,OFFHEAP_MEM = "6g", "2g"

try:
    import pyarrow as pa
    _av = tuple(int(x) for x in pa.__version__.split(".")[:2])
    ARROW_ENABLED = "true" if _av>=(1,0) else "false"
except ImportError: ARROW_ENABLED = "false"

# Absolute paths — works from any directory
SCRIPT_DIR         = os.path.dirname(os.path.abspath(__file__))
DATA_DIR           = os.path.join(SCRIPT_DIR, "..", "data")
PARQUET_PATH       = os.path.join(DATA_DIR, "processed", "historical_parquet")
LASSO_MODEL_PATH   = os.path.join(DATA_DIR, "models", "lasso_model")
GBT_MODEL_PATH     = os.path.join(DATA_DIR, "models", "gbt_model")
CHARTS_DIR         = os.path.join(DATA_DIR, "charts")
CSV_OUTPUT_PATH    = os.path.join(DATA_DIR, "benchmark_results.csv")
SESSION_CHECKPOINT = os.path.join(DATA_DIR, f"checkpoints_{int(time.time())}")

for _d in [CHARTS_DIR, SESSION_CHECKPOINT, os.path.join(DATA_DIR,"models")]:
    os.makedirs(_d, exist_ok=True)

print(f"  Cores    : {NUM_CORES}")
print(f"  RAM      : {_total_gb:.1f} GB  |  driver={DRIVER_MEM}  offheap={OFFHEAP_MEM}")
print(f"  Arrow    : {ARROW_ENABLED}")
print(f"  Data dir : {os.path.abspath(DATA_DIR)}\n")

# =============================================================================
# PYSPARK IMPORTS
# =============================================================================
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, stddev, lead, lag, when, lit, percent_rank
from pyspark.sql.functions import abs as spark_abs
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.storagelevel import StorageLevel

# =============================================================================
# CONFIGURATION
# =============================================================================

# Toggle with PDC_RUN_MODE=demo or PDC_RUN_MODE=benchmark.
RUN_LIVE_DEMO = os.environ.get("PDC_RUN_MODE", "demo").strip().lower() != "benchmark"
BENCHMARK_FRACTIONS = [
    float(x) for x in os.environ.get("PDC_FRACTIONS", "0.25,0.50,0.75,1.0").split(",")
    if x.strip()
]

# ── Feature sets ─────────────────────────────────────────────────────────────
# Lasso uses normalized features only — required to fix mixed-scale RMSE issue
LASSO_FEATURE_COLS = [
    "Close_norm",       # z-score of Close vs 30-day history (scale-invariant)
    "Volume_norm",      # z-score of Volume vs 10-day history
    "MA_ratio_10_30",   # MA_10/MA_30 — trend signal with no price units
    "Volatility",       # stddev of Close over 10 days
    "Momentum_1",       # 1-day % return
    "Momentum_5",       # 5-day % return
    "Vol_MA_10",        # volume moving average
]

# GBT uses the full feature set — tree models handle raw scales natively
GBT_FEATURE_COLS = [
    "Open", "High", "Low", "Close", "Volume",
    "MA_10", "MA_30", "Volatility", "Vol_MA_10",
    "Momentum_1", "Momentum_5",
    "Close_norm", "Volume_norm", "MA_ratio_10_30",
]

LASSO_LABEL = "Target_Return"     # % return next day (scale-invariant)
GBT_LABEL   = "Target_Direction"  # UP=1.0 / DOWN=0.0

OOM_SAFE_THRESHOLD_GB = 4.0
MIN_VALID_PRICE = 1.0
MAX_ABS_TARGET_RETURN = 0.25


# =============================================================================
# SPARK SESSION BUILDER
# =============================================================================
def build_spark_session(is_serial: bool, log_gc: bool = False) -> SparkSession:
    app_name = "PDC_Serial_Baseline" if is_serial else f"PDC_Parallel_{NUM_CORES}_Cores"
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory",   DRIVER_MEM)
        .config("spark.executor.memory", DRIVER_MEM)
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")
        .config("spark.ui.port", "0")
        .config("spark.sql.execution.arrow.pyspark.enabled", ARROW_ENABLED)
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")
    )
    if is_serial:
        builder = (builder.master("local[1]")
                   .config("spark.sql.shuffle.partitions","1")
                   .config("spark.sql.adaptive.enabled","false")
                   .config("spark.speculation","false"))
    else:
        builder = (builder.master(f"local[{NUM_CORES}]")
                   .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                   .config("spark.kryo.unsafe","true")
                   .config("spark.kryoserializer.buffer.max","512m")
                   .config("spark.scheduler.mode","FAIR")
                   .config("spark.sql.adaptive.enabled","true")
                   .config("spark.sql.adaptive.coalescePartitions.enabled","true")
                   .config("spark.sql.adaptive.skewJoin.enabled","true")
                   .config("spark.reducer.maxSizeInFlight","96m")
                   .config("spark.shuffle.file.buffer","1m")
                   .config("spark.sql.shuffle.partitions", str(max(32, NUM_CORES * 24)))
                   .config("spark.default.parallelism",str(NUM_CORES*2))
                   .config("spark.speculation","true")
                   .config("spark.memory.offHeap.enabled","true")
                   .config("spark.memory.offHeap.size",OFFHEAP_MEM))
    if log_gc:
        builder = builder.config("spark.executor.extraJavaOptions",
                                 "-verbose:gc -XX:+PrintGCDetails -XX:+UseG1GC")
    return builder.getOrCreate()


# =============================================================================
# CHART GENERATION
# =============================================================================
def generate_charts(all_results: list, gantt_data: dict, output_dir: str):
    try:
        import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
    except ImportError:
        print("  [Charts] matplotlib not available."); return

    os.makedirs(output_dir, exist_ok=True)
    fracs = [r["fraction"]*100 for r in all_results]
    spds  = [r["speedup_x"]    for r in all_results]
    effs  = [r["efficiency"]   for r in all_results]

    # Speedup curve
    fig,ax = plt.subplots(figsize=(8,5))
    ax.plot(fracs,spds,marker="o",linewidth=2.5,color="#2563EB",label="Measured speedup",zorder=3)
    ax.fill_between(fracs,spds,alpha=0.08,color="#2563EB")
    ax.axhline(y=1, color="#EF4444",linewidth=1.2,linestyle="--",label="Break-even (1×)")
    ax.axhline(y=5, color="#F59E0B",linewidth=1.5,linestyle="--",label="Rubric target: 5×")
    ax.axhline(y=10,color="#10B981",linewidth=1.5,linestyle="--",label="Rubric target: 10×")
    for x,y in zip(fracs,spds):
        ax.annotate(f"{y:.2f}×",(x,y),textcoords="offset points",xytext=(0,10),
                    ha="center",fontsize=9,color="#1E40AF")
    ax.set_xlabel("Data fraction (%)",fontsize=11)
    ax.set_ylabel("Speedup  (T_serial / T_parallel)",fontsize=11)
    ax.set_title(f"Speedup Curve — Serial vs Parallel  ({NUM_CORES} cores)",
                 fontsize=13,fontweight="bold")
    ax.legend(fontsize=9); ax.set_xticks(fracs); ax.grid(axis="y",alpha=0.3); ax.set_ylim(bottom=0)
    fig.tight_layout()
    p1 = os.path.join(output_dir,"speedup_curve.png")
    fig.savefig(p1,dpi=150); plt.close(fig); print(f"  [Charts] Speedup curve    → {p1}")

    # Efficiency curve
    fig,ax = plt.subplots(figsize=(8,5))
    ax.plot(fracs,effs,marker="s",linewidth=2.5,color="#7C3AED",
            label=f"Efficiency  E = S / {NUM_CORES}",zorder=3)
    ax.fill_between(fracs,effs,alpha=0.08,color="#7C3AED")
    ax.axhline(y=1.0,color="#6B7280",linewidth=1.2,linestyle=":",label="Ideal (E = 1.0)")
    for x,y in zip(fracs,effs):
        ax.annotate(f"{y:.3f}",(x,y),textcoords="offset points",xytext=(0,10),
                    ha="center",fontsize=9,color="#5B21B6")
    ax.set_xlabel("Data fraction (%)",fontsize=11)
    ax.set_ylabel("Parallel efficiency  E = S / p",fontsize=11)
    ax.set_title(f"Parallel Efficiency Curve  (p = {NUM_CORES} cores)",
                 fontsize=13,fontweight="bold")
    ax.legend(fontsize=9); ax.set_xticks(fracs); ax.grid(axis="y",alpha=0.3)
    ax.set_ylim(bottom=0,top=max(effs)*1.3 if effs else 1.5)
    fig.tight_layout()
    p2 = os.path.join(output_dir,"efficiency_curve.png")
    fig.savefig(p2,dpi=150); plt.close(fig); print(f"  [Charts] Efficiency curve → {p2}")

    # Gantt chart
    if gantt_data:
        fig,ax = plt.subplots(figsize=(9,3.5))
        colors = {"Lasso":"#2563EB","GBT":"#7C3AED"}
        yticks,ylabels = [],[]
        for i,(name,(ts,dur)) in enumerate(gantt_data.items()):
            ax.barh(y=i,width=dur,left=ts,height=0.4,color=colors.get(name,"#6B7280"),alpha=0.85)
            ax.text(ts+dur/2,i,f"{name}  ({dur:.1f}s)",
                    ha="center",va="center",color="white",fontsize=9,fontweight="bold")
            yticks.append(i); ylabels.append(name)
        le = gantt_data["Lasso"][0]+gantt_data["Lasso"][1]
        ge = gantt_data["GBT"][0]+gantt_data["GBT"][1]
        os_ = max(gantt_data["Lasso"][0],gantt_data["GBT"][0])
        oe  = min(le,ge)
        if oe>os_:
            ax.axvspan(os_,oe,alpha=0.18,color="#F59E0B",
                       label=f"Concurrent overlap ({oe-os_:.1f}s)")
        ax.set_yticks(yticks); ax.set_yticklabels(ylabels,fontsize=11)
        ax.set_xlabel("Wall-clock time (seconds from pipeline start)",fontsize=10)
        ax.set_title("Concurrent Training — FAIR Scheduler Gantt Chart\n"
                     "Amber = proven parallel overlap (Lasso baseline + GBT main model)",
                     fontsize=11,fontweight="bold")
        ax.legend(fontsize=9,loc="lower right"); ax.grid(axis="x",alpha=0.3)
        fig.tight_layout()
        p3 = os.path.join(output_dir,"gantt_overlap.png")
        fig.savefig(p3,dpi=150); plt.close(fig); print(f"  [Charts] Gantt chart      → {p3}")


# =============================================================================
# THREAD FUNCTIONS
# =============================================================================
def train_lasso(train_data, test_data, pipeline_start: float) -> dict:
    """
    BASELINE MODEL — Lasso Regression.
    Predicts next-day % return. Linear model used for comparison with GBT.
    Expected to perform worse — the gap proves GBT's value.
    """
    t_start = time.perf_counter()
    print(f"  [Thread-Lasso] Started at T+{t_start-pipeline_start:.3f}s")

    # Lasso-specific assembler — normalized features only
    asm = VectorAssembler(inputCols=LASSO_FEATURE_COLS,outputCol="lasso_features",
                          handleInvalid="skip")
    tr = asm.transform(train_data)
    te = asm.transform(test_data)

    lasso = LinearRegression(
        featuresCol="lasso_features",
        labelCol=LASSO_LABEL,
        elasticNetParam=1.0,      # Pure L1 Lasso
        regParam=0.001,           # [FIX-3] tuned for tiny % return values
        maxIter=200,
        standardization=True
    )
    model = lasso.fit(tr)
    preds = model.transform(te)

    mse  = RegressionEvaluator(labelCol=LASSO_LABEL,predictionCol="prediction",
                                metricName="mse").evaluate(preds)
    rmse = mse**0.5
    r2   = RegressionEvaluator(labelCol=LASSO_LABEL,predictionCol="prediction",
                                metricName="r2").evaluate(preds)

    model.write().overwrite().save(LASSO_MODEL_PATH)

    t_end = time.perf_counter(); dur = t_end-t_start
    print(f"  [Thread-Lasso] Done T+{t_end-pipeline_start:.3f}s | "
          f"{dur:.2f}s | MSE={mse:.6f} | RMSE={rmse:.6f} | R²={r2:.4f}")
    return {"name":"Lasso","t_start":t_start,"t_end":t_end,
            "duration":dur,"mse":mse,"rmse":rmse,"r2":r2}


def train_gbt(train_data, test_data, pipeline_start: float) -> dict:
    """
    MAIN MODEL — GBTClassifier.
    Predicts direction (UP=1 / DOWN=0). Non-linear, Spark-native, distributed.
    This model is saved and loaded by spark_worker.py for live streaming inference.
    """
    t_start = time.perf_counter()
    print(f"  [Thread-GBT]   Started at T+{t_start-pipeline_start:.3f}s")

    # GBT-specific assembler — richer feature set
    asm = VectorAssembler(inputCols=GBT_FEATURE_COLS,outputCol="gbt_features",
                          handleInvalid="skip")
    tr = asm.transform(train_data)
    te = asm.transform(test_data)

    gbt = GBTClassifier(
        featuresCol="gbt_features",
        labelCol=GBT_LABEL,
        maxDepth=5,
        maxIter=50,
        maxBins=256,              # High-precision histogram splits
        subsamplingRate=0.8,      # Stochastic boosting — speed + regularization
        featureSubsetStrategy="auto",
        cacheNodeIds=True,        # Faster per-iteration
        checkpointInterval=10,    # Truncates DAG lineage every 10 trees
        stepSize=0.1
    )
    model = gbt.fit(tr)
    preds = model.transform(te)
    acc   = MulticlassClassificationEvaluator(
        labelCol=GBT_LABEL,predictionCol="prediction",metricName="accuracy"
    ).evaluate(preds)

    model.write().overwrite().save(GBT_MODEL_PATH)

    t_end = time.perf_counter(); dur = t_end-t_start
    print(f"  [Thread-GBT]   Done T+{t_end-pipeline_start:.3f}s | "
          f"{dur:.2f}s | Accuracy={acc*100:.2f}%")
    return {"name":"GBT","t_start":t_start,"t_end":t_end,
            "duration":dur,"accuracy":acc}


# =============================================================================
# CORE PIPELINE
# =============================================================================
def execute_pipeline(spark: SparkSession,
                     is_serial: bool,
                     fraction_override=None) -> dict:
    mode_label = "SERIAL [local[1]]" if is_serial else f"PARALLEL [local[{NUM_CORES}]]"
    print(f"\n{'='*70}\n  PIPELINE MODE: {mode_label}\n{'='*70}")

    spark.sparkContext.setCheckpointDir(SESSION_CHECKPOINT)

    # JVM warmup
    print("  Warming up JVM...")
    spark.range(1).count()

    # Ingestion + column pruning
    print("  Loading Parquet with column pruning...")
    df = (spark.read.parquet(PARQUET_PATH)
          .select("symbol","timestamp","Open","High","Low","Close","Volume"))

    # Repartition + pre-sort
    print("  Repartitioning and pre-sorting...")
    df_opt = df.repartition("symbol").sortWithinPartitions("timestamp")

    # Feature engineering
    print("  Engineering distributed window features...")
    windowSpec = Window.partitionBy("symbol").orderBy("timestamp")
    rolling10  = windowSpec.rowsBetween(-10, 0)
    rolling30  = windowSpec.rowsBetween(-30, 0)

    df_features = (
        df_opt
        # ── Raw window features ───────────────────────────────────────────────
        .withColumn("MA_10",      avg(col("Close")).over(rolling10))
        .withColumn("MA_30",      avg(col("Close")).over(rolling30))
        .withColumn("Volatility", stddev(col("Close")).over(rolling10))
        .withColumn("Vol_MA_10",  avg(col("Volume")).over(rolling10))
        .withColumn("Momentum_1",
            when(lag(col("Close"),1).over(windowSpec)!=0,
                 (col("Close")-lag(col("Close"),1).over(windowSpec))
                 /lag(col("Close"),1).over(windowSpec)).otherwise(lit(0.0)))
        .withColumn("Momentum_5",
            when(lag(col("Close"),5).over(windowSpec)!=0,
                 (col("Close")-lag(col("Close"),5).over(windowSpec))
                 /lag(col("Close"),5).over(windowSpec)).otherwise(lit(0.0)))
        # ── [FIX-4] Normalized features — fixes cross-symbol scale bias ───────
        .withColumn("Close_norm",
            when(stddev(col("Close")).over(rolling30)>lit(1e-8),
                 (col("Close")-avg(col("Close")).over(rolling30))
                 /stddev(col("Close")).over(rolling30)).otherwise(lit(0.0)))
        .withColumn("Volume_norm",
            when(stddev(col("Volume").cast("double")).over(rolling10)>lit(1e-8),
                 (col("Volume")-avg(col("Volume")).over(rolling10))
                 /stddev(col("Volume").cast("double")).over(rolling10)).otherwise(lit(0.0)))
        .withColumn("MA_ratio_10_30",
            when(col("MA_30")!=0,col("MA_10")/col("MA_30")).otherwise(lit(1.0)))
        .withColumn("Next_Close", lead(col("Close"),1).over(windowSpec))
        .filter((col("Close") > lit(MIN_VALID_PRICE)) &
                (col("Next_Close") > lit(MIN_VALID_PRICE)))
        # Lasso target: bounded next-day percent return.
        .withColumn("Target_Return", (col("Next_Close")-col("Close"))/col("Close"))
        .filter(spark_abs(col("Target_Return")) <= lit(MAX_ABS_TARGET_RETURN))
        # GBT target: direction.
        .withColumn("Target_Direction",
            when(col("Next_Close")>col("Close"),1.0)
            .otherwise(0.0))
        .drop("Next_Close")
        .withColumn("Split_Rank", percent_rank().over(windowSpec))
        .dropna()
        .replace(float("inf"), None).replace(float("-inf"), None)
        .dropna()
    )

    # Persist after window computation
    print("  Persisting window features...")
    df_features.persist(StorageLevel.MEMORY_AND_DISK)
    total_rows = df_features.count()
    print(f"  Feature rows materialized: {total_rows:,}")

    train_full = df_features.filter(col("Split_Rank") < lit(0.8)).drop("Split_Rank")
    test_data  = df_features.filter(col("Split_Rank") >= lit(0.8)).drop("Split_Rank")

    if fraction_override is not None:
        actual_fraction = fraction_override
        print(f"  Fraction override: {actual_fraction*100:.0f}%")
    else:
        tr_count = train_full.count()
        est_gb   = (tr_count*len(GBT_FEATURE_COLS)*8)/(1024**3)*2.5
        actual_fraction = 1.0 if est_gb<OOM_SAFE_THRESHOLD_GB else 0.75
        print(f"  OOM estimate: ~{est_gb:.2f} GB → using {actual_fraction*100:.0f}%")

    train_sampled = train_full if actual_fraction>=1.0 else \
                    train_full.sample(fraction=actual_fraction, seed=42)

    train_sampled.persist(StorageLevel.MEMORY_AND_DISK)
    test_data.persist(StorageLevel.MEMORY_AND_DISK)
    train_count = train_sampled.count()
    test_count  = test_data.count()
    print(f"  Train rows: {train_count:,} | Test rows: {test_count:,}")

    # Concurrent model training
    print(f"\n{'='*70}")
    if is_serial:
        print("  TRAINING MODELS SEQUENTIALLY (serial baseline)")
    else:
        print("  LAUNCHING CONCURRENT TRAINING (ThreadPoolExecutor)")
        print("  Lasso baseline + GBT main model submitted simultaneously.")
        print("  FAIR scheduler interleaves both DAGs across all cores.")
    print(f"{'='*70}")

    pipeline_start = time.perf_counter()

    if is_serial:
        lr = train_lasso(train_sampled, test_data, pipeline_start)
        gr = train_gbt(train_sampled,   test_data, pipeline_start)
        results = [lr, gr]
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            fl = ex.submit(train_lasso, train_sampled, test_data, pipeline_start)
            fg = ex.submit(train_gbt,   train_sampled, test_data, pipeline_start)
            results = [fl.result(), fg.result()]

    total_time = time.perf_counter()-pipeline_start
    lasso_r = next(r for r in results if r["name"]=="Lasso")
    gbt_r   = next(r for r in results if r["name"]=="GBT")

    overlap_seconds = max((lasso_r["duration"]+gbt_r["duration"])-total_time, 0.0)

    train_sampled.unpersist()
    test_data.unpersist()
    df_features.unpersist()

    print(f"\n{'='*70}")
    print(f"  PIPELINE COMPLETE ({mode_label})")
    print(f"  Total wall-clock  : {total_time:.2f}s")
    print(f"  Lasso (baseline)  : MSE={lasso_r['mse']:.6f} | "
          f"RMSE={lasso_r['rmse']:.6f} | R²={lasso_r['r2']:.4f}")
    print(f"  GBT   (main)      : Accuracy={gbt_r['accuracy']*100:.2f}%")
    print(f"  Overlap           : {overlap_seconds:.2f}s "
          f"({'CONCURRENT EXECUTION PROVEN' if overlap_seconds>0.1 else 'minimal'})")
    print(f"{'='*70}")

    return {
        "mode":           "serial" if is_serial else "parallel",
        "fraction":       actual_fraction,
        "total_time":     total_time,
        "lasso_mse":      lasso_r["mse"],
        "lasso_rmse":     lasso_r["rmse"],
        "lasso_r2":       lasso_r["r2"],
        "lasso_duration": lasso_r["duration"],
        "lasso_t_start":  lasso_r["t_start"]-pipeline_start,
        "gbt_accuracy":   gbt_r["accuracy"],
        "gbt_duration":   gbt_r["duration"],
        "gbt_t_start":    gbt_r["t_start"]-pipeline_start,
        "overlap_seconds":overlap_seconds,
        "train_rows":     train_count,
        "test_rows":      test_count,
    }


# =============================================================================
# BENCHMARK SUITE
# =============================================================================
def run_benchmark_suite(log_gc: bool = True):
    fractions = BENCHMARK_FRACTIONS
    all_results = []; gantt_data = {}

    print("\n"+"*"*70)
    print("  PDC AUTOMATED SCALABILITY BENCHMARK SUITE")
    frac_label = "/".join(f"{f*100:.0f}" for f in fractions)
    print(f"  Cores: {NUM_CORES}  |  Memory: {DRIVER_MEM}  |  Fractions: {frac_label}%")
    print("*"*70)

    for frac in fractions:
        print(f"\n\n{'#'*70}\n  BENCHMARK FRACTION: {frac*100:.0f}%\n{'#'*70}")

        sp_s = build_spark_session(is_serial=True,  log_gc=log_gc)
        sp_s.sparkContext.setLogLevel("ERROR")
        ser_r = execute_pipeline(sp_s, is_serial=True, fraction_override=frac)
        sp_s.stop()

        sp_p = build_spark_session(is_serial=False, log_gc=log_gc)
        sp_p.sparkContext.setLogLevel("ERROR")
        par_r = execute_pipeline(sp_p, is_serial=False, fraction_override=frac)
        sp_p.stop()

        speedup    = ser_r["total_time"]/par_r["total_time"]
        efficiency = speedup/NUM_CORES
        print(f"\n  >>> SPEEDUP: {speedup:.2f}× | EFFICIENCY: {efficiency:.4f} <<<")

        all_results.append({
            "fraction":        frac,
            "serial_time_s":   round(ser_r["total_time"],  2),
            "parallel_time_s": round(par_r["total_time"],  2),
            "speedup_x":       round(speedup,   2),
            "efficiency":      round(efficiency,4),
            "num_cores":       NUM_CORES,
            "lasso_mse":       round(par_r["lasso_mse"],  6),
            "lasso_rmse":      round(par_r["lasso_rmse"], 6),
            "lasso_r2":        round(par_r["lasso_r2"],   4),
            "gbt_accuracy_pct":round(par_r["gbt_accuracy"]*100,2),
            "overlap_seconds": round(par_r["overlap_seconds"],2),
            "train_rows":      par_r["train_rows"],
        })
        if frac==max(fractions):
            gantt_data = {
                "Lasso": (par_r["lasso_t_start"], par_r["lasso_duration"]),
                "GBT":   (par_r["gbt_t_start"],   par_r["gbt_duration"]),
            }

    fieldnames = ["fraction","serial_time_s","parallel_time_s","speedup_x","efficiency",
                  "num_cores","lasso_mse","lasso_rmse","lasso_r2",
                  "gbt_accuracy_pct","overlap_seconds","train_rows"]
    with open(CSV_OUTPUT_PATH,"w",newline="") as f:
        w = csv.DictWriter(f,fieldnames=fieldnames)
        w.writeheader(); w.writerows(all_results)

    print("\n"+"*"*70+"\n  FINAL BENCHMARK RESULTS\n"+"*"*70)
    print(f"  {'Frac':>6} {'Serial':>10} {'Parallel':>10} {'Speedup':>10} "
          f"{'Efficiency':>12} {'Overlap':>10} {'Rows':>10}")
    print("  "+"-"*74)
    for r in all_results:
        print(f"  {r['fraction']*100:>5.0f}%  {r['serial_time_s']:>9.2f}s  "
              f"{r['parallel_time_s']:>9.2f}s  {r['speedup_x']:>9.2f}×  "
              f"{r['efficiency']:>11.4f}  {r['overlap_seconds']:>9.2f}s  "
              f"{r['train_rows']:>9,}")
    print(f"\n  CSV  → {CSV_OUTPUT_PATH}\n"+"*"*70)

    print("\n  Generating report charts...")
    generate_charts(all_results, gantt_data, output_dir=CHARTS_DIR)
    print(f"  Charts saved to: {CHARTS_DIR}")


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    if RUN_LIVE_DEMO:
        print(f"\n  MODE: LIVE DEMO  (Parallel | {NUM_CORES} cores | {_total_gb:.1f}GB RAM)")
        spark = build_spark_session(is_serial=False, log_gc=False)
        spark.sparkContext.setLogLevel("ERROR")
        result = execute_pipeline(spark, is_serial=False)
        spark.stop()
        print("\n  DEMO METRICS:")
        print(f"    Total pipeline time  : {result['total_time']:.2f}s")
        print(f"    Lasso MSE (baseline) : {result['lasso_mse']:.6f}  (predicting % return)")
        print(f"    Lasso RMSE           : {result['lasso_rmse']:.6f}")
        print(f"    Lasso R²             : {result['lasso_r2']:.4f}")
        print(f"    GBT Accuracy (main)  : {result['gbt_accuracy']*100:.2f}%")
        print(f"    Thread overlap       : {result['overlap_seconds']:.2f}s  ← concurrent proof")
        print(f"    Cores utilized       : {NUM_CORES}")
        print(f"\n  For full CSV + charts: set RUN_LIVE_DEMO = False and re-run.")
    else:
        print("\n  MODE: BENCHMARK SUITE  (Serial vs Parallel | All Fractions)")
        run_benchmark_suite(log_gc=True)
