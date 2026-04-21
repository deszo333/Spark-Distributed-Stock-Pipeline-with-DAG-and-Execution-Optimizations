# =============================================================================
# run_lgbm_compare.py — runs both benchmark scripts and compares results
# =============================================================================

import json
import subprocess
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent
SCRIPTS = {
    "spark_lgbm": BASE / "lgbm_spark_forecaster.py",
    "native_lgbm": BASE / "lgbm_native_forecaster.py",
}

def run_script(path: Path) -> dict:
    print(f"\n>>> Running {path.name}\n")
    proc = subprocess.run([sys.executable, str(path)], capture_output=True, text=True)
    print(proc.stdout)
    if proc.returncode != 0:
        print(proc.stderr)
        raise SystemExit(f"{path.name} failed with exit code {proc.returncode}")

    result = None
    for line in proc.stdout.splitlines()[::-1]:
        if line.startswith("RESULT_JSON="):
            result = json.loads(line.split("=", 1)[1])
            break
    if result is None:
        raise RuntimeError(f"Could not parse RESULT_JSON from {path.name}")
    return result

def main():
    results = {}
    for name, path in SCRIPTS.items():
        results[name] = run_script(path)

    spark = results["spark_lgbm"]
    native = results["native_lgbm"]

    print("\n" + "=" * 72)
    print("COMPARISON SUMMARY")
    print("=" * 72)
    print(f"Spark LightGBM   : {spark['total_time']:.2f}s | acc={spark['accuracy']*100:.2f}% | rows={spark['train_rows']:,}")
    print(f"Native LightGBM  : {native['total_time']:.2f}s | acc={native['accuracy']*100:.2f}% | rows={native['train_rows']:,}")

    if native["total_time"] > 0:
        speedup = spark["total_time"] / native["total_time"]
        print(f"Spark / Native time ratio: {speedup:.2f}×")

if __name__ == "__main__":
    main()
