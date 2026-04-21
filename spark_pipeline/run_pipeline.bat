@echo off
cd /d "%~dp0"
title PDC Parallel ML Pipeline
echo ========================================================
echo   STARTING PDC AUTOMATED PIPELINE
echo ========================================================
echo.

set PYSPARK_SUBMIT_ARGS=--driver-memory 8g --executor-memory 8g pyspark-shell
set PDC_DRIVER_MEMORY=8g
set PDC_OFFHEAP_MEMORY=2g

:: --- ACTIVATE VIRTUAL ENVIRONMENT (From Parent Folder) ---
if exist "..\venv\Scripts\activate.bat" (
    echo [System] Activating Virtual Environment...
    call "..\venv\Scripts\activate.bat"
    echo.
) else (
    echo [System] No 'venv' folder found one level up. Using global Python.
    echo.
)
:: ---------------------------------------------------------

echo [1/2] Executing Data Ingestion (Multiplier)...
python ingestion.py

:: Check if ingestion crashed. If yes, stop the script.
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ERROR: Ingestion failed! Stopping pipeline.
    pause
    exit /b %ERRORLEVEL%
)

echo.
echo [2/2] Executing ML Forecaster (Training / Benchmark)...
set PDC_RUN_MODE=benchmark
python ml_forecaster.py

:: Check if forecaster crashed.
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ERROR: ML Forecaster failed!
    pause
    exit /b %ERRORLEVEL%
)

echo.
echo ========================================================
echo   PIPELINE COMPLETE! Check the "data" folder.
echo ========================================================
pause