@echo off
title PDC Parallel ML Pipeline
echo ========================================================
echo   STARTING PDC AUTOMATED PIPELINE
echo ========================================================
echo.

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