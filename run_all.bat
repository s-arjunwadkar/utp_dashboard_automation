@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

echo ==============================================
echo UTP Snowflake Pipeline Runner
echo Started at %DATE% %TIME%
echo ==============================================

echo.
echo [STEP 1] Ingest Change Orders
echo ----------------------------------------------
python .\bronze\ingest_change_orders_file_bronze.py || goto :error

echo.
echo [STEP 2] Ingest Carryovers
echo ----------------------------------------------
python .\bronze\ingest_carryovers_file_bronze.py || goto :error

echo.
echo [STEP 3] Ingest Cost Overruns
echo ----------------------------------------------
python .\bronze\ingest_cost_overruns_file_bronze.py || goto :error

echo.
echo [STEP 4] Ingest Current Analysis
echo ----------------------------------------------
python .\bronze\ingest_current_analysis_file_bronze.py || goto :error

echo.
echo [STEP 5] Run Snowflake Pipeline
echo ----------------------------------------------
snowsql -c dev -f .\run_pipeline.sql || goto :error

echo.
echo ==============================================
echo Pipeline completed successfully
echo Finished at %DATE% %TIME%
echo ==============================================
goto :end


:error
echo.
echo ==============================================
echo ERROR: Pipeline stopped due to failure
echo Time: %DATE% %TIME%
echo ==============================================
exit /b 1


:end
echo.
pause