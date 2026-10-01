@echo off
setlocal
cd /d D:\ProgramData\Backup\stock-monitor
scp "D:\ProgramData\Backup\stock-monitor\monitor_stock_valuation.py" "D:\ProgramData\Backup\stock-monitor\monitor_config.yaml" "D:\ProgramData\Backup\stock-monitor\stocks.txt" "D:\ProgramData\Backup\stock-monitor\requirements.txt" "ubuntu@111.229.8.177:/opt/stock-monitor/"
if errorlevel 1 (
  echo.
  echo Upload failed.
  exit /b 1
)
echo.
echo Upload completed.
