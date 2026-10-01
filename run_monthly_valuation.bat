@echo off
setlocal
cd /d D:\ProgramData\Backup\stock-monitor
python monitor_stock_valuation.py --config monitor_config.yaml --pool stocks.txt
