@echo off
set PIPEDRIVE_API_TOKEN=%PIPEDRIVE_API_TOKEN%
set SLACK_WEBHOOK_URL=%SLACK_WEBHOOK_URL%
python shopl_sales_report.py %*
