$ErrorActionPreference = "Stop"

Set-Location (Split-Path -Parent $PSScriptRoot)

docker compose up -d postgres
.\.venv\Scripts\python.exe -m transit_reliability.db_migrate
.\.venv\Scripts\python.exe -m transit_reliability.ingestion.worker --refresh-reference --once
.\.venv\Scripts\python.exe -m streamlit run src\transit_reliability\dashboard\app.py

