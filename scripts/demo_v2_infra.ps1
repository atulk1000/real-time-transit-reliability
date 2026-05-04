$ErrorActionPreference = "Stop"

Set-Location (Split-Path -Parent $PSScriptRoot)

docker compose up -d postgres redpanda redpanda-init redpanda-console spark-reference-stream spark-train-stream
.\.venv\Scripts\python.exe -m transit_reliability.db_migrate

Write-Host ""
Write-Host "V2 infra is starting."
Write-Host "Redpanda Console: http://127.0.0.1:8080"
Write-Host "Next terminal: python -m transit_reliability.ingestion.kafka_producer --refresh-reference"

