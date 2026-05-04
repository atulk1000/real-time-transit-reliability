$ErrorActionPreference = "Stop"

Set-Location (Split-Path -Parent $PSScriptRoot)

.\.venv\Scripts\python.exe -m transit_reliability.ingestion.kafka_producer --refresh-reference

