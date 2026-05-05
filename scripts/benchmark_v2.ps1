param(
    [int]$DurationMinutes = 30,
    [int]$DurationSeconds = 0
)

$ErrorActionPreference = "Stop"

Set-Location (Split-Path -Parent $PSScriptRoot)

$TotalSeconds = if ($DurationSeconds -gt 0) { $DurationSeconds } else { $DurationMinutes * 60 }

if ($TotalSeconds -le 0) {
    throw "Benchmark duration must be greater than zero seconds."
}

$Query = @"
SELECT
    (SELECT count(*) FROM bronze_wmata_train_positions),
    (SELECT count(*) FROM silver_train_position_events),
    (SELECT count(*) FROM gold_line_activity_history),
    (SELECT count(*) FROM gold_feed_health_history),
    COALESCE((
        SELECT EXTRACT(EPOCH FROM (now() - max(fetched_at)))::int
        FROM silver_train_position_events
    ), -1);
"@

function Get-BenchmarkSnapshot {
    $raw = docker compose exec -T postgres psql `
        -U transit `
        -d transit_reliability `
        -At `
        -F "," `
        -c $Query

    $parts = $raw.Trim().Split(",")

    [PSCustomObject]@{
        CapturedAt = Get-Date
        BronzeTrainPositions = [int64]$parts[0]
        SilverTrainPositionEvents = [int64]$parts[1]
        LineActivityHistory = [int64]$parts[2]
        FeedHealthHistory = [int64]$parts[3]
        LatestSilverLagSeconds = [int]$parts[4]
    }
}

Write-Host "Starting V2 benchmark for $TotalSeconds seconds."
Write-Host "Make sure Redpanda, Spark streams, Postgres, and the producer are running."
Write-Host ""

$start = Get-BenchmarkSnapshot
Start-Sleep -Seconds $TotalSeconds
$end = Get-BenchmarkSnapshot

$elapsedSeconds = ($end.CapturedAt - $start.CapturedAt).TotalSeconds
$silverDelta = $end.SilverTrainPositionEvents - $start.SilverTrainPositionEvents
$bronzeDelta = $end.BronzeTrainPositions - $start.BronzeTrainPositions

Write-Host ""
Write-Host "Benchmark result"
Write-Host "----------------"
Write-Host ("Elapsed seconds:          {0:N0}" -f $elapsedSeconds)
Write-Host ("Bronze rows ingested:     {0:N0}" -f $bronzeDelta)
Write-Host ("Silver records produced:  {0:N0}" -f $silverDelta)
Write-Host ("Silver records / second:  {0:N2}" -f ($silverDelta / $elapsedSeconds))
Write-Host ("Line history rows added:  {0:N0}" -f ($end.LineActivityHistory - $start.LineActivityHistory))
Write-Host ("Feed history rows added:  {0:N0}" -f ($end.FeedHealthHistory - $start.FeedHealthHistory))
Write-Host ("Latest silver lag seconds at end: {0}" -f $end.LatestSilverLagSeconds)
Write-Host ""
Write-Host "Start snapshot:"
$start | Format-List
Write-Host "End snapshot:"
$end | Format-List
