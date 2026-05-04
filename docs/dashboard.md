# Train Dashboard Definition

## Product Frame

The train MVP is a WMATA Metro operations dashboard. The user is a data/platform engineer, transit analyst, or operations lead who wants a live train board by line color.

## Main View

### KPI Strip

- Active trains: unique trains seen in the latest snapshot
- Stale trains: trains not refreshed within the freshness threshold
- Stale share: stale trains divided by active trains
- Lines active: line colors represented in the latest board

### Live Rail Map

The MVP intentionally uses a table-first train board instead of a map because WMATA train positions are circuit-based rather than latitude/longitude vehicle points.

### Line Summary Table

Columns:

- line_code
- line_name
- active_trains
- stale_trains
- stale_pct
- last_seen_at

### Anomaly Panel

Initial status fields:

- Fresh or stale train update
- Missing line code
- Missing destination
- Location unavailable when a circuit cannot be mapped

Later possible anomalies:

- Train bunching
- Station delay hotspot
- Service alert impact

## Train Board

The train board shows:

- Train number
- Current location
- Next station
- Origin and destination
- Direction
- Car count
- Service status
- Feed freshness

## Historical Metrics

Historical metrics are shown in a separate dashboard tab and are populated by the V2 Spark Structured Streaming path.

Initial metrics:

- Active trains by line over time
- Location unavailable count by line over time
- Feed-health snapshots over time

## Later Bus Extension

The same dashboard shell should support bus routes later, but bus route detail should emphasize headway adherence and bunching rather than station timeline reliability.
