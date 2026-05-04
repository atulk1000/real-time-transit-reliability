$ErrorActionPreference = "Stop"

Set-Location (Split-Path -Parent $PSScriptRoot)

.\.venv\Scripts\python.exe -c @"
import psycopg

conn = psycopg.connect('postgresql://transit:transit@localhost:55432/transit_reliability')
cur = conn.cursor()
for table in [
    'bronze_wmata_train_positions',
    'silver_train_position_events',
    'gold_current_train_board',
    'gold_line_activity_history',
    'gold_feed_health_history',
]:
    cur.execute(f'select count(*) from {table}')
    print(f'{table}: {cur.fetchone()[0]}')
conn.close()
"@

