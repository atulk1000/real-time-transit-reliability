CREATE TABLE IF NOT EXISTS bronze_wmata_train_positions (
    id BIGSERIAL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    response_status INTEGER NOT NULL,
    request_url TEXT NOT NULL,
    request_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_body JSONB NOT NULL,
    record_count INTEGER,
    content_hash TEXT NOT NULL,
    ingestion_batch_id UUID NOT NULL,
    PRIMARY KEY (id, fetched_at)
) PARTITION BY RANGE (fetched_at);

CREATE TABLE IF NOT EXISTS bronze_wmata_train_positions_2026_05
    PARTITION OF bronze_wmata_train_positions
    FOR VALUES FROM ('2026-05-01 00:00:00+00') TO ('2026-06-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS bronze_wmata_train_positions_2026_06
    PARTITION OF bronze_wmata_train_positions
    FOR VALUES FROM ('2026-06-01 00:00:00+00') TO ('2026-07-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS bronze_wmata_train_positions_default
    PARTITION OF bronze_wmata_train_positions DEFAULT;

CREATE TABLE IF NOT EXISTS bronze_wmata_standard_routes (
    id BIGSERIAL PRIMARY KEY,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    response_status INTEGER NOT NULL,
    request_url TEXT NOT NULL,
    request_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_body JSONB NOT NULL,
    record_count INTEGER,
    content_hash TEXT NOT NULL,
    ingestion_batch_id UUID NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze_wmata_stations (
    id BIGSERIAL PRIMARY KEY,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    response_status INTEGER NOT NULL,
    request_url TEXT NOT NULL,
    request_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_body JSONB NOT NULL,
    record_count INTEGER,
    content_hash TEXT NOT NULL,
    ingestion_batch_id UUID NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze_wmata_lines (
    id BIGSERIAL PRIMARY KEY,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    response_status INTEGER NOT NULL,
    request_url TEXT NOT NULL,
    request_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_body JSONB NOT NULL,
    record_count INTEGER,
    content_hash TEXT NOT NULL,
    ingestion_batch_id UUID NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bronze_train_positions_fetched_at
    ON bronze_wmata_train_positions (fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_bronze_train_positions_ingestion_batch
    ON bronze_wmata_train_positions (ingestion_batch_id);

CREATE INDEX IF NOT EXISTS idx_bronze_train_positions_content_hash
    ON bronze_wmata_train_positions (content_hash);

CREATE INDEX IF NOT EXISTS idx_bronze_standard_routes_fetched_at
    ON bronze_wmata_standard_routes (fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_bronze_standard_routes_content_hash
    ON bronze_wmata_standard_routes (content_hash);

CREATE INDEX IF NOT EXISTS idx_bronze_stations_fetched_at
    ON bronze_wmata_stations (fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_bronze_stations_content_hash
    ON bronze_wmata_stations (content_hash);

CREATE INDEX IF NOT EXISTS idx_bronze_lines_fetched_at
    ON bronze_wmata_lines (fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_bronze_lines_content_hash
    ON bronze_wmata_lines (content_hash);

CREATE TABLE IF NOT EXISTS silver_train_position_events (
    id BIGSERIAL PRIMARY KEY,
    source_raw_id BIGINT NOT NULL,
    ingestion_batch_id UUID NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_hash TEXT NOT NULL UNIQUE,
    train_id TEXT,
    train_number TEXT,
    line_code TEXT,
    direction_num INTEGER,
    circuit_id INTEGER,
    destination_station_code TEXT,
    seconds_at_location INTEGER,
    service_type TEXT,
    car_count INTEGER,
    is_valid BOOLEAN NOT NULL,
    validation_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_silver_train_position_events_fetched_at
    ON silver_train_position_events (fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_silver_train_position_events_train_id
    ON silver_train_position_events (train_id, fetched_at DESC);

CREATE TABLE IF NOT EXISTS silver_station_reference (
    station_code TEXT PRIMARY KEY,
    station_name TEXT NOT NULL,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    line_code_1 TEXT,
    line_code_2 TEXT,
    line_code_3 TEXT,
    line_code_4 TEXT,
    station_together_1 TEXT,
    station_together_2 TEXT,
    source_raw_id BIGINT NOT NULL REFERENCES bronze_wmata_stations(id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver_line_reference (
    line_code TEXT PRIMARY KEY,
    display_name TEXT,
    start_station_code TEXT,
    end_station_code TEXT,
    internal_destination_1 TEXT,
    internal_destination_2 TEXT,
    source_raw_id BIGINT NOT NULL REFERENCES bronze_wmata_lines(id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver_route_circuits (
    line_code TEXT NOT NULL,
    track_num INTEGER NOT NULL,
    seq_num INTEGER NOT NULL,
    circuit_id INTEGER NOT NULL,
    station_code TEXT,
    source_raw_id BIGINT NOT NULL REFERENCES bronze_wmata_standard_routes(id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (line_code, track_num, seq_num, circuit_id)
);

CREATE INDEX IF NOT EXISTS idx_silver_route_circuits_lookup
    ON silver_route_circuits (line_code, track_num, circuit_id);

CREATE TABLE IF NOT EXISTS gold_current_train_board (
    train_id TEXT PRIMARY KEY,
    train_number TEXT,
    line_code TEXT,
    line_name TEXT,
    current_location TEXT,
    current_station_code TEXT,
    current_station_name TEXT,
    next_station_code TEXT,
    next_station_name TEXT,
    origin_station_code TEXT,
    origin_station_name TEXT,
    destination_station_code TEXT,
    destination_station_name TEXT,
    direction_num INTEGER,
    direction_label TEXT,
    car_count INTEGER,
    service_status TEXT,
    seconds_at_location INTEGER,
    last_seen_at TIMESTAMPTZ NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    freshness_status TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gold_current_train_board_line
    ON gold_current_train_board (line_code, direction_num);

CREATE TABLE IF NOT EXISTS gold_feed_health (
    source_name TEXT PRIMARY KEY,
    last_successful_fetch_at TIMESTAMPTZ,
    latest_record_count INTEGER,
    latest_response_status INTEGER,
    latest_content_hash TEXT,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold_line_activity_history (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    line_code TEXT NOT NULL,
    line_name TEXT,
    active_trains INTEGER NOT NULL,
    stale_trains INTEGER NOT NULL,
    location_unavailable_trains INTEGER NOT NULL,
    normal_service_trains INTEGER NOT NULL,
    no_passenger_trains INTEGER NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, line_code)
);

CREATE INDEX IF NOT EXISTS idx_gold_line_activity_history_line_time
    ON gold_line_activity_history (line_code, window_start DESC);

CREATE TABLE IF NOT EXISTS gold_feed_health_history (
    source_name TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    latest_record_count INTEGER,
    latest_response_status INTEGER,
    latest_content_hash TEXT,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_name, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_gold_feed_health_history_source_time
    ON gold_feed_health_history (source_name, observed_at DESC);
