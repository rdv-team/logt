CREATE DATABASE IF NOT EXISTS tracelog;

-- operations
DROP TABLE IF EXISTS tracelog.operations;

CREATE TABLE tracelog.operations
(
    -- Ключи фильтрации
    dataset               LowCardinality(String),

    session_id            UInt64,
    client_id             UInt64,
    connect_id             UInt64,
    user                  LowCardinality(String),
    vrs_session           String,

    -- Время с микросекундами (истинная точность)
    ts_vrsrequest_us      DateTime64(6, 'UTC'),
    ts_vrsresponse_us     DateTime64(6, 'UTC'),

    -- Время в секундах
    ts_vrsrequest         DateTime('UTC'),
    ts_vrsresponse        DateTime('UTC'),

    -- Прочее
    duration_us           UInt64,
    context               LowCardinality(String) CODEC(ZSTD(6)),

    INDEX idx_ts_vrsresponse  ts_vrsresponse  TYPE minmax GRANULARITY 1,
    INDEX idx_ctx_ng  context TYPE ngrambf_v1(3, 256, 3, 0) GRANULARITY 2
)
ENGINE = MergeTree
PARTITION BY (dataset)
ORDER BY (dataset, ts_vrsresponse, session_id, client_id, ts_vrsresponse_us)
SETTINGS index_granularity = 8192;

-- events
DROP TABLE IF EXISTS tracelog.events;

CREATE TABLE tracelog.events
(
    dataset                 LowCardinality(String),

    session_id              UInt64,
    client_id               UInt64,
    connect_id              UInt64,
    user                    LowCardinality(String),

    ts_event_us             DateTime64(6, 'UTC'),
    ts_event                DateTime('UTC'),

    ts_vrsrequest_us        DateTime64(6, 'UTC'),
    ts_vrsresponse_us       DateTime64(6, 'UTC'),

    event_name              LowCardinality(String),
    duration_us             UInt64,
    space_us                Int64,
    
    event_string            LowCardinality(String) CODEC(ZSTD(6)),
    context                 LowCardinality(String) CODEC(ZSTD(6))

)
ENGINE = MergeTree
PARTITION BY (dataset)
ORDER BY (dataset, session_id, client_id, ts_vrsrequest_us, ts_vrsresponse_us, ts_event_us)
SETTINGS
    index_granularity = 2048,
    index_granularity_bytes = 0;

-- event_stats
DROP TABLE IF EXISTS tracelog.event_stats;

CREATE TABLE tracelog.event_stats
(
    dataset                 LowCardinality(String),

    session_id              UInt64,
    client_id               UInt64,
    connect_id              UInt64,

    ts_vrsrequest_us        DateTime64(6, 'UTC'),
    ts_vrsresponse_us       DateTime64(6, 'UTC'),

    event_name              LowCardinality(String),
    total_duration_us       UInt64,
    percentage              Float32,
    count                   UInt32,
    
    INDEX idx_event_name event_name TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY (dataset)
ORDER BY (dataset, session_id, client_id, ts_vrsrequest_us, ts_vrsresponse_us, event_name)
SETTINGS
    index_granularity = 2048,
    index_granularity_bytes = 0;

-- calls (batch_calls)
DROP TABLE IF EXISTS tracelog.calls;

CREATE TABLE tracelog.calls
(
    dataset                 LowCardinality(String),

    session_id              UInt64,
    client_id               UInt64,
    connect_id              UInt64,

    ts_vrsrequest_us        DateTime64(6, 'UTC'),
    ts_vrsresponse_us       DateTime64(6, 'UTC'),

    event_name              LowCardinality(String),
    duration_us             UInt64,
    cpu_time                UInt64,
    memory                  Int64,
    memory_peak             UInt64
)
ENGINE = MergeTree
PARTITION BY (dataset)
ORDER BY (dataset, session_id, client_id, ts_vrsrequest_us, ts_vrsresponse_us)
SETTINGS
    index_granularity = 2048,
    index_granularity_bytes = 0;
