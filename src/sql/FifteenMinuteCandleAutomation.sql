-- ====================================================
-- FULLY AUTOMATED 15-MINUTE CANDLES SOLUTION
-- ====================================================

-- 1. Create enhanced staging table for 15-minute candle mapping
CREATE TABLE IF NOT EXISTS trade_candle_mapping_15m (
    trade_id String,
    contract_address String,
    candle_timestamp DateTime64(3),
    PRIMARY KEY (trade_id)
) ENGINE = MergeTree()
ORDER BY (trade_id);

-- 2. Create the final 15-minute candles table (keeping same structure)
CREATE TABLE IF NOT EXISTS candles_15m (
    candle_id String,
    contract_address String,
    timestamp DateTime64(3),
    end_timestamp DateTime64(3),
    open_price Decimal(38,18),
    high_price Decimal(38,18),
    low_price Decimal(38,18),
    close_price Decimal(38,18),
    latest_trade_timestamp DateTime64(3),
    volume Decimal(38,18),
    trade_count UInt32,
    is_complete UInt8,
    PRIMARY KEY (candle_id)
) ENGINE = ReplacingMergeTree(latest_trade_timestamp)
ORDER BY (candle_id, contract_address, timestamp);

-- 3. Materialized View: Trade to 15-Minute Mapping
DROP VIEW IF EXISTS trade_to_mapping_15m;

CREATE MATERIALIZED VIEW IF NOT EXISTS trade_to_mapping_15m
TO trade_candle_mapping_15m
AS
SELECT
    trade_id,
    contract_address,
    toStartOfFifteenMinutes(trade_timestamp) AS candle_timestamp
FROM trade;

-- 4. Materialized View: 15-Minute Mapping to Candles
DROP VIEW IF EXISTS mapping_to_candles_15m;

CREATE MATERIALIZED VIEW IF NOT EXISTS mapping_to_candles_15m
TO candles_15m
AS
WITH 
    trade_mappings AS (
        SELECT 
            tcm.trade_id,
            tcm.contract_address,
            tcm.candle_timestamp
        FROM trade_candle_mapping_15m tcm
    ),
    trade_data AS (
        SELECT
            tm.trade_id,
            tm.contract_address,
            tm.candle_timestamp AS timestamp,
            addMinutes(tm.candle_timestamp, 15) AS end_timestamp,
            t.tokenPrice,
            t.trade_timestamp,
            t.volume
        FROM trade_mappings tm
        JOIN trade t ON tm.trade_id = t.trade_id
    ),
    candle_aggregation AS (
        SELECT
            contract_address,
            timestamp,
            end_timestamp,
            argMin(tokenPrice, trade_timestamp) AS open_price,
            max(tokenPrice) AS high_price,
            min(tokenPrice) AS low_price,
            argMax(tokenPrice, trade_timestamp) AS close_price,
            max(trade_timestamp) AS latest_trade_timestamp,
            sum(volume) AS volume,
            count() AS trade_count,
            if(now64(3) >= end_timestamp, 1, 0) AS is_complete
        FROM trade_data
        GROUP BY contract_address, timestamp, end_timestamp
    )
SELECT
    concat(ca.contract_address, ':15m:', toString(ca.timestamp)) AS candle_id,
    ca.contract_address,
    ca.timestamp,
    ca.end_timestamp,
    if(c.candle_id IS NULL, 
        ca.open_price, 
        if(ca.latest_trade_timestamp < c.latest_trade_timestamp, 
           c.open_price, 
           ca.open_price)
    ) AS open_price,
    greatest(ifNull(c.high_price, 0), ca.high_price) AS high_price,
    if(c.candle_id IS NULL,
       ca.low_price,
       if(c.low_price = 0, 
          ca.low_price,
          if(ca.low_price = 0,
             c.low_price,
             least(c.low_price, ca.low_price)))
    ) AS low_price,
    if(c.candle_id IS NULL,
       ca.close_price,
       if(ca.latest_trade_timestamp > c.latest_trade_timestamp,
          ca.close_price,
          c.close_price)
    ) AS close_price,
    greatest(ifNull(c.latest_trade_timestamp, toDateTime64('1970-01-01 00:00:00', 3)), 
             ca.latest_trade_timestamp) AS latest_trade_timestamp,
    ifNull(c.volume, toDecimal64(0, 18)) + ca.volume AS volume,
    ifNull(c.trade_count, 0) + ca.trade_count AS trade_count,
    greatest(ifNull(c.is_complete, 0), ca.is_complete) AS is_complete
FROM candle_aggregation ca
LEFT JOIN candles_15m c ON ca.contract_address = c.contract_address AND ca.timestamp = c.timestamp;

-- 5. Add 15-minute metrics to the data_accounting view
CREATE OR REPLACE VIEW data_accounting AS
SELECT
    'Total trades' AS metric,
    count() AS value
FROM trade
UNION ALL
SELECT
    'Trades in 1-second mapping table' AS metric,
    count() AS value
FROM trade_candle_mapping
UNION ALL
SELECT
    'Trades in 1-minute mapping table' AS metric,
    count() AS value
FROM trade_candle_mapping_1m
UNION ALL
SELECT
    'Trades in 15-minute mapping table' AS metric,
    count() AS value
FROM trade_candle_mapping_15m
UNION ALL
SELECT
    'Total 1-second candles' AS metric,
    count() AS value
FROM candles_1s
UNION ALL
SELECT
    'Total 1-minute candles' AS metric,
    count() AS value
FROM candles_1m
UNION ALL
SELECT
    'Total 15-minute candles' AS metric,
    count() AS value
FROM candles_15m
UNION ALL
SELECT
    'Total volume in trades' AS metric,
    sum(volume) AS value
FROM trade
UNION ALL
SELECT
    'Total volume in 1-second candles' AS metric,
    sum(volume) AS value
FROM candles_1s
UNION ALL
SELECT
    'Total volume in 1-minute candles' AS metric,
    sum(volume) AS value
FROM candles_1m
UNION ALL
SELECT
    'Total volume in 15-minute candles' AS metric,
    sum(volume) AS value
FROM candles_15m
UNION ALL
SELECT
    'Trade count in trade table' AS metric,
    count() AS value
FROM trade
UNION ALL
SELECT
    'Trade count in 1-second candles' AS metric,
    sum(trade_count) AS value
FROM candles_1s
UNION ALL
SELECT
    'Trade count in 1-minute candles' AS metric,
    sum(trade_count) AS value
FROM candles_1m
UNION ALL
SELECT
    'Trade count in 15-minute candles' AS metric,
    sum(trade_count) AS value
FROM candles_15m
UNION ALL
SELECT
    'Missing trades in 1-second candles' AS metric,
    (SELECT count() FROM trade) - (SELECT sum(trade_count) FROM candles_1s) AS value
UNION ALL
SELECT
    'Missing trades in 1-minute candles' AS metric,
    (SELECT count() FROM trade) - (SELECT sum(trade_count) FROM candles_1m) AS value
UNION ALL
SELECT
    'Missing trades in 15-minute candles' AS metric,
    (SELECT count() FROM trade) - (SELECT sum(trade_count) FROM candles_15m) AS value;

-- 6. Add 15-minute audit view
CREATE OR REPLACE VIEW trade_audit_15m AS
WITH 
    trade_counts AS (
        SELECT
            contract_address,
            toStartOfFifteenMinutes(trade_timestamp) AS timestamp,
            count() AS raw_count,
            sum(volume) AS raw_volume
        FROM trade
        GROUP BY contract_address, timestamp
    ),
    candle_counts AS (
        SELECT
            contract_address,
            timestamp,
            trade_count AS candle_count,
            volume AS candle_volume
        FROM candles_15m
    )
SELECT
    tc.contract_address,
    tc.timestamp,
    tc.raw_count,
    cc.candle_count,
    tc.raw_count - ifNull(cc.candle_count, 0) AS count_difference,
    tc.raw_volume,
    cc.candle_volume,
    tc.raw_volume - ifNull(cc.candle_volume, 0) AS volume_difference
FROM trade_counts tc
FULL OUTER JOIN candle_counts cc 
    ON tc.contract_address = cc.contract_address 
    AND tc.timestamp = cc.timestamp
WHERE 
    tc.raw_count != ifNull(cc.candle_count, 0) OR
    abs(tc.raw_volume - ifNull(cc.candle_volume, 0)) > 0.000001
ORDER BY abs(tc.raw_count - ifNull(cc.candle_count, 0)) DESC
LIMIT 100;