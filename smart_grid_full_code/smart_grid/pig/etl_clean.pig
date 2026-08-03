-- ============================================================
--  pig/etl_clean.pig
--  Apache Pig ETL Pipeline for Smart Grid Data
--
--  Steps:
--    1. Load raw London Smart Meter CSVs from HDFS
--    2. Parse, validate, and clean records
--    3. Load household metadata and join on meter ID
--    4. Aggregate 30-min readings to hourly totals
--    5. Compute per-household baseline statistics
--    6. Store clean data back to HDFS as CSV
--       (Spark will convert to Parquet in the next step)
--
--  Run:
--    pig -f pig/etl_clean.pig \
--        -param INPUT=/smart_grid/raw/london/halfhourly_dataset \
--        -param META=/smart_grid/raw/london/informations_households.csv \
--        -param OUTPUT=/smart_grid/clean/meter_readings \
--        -param STATS_OUT=/smart_grid/clean/household_stats
--
--  Or use default paths below.
-- ============================================================

-- ── Parameters (override on command line with -param) ────────
%default INPUT     '/smart_grid/raw/london/halfhourly_dataset'
%default META      '/smart_grid/raw/london/informations_households.csv'
%default WEATHER   '/smart_grid/raw/london/weather_hourly_darksky.csv'
%default OUTPUT    '/smart_grid/clean/meter_readings'
%default STATS_OUT '/smart_grid/clean/household_stats'

-- ============================================================
-- STEP 1: Load raw meter reading CSVs
-- Format: LCLid, tstp, energy(kWh/hh)
-- Example row: MAC000002,2012-10-12 00:30:00.0000000,0.078
-- ============================================================
raw_readings = LOAD '$INPUT/*.csv'
    USING PigStorage(',')
    AS (
        meter_id:   chararray,
        ts_raw:     chararray,
        kwh:        chararray
    );

-- ── Remove header rows (files have headers) ──────────────────
no_headers = FILTER raw_readings
    BY meter_id != 'LCLid'
    AND meter_id != 'MAC';

-- ── Cast and validate numeric fields ─────────────────────────
-- Some rows have 'Null' or 'NULL' for kwh — filter those out
valid_readings = FILTER no_headers
    BY kwh != 'Null'
    AND kwh != 'NULL'
    AND kwh != ''
    AND kwh != 'null'
    AND TRIM(kwh) MATCHES '-?[0-9]+\\.?[0-9]*';

-- ── Cast kwh to float and parse timestamp ────────────────────
-- Trim microseconds from timestamp: "2012-10-12 00:30:00.0000000"
--   → "2012-10-12 00:30:00"
typed_readings = FOREACH valid_readings GENERATE
    TRIM(meter_id)                      AS meter_id:    chararray,
    SUBSTRING(TRIM(ts_raw), 0, 19)      AS ts_str:      chararray,
    (float) TRIM(kwh)                   AS kwh:         float,
    SUBSTRING(TRIM(ts_raw), 0, 10)      AS reading_date:chararray,
    SUBSTRING(TRIM(ts_raw), 0, 7)       AS year_month:  chararray,
    SUBSTRING(TRIM(ts_raw), 0, 4)       AS year:        chararray,
    SUBSTRING(TRIM(ts_raw), 11, 13)     AS hour_str:    chararray;

-- ── Remove physically impossible values ──────────────────────
-- kWh per 30-min slot > 100 is almost certainly corrupt data
-- Negative values are also invalid
range_filtered = FILTER typed_readings
    BY kwh >= 0.0
    AND kwh <= 100.0;

-- ── Remove records with malformed timestamps ─────────────────
valid_ts = FILTER range_filtered
    BY SIZE(ts_str) == 19;

-- ============================================================
-- STEP 2: Load household metadata
-- Format: LCLid, stdorToU, Acorn, Acorn_grouped, file
-- ============================================================
metadata = LOAD '$META'
    USING PigStorage(',')
    AS (
        meter_id:     chararray,
        tariff_type:  chararray,
        acorn:        chararray,
        acorn_group:  chararray,
        data_file:    chararray
    );

-- Remove header
meta_clean = FILTER metadata BY meter_id != 'LCLid';

meta_typed = FOREACH meta_clean GENERATE
    TRIM(meter_id)    AS meter_id:    chararray,
    TRIM(tariff_type) AS tariff_type: chararray,
    TRIM(acorn)       AS acorn:       chararray,
    TRIM(acorn_group) AS acorn_group: chararray;

-- ============================================================
-- STEP 3: Join readings with household metadata
-- ============================================================
readings_with_meta = JOIN valid_ts BY meter_id, meta_typed BY meter_id;

enriched = FOREACH readings_with_meta GENERATE
    valid_ts::meter_id    AS meter_id:    chararray,
    valid_ts::ts_str      AS ts_str:      chararray,
    valid_ts::kwh         AS kwh:         float,
    valid_ts::reading_date AS reading_date:chararray,
    valid_ts::year_month  AS year_month:  chararray,
    valid_ts::year        AS year:        chararray,
    valid_ts::hour_str    AS hour:        chararray,
    meta_typed::tariff_type AS tariff_type:chararray,
    meta_typed::acorn     AS acorn:       chararray,
    meta_typed::acorn_group AS acorn_group:chararray;

-- ============================================================
-- STEP 4: Aggregate to hourly totals
-- Sum the two 30-minute readings within each hour
-- ============================================================
by_hour = GROUP enriched BY (meter_id, reading_date, hour, tariff_type, acorn_group);

hourly_totals = FOREACH by_hour GENERATE
    group.meter_id       AS meter_id:    chararray,
    group.reading_date   AS reading_date:chararray,
    group.hour           AS hour:        chararray,
    group.tariff_type    AS tariff_type: chararray,
    group.acorn_group    AS acorn_group: chararray,
    SUM(enriched.kwh)    AS kwh_hourly:  float,
    AVG(enriched.kwh)    AS kwh_avg:     float,
    MAX(enriched.kwh)    AS kwh_max:     float,
    COUNT(enriched.kwh)  AS reading_count:long;

-- ── Filter hours where we have both 30-min readings ──────────
-- reading_count should be 2; accept 1 if data is sparse
complete_hours = FILTER hourly_totals BY reading_count >= 1;

-- ============================================================
-- STEP 5: Compute per-household statistics (used by ML model)
-- ============================================================
by_meter = GROUP enriched BY meter_id;

household_stats = FOREACH by_meter GENERATE
    group                           AS meter_id:       chararray,
    COUNT(enriched.kwh)             AS total_readings: long,
    AVG(enriched.kwh)               AS global_mean:    double,
    MIN(enriched.kwh)               AS global_min:     float,
    MAX(enriched.kwh)               AS global_max:     float,
    -- Variance approximation: E[X^2] - E[X]^2
    SQRT(
        AVG(enriched.kwh * enriched.kwh)
        - AVG(enriched.kwh) * AVG(enriched.kwh)
    )                               AS global_stddev:  double;

-- ============================================================
-- STEP 6: Load weather data and prepare for joining
-- Format: time, summary, icon, precipIntensity, ...
--         temperatureHigh, temperatureLow, humidity, ...
-- ============================================================
weather_raw = LOAD '$WEATHER'
    USING PigStorage(',')
    AS (
        ts_raw:             chararray,
        summary:            chararray,
        icon:               chararray,
        precip_intensity:   chararray,
        precip_prob:        chararray,
        temp_high:          chararray,
        temp_low:           chararray,
        humidity:           chararray,
        wind_speed:         chararray
    );

weather_clean = FILTER weather_raw
    BY ts_raw != 'time'
    AND temp_high != ''
    AND temp_low  != '';

weather_typed = FOREACH weather_clean GENERATE
    SUBSTRING(TRIM(ts_raw), 0, 10)  AS w_date:       chararray,
    (float) TRIM(temp_high)         AS temp_high_c:  float,
    (float) TRIM(temp_low)          AS temp_low_c:   float,
    (float) TRIM(humidity)          AS humidity:     float;

-- Deduplicate weather by date (take first record per day)
weather_by_date = GROUP weather_typed BY w_date;
weather_daily = FOREACH weather_by_date GENERATE
    group                               AS w_date:      chararray,
    AVG(weather_typed.temp_high_c)      AS temp_high_c: double,
    AVG(weather_typed.temp_low_c)       AS temp_low_c:  double,
    AVG(weather_typed.humidity)         AS humidity:    double;

-- ── Join hourly readings with daily weather ───────────────────
hourly_with_weather = JOIN complete_hours BY reading_date, weather_daily BY w_date;

final_clean = FOREACH hourly_with_weather GENERATE
    complete_hours::meter_id     AS meter_id:    chararray,
    complete_hours::reading_date AS reading_date:chararray,
    complete_hours::hour         AS hour:        chararray,
    complete_hours::tariff_type  AS tariff_type: chararray,
    complete_hours::acorn_group  AS acorn_group: chararray,
    complete_hours::kwh_hourly   AS kwh_hourly:  float,
    complete_hours::kwh_max      AS kwh_max:     float,
    weather_daily::temp_high_c   AS temp_high_c: double,
    weather_daily::temp_low_c    AS temp_low_c:  double,
    weather_daily::humidity      AS humidity:    double;

-- ============================================================
-- STEP 7: Remove existing output and store results
-- ============================================================
-- Clean output directory first (run before pig if needed):
--   hdfs dfs -rm -r /smart_grid/clean/meter_readings
--   hdfs dfs -rm -r /smart_grid/clean/household_stats

STORE final_clean INTO '$OUTPUT'
    USING PigStorage(',');

STORE household_stats INTO '$STATS_OUT'
    USING PigStorage(',');

-- ============================================================
-- DIAGNOSTIC: Quick count to verify output
-- ============================================================
total_clean   = FOREACH (GROUP final_clean ALL) GENERATE COUNT(final_clean);
total_stats   = FOREACH (GROUP household_stats ALL) GENERATE COUNT(household_stats);

DUMP total_clean;
DUMP total_stats;
