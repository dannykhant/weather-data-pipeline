CREATE DATABASE weather_db;

CREATE SCHEMA bronze;

USE SCHEMA weather_db.bronze;

CREATE OR REPLACE TABLE weather_bronze (
    payload variant,
    source_file string,
    load_ts timestamp_ntz default current_timestamp
);

-- note: need to create a storage integration first
CREATE STAGE weather_stage
  STORAGE_INTEGRATION = read_weather_s3_integration
  URL = 's3://saanay-data-lake/weather/raw/';

CREATE OR REPLACE FILE FORMAT weather_json_format
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE
  COMMENT = 'weather_json_format';

SELECT $1
FROM @weather_stage/date=2025-12-22/weather_2025-12-22.json
(file_format => 'weather_json_format');
