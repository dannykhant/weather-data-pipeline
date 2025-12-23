USE weather_db;

-- root task: stage to bronze
CREATE OR REPLACE TASK weather_db.bronze.load_stage_to_bronze
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
    SCHEDULE='USING CRON 0 1 * * *  Asia/Bangkok'
AS
COPY INTO weather_db.bronze.weather_bronze (payload, source_file, load_ts)
FROM (
    SELECT
        $1,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP
    FROM @weather_db.bronze.weather_stage
)
FILE_FORMAT = 'weather_db.bronze.weather_json_format';

-- silver task: bronze to silver
CREATE OR REPLACE TASK weather_db.bronze.load_bronze_to_silver
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
    AFTER weather_db.bronze.load_stage_to_bronze
    WHEN SYSTEM$STREAM_HAS_DATA('bronze.weather_bronze_stream')
AS
MERGE INTO silver.weather_silver tgt
USING (
    SELECT
        fd.value:date::date as date,
        h.value:time::datetime as hour,
        payload:location:name::string as city,
        payload:location:country::string as country,
        payload:location:lat::float as lat,
        payload:location:lon::float as lon,
        fd.value:astro:sunrise::string as sunrise,
        fd.value:astro:sunset::string as sunset,
        fd.value:day:avgtemp_c::float as avg_daily_temp,
        fd.value:day:avghumidity::int as avg_daily_humidity,
        fd.value:day:uv::float as daily_uv,
        h.value:condition:text as condition,
        h.value:temp_c::float as hourly_temp,
        h.value:humidity::int as hourly_humidity,
        current_timestamp as updated_at
    FROM
        bronze.weather_bronze_stream,
        LATERAL FLATTEN(input => payload:forecast:forecastday) fd,
        LATERAL FLATTEN(input => fd.value:hour) h
) src
ON tgt.date = src.date
AND tgt.hour = src.hour
WHEN NOT MATCHED THEN
    INSERT (
        date,
        hour,
        city,
        country,
        lat,
        lon,
        sunrise,
        sunset,
        avg_daily_temp,
        avg_daily_humidity,
        daily_uv,
        condition,
        hourly_temp,
        hourly_humidity,
        updated_at
    )
    VALUES (
        src.date,
        src.hour,
        src.city,
        src.country,
        src.lat,
        src.lon,
        src.sunrise,
        src.sunset,
        src.avg_daily_temp,
        src.avg_daily_humidity,
        src.daily_uv,
        src.condition,
        src.hourly_temp,
        src.hourly_humidity,
        src.updated_at
    );

SHOW TASKS;

ALTER TASK bronze.load_bronze_to_silver RESUME;
ALTER TASK bronze.load_stage_to_bronze RESUME;

EXECUTE TASK bronze.load_stage_to_bronze;

DESC TASK bronze.load_bronze_to_silver;

-- Query task history for the current schema within the last 7 days
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY SCHEDULED_TIME DESC;
