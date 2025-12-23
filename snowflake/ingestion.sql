-- bronze ingestion

USE SCHEMA weather_db.bronze;

COPY INTO bronze.weather_bronze (payload, source_file, load_ts)
FROM (
    SELECT
        $1,
        METADATA$FILENAME,
        CURRENT_TIMESTAMP
    FROM @bronze.weather_stage
)
FILE_FORMAT = 'bronze.weather_json_format';

SELECT 
    payload:location,
    payload:forecast,
    source_file,
    load_ts
FROM bronze.weather_bronze;

TRUNCATE TABLE bronze.weather_bronze;

-- silver ingestion

CREATE STREAM bronze.weather_bronze_stream
ON TABLE bronze.weather_bronze;

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

SELECT * FROM silver.weather_silver;

TRUNCATE TABLE silver.weather_silver;
