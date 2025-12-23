USE DATABASE weather_db;

CREATE SCHEMA silver;

CREATE OR REPLACE TABLE weather_silver AS    
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
    bronze.weather_bronze,
    LATERAL FLATTEN(input => payload:forecast:forecastday) fd,
    LATERAL FLATTEN(input => fd.value:hour) h
LIMIt 0;
