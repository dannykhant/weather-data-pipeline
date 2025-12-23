import streamlit as st
from snowflake.snowpark.context import get_active_session

# --------------------------------------------------
# Snowflake session
# --------------------------------------------------
session = get_active_session()

st.set_page_config(
    page_title="Bangkok Weather Dashboard",
    layout="wide"
)

st.title("Bangkok Weather Dashboard")
st.caption("Silver-layer analytics from WEATHER_DB.SILVER.WEATHER_SILVER")

# --------------------------------------------------
# Sidebar – date filter only
# --------------------------------------------------
st.sidebar.header("Date Filter")

date_bounds = session.sql("""
    SELECT 
        MIN(DATE) AS MIN_DATE,
        MAX(DATE) AS MAX_DATE
    FROM WEATHER_DB.SILVER.WEATHER_SILVER
""").to_pandas().iloc[0]

start_date, end_date = st.sidebar.date_input(
    "Date range",
    value=(date_bounds["MIN_DATE"], date_bounds["MAX_DATE"])
)

# --------------------------------------------------
# Load data
# --------------------------------------------------
query = f"""
SELECT
    DATE,
    HOUR,
    LAT,
    LON,
    SUNRISE,
    SUNSET,
    AVG_DAILY_TEMP,
    AVG_DAILY_HUMIDITY,
    DAILY_UV,
    CONDITION,
    HOURLY_TEMP,
    HOURLY_HUMIDITY,
    UPDATED_AT
FROM WEATHER_DB.SILVER.WEATHER_SILVER
WHERE DATE BETWEEN '{start_date}' AND '{end_date}'
ORDER BY HOUR
"""

df = session.sql(query).to_pandas()

# --------------------------------------------------
# KPI section
# --------------------------------------------------
st.subheader("Daily Overview")

k1, k2, k3, k4 = st.columns(4)

k1.metric(
    "Avg Daily Temperature (°C)",
    f"{df['AVG_DAILY_TEMP'].mean():.1f}"
)

k2.metric(
    "Avg Daily Humidity (%)",
    f"{df['AVG_DAILY_HUMIDITY'].mean():.0f}"
)

k3.metric(
    "Daily UV Index",
    f"{df['DAILY_UV'].mean():.1f}"
)

k4.metric(
    "Weather Condition",
    df["CONDITION"].mode().iloc[0]
)

# --------------------------------------------------
# Hourly trends
# --------------------------------------------------
st.subheader("Hourly Trends")

c1, c2 = st.columns(2)

with c1:
    st.line_chart(
        df.set_index("HOUR")["HOURLY_TEMP"],
        height=300
    )
    st.caption("Hourly Temperature (°C)")

with c2:
    st.line_chart(
        df.set_index("HOUR")["HOURLY_HUMIDITY"],
        height=300
    )
    st.caption("Hourly Humidity (%)")

# --------------------------------------------------
# Sun cycle context
# --------------------------------------------------
st.subheader("Sunrise & Sunset")

sun_df = (
    df[["DATE", "SUNRISE", "SUNSET"]]
    .drop_duplicates()
    .sort_values("DATE")
)

st.dataframe(sun_df, use_container_width=True)

# --------------------------------------------------
# Location (static, local)
# --------------------------------------------------
st.subheader("Location")

st.map(
    df[["LAT", "LON"]].drop_duplicates()
)

# --------------------------------------------------
# Data freshness
# --------------------------------------------------
st.subheader("Data Freshness")

st.info(
    f"Latest record updated at: {df['UPDATED_AT'].max()}"
)

# --------------------------------------------------
# Raw data
# --------------------------------------------------
with st.expander("Show silver-layer data"):
    st.dataframe(df, use_container_width=True)
