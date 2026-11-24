from pathlib import Path
from streamlit_autorefresh import st_autorefresh

import pandas as pd
import plotly.express as px
import streamlit as st


# --------------------------------------------------
# PAGE CONFIGURATION
# --------------------------------------------------

st.set_page_config(
    page_title="StreamSense | IoT Analytics",
    page_icon="📡",
    layout="wide"
)

# --------------------------------------------------
# AUTOMATIC DASHBOARD REFRESH
# --------------------------------------------------

refresh_count = st_autorefresh(
    interval=10_000,
    limit=None,
    key="streamsense_auto_refresh"
)

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_PATH = BASE_DIR / "output" / "parquet"
ANALYTICS_PATH = BASE_DIR / "output" / "minute_analytics"


# --------------------------------------------------
# DATA LOADING
# --------------------------------------------------


@st.cache_data(ttl=5)
def load_sensor_data():
    required = {
        "sensor_id", "temperature", "humidity", "timestamp"
    }

    valid_frames = []

    for file in RAW_PATH.glob("*.parquet"):
        try:
            if file.stat().st_size == 0:
                continue

            temp_df = pd.read_parquet(file)

            if required.issubset(temp_df.columns):
                valid_frames.append(temp_df)

        except Exception:
            continue

    if not valid_frames:
        raise ValueError("No readable sensor Parquet files found.")

    df = pd.concat(valid_frames, ignore_index=True)

    df["sensor_id"] = df["sensor_id"].astype(str)
    df["readable_time"] = pd.to_datetime(
        df["timestamp"], unit="s", errors="coerce"
    )

    df = df.dropna(
        subset=["sensor_id", "temperature", "humidity", "readable_time"]
    )

    return df


@st.cache_data(ttl=5)
def load_minute_analytics():
    df = pd.read_parquet(ANALYTICS_PATH)

    df["minute"] = pd.to_datetime(
        df["minute"], errors="coerce"
    )

    return df.dropna(subset=["minute"])


# --------------------------------------------------
# LOAD AND VALIDATE
# --------------------------------------------------

st.title("📡 StreamSense")
st.subheader("Real-Time IoT Data Analytics")

st.caption(
    "Kafka • Apache Spark Structured Streaming • Parquet • Python"
)

st.caption(
    f"Dashboard refresh cycle: 10 seconds | "
    f"Refresh number: {refresh_count}"
)

try:
    sensor_df = load_sensor_data()
    analytics_df = load_minute_analytics()

except Exception as error:
    st.error(f"Unable to load project data: {error}")
    st.info(
        "Ensure Spark has generated the Parquet files "
        "in the output folder."
    )
    st.stop()


# --------------------------------------------------
# SIDEBAR FILTERS
# --------------------------------------------------

st.sidebar.title("Dashboard Controls")

sensors = sorted(sensor_df["sensor_id"].unique())

selected_sensors = st.sidebar.multiselect(
    "Select sensors",
    options=sensors,
    default=sensors
)

threshold = st.sidebar.slider(
    "Temperature alert threshold (°C)",
    min_value=20,
    max_value=35,
    value=28
)

filtered_df = sensor_df[
    sensor_df["sensor_id"].isin(selected_sensors)
].copy()

if filtered_df.empty:
    st.warning("Select at least one sensor.")
    st.stop()


# --------------------------------------------------
# KPI METRICS
# --------------------------------------------------

total_readings = len(filtered_df)
active_sensors = filtered_df["sensor_id"].nunique()
avg_temperature = filtered_df["temperature"].mean()
avg_humidity = filtered_df["humidity"].mean()

alert_count = (
    filtered_df["temperature"] > threshold
).sum()

st.markdown("### Overview")

c1, c2, c3, c4, c5 = st.columns(5)

c1.metric("Total Readings", f"{total_readings:,}")
c2.metric("Active Sensors", active_sensors)
c3.metric("Avg Temperature", f"{avg_temperature:.2f} °C")
c4.metric("Avg Humidity", f"{avg_humidity:.2f} %")
c5.metric("Threshold Alerts", f"{alert_count:,}")


st.divider()


# --------------------------------------------------
# TEMPERATURE AND HUMIDITY TRENDS
# --------------------------------------------------

st.markdown("### Sensor Trends")

left, right = st.columns(2)

with left:
    temperature_fig = px.line(
        filtered_df.sort_values("readable_time"),
        x="readable_time",
        y="temperature",
        color="sensor_id",
        title="Temperature Over Time",
        labels={
            "readable_time": "Time",
            "temperature": "Temperature (°C)",
            "sensor_id": "Sensor"
        }
    )

    temperature_fig.add_hline(
        y=threshold,
        line_dash="dash",
        annotation_text="Alert threshold"
    )

    st.plotly_chart(
        temperature_fig,
        use_container_width=True
    )


with right:
    humidity_fig = px.line(
        filtered_df.sort_values("readable_time"),
        x="readable_time",
        y="humidity",
        color="sensor_id",
        title="Humidity Over Time",
        labels={
            "readable_time": "Time",
            "humidity": "Humidity (%)",
            "sensor_id": "Sensor"
        }
    )

    st.plotly_chart(
        humidity_fig,
        use_container_width=True
    )


# --------------------------------------------------
# SENSOR COMPARISON
# --------------------------------------------------

st.divider()
st.markdown("### Sensor-wise Comparison")

sensor_summary = (
    filtered_df.groupby("sensor_id")
    .agg(
        avg_temperature=("temperature", "mean"),
        avg_humidity=("humidity", "mean"),
        total_readings=("temperature", "count")
    )
    .reset_index()
)

comparison_fig = px.bar(
    sensor_summary,
    x="sensor_id",
    y="avg_temperature",
    color="sensor_id",
    title="Average Temperature by Sensor",
    labels={
        "sensor_id": "Sensor ID",
        "avg_temperature": "Average Temperature (°C)"
    }
)

st.plotly_chart(
    comparison_fig,
    use_container_width=True
)

st.dataframe(
    sensor_summary.round(2),
    use_container_width=True,
    hide_index=True
)


# --------------------------------------------------
# SENSOR HEALTH MONITORING
# --------------------------------------------------

st.divider()
st.markdown("### Sensor Health Monitoring")

latest_sensor_data = (
    filtered_df
    .sort_values("readable_time")
    .drop_duplicates(
        subset=["sensor_id"],
        keep="last"
    )
    .copy()
)

latest_sensor_data["status"] = latest_sensor_data[
    "temperature"
].apply(
    lambda temp: "Alert" if temp > threshold else "Normal"
)

health_summary = latest_sensor_data[
    [
        "sensor_id",
        "temperature",
        "humidity",
        "readable_time",
        "status"
    ]
].rename(
    columns={
        "sensor_id": "Sensor ID",
        "temperature": "Latest Temperature (°C)",
        "humidity": "Latest Humidity (%)",
        "readable_time": "Last Reading Time",
        "status": "Reading Status"
    }
)

normal_count = (
    latest_sensor_data["status"] == "Normal"
).sum()

alert_sensor_count = (
    latest_sensor_data["status"] == "Alert"
).sum()

h1, h2, h3 = st.columns(3)

h1.metric("Sensors Reporting", len(latest_sensor_data))
h2.metric("Normal Readings", int(normal_count))
h3.metric("Sensors Above Threshold", int(alert_sensor_count))

st.dataframe(
    health_summary,
    use_container_width=True,
    hide_index=True
)

st.caption(
    "Status is based on each sensor's latest available reading "
    "and the selected temperature threshold."
)



# --------------------------------------------------
# THRESHOLD ANALYSIS
# --------------------------------------------------

st.divider()
st.markdown("### Temperature Threshold Analysis")

alert_df = filtered_df[
    filtered_df["temperature"] > threshold
]

a1, a2 = st.columns(2)

with a1:
    st.metric("Readings Above Threshold", len(alert_df))

    if len(alert_df) > 0:
        st.warning(
            f"{len(alert_df)} readings exceeded "
            f"{threshold} °C."
        )
    else:
        st.success("No readings exceeded the threshold.")

with a2:
    alert_summary = (
        alert_df.groupby("sensor_id")
        .size()
        .reset_index(name="alert_count")
    )

    if not alert_summary.empty:
        alert_fig = px.bar(
            alert_summary,
            x="sensor_id",
            y="alert_count",
            title="Threshold Readings by Sensor",
            labels={
                "sensor_id": "Sensor",
                "alert_count": "Number of readings"
            }
        )

        st.plotly_chart(
            alert_fig,
            use_container_width=True
        )


# --------------------------------------------------
# ALERT HISTORY
# --------------------------------------------------

st.divider()
st.markdown("### Sensor Alert History")

alert_history = (
    filtered_df[
        filtered_df["temperature"] > threshold
    ]
    .sort_values("readable_time", ascending=False)
    .copy()
)

if not alert_history.empty:

    alert_history["Alert Status"] = "Threshold Exceeded"

    alert_history = alert_history.rename(
        columns={
            "sensor_id": "Sensor ID",
            "temperature": "Temperature (°C)",
            "humidity": "Humidity (%)",
            "readable_time": "Timestamp"
        }
    )

    st.metric(
        "Total Recorded Alert Events",
        f"{len(alert_history):,}"
    )

    st.dataframe(
        alert_history[
            [
                "Sensor ID",
                "Temperature (°C)",
                "Humidity (%)",
                "Timestamp",
                "Alert Status"
            ]
        ].head(100),
        use_container_width=True,
        hide_index=True
    )

    alert_csv = alert_history.to_csv(index=False).encode("utf-8")

    st.download_button(
        "Download Alert History (CSV)",
        data=alert_csv,
        file_name="streamsense_alert_history.csv",
        mime="text/csv"
    )

else:
    st.success("No threshold-exceeding readings recorded.")


# --------------------------------------------------
# LIVE MINUTE-LEVEL ANALYTICS
# --------------------------------------------------

st.divider()
st.markdown("### Live Minute-level Analytics")

if not analytics_df.empty:

    m1, m2, m3 = st.columns(3)

    m1.metric(
        "Completed Minutes",
        len(analytics_df)
    )

    m2.metric(
        "Avg Temperature",
        f"{analytics_df['avg_temperature'].mean():.2f} °C"
    )

    m3.metric(
        "Total Aggregated Readings",
        f"{analytics_df['total_readings'].sum():,}"
    )

    left, right = st.columns(2)

    with left:
        minute_temp_fig = px.line(
            analytics_df.sort_values("minute"),
            x="minute",
            y="avg_temperature",
            markers=True,
            title="Average Temperature per Minute"
        )
        st.plotly_chart(
            minute_temp_fig,
            use_container_width=True
        )

    with right:
        minute_humidity_fig = px.line(
            analytics_df.sort_values("minute"),
            x="minute",
            y="avg_humidity",
            markers=True,
            title="Average Humidity per Minute"
        )
        st.plotly_chart(
            minute_humidity_fig,
            use_container_width=True
        )

    st.markdown("#### Minute-wise Reading Details")

    st.dataframe(
        analytics_df.sort_values(
            "minute", ascending=False
        ),
        use_container_width=True,
        hide_index=True
    )

else:
    st.info("Waiting for completed streaming minute windows...")



# --------------------------------------------------
# RAW DATA TABLE AND DOWNLOAD
# --------------------------------------------------

st.divider()
st.markdown("### Sensor Records")

st.dataframe(
    filtered_df.sort_values(
        "readable_time", ascending=False
    ),
    use_container_width=True,
    hide_index=True
)

csv_data = filtered_df.to_csv(index=False).encode("utf-8")

st.download_button(
    "Download Sensor Data (CSV)",
    data=csv_data,
    file_name="streamsense_sensor_data.csv",
    mime="text/csv"
)


# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.divider()

st.caption(
    "StreamSense | Academic Big Data Analytics Project | "
    "Data sourced from Spark Parquet output"
)