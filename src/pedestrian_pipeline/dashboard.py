"""Streamlit dashboard over the ingested warehouse.

Run with: streamlit run src/pedestrian_pipeline/dashboard.py
"""

from pathlib import Path

import altair as alt
import polars as pl
import streamlit as st

from pedestrian_pipeline.config import Settings
from pedestrian_pipeline.quality import Severity, run_checks
from pedestrian_pipeline.warehouse import Warehouse

SURFACE = "#fcfcfb"
SERIES = "#2a78d6"
INK = "#0b0b0b"
MUTED = "#898781"
GRID = "#e1e0d9"
GOOD = "#0ca30c"
CRITICAL = "#d03b3b"

st.set_page_config(
    page_title="Melbourne Pedestrian Pipeline",
    page_icon="🚶",
    layout="wide",
)


@st.cache_resource
def open_warehouse(path: str) -> Warehouse:
    return Warehouse(Path(path), read_only=True).__enter__()


@st.cache_data(ttl=300)
def query(path: str, sql: str) -> pl.DataFrame:
    return open_warehouse(path).query(sql)


def hourly_chart(frame: pl.DataFrame) -> alt.LayerChart:
    base = alt.Chart(frame).encode(
        x=alt.X(
            "local_hour:Q",
            title="Hour of day (Melbourne local)",
            axis=alt.Axis(tickCount=12),
            scale=alt.Scale(domain=[0, 23], nice=False),
        ),
        y=alt.Y("avg_per_sensor_minute:Q", title="Pedestrians per sensor-minute"),
        tooltip=[
            alt.Tooltip("local_hour:Q", title="Hour"),
            alt.Tooltip("avg_per_sensor_minute:Q", title="Per sensor-minute"),
            alt.Tooltip("pedestrians:Q", title="Total counted", format=","),
            alt.Tooltip("sensors_reporting:Q", title="Sensors reporting"),
        ],
    )
    area = base.mark_area(
        line={"color": SERIES, "strokeWidth": 2},
        color=alt.Gradient(
            gradient="linear",
            stops=[
                alt.GradientStop(color="#ffffff", offset=0),
                alt.GradientStop(color=SERIES, offset=1),
            ],
            x1=1,
            x2=1,
            y1=1,
            y2=0,
        ),
        opacity=0.25,
        interpolate="monotone",
    )
    # An hour whose neighbours are both null has no segment to draw, so the area
    # mark alone would render it as nothing. The point layer keeps isolated
    # observations visible.
    points = base.mark_point(filled=True, size=55, color=SERIES, opacity=1)
    return alt.layer(area, points, height=280)


def ranked_bar(frame: pl.DataFrame, value: str, label: str, height: int) -> alt.Chart:
    return (
        alt.Chart(frame, height=height)
        .mark_bar(color=SERIES, cornerRadiusEnd=4, height=alt.RelativeBandSize(0.72))
        .encode(
            x=alt.X(f"{value}:Q", title=label, axis=alt.Axis(format="~s")),
            y=alt.Y("sensor_description:N", title=None, sort="-x"),
            tooltip=[
                alt.Tooltip("sensor_description:N", title="Sensor"),
                alt.Tooltip(f"{value}:Q", title=label, format=","),
            ],
        )
    )


def apply_chrome(chart: alt.Chart) -> alt.Chart:
    return (
        chart.configure_view(stroke=None)
        .configure_axis(
            domainColor=GRID,
            gridColor=GRID,
            tickColor=GRID,
            labelColor=MUTED,
            titleColor=MUTED,
            labelFontSize=12,
            titleFontSize=12,
            titleFontWeight="normal",
        )
        .configure_axisY(grid=False, domain=False, ticks=False, labelLimit=260, labelOverlap=False)
        .configure_axisX(grid=True)
    )


def main() -> None:
    settings = Settings()

    st.sidebar.title("Melbourne Pedestrian Pipeline")
    database = Path(st.sidebar.text_input("Warehouse", value=str(settings.database_path)))

    if not database.exists():
        st.title("Melbourne Pedestrian Pipeline")
        st.warning(f"No warehouse found at `{database}`. Run `uv run ingest` to build one.")
        return

    path = str(database)
    top_n = st.sidebar.slider("Sensors to rank", 5, 25, 12)
    st.sidebar.caption(
        "Readings are upserted on (location_id, sensing_datetime), so re-running "
        "the pipeline over an overlapping window never double-counts."
    )

    freshness = query(
        path,
        """
        SELECT count(*) AS readings, count(DISTINCT location_id) AS sensors,
               min(local_datetime) AS earliest, max(local_datetime) AS latest,
               date_diff('minute', max(sensing_datetime), max(ingested_at)) AS lag_minutes
        FROM fact_pedestrian_count
        """,
    ).row(0, named=True)
    registered = query(path, "SELECT count(*) AS n FROM dim_sensor").item(0, "n")

    st.title("Melbourne CBD foot traffic")
    st.caption(
        f"Window {freshness['earliest']:%d %b %H:%M} – {freshness['latest']:%d %b %H:%M} "
        f"(Melbourne local) · ingestion lag {freshness['lag_minutes']} min"
    )

    kpis = st.columns(4)
    kpis[0].metric("Readings stored", f"{freshness['readings']:,}")
    kpis[1].metric(
        "Sensors reporting",
        f"{freshness['sensors']} / {registered}",
        delta=f"{freshness['sensors'] - registered} inactive",
        delta_color="inverse",
    )
    busiest = query(
        path,
        "SELECT max(total_of_directions) AS m FROM fact_pedestrian_count",
    ).item(0, "m")
    kpis[2].metric("Busiest single minute", f"{busiest:,}")

    report = run_checks(open_warehouse(path))
    kpis[3].metric(
        "Quality checks",
        f"{len(report.results) - len(report.failures)} / {len(report.results)} passing",
        delta="all green" if report.passed else f"{len(report.errors)} error(s)",
        delta_color="normal" if report.passed else "inverse",
    )

    st.divider()

    st.subheader("The city's daily rhythm")
    st.caption(
        "Pedestrians per sensor-minute by hour of day, in Melbourne local time. Averaging "
        "per sensor-minute rather than plotting raw totals keeps partly-captured hours "
        "comparable — the final hour of a window holds only a few minutes of readings, so "
        "its total understates it by an order of magnitude. Gaps are hours the ingested "
        "window does not cover; they fill in as the hourly job accumulates history."
    )
    # Left-join onto a full 0-23 spine so uncovered hours arrive as null and break
    # the line, rather than being drawn as a plausible-looking zero.
    hourly = query(
        path,
        """
        WITH spine AS (SELECT unnest(generate_series(0, 23)) AS local_hour),
             agg AS (
                 SELECT local_hour,
                        count(*) AS readings,
                        count(DISTINCT location_id) AS sensors_reporting,
                        sum(total_of_directions) AS pedestrians
                 FROM fact_pedestrian_count GROUP BY 1
             )
        SELECT s.local_hour, a.pedestrians, a.sensors_reporting,
               round(a.pedestrians::DOUBLE / a.readings, 1) AS avg_per_sensor_minute
        FROM spine s LEFT JOIN agg a USING (local_hour)
        ORDER BY s.local_hour
        """,
    )
    st.altair_chart(apply_chrome(hourly_chart(hourly)), width="stretch")

    left, right = st.columns(2)

    with left:
        st.subheader("Busiest locations")
        st.caption("Total pedestrians counted across the ingested window.")
        busiest_sensors = query(
            path,
            f"""
            SELECT d.sensor_description, sum(f.total_of_directions) AS pedestrians
            FROM fact_pedestrian_count f JOIN dim_sensor d USING (location_id)
            GROUP BY 1 ORDER BY pedestrians DESC LIMIT {top_n}
            """,
        )
        st.altair_chart(
            apply_chrome(ranked_bar(busiest_sensors, "pedestrians", "Pedestrians", 30 * top_n)),
            width="stretch",
        )

    with right:
        st.subheader("Least reliable sensors")
        st.caption(
            "The lowest-coverage sensors, as a share of the ingested window. No sensor "
            "reports continuously, so averaging across sensors without weighting for "
            "coverage produces the wrong answer."
        )
        coverage = query(
            path,
            f"""
            WITH w AS (SELECT count(DISTINCT sensing_datetime) AS e FROM fact_pedestrian_count)
            SELECT d.sensor_description,
                   round(100.0 * count(*) / (SELECT e FROM w), 1) AS coverage_pct
            FROM fact_pedestrian_count f JOIN dim_sensor d USING (location_id)
            GROUP BY 1 ORDER BY coverage_pct ASC LIMIT {top_n}
            """,
        )
        st.altair_chart(
            apply_chrome(
                ranked_bar(coverage, "coverage_pct", "Minutes reported (% of window)", 30 * top_n)
            ),
            width="stretch",
        )

    st.divider()

    map_left, map_right = st.columns([3, 2])

    with map_left:
        st.subheader("Where the sensors are")
        st.caption("Marker size scales with total pedestrians counted at that location.")
        locations = query(
            path,
            """
            SELECT d.latitude, d.longitude, sum(f.total_of_directions) AS pedestrians
            FROM fact_pedestrian_count f JOIN dim_sensor d USING (location_id)
            GROUP BY 1, 2
            """,
        ).with_columns((pl.col("pedestrians").sqrt() * 0.5 + 12).alias("size"))
        # Semi-transparent so the dense CBD cluster stays readable where markers overlap.
        st.map(locations.to_pandas(), size="size", color=f"{SERIES}b3", zoom=13)

    with map_right:
        st.subheader("Data quality")
        st.caption(
            "Assertions run against the warehouse after every load. Blocking failures "
            "exit the run non-zero; advisory ones are reported and tolerated."
        )
        for result in report.results:
            icon = "✓" if result.passed else "✗"
            colour = GOOD if result.passed else CRITICAL
            # The severity describes what a failure would cost, so label it that way
            # rather than "error" — a green tick beside the word "error" reads as one.
            tier = "blocking" if result.check.severity is Severity.ERROR else "advisory"
            st.markdown(
                f"<div style='padding:6px 0;border-bottom:1px solid {GRID};'>"
                f"<span style='color:{colour};font-weight:600;'>{icon}</span> "
                f"<span style='color:{INK};'>{result.check.name}</span> "
                f"<span style='color:{MUTED};float:right;font-size:0.85em;'>"
                f"{tier}</span></div>",
                unsafe_allow_html=True,
            )


# Streamlit executes the script as __main__, so this runs under `streamlit run`
# while leaving the chart builders importable for tests.
if __name__ == "__main__":
    main()
