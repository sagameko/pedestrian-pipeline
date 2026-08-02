-- Analytical queries over the ingested star schema.
-- Each query is delimited by a "-- name:" marker and loaded by analytics/__init__.py.

-- name: busiest_sensors
-- Which parts of the CBD carry the most foot traffic?
SELECT
    d.sensor_description,
    d.location_type,
    count(*)                        AS readings,
    sum(f.total_of_directions)      AS pedestrians,
    round(avg(f.total_of_directions), 1) AS avg_per_minute,
    max(f.total_of_directions)      AS busiest_minute
FROM fact_pedestrian_count f
JOIN dim_sensor d USING (location_id)
GROUP BY d.sensor_description, d.location_type
ORDER BY pedestrians DESC
LIMIT 15;

-- name: hourly_profile
-- The city's daily rhythm, in Melbourne local time.
SELECT
    f.local_hour,
    count(DISTINCT f.location_id)        AS sensors_reporting,
    sum(f.total_of_directions)           AS pedestrians,
    round(avg(f.total_of_directions), 1) AS avg_per_sensor_minute
FROM fact_pedestrian_count f
GROUP BY f.local_hour
ORDER BY f.local_hour;

-- name: directional_imbalance
-- Sensors with lopsided flow: useful for spotting commute direction and
-- one-way pedestrian routes. Only sensors with labelled directions qualify.
SELECT
    d.sensor_description,
    d.direction_1_label,
    d.direction_2_label,
    sum(f.direction_1) AS direction_1_total,
    sum(f.direction_2) AS direction_2_total,
    round(
        100.0 * abs(sum(f.direction_1) - sum(f.direction_2))
        / nullif(sum(f.total_of_directions), 0),
        1
    ) AS imbalance_pct
FROM fact_pedestrian_count f
JOIN dim_sensor d USING (location_id)
WHERE d.direction_1_label IS NOT NULL
GROUP BY d.sensor_description, d.direction_1_label, d.direction_2_label
HAVING sum(f.total_of_directions) > 0
ORDER BY imbalance_pct DESC
LIMIT 15;

-- name: peak_hour_by_sensor
-- Each sensor's busiest local hour, using a window function to rank within sensor.
WITH hourly AS (
    SELECT
        f.location_id,
        f.local_hour,
        sum(f.total_of_directions) AS pedestrians
    FROM fact_pedestrian_count f
    GROUP BY f.location_id, f.local_hour
),
ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY location_id ORDER BY pedestrians DESC) AS rn
    FROM hourly
)
SELECT
    d.sensor_description,
    r.local_hour AS peak_hour,
    r.pedestrians
FROM ranked r
JOIN dim_sensor d USING (location_id)
WHERE r.rn = 1
ORDER BY r.pedestrians DESC
LIMIT 15;

-- name: sensor_coverage
-- Data completeness: how many minutes did each sensor actually report,
-- against the widest coverage any sensor achieved in the same window?
WITH window_minutes AS (
    SELECT count(DISTINCT sensing_datetime) AS expected FROM fact_pedestrian_count
)
SELECT
    d.sensor_description,
    count(*) AS readings,
    (SELECT expected FROM window_minutes) AS window_minutes,
    round(100.0 * count(*) / nullif((SELECT expected FROM window_minutes), 0), 1) AS coverage_pct
FROM fact_pedestrian_count f
JOIN dim_sensor d USING (location_id)
GROUP BY d.sensor_description
ORDER BY coverage_pct ASC
LIMIT 15;

-- name: ingestion_freshness
-- Operational view: how current is the warehouse, and how far back does it reach?
SELECT
    count(*)                       AS total_readings,
    count(DISTINCT location_id)    AS sensors,
    min(sensing_datetime)          AS earliest_reading,
    max(sensing_datetime)          AS latest_reading,
    max(ingested_at)               AS last_ingested_at,
    date_diff('minute', max(sensing_datetime), max(ingested_at)) AS lag_minutes
FROM fact_pedestrian_count;
