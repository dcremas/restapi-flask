"""The two datasets, and the SQL behind every endpoint.

Both datasets expose the same five resources, so they are described by the same
structure and served by the same route code. Only the table names, column
mapping and time column differ.

Three things here are deliberate corrections to the previous implementation:

1. **`dew` really is the dew point.** The old handlers selected twelve columns
   and then built the response dict from indices 0-10, so `"dew"` was populated
   with `prp` (precipitation) and the actual dew point was never returned at
   all. Selecting into a dict by name makes that class of mistake impossible.

2. **Date filtering is a range, not EXTRACT().** The old date endpoint compared
   `EXTRACT(year/month/day FROM date)` against integers, which no index can
   satisfy — every request sequentially scanned 8M rows. A half-open range on
   the bare column uses the index added in deploy/sql/indexes.sql.

3. **`loc_subset` is deduplicated before joining.** It holds one row per station
   *per year* (112 stations x 2019-2024 = 672 rows), so joining on `station`
   alone multiplied every observation by six. DISTINCT ON collapses it to the
   most recent row per station.
"""

from __future__ import annotations

from dataclasses import dataclass

# The station universe. One CTE per dataset because the two databases carry
# station metadata differently — assuming otherwise is how the forecast queries
# first came out referencing a table that does not exist there.
#
# weatherdata has `loc_subset`: the curated 112 stations, already carrying
# region/sub_region/state/lat/lon, but one row per station PER YEAR (112 x
# 2019-2024 = 672). Joining on station alone therefore multiplies every
# observation sixfold; DISTINCT ON collapses it to the newest row per station.
_HIST_STATIONS_CTE = """
WITH stations AS (
    SELECT DISTINCT ON (station)
        station, station_name, region, sub_region, state, lat, lon
    FROM loc_subset
    ORDER BY station, year DESC
)
"""

# apple_weatherkit has no loc_subset. It carries the full 28,345-row NOAA
# `locations` table plus `regions` keyed on state, so the station universe is
# derived from the stations that actually have forecasts. locations has no
# duplicate stations, so no dedupe is needed on that side.
_FCST_STATIONS_CTE = """
WITH stations AS (
    SELECT l.station, l.station_name, r.region, r.sub_region, l.state,
           l.lat, l.lon
    FROM locations l
    LEFT JOIN regions r ON r.state = l.state
    WHERE EXISTS (SELECT 1 FROM hourlyforecasts f WHERE f.station = l.station)
)
"""

# Quality filters carried over from the original queries. Kept because they
# encode real data cleaning: source/report_type select the hourly synoptic
# reports, and the bounds discard physically impossible sensor readings.
_HIST_QUALITY = """
    AND o.source IN ('6', '7')
    AND o.report_type = 'FM-15'
    AND o.slp BETWEEN 20.00 AND 35.00
    AND o.prp <= 10.00
"""


@dataclass(frozen=True)
class Dataset:
    key: str                 # url segment, e.g. "historical"
    title: str
    description: str
    database: str            # config key for the Database instance
    time_field: str          # name of the timestamp field in responses
    measures: tuple[str, ...]  # measurement fields, for documentation

    # SQL, each returning named columns that map straight to JSON.
    sql_stations: str
    sql_dates: str
    sql_observations: str    # accepts %(limit)s/%(offset)s and optional filters
    sql_count_hint: str      # cheap existence check for 404 vs empty page


HISTORICAL = Dataset(
    key="historical",
    title="Historical hourly observations",
    description=(
        "Cleaned hourly surface observations for 112 US airport weather "
        "stations, sourced from the NOAA public record."
    ),
    database="weatherdata",
    time_field="observed_at",
    measures=("tmp", "dew", "slp", "wnd", "prp", "vis", "cig"),
    sql_stations=_HIST_STATIONS_CTE + """
    SELECT s.station, s.station_name, s.state, s.region, s.sub_region,
           s.lat, s.lon
    FROM stations s
    ORDER BY s.region, s.sub_region, s.state, s.station_name
    """,
    sql_dates=_HIST_STATIONS_CTE + """
    SELECT o.date::date AS date, count(*) AS observations
    FROM observations o
    JOIN stations s ON s.station = o.station
    WHERE o.date >= %(start)s AND o.date < %(end)s
    """ + _HIST_QUALITY + """
    GROUP BY 1
    ORDER BY 1
    """,
    sql_observations=_HIST_STATIONS_CTE + """
    SELECT o.station, s.station_name, s.state, s.region, s.sub_region,
           o.date AS observed_at,
           o.tmp, o.dew, o.slp, o.wnd, o.prp, o.vis, o.cig
    FROM observations o
    JOIN stations s ON s.station = o.station
    WHERE (%(station)s::text IS NULL OR o.station = %(station)s)
      AND (%(start)s::timestamp IS NULL OR o.date >= %(start)s)
      AND (%(end)s::timestamp   IS NULL OR o.date <  %(end)s)
    """ + _HIST_QUALITY + """
    ORDER BY o.station, o.date
    LIMIT %(limit)s OFFSET %(offset)s
    """,
    sql_count_hint="SELECT 1 FROM loc_subset WHERE station = %(station)s LIMIT 1",
)


FORECAST = Dataset(
    key="forecast",
    title="Hourly forecasts",
    description=(
        "Rolling hourly forecasts for the same 112 stations, retrieved from "
        "Apple WeatherKit. Roughly a 20-day window, refreshed continuously."
    ),
    database="apple_weatherkit",
    time_field="forecast_for",
    measures=(
        "temp_f", "feelslike_f", "humidity", "pressure_in",
        "precip_in", "wind_mph", "gust_mph", "cloud", "vis_miles",
    ),
    sql_stations=_FCST_STATIONS_CTE + """
    SELECT s.station, s.station_name, s.state, s.region, s.sub_region,
           s.lat, s.lon
    FROM stations s
    ORDER BY s.region, s.sub_region, s.state, s.station_name
    """,
    sql_dates=_FCST_STATIONS_CTE + """
    SELECT f.time::date AS date, count(*) AS observations
    FROM hourlyforecasts f
    JOIN stations s ON s.station = f.station
    WHERE f.time >= %(start)s AND f.time < %(end)s
    GROUP BY 1
    ORDER BY 1
    """,
    sql_observations=_FCST_STATIONS_CTE + """
    SELECT f.station, s.station_name, s.state, s.region, s.sub_region,
           f.time AS forecast_for,
           f.temp_f, f.feelslike_f, f.humidity, f.pressure_in,
           f.precip_in, f.wind_mph, f.gust_mph, f.cloud, f.vis_miles
    FROM hourlyforecasts f
    JOIN stations s ON s.station = f.station
    WHERE (%(station)s::text IS NULL OR f.station = %(station)s)
      AND (%(start)s::timestamp IS NULL OR f.time >= %(start)s)
      AND (%(end)s::timestamp   IS NULL OR f.time <  %(end)s)
    ORDER BY f.station, f.time
    LIMIT %(limit)s OFFSET %(offset)s
    """,
    # locations, not loc_subset: the latter does not exist in this database.
    sql_count_hint=(
        "SELECT 1 FROM locations l WHERE l.station = %(station)s "
        "AND EXISTS (SELECT 1 FROM hourlyforecasts f WHERE f.station = l.station) LIMIT 1"
    ),
)


DATASETS: dict[str, Dataset] = {d.key: d for d in (HISTORICAL, FORECAST)}
