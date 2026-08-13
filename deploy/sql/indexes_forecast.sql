-- Same shape for the forecast dataset. Only 53k rows today, so these matter far
-- less than the historical ones — but the table is rewritten continuously by the
-- ingest pipeline and the query shapes are identical, so it is not worth relying
-- on the row count staying small.
--
--     sudo -u postgres psql -d apple_weatherkit -v ON_ERROR_STOP=1 -f indexes_forecast.sql

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hourlyforecasts_station_time
    ON public.hourlyforecasts (station, time);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_hourlyforecasts_time
    ON public.hourlyforecasts (time);

ANALYZE public.hourlyforecasts;
