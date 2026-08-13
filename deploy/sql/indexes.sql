-- Indexes the API depends on.
--
-- CREATE INDEX CONCURRENTLY so the live tables stay readable and writable while
-- these build; that also means each statement must run OUTSIDE a transaction, so
-- this file must NOT be wrapped in BEGIN/COMMIT and must not be run with -1.
--
-- IF NOT EXISTS makes the file re-runnable. Note that a CONCURRENTLY build which
-- fails partway leaves an INVALID index behind; the query at the bottom reports
-- any, and they must be dropped before re-running.
--
--     sudo -u postgres psql -d weatherdata -v ON_ERROR_STOP=1 -f indexes.sql

-- observations: 8.0M rows, previously indexed on station alone. The API filters
-- by station AND a date range, and also by date alone, so:
--
--   (station, date) serves "one station over a window" — the common case — and
--                   also plain station lookups, making obs_station redundant.
--   (date)          serves "everything on this day", across all stations.
--
-- Before this, the date endpoint compared EXTRACT(year/month/day FROM date) to
-- integers. No index can satisfy a function call on the column, so every single
-- date request sequentially scanned all 8M rows.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_observations_station_date
    ON public.observations (station, date);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_observations_date
    ON public.observations (date);

ANALYZE public.observations;

-- Report any index left INVALID by a failed CONCURRENTLY build.
SELECT c.relname AS invalid_index
FROM pg_class c
JOIN pg_index i ON i.indexrelid = c.oid
WHERE NOT i.indisvalid AND c.relname LIKE 'idx_%';
