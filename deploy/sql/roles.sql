-- Least-privilege role for the API: SELECT on exactly the tables it reads.
--
-- Run once per database, as the owner. The password arrives as a psql variable so
-- it never appears in this file or in shell history:
--
--   sudo -u postgres psql -d weatherdata -v ON_ERROR_STOP=1 \
--        -v api_pw="<password>" -f roles.sql
--
-- Uses \gexec (build the statement as a string, then run it) rather than a DO
-- block, because a psql variable cannot be interpolated into a PL/pgSQL body —
-- it would be inside a string literal. This also keeps the password out of any
-- statement that could be logged verbatim.
--
-- The app additionally sets default_transaction_read_only on every connection
-- (see db.py). Two independent locks on the same door: a grant added here by
-- mistake still could not be used to write.

\set ON_ERROR_STOP on

-- 1. Create the role only if it is missing; ALTER sets the password either way,
--    so re-running this rotates the password rather than failing.
SELECT 'CREATE ROLE api_ro LOGIN'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_ro')
\gexec

SELECT format('ALTER ROLE api_ro PASSWORD %L', :'api_pw')
\gexec

-- 2. No privilege escalation paths.
ALTER ROLE api_ro NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;

-- 3. Bound what one client can consume.
--    Steady state is GUNICORN_WORKERS x databases x POOL_MAX_SIZE = 2 x 2 x 2 = 8.
--    A restart briefly doubles that while old and new workers overlap, so 20
--    covers it and still caps a runaway client well below Postgres' own
--    max_connections.
ALTER ROLE api_ro CONNECTION LIMIT 20;

-- 4. Server-side guards, so they hold even if the application forgets to set
--    them. read_only makes writes impossible; the timeouts stop one query
--    against the 8M-row table from occupying a backend indefinitely.
ALTER ROLE api_ro SET default_transaction_read_only = on;
ALTER ROLE api_ro SET statement_timeout = '8s';
ALTER ROLE api_ro SET lock_timeout = '2s';
ALTER ROLE api_ro SET idle_in_transaction_session_timeout = '10s';

-- 5. Connect + schema usage on whichever database this is being run against.
SELECT format('GRANT CONNECT ON DATABASE %I TO api_ro', current_database())
\gexec

GRANT USAGE ON SCHEMA public TO api_ro;

-- 6. Explicit table grants, and only for tables that exist here — the two
--    databases hold different subsets. Deliberately NOT "ALL TABLES IN SCHEMA":
--    a table added later by a pipeline must not become web-readable by default,
--    and there is no ALTER DEFAULT PRIVILEGES for the same reason.
SELECT format('GRANT SELECT ON public.%I TO api_ro', table_name)
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE'
  AND table_name IN ('observations', 'loc_subset', 'locations', 'regions',
                     'hourlyforecasts')
\gexec

-- 7. Report what the role can actually reach, so the output of this script is
--    the audit rather than an assumption.
SELECT current_database() AS database, table_name, privilege_type
FROM information_schema.table_privileges
WHERE grantee = 'api_ro'
ORDER BY table_name;
