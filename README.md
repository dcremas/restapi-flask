# Weather Data API

Read-only JSON API over the weather data on this EC2 host — the cleaned sources
behind the visualizations on dustincremascoli.com.

**https://api.dustincremascoli.com** · docs at `/docs` · spec at `/openapi.json`

Two datasets, one service:

| Dataset | Database | Contents |
|---|---|---|
| `historical` | `weatherdata` | NOAA hourly surface observations — 8.0M rows, 112 stations, 2019 to present |
| `forecast` | `apple_weatherkit` | Apple WeatherKit hourly forecasts — rolling ~20-day window, same 112 stations |

```
app.py         factory, config, JSON provider, request lifecycle, /docs, /health, /ready
db.py          connection pools, per-connection guards, health probes
datasets.py    the two Dataset records: all SQL lives here
routes.py      the /v1 endpoints: validation, pagination, caching
spec.py        the OpenAPI document, generated from datasets.py
errors.py      RFC 9457 problem+json handlers
tests/         pytest suite (49 tests) against the real databases — read-only
deploy/        systemd unit, gunicorn config, nginx vhost, provision/deploy, SQL
```

Flask + gunicorn behind nginx — the same stack as the two websites on this box,
so there is one server runtime to operate rather than three.

## Endpoints

Five per dataset, plus operations. Everything is `GET`, unauthenticated and
cacheable.

```
GET /v1/{dataset}/stations                      the 112 stations, with region and coordinates
GET /v1/{dataset}/dates                         dates present, with a per-day record count
GET /v1/{dataset}/observations                  a page of records
GET /v1/{dataset}/observations/station/{id}     one station
GET /v1/{dataset}/observations/date/{date}      one calendar day, all stations

GET /                                           JSON index describing the service
GET /docs  /openapi.json                        Swagger UI and the spec
GET /health                                     liveness — never touches Postgres
GET /ready                                      readiness — per-dataset database check
```

Query parameters: `limit` (1–1000, default 100), `offset` (≤100000),
`station` (11 digits), `from` / `to` (ISO dates, both inclusive).

`{dataset}` is `historical` or `forecast`.

## What makes it reliable

Each of these is a fix for something that was actually wrong or missing before.

- **Connection pooling that survives a database restart.** The previous
  implementation opened one connection at import and never re-established it: a
  Postgres bounce broke every subsequent request permanently, and nothing
  noticed. Pools re-validate a connection before handing it out (`check`), cap
  lifetime at 30 minutes, and turn a driver failure into a 503 with
  `Retry-After` rather than a 500.
- **Indexes that match the queries.** `observations` had an index on `station`
  only, and the old date endpoint filtered on `EXTRACT(year/month/day FROM
  date)` — a function call no index can satisfy, so **every date request
  sequentially scanned 8M rows**. Rewritten as a half-open range against
  `(station, date)` and `(date)`: a single-day count went from a parallel seq
  scan to an Index Only Scan at **0.4 ms**, and a station-month page to
  **0.12 ms**.
- **Timeouts at every layer, ordered so the innermost fires first.** Postgres
  `statement_timeout` 8s → nginx `proxy_read_timeout` 15s → gunicorn `timeout`
  30s. A slow query is therefore cancelled by Postgres and reported as a clean
  503, instead of nginx reporting a gateway error or gunicorn killing a worker
  mid-request.
- **Bounded work per request.** `limit` is capped at 1000 and `offset` at
  100000; `/dates` defaults to a 60-day window rather than grouping the whole
  table. No endpoint can be asked to materialise 8M rows.
- **Partial failure stays partial.** Pools are per dataset and opened lazily, so
  if one database is unreachable the other's endpoints keep serving and `/ready`
  reports `degraded` rather than taking the whole API out of rotation.
- **`/health` never touches Postgres**, so a monitor can distinguish "API down"
  from "database down".
- **Caching in three places.** The app sets `Cache-Control` per dataset (1h
  historical, 5m forecast) and an `ETag`, so a repeat caller gets a 304 and
  transfers nothing; nginx caches upstream responses with `proxy_cache_lock` so
  a cold cache under load is one query rather than a stampede, and serves stale
  on error rather than passing a 5xx through.
- **Least privilege.** The app connects as `api_ro`: `SELECT` on exactly five
  tables (not `ALL TABLES` — a table added later by a pipeline must not become
  web-readable), `default_transaction_read_only`, its own statement and lock
  timeouts, and a connection limit. Verified: `CREATE TABLE` is refused as
  read-only and an ungranted table is denied.
- **Systemd sandbox.** `ProtectSystem=strict`, `ProtectHome=read-only`, no
  writable path but `/run/restapi`, `MemoryMax=512M`, `TasksMax=64`, syscall
  filtered. The service writes no files at all, so it is given nowhere to write.
- **Rate limits in nginx**, not in the app — they protect Postgres rather than
  just the Python process. 120 req/min sustained plus a 10 req/s burst per IP;
  `/health` and `/ready` are exempt so a monitor cannot lock itself out.

## What makes it professional

- **Accurate HTTP status codes.** The old API returned **200** with a plain
  string reading `"Error, Observations not found, 404."` for every failure — a
  client checking the status saw success and a client parsing JSON got a string
  where it expected a list. Now: 400 invalid parameter, 404 unknown
  station/dataset/route, 405 wrong method, 429 rate limited, 503 store
  unavailable.
- **RFC 9457 `application/problem+json`** for every error, carrying a
  `request_id` that appears in the structured logs so a caller can quote it.
- **ISO 8601 timestamps.** Flask's default JSON encoder emits RFC 822
  (`"Mon, 15 Jan 2024 00:54:00 GMT"`), which no standard date parser accepts;
  a custom provider emits `2024-01-15T00:54:00Z`.
- **A generated OpenAPI 3.0 spec** built from the same `Dataset` records the
  routes use, so the docs cannot drift from the implementation. A test asserts
  every documented path is actually routable.
- **Swagger UI served from a vendored bundle**, so `/docs` needs no CDN.
- **Consistent pagination** with `limit`/`offset`/`has_more` and a `next` link.
  No total count: `COUNT(*)` over the filtered 8M-row table would cost as much
  as the page itself, so `has_more` is derived by fetching one extra row.
- **Input validation with useful messages.** A station id must be 11 digits and
  a date must parse — `int(date[:4])` in the old code turned a typo into a 500.
- **CORS** open for GET, so browser clients work; safe because there are no
  cookies, no credentials and no mutating verbs.
- **Structured JSON logs** on stdout with request id, path, status and duration,
  captured by journald.

## Corrected data bug

The old handlers selected twelve columns and built the response from indices
0–10, so **`dew` was populated with `prp`** (precipitation) and the real dew
point was never returned. Any client reading `dew` was reading precipitation.

Rows are now selected into a dict by column name, which makes that class of
mistake impossible. A test pins it down using physics rather than a fixture:
over a Chicago January, dew points must go below zero, and precipitation cannot
— so if the two were ever the same column again, it fails.

`loc_subset` also holds one row per station **per year** (112 × 2019–2024 = 672),
so the old join on `station` alone multiplied every observation sixfold.
`DISTINCT ON` collapses it to the newest row per station; a test asserts the
station list has no duplicates.

## Local development

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt

# The databases are localhost-only on the box; tunnel to them.
ssh -f -N -L 15433:127.0.0.1:5432 -i <key> ec2-user@<host>

PG_HOST=127.0.0.1 PG_PORT=15433 PG_USER=api_ro PG_PASSWORD=<pw> \
  .venv/bin/python -c "from app import app; app.run(port=5100)"
```

The `api_ro` password lives at `/root/.restapi/api_ro.pw` on the server, and in
`/etc/restapi/restapi.env`.

## Tests

```bash
PG_HOST=127.0.0.1 PG_PORT=15433 PG_USER=api_ro PG_PASSWORD=<pw> \
  .venv/bin/python -m pytest -q
```

49 tests against the real databases. Safe to run against production: every
query is a SELECT and the role cannot write even if a test tried. Coverage
includes each error status and its content type, the dew/prp correctness check,
pagination contiguity across pages, ETag→304, per-dataset cache lifetimes, a
simulated database outage returning 503 rather than 500, and a one-dataset
outage leaving the other serving.

## Deploying

```bash
cp deploy/deploy.env.example deploy/deploy.env   # gitignored; host + SSH key
deploy/deploy.sh              # sync + pip install + restart
deploy/deploy.sh --provision  # sync, then run provision.sh on the server
```

As with the other services here, **`deploy.sh` alone installs neither the
systemd unit nor the nginx vhost** — only `--provision` does.

First-time setup, in order:

```bash
# 1. Database role, per database, as the owner. Generates nothing itself —
#    provision.sh reads the password from /root/.restapi/api_ro.pw.
sudo -u postgres psql -d weatherdata      -v ON_ERROR_STOP=1 -v api_pw="$(sudo cat /root/.restapi/api_ro.pw)" -f deploy/sql/roles.sql
sudo -u postgres psql -d apple_weatherkit -v ON_ERROR_STOP=1 -v api_pw="$(sudo cat /root/.restapi/api_ro.pw)" -f deploy/sql/roles.sql

# 2. Indexes. CONCURRENTLY, so the tables stay live — and so these must NOT run
#    inside a transaction (no -1 flag).
sudo -u postgres psql -d weatherdata      -v ON_ERROR_STOP=1 -f deploy/sql/indexes.sql
sudo -u postgres psql -d apple_weatherkit -v ON_ERROR_STOP=1 -f deploy/sql/indexes_forecast.sql

# 3. Service, unit and vhost.
deploy/deploy.sh --provision

# 4. TLS, once DNS resolves.
sudo certbot --nginx -d api.dustincremascoli.com
deploy/deploy.sh --provision   # re-run to install the hardened TLS vhost
```

Secrets live in `/etc/restapi/restapi.env` on the server, root-owned and
group-readable by the service account. They are never rsynced — `deploy.sh`
excludes `.env`.
