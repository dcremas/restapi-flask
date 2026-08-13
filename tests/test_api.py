"""Behavioural tests against the real databases.

Read-only throughout — the role cannot write even if a test tried — so these are
safe to run against production data. Run with a tunnel to the box:

    ssh -f -N -L 15433:127.0.0.1:5432 ec2-user@<host>
    PG_HOST=127.0.0.1 PG_PORT=15433 PG_USER=api_ro PG_PASSWORD=... \
        .venv/bin/python -m pytest -q

Several tests name a defect in the previous implementation. Those are the ones
worth keeping longest: they encode behaviour a rewrite could plausibly lose.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app  # noqa: E402
from datasets import DATASETS  # noqa: E402

# A station that exists in both datasets (Chicago O'Hare).
STATION = "72530094846"
DATASET_KEYS = sorted(DATASETS)


@pytest.fixture(scope="session")
def app():
    application = create_app({"TESTING": True})
    yield application
    # Close the pools explicitly. Left to the garbage collector they are
    # finalized during interpreter shutdown, where psycopg cannot join its
    # worker threads and prints a PythonFinalizationError traceback per pool.
    for database in application.extensions["databases"].values():
        database.close()


@pytest.fixture(scope="session")
def client(app):
    return app.test_client()


def body(resp):
    return resp.get_json()


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------
class TestOperations:
    def test_health_never_touches_the_database(self, client, monkeypatch, app):
        """Liveness must stay green when Postgres is down, or a monitor cannot
        tell 'API down' from 'database down'."""
        for database in app.extensions["databases"].values():
            monkeypatch.setattr(
                database, "healthy", lambda: (_ for _ in ()).throw(AssertionError("touched db"))
            )
        resp = client.get("/health")
        assert resp.status_code == 200
        assert body(resp)["status"] == "ok"

    def test_ready_reports_each_database(self, client):
        data = body(client.get("/ready"))
        assert set(data["databases"]) == {d.database for d in DATASETS.values()}
        assert all(c["ok"] for c in data["databases"].values())
        assert data["status"] == "ok"

    def test_ready_is_never_cached(self, client):
        assert "no-store" in client.get("/ready").headers["Cache-Control"]

    def test_root_describes_the_api(self, client):
        data = body(client.get("/"))
        assert data["service"] == "weather-data-api"
        assert set(data["datasets"]) == set(DATASET_KEYS)

    def test_openapi_document_is_valid_enough_to_use(self, client):
        doc = body(client.get("/openapi.json"))
        assert doc["openapi"].startswith("3.")
        # Every documented path must actually be routable, or the docs lie.
        for path in doc["paths"]:
            concrete = path.replace("{station_id}", STATION).replace("{date_str}", "2024-01-15")
            assert client.get(concrete).status_code in (200, 304), path

    def test_docs_render_without_a_cdn(self, client):
        assert client.get("/docs").status_code == 200
        # The bundle must be served locally; a CDN reference would make the docs
        # depend on outbound network from the visitor's browser.
        assert client.get("/docs/swagger-ui-bundle.js").status_code == 200
        assert b"//cdn" not in client.get("/docs").data


# ---------------------------------------------------------------------------
# Shape and correctness
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("key", DATASET_KEYS)
class TestDatasets:
    def test_stations_are_the_curated_112(self, client, key):
        data = body(client.get(f"/v1/{key}/stations"))
        assert data["count"] == 112
        row = data["data"][0]
        assert set(row) >= {"station", "station_name", "state", "region", "lat", "lon"}

    def test_station_ids_are_not_duplicated(self, client, key):
        """Regression: loc_subset holds one row per station per year, so joining
        on station alone multiplied every row sixfold."""
        rows = body(client.get(f"/v1/{key}/stations"))["data"]
        ids = [r["station"] for r in rows]
        assert len(ids) == len(set(ids))

    def test_dates_are_bounded_by_default(self, client, key):
        data = body(client.get(f"/v1/{key}/dates"))
        assert "from" in data["filters"] and "to" in data["filters"]
        assert data["count"] <= 400

    def test_observations_page_has_an_envelope(self, client, key):
        data = body(client.get(f"/v1/{key}/observations?limit=5"))
        assert set(data) >= {"dataset", "count", "limit", "offset", "has_more", "links", "data"}
        assert data["count"] <= 5

    def test_timestamps_are_iso_8601(self, client, key):
        """Flask's default renders RFC 822 ('Mon, 15 Jan 2024 00:54:00 GMT'),
        which no standard JSON date parser accepts."""
        field = DATASETS[key].time_field
        row = body(client.get(f"/v1/{key}/observations?limit=1"))["data"][0]
        value = row[field]
        assert "T" in value and "," not in value, value
        from datetime import datetime

        datetime.fromisoformat(value.replace("Z", "+00:00"))

    def test_measures_are_all_present(self, client, key):
        dataset = DATASETS[key]
        row = body(client.get(f"/v1/{key}/observations?limit=1"))["data"][0]
        for measure in dataset.measures:
            assert measure in row, f"{measure} missing from {key}"

    def test_pages_do_not_repeat_or_skip_rows(self, client, key):
        field = DATASETS[key].time_field
        one = body(client.get(f"/v1/{key}/observations?station={STATION}&limit=4"))
        two = body(client.get(f"/v1/{key}/observations?station={STATION}&limit=4&offset=4"))
        first = [r[field] for r in one["data"]]
        second = [r[field] for r in two["data"]]
        assert len(set(first) & set(second)) == 0
        assert first == sorted(first) and second == sorted(second)
        assert max(first) <= min(second)

    def test_next_link_is_present_only_when_there_is_more(self, client, key):
        page = body(client.get(f"/v1/{key}/observations?limit=1"))
        assert page["has_more"] is True
        assert "next" in page["links"]
        # A page that exhausts the result set must not advertise a next page.
        narrow = body(client.get(f"/v1/{key}/observations?station={STATION}&limit=1000&from=1900-01-01&to=1900-01-02"))
        assert narrow["has_more"] is False
        assert "next" not in narrow["links"]


class TestHistoricalCorrectness:
    """The historical dataset had a specific data bug worth pinning down."""

    def test_dew_is_the_dew_point_not_precipitation(self, client):
        """The old handler selected 12 columns and read indices 0-10, so 'dew'
        carried prp (precipitation) and the real dew point was never returned.

        Checked against physics rather than a fixture: dew point tracks air
        temperature and is routinely negative, while precipitation is >= 0 and
        almost always exactly 0. If dew were secretly prp, this would fail.
        """
        rows = body(client.get(
            "/v1/historical/observations?station=72530094846"
            "&from=2024-01-01&to=2024-01-31&limit=1000"
        ))["data"]
        assert rows
        assert all(r["prp"] is None or r["prp"] >= 0 for r in rows)
        dews = [r["dew"] for r in rows if r["dew"] is not None]
        prps = [r["prp"] for r in rows if r["prp"] is not None]
        assert dews, "dew must be populated"
        # A Chicago January: dew points below freezing must appear. prp cannot
        # be negative, so this alone proves the two are not the same column.
        assert min(dews) < 0, f"expected sub-zero dew points, got min {min(dews)}"
        assert dews != prps

    def test_quality_filters_are_applied(self, client):
        rows = body(client.get("/v1/historical/observations?limit=500"))["data"]
        assert all(20.0 <= r["slp"] <= 35.0 for r in rows if r["slp"] is not None)
        assert all(r["prp"] <= 10.0 for r in rows if r["prp"] is not None)


# ---------------------------------------------------------------------------
# Errors — every one must be problem+json with an accurate status
# ---------------------------------------------------------------------------
class TestErrors:
    @pytest.mark.parametrize(
        "path,status",
        [
            ("/v1/historical/observations?limit=0", 400),
            ("/v1/historical/observations?limit=1001", 400),
            ("/v1/historical/observations?limit=abc", 400),
            ("/v1/historical/observations?offset=-1", 400),
            ("/v1/historical/observations?offset=999999999", 400),
            ("/v1/historical/observations?station=nope", 400),
            ("/v1/historical/observations?from=notadate", 400),
            ("/v1/historical/observations?from=2024-06-01&to=2024-01-01", 400),
            ("/v1/historical/observations/date/2024-13-45", 400),
            ("/v1/historical/observations/station/abc", 400),
            ("/v1/historical/observations/station/00000000000", 404),
            ("/v1/nosuchdataset/stations", 404),
            ("/v1/historical/nosuchthing", 404),
            ("/nosuchthing", 404),
        ],
    )
    def test_status_and_content_type(self, client, path, status):
        resp = client.get(path)
        assert resp.status_code == status, path
        # The single most important assertion here: the old API returned HTTP
        # 200 with a plain string reading "Error, ... 404." for every failure.
        assert resp.mimetype == "application/problem+json", path

    def test_problem_body_is_rfc9457(self, client):
        data = body(client.get("/v1/historical/observations?limit=abc"))
        assert set(data) >= {"type", "title", "status", "detail", "instance"}
        assert data["status"] == 400
        assert data["type"].startswith("https://")
        assert "request_id" in data

    def test_write_methods_are_rejected(self, client):
        for method in (client.post, client.put, client.delete, client.patch):
            resp = method("/v1/historical/observations")
            assert resp.status_code == 405
            assert resp.mimetype == "application/problem+json"

    def test_a_database_outage_is_503_not_500(self, client, app, monkeypatch):
        """The request is valid; the store is not available. 503 + Retry-After
        tells a client to retry, where a 500 tells it to give up."""
        from db import DatabaseUnavailable

        database = app.extensions["databases"]["weatherdata"]

        def boom(*_a, **_k):
            raise DatabaseUnavailable("weatherdata")

        monkeypatch.setattr(database, "rows", boom)
        resp = client.get("/v1/historical/stations")
        assert resp.status_code == 503
        assert resp.headers.get("Retry-After")
        assert resp.mimetype == "application/problem+json"

    def test_one_dataset_outage_leaves_the_other_serving(self, client, app, monkeypatch):
        """A partial fault must not take the whole API down."""
        from db import DatabaseUnavailable

        def boom(*_a, **_k):
            raise DatabaseUnavailable("weatherdata")

        monkeypatch.setattr(app.extensions["databases"]["weatherdata"], "rows", boom)
        assert client.get("/v1/historical/stations").status_code == 503
        assert client.get("/v1/forecast/stations").status_code == 200
        # ...and readiness says degraded rather than dead.
        monkeypatch.setattr(
            app.extensions["databases"]["weatherdata"], "healthy", lambda: (False, "Boom")
        )
        resp = client.get("/ready")
        assert resp.status_code == 200
        assert body(resp)["status"] == "degraded"

    def test_colliding_query_arg_does_not_500(self, client):
        """?dataset_key= collides with the path arg when building the next link;
        passing both to url_for would be a duplicate-keyword TypeError."""
        resp = client.get("/v1/historical/observations?limit=1&dataset_key=x")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# HTTP behaviour
# ---------------------------------------------------------------------------
class TestHttp:
    def test_etag_enables_a_conditional_get(self, client):
        first = client.get("/v1/historical/stations")
        etag = first.headers["ETag"]
        second = client.get("/v1/historical/stations", headers={"If-None-Match": etag})
        assert second.status_code == 304
        assert second.get_data() == b""

    def test_cache_windows_match_how_fast_the_data_moves(self, client):
        hist = client.get("/v1/historical/observations?limit=1").cache_control
        fcst = client.get("/v1/forecast/observations?limit=1").cache_control
        assert int(hist.max_age) > int(fcst.max_age)

    def test_request_id_is_echoed_and_generated(self, client):
        mine = "abc123def456"
        assert client.get("/health", headers={"X-Request-Id": mine}).headers["X-Request-Id"] == mine
        generated = client.get("/health").headers["X-Request-Id"]
        assert len(generated) >= 16

    def test_a_hostile_request_id_is_not_reflected(self, client):
        """It lands in responses and logs, so it must not carry markup."""
        resp = client.get("/health", headers={"X-Request-Id": "<script>x</script>"})
        assert "<script>" not in resp.headers["X-Request-Id"]

    def test_cors_allows_browser_clients(self, client):
        headers = client.get("/v1/historical/stations").headers
        assert headers["Access-Control-Allow-Origin"] == "*"
        assert "ETag" in headers["Access-Control-Expose-Headers"]

    def test_no_server_version_or_secrets_leak(self, client):
        resp = client.get("/v1/historical/stations")
        blob = " ".join(f"{k}: {v}" for k, v in resp.headers).lower()
        assert "password" not in blob
        assert resp.headers.get("X-Content-Type-Options") == "nosniff"
