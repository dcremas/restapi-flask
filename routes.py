"""Routes.

Both datasets are served by the same handlers — they differ only in the SQL
carried by their Dataset record — so there is one implementation of pagination,
validation and caching rather than two that can drift.

Every endpoint is GET, read-only, unauthenticated and cacheable.
"""

from __future__ import annotations

import hashlib
import re
from datetime import date as date_cls
from datetime import datetime, timedelta, timezone

from flask import Blueprint, current_app, jsonify, request, url_for

from datasets import DATASETS, Dataset
from errors import BadRequest, NotFound

bp = Blueprint("v1", __name__, url_prefix="/v1")

# Station ids are NOAA USAF+WBAN concatenations: exactly 11 digits. Validating
# the shape here turns a garbage path segment into a 400 with a clear message,
# rather than a query that scans and returns nothing.
STATION_RE = re.compile(r"^\d{11}$")

DEFAULT_LIMIT = 100
MAX_LIMIT = 1000
# Deep offsets make Postgres walk and discard rows; past this a caller should be
# narrowing by station or date instead. Refusing is kinder than being slow.
MAX_OFFSET = 100_000


# ---------------------------------------------------------------------------
# Parameter parsing
# ---------------------------------------------------------------------------
def _int_arg(name: str, default: int, low: int, high: int) -> int:
    raw = request.args.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise BadRequest(f"{name!r} must be an integer, got {raw!r}.") from None
    if not low <= value <= high:
        raise BadRequest(f"{name!r} must be between {low} and {high}, got {value}.")
    return value


def _pagination() -> tuple[int, int]:
    return (
        _int_arg("limit", DEFAULT_LIMIT, 1, MAX_LIMIT),
        _int_arg("offset", 0, 0, MAX_OFFSET),
    )


def _parse_date(value: str, field: str) -> date_cls:
    """Strict ISO date. The old code did int(value[:4]) and raised on anything
    unexpected, turning a typo into a 500."""
    try:
        return date_cls.fromisoformat(value)
    except ValueError:
        raise BadRequest(
            f"{field!r} must be an ISO date (YYYY-MM-DD), got {value!r}."
        ) from None


def _window() -> tuple[datetime | None, datetime | None]:
    """Optional ?from=/?to= filters, as a half-open [start, end) range.

    `to` is inclusive to the caller and exclusive in SQL — asking for
    to=2024-01-31 should include that whole day, which a bare `< 2024-01-31`
    would silently drop.
    """
    start = end = None
    if raw := request.args.get("from"):
        start = datetime.combine(_parse_date(raw, "from"), datetime.min.time())
    if raw := request.args.get("to"):
        end = datetime.combine(
            _parse_date(raw, "to") + timedelta(days=1), datetime.min.time()
        )
    if start and end and end <= start:
        raise BadRequest("'from' must be on or before 'to'.")
    return start, end


def _station_arg(dataset: Dataset) -> str | None:
    """Validated optional ?station= filter.

    Without this, a malformed id reached SQL as a parameter and produced an
    empty 200 — indistinguishable from "this station has no data".
    """
    station = request.args.get("station")
    if station in (None, ""):
        return None
    if not STATION_RE.match(station):
        raise BadRequest(
            f"'station' must be an 11-digit station id, got {station!r}. "
            f"See /v1/{dataset.key}/stations."
        )
    return station


def _dataset(key: str) -> Dataset:
    try:
        return DATASETS[key]
    except KeyError:
        raise NotFound(
            f"Unknown dataset {key!r}. Available: {', '.join(sorted(DATASETS))}."
        ) from None


def _db(dataset: Dataset):
    return current_app.extensions["databases"][dataset.database]


# ---------------------------------------------------------------------------
# Response shaping
# ---------------------------------------------------------------------------
def _respond(payload: dict, *, max_age: int):
    """JSON with an ETag and a cache lifetime.

    The ETag lets a repeat caller get a 304 and transfer nothing; nginx and any
    intermediary can serve the body from cache for max_age. Historical data is
    effectively immutable, so it gets a long window; forecasts turn over hourly.
    """
    response = jsonify(payload)
    body = response.get_data()
    response.set_etag(hashlib.sha256(body).hexdigest()[:32])
    response.cache_control.public = True
    response.cache_control.max_age = max_age
    # Honour If-None-Match: turns the response into a 304 when unchanged.
    return response.make_conditional(request)


def _page(dataset: Dataset, rows: list[dict], limit: int, offset: int, **filters) -> dict:
    """Envelope every collection the same way.

    No total row count: COUNT(*) over the filtered 8M-row table costs as much as
    the page itself and would double every request's work. `has_more` is derived
    by asking for one row more than requested, which is free.
    """
    has_more = len(rows) > limit
    page = rows[:limit]

    links = {"self": request.url}
    if has_more:
        view_args = request.view_args or {}
        # Drop any query arg whose name collides with a path arg: passing both
        # to url_for is a duplicate-keyword TypeError, i.e. a 500 on the happy
        # path, triggerable by ?dataset_key=x.
        args = {k: v for k, v in request.args.items() if k not in view_args}
        args["limit"], args["offset"] = limit, offset + limit
        links["next"] = url_for(request.endpoint, **view_args, **args, _external=True)

    return {
        "dataset": dataset.key,
        "count": len(page),
        "limit": limit,
        "offset": offset,
        "has_more": has_more,
        "filters": {k: v for k, v in filters.items() if v is not None},
        "links": links,
        "data": page,
    }


def _cache_seconds(dataset: Dataset) -> int:
    return current_app.config[
        "CACHE_HISTORICAL" if dataset.key == "historical" else "CACHE_FORECAST"
    ]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@bp.get("/<dataset_key>/stations")
def stations(dataset_key: str):
    """Every station in the dataset, with its region and coordinates."""
    dataset = _dataset(dataset_key)
    rows = _db(dataset).rows(dataset.sql_stations)
    return _respond(
        {"dataset": dataset.key, "count": len(rows), "data": rows},
        # The station list changes when the pipeline adds a location: rare.
        max_age=max(_cache_seconds(dataset), 3600),
    )


@bp.get("/<dataset_key>/dates")
def dates(dataset_key: str):
    """Dates present in the dataset, with a per-day record count."""
    dataset = _dataset(dataset_key)
    start, end = _window()
    # Unbounded, this groups the whole table. Default to a bounded window and
    # let the caller widen it deliberately.
    if start is None and end is None:
        # Naive UTC, matching the `timestamp without time zone` columns.
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        end = now + timedelta(days=30)
        start = end - timedelta(days=current_app.config["DEFAULT_DATE_WINDOW_DAYS"])
    rows = _db(dataset).rows(
        dataset.sql_dates, {"start": start, "end": end}
    )
    return _respond(
        {
            "dataset": dataset.key,
            "count": len(rows),
            "filters": {"from": start.date().isoformat(), "to": (end - timedelta(days=1)).date().isoformat()},
            "data": rows,
        },
        max_age=_cache_seconds(dataset),
    )


@bp.get("/<dataset_key>/observations")
def observations(dataset_key: str):
    """A page of records, optionally filtered by station and date range."""
    dataset = _dataset(dataset_key)
    limit, offset = _pagination()
    start, end = _window()

    station = _station_arg(dataset)

    rows = _db(dataset).rows(
        dataset.sql_observations,
        {"station": station, "start": start, "end": end,
         "limit": limit + 1, "offset": offset},
    )
    return _respond(
        _page(dataset, rows, limit, offset, station=station,
              **{"from": start.date().isoformat() if start else None,
                 "to": (end - timedelta(days=1)).date().isoformat() if end else None}),
        max_age=_cache_seconds(dataset),
    )


@bp.get("/<dataset_key>/observations/station/<station_id>")
def observations_by_station(dataset_key: str, station_id: str):
    """Records for one station. 404 when the station isn't in the dataset."""
    dataset = _dataset(dataset_key)
    if not STATION_RE.match(station_id):
        raise BadRequest(
            f"Station id must be 11 digits, got {station_id!r}. "
            f"See /v1/{dataset.key}/stations."
        )
    db = _db(dataset)
    # Distinguish "no such station" (404) from "this station has no rows in the
    # requested window" (200 with an empty page). The old code returned the same
    # thing for both, and with a 200 status either way.
    if not db.rows(dataset.sql_count_hint, {"station": station_id}):
        raise NotFound(f"No station with id {station_id} in the {dataset.key} dataset.")

    limit, offset = _pagination()
    start, end = _window()
    rows = db.rows(
        dataset.sql_observations,
        {"station": station_id, "start": start, "end": end,
         "limit": limit + 1, "offset": offset},
    )
    return _respond(
        _page(dataset, rows, limit, offset, station=station_id),
        max_age=_cache_seconds(dataset),
    )


@bp.get("/<dataset_key>/observations/date/<date_str>")
def observations_by_date(dataset_key: str, date_str: str):
    """Records for a single calendar day, across all stations."""
    dataset = _dataset(dataset_key)
    day = _parse_date(date_str, "date")
    limit, offset = _pagination()

    start = datetime.combine(day, datetime.min.time())
    rows = _db(dataset).rows(
        dataset.sql_observations,
        {"station": _station_arg(dataset),
         "start": start, "end": start + timedelta(days=1),
         "limit": limit + 1, "offset": offset},
    )
    return _respond(
        _page(dataset, rows, limit, offset, date=day.isoformat()),
        max_age=_cache_seconds(dataset),
    )
