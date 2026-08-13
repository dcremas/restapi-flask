"""The OpenAPI 3.0 document, generated from the dataset definitions.

Generated rather than hand-maintained so the spec cannot drift from the routes:
both come from the same `Dataset` records. Served at /openapi.json and rendered
by Swagger UI at /docs.
"""

from __future__ import annotations

from datasets import DATASETS, Dataset

_PAGINATION_PARAMS = [
    {
        "name": "limit", "in": "query", "required": False,
        "description": "Records per page (1-1000).",
        "schema": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100},
    },
    {
        "name": "offset", "in": "query", "required": False,
        "description": "Records to skip. Capped at 100000 — narrow by station or date instead.",
        "schema": {"type": "integer", "minimum": 0, "maximum": 100000, "default": 0},
    },
]

_WINDOW_PARAMS = [
    {
        "name": "from", "in": "query", "required": False,
        "description": "Earliest date to include (inclusive), as YYYY-MM-DD.",
        "schema": {"type": "string", "format": "date"},
    },
    {
        "name": "to", "in": "query", "required": False,
        "description": "Latest date to include (inclusive), as YYYY-MM-DD.",
        "schema": {"type": "string", "format": "date"},
    },
]

_STATION_FILTER = {
    "name": "station", "in": "query", "required": False,
    "description": "Restrict to one 11-digit station id.",
    "schema": {"type": "string", "pattern": "^[0-9]{11}$"},
}

_STATION_PATH = {
    "name": "station_id", "in": "path", "required": True,
    "description": "11-digit NOAA station id (USAF + WBAN).",
    "schema": {"type": "string", "pattern": "^[0-9]{11}$"},
    "example": "72509014739",
}

_DATE_PATH = {
    "name": "date_str", "in": "path", "required": True,
    "description": "Calendar day as YYYY-MM-DD.",
    "schema": {"type": "string", "format": "date"},
    "example": "2024-01-15",
}

_PROBLEM = {
    "description": "Problem details (RFC 9457).",
    "content": {
        "application/problem+json": {
            "schema": {"$ref": "#/components/schemas/Problem"},
        }
    },
}

_RESPONSES = {
    "400": _PROBLEM | {"description": "Invalid parameter."},
    "404": _PROBLEM | {"description": "Unknown dataset, station or endpoint."},
    "429": _PROBLEM | {"description": "Rate limit exceeded."},
    "503": _PROBLEM | {"description": "Data store temporarily unreachable."},
}


def _page_response(dataset: Dataset) -> dict:
    return {
        "200": {
            "description": "A page of records.",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/Page"},
                }
            },
        },
        "304": {"description": "Not modified (matched If-None-Match)."},
        **_RESPONSES,
    }


def build(base_url: str | None = None) -> dict:
    paths: dict = {}

    for key, dataset in DATASETS.items():
        tag = dataset.title
        prefix = f"/v1/{key}"

        paths[f"{prefix}/stations"] = {
            "get": {
                "tags": [tag],
                "operationId": f"{key}_stations",
                "summary": "List every station in this dataset",
                "description": (
                    "The 112 curated airport stations, with region, state and "
                    "coordinates. Cached for at least an hour."
                ),
                "responses": {
                    "200": {
                        "description": "The station list.",
                        "content": {"application/json": {"schema": {
                            "$ref": "#/components/schemas/StationList"}}},
                    },
                    **_RESPONSES,
                },
            }
        }

        paths[f"{prefix}/dates"] = {
            "get": {
                "tags": [tag],
                "operationId": f"{key}_dates",
                "summary": "List dates present, with a per-day record count",
                "description": (
                    "Defaults to a recent window rather than the whole table; "
                    "widen it with from/to."
                ),
                "parameters": _WINDOW_PARAMS,
                "responses": {
                    "200": {
                        "description": "Dates and counts.",
                        "content": {"application/json": {"schema": {
                            "$ref": "#/components/schemas/DateList"}}},
                    },
                    **_RESPONSES,
                },
            }
        }

        paths[f"{prefix}/observations"] = {
            "get": {
                "tags": [tag],
                "operationId": f"{key}_observations",
                "summary": "A page of records, optionally filtered",
                "description": (
                    f"Measurement fields: {', '.join(dataset.measures)}. "
                    f"Timestamps are naive UTC in `{dataset.time_field}`."
                ),
                "parameters": [_STATION_FILTER, *_WINDOW_PARAMS, *_PAGINATION_PARAMS],
                "responses": _page_response(dataset),
            }
        }

        paths[f"{prefix}/observations/station/{{station_id}}"] = {
            "get": {
                "tags": [tag],
                "operationId": f"{key}_observations_by_station",
                "summary": "Records for one station",
                "description": "404 if the station is not in the dataset; an empty page if it simply has no records in the window.",
                "parameters": [_STATION_PATH, *_WINDOW_PARAMS, *_PAGINATION_PARAMS],
                "responses": _page_response(dataset),
            }
        }

        paths[f"{prefix}/observations/date/{{date_str}}"] = {
            "get": {
                "tags": [tag],
                "operationId": f"{key}_observations_by_date",
                "summary": "Records for a single calendar day",
                "parameters": [_DATE_PATH, _STATION_FILTER, *_PAGINATION_PARAMS],
                "responses": _page_response(dataset),
            }
        }

    paths["/health"] = {
        "get": {
            "tags": ["Operations"],
            "operationId": "health",
            "summary": "Liveness — does not touch the database",
            "description": (
                "Deliberately independent of Postgres so a monitor can tell "
                "'API down' apart from 'database down'."
            ),
            "responses": {"200": {"description": "The service is running."}},
        }
    }
    paths["/ready"] = {
        "get": {
            "tags": ["Operations"],
            "operationId": "ready",
            "summary": "Readiness — checks each dataset's database",
            "description": (
                "200 when every dataset answers; 200 with status 'degraded' "
                "when some do; 503 when none do."
            ),
            "responses": {
                "200": {"description": "Ready, possibly degraded."},
                "503": {"description": "No dataset is reachable."},
            },
        }
    }

    doc = {
        "openapi": "3.0.3",
        "info": {
            "title": "Dustin Cremascoli — Weather Data API",
            "version": "1.0.0",
            "description": (
                "Read-only access to the cleaned weather data behind the "
                "visualizations on dustincremascoli.com.\n\n"
                "**Two datasets.** `historical` is NOAA hourly surface "
                "observations; `forecast` is a rolling Apple WeatherKit window. "
                "Both cover the same 112 US airport stations.\n\n"
                "**No authentication.** Every endpoint is a public GET. Be "
                "considerate: responses are cacheable and rate limited per IP.\n\n"
                "**Errors** are `application/problem+json` (RFC 9457) with an "
                "accurate HTTP status and a `request_id` you can quote."
            ),
            "contact": {"name": "Dustin Cremascoli", "url": "https://www.dustincremascoli.com"},
            "license": {"name": "CC BY 4.0", "url": "https://creativecommons.org/licenses/by/4.0/"},
        },
        "servers": [{"url": base_url or "/", "description": "This server"}],
        "tags": [
            *[{"name": d.title, "description": d.description} for d in DATASETS.values()],
            {"name": "Operations", "description": "Health and readiness probes."},
        ],
        "paths": paths,
        "components": {
            "schemas": {
                "Problem": {
                    "type": "object",
                    "description": "RFC 9457 problem details.",
                    "properties": {
                        "type": {"type": "string", "format": "uri"},
                        "title": {"type": "string"},
                        "status": {"type": "integer"},
                        "detail": {"type": "string"},
                        "instance": {"type": "string"},
                        "request_id": {"type": "string"},
                    },
                    "required": ["type", "title", "status"],
                },
                "Page": {
                    "type": "object",
                    "properties": {
                        "dataset": {"type": "string"},
                        "count": {"type": "integer", "description": "Records in this page."},
                        "limit": {"type": "integer"},
                        "offset": {"type": "integer"},
                        "has_more": {
                            "type": "boolean",
                            "description": "Whether a further page exists. There is no total count: COUNT(*) over the filtered 8M-row table would cost as much as the page itself.",
                        },
                        "filters": {"type": "object"},
                        "links": {
                            "type": "object",
                            "properties": {
                                "self": {"type": "string", "format": "uri"},
                                "next": {"type": "string", "format": "uri"},
                            },
                        },
                        "data": {"type": "array", "items": {"type": "object"}},
                    },
                },
                "StationList": {
                    "type": "object",
                    "properties": {
                        "dataset": {"type": "string"},
                        "count": {"type": "integer"},
                        "data": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "station": {"type": "string", "example": "72509014739"},
                                    "station_name": {"type": "string"},
                                    "state": {"type": "string"},
                                    "region": {"type": "string"},
                                    "sub_region": {"type": "string"},
                                    "lat": {"type": "number"},
                                    "lon": {"type": "number"},
                                },
                            },
                        },
                    },
                },
                "DateList": {
                    "type": "object",
                    "properties": {
                        "dataset": {"type": "string"},
                        "count": {"type": "integer"},
                        "filters": {"type": "object"},
                        "data": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "date": {"type": "string", "format": "date"},
                                    "observations": {"type": "integer"},
                                },
                            },
                        },
                    },
                },
            }
        },
    }
    return doc
