"""Weather Data API — application factory.

A read-only JSON API over the two weather datasets on this host, replacing the
pair of Heroku services that previously served them.

Runs under gunicorn behind nginx, exactly like the two websites on this box, so
there is one server stack to operate rather than three. Everything
request-scoped (request id, timing, access log) is set up here; the SQL lives in
datasets.py and the HTTP shaping in routes.py.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

from dotenv import load_dotenv
from flask import Flask, Response, g, jsonify, request, send_from_directory
from flask.json.provider import DefaultJSONProvider
from werkzeug.middleware.proxy_fix import ProxyFix

import errors
from datasets import DATASETS
from db import Database

load_dotenv()

REQUEST_ID_HEADER = "X-Request-Id"


class IsoJSONProvider(DefaultJSONProvider):
    """Serialize dates as ISO 8601.

    Flask's default renders a datetime as an RFC 822 HTTP date
    ("Mon, 15 Jan 2024 00:54:00 GMT"), which is right for headers and wrong for
    a JSON API — callers expect "2024-01-15T00:54:00" and every language's date
    parser accepts it without a custom format string.

    The columns are `timestamp without time zone` holding UTC, so a "Z" suffix
    is added to say so explicitly rather than leaving the zone ambiguous.
    """

    @staticmethod
    def default(o):
        if isinstance(o, datetime):
            return o.isoformat() + ("" if o.tzinfo else "Z")
        if isinstance(o, date):
            return o.isoformat()
        if isinstance(o, Decimal):
            # Numeric columns would otherwise raise. float is right here: these
            # are physical measurements, not money.
            return float(o)
        return DefaultJSONProvider.default(o)


def _conninfo(prefix: str, database: str) -> str:
    """Build a libpq conninfo string for one database.

    Per-database URL first (e.g. DATABASE_URL_WEATHERDATA), then a shared host
    with the database name substituted. Keeping the password out of the app's
    own vocabulary — it only ever appears in the env file — is why this returns a
    conninfo string rather than components.
    """
    specific = os.getenv(f"DATABASE_URL_{prefix}")
    if specific:
        return specific
    host = os.getenv("PG_HOST", "127.0.0.1")
    port = os.getenv("PG_PORT", "5432")
    user = os.getenv("PG_USER", "api_ro")
    password = os.getenv("PG_PASSWORD", "")
    parts = [f"host={host}", f"port={port}", f"dbname={database}", f"user={user}"]
    if password:
        parts.append(f"password={password}")
    # Fail a connection attempt quickly rather than tying up a worker.
    parts.append("connect_timeout=5")
    return " ".join(parts)


def _configure_logging(app: Flask) -> None:
    """One JSON object per line on stdout, which journald captures.

    Structured because these lines are read by grep and jq during an incident;
    a request id that cannot be searched for is not much use.
    """
    class JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            payload = {
                "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "level": record.levelname,
                "logger": record.name,
                "msg": record.getMessage(),
            }
            for key in ("request_id", "method", "path", "status", "duration_ms", "remote"):
                if (value := getattr(record, key, None)) is not None:
                    payload[key] = value
            if record.exc_info:
                payload["exc"] = self.formatException(record.exc_info)
            return json.dumps(payload, ensure_ascii=False)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(app.config["LOG_LEVEL"])
    # gunicorn installs its own handlers; replace them so every line is JSON.
    for name in ("gunicorn.error", "gunicorn.access", "werkzeug"):
        gl = logging.getLogger(name)
        gl.handlers = [handler]
        gl.propagate = False


def create_app(config: dict | None = None) -> Flask:
    app = Flask(__name__)
    app.json = IsoJSONProvider(app)
    # Compact separators: these payloads are up to 1000 records, and nginx gzips
    # them anyway — the pretty-printing Flask does by default is pure bytes.
    app.json.compact = True

    app.config.update(
        LOG_LEVEL=os.getenv("LOG_LEVEL", "INFO").upper(),
        # Cache lifetimes. Historical observations never change once ingested;
        # forecasts are refreshed hourly, so a long TTL would serve stale data.
        CACHE_HISTORICAL=int(os.getenv("CACHE_HISTORICAL", "3600")),
        CACHE_FORECAST=int(os.getenv("CACHE_FORECAST", "300")),
        # /dates over the whole 8M-row table is a full grouping; default to a
        # window and let a caller widen it on purpose.
        DEFAULT_DATE_WINDOW_DAYS=int(os.getenv("DEFAULT_DATE_WINDOW_DAYS", "60")),
        STATEMENT_TIMEOUT_MS=int(os.getenv("STATEMENT_TIMEOUT_MS", "8000")),
        # Connections are per worker, per database. Total steady-state usage is
        # GUNICORN_WORKERS x databases x POOL_MAX_SIZE = 2 x 2 x 2 = 8, which must
        # stay under api_ro's CONNECTION LIMIT (20, leaving headroom for the brief
        # overlap while old and new workers coexist during a restart). Defaulting
        # this to 4 would exceed the limit and fail only under load.
        POOL_MAX_SIZE=int(os.getenv("POOL_MAX_SIZE", "2")),
        JSON_SORT_KEYS=False,
        # No trailing-slash redirects: /v1/historical/stations/ should 404
        # rather than 308 to a different URL, so clients don't depend on it.
        STRICT_SLASHES=False,
    )
    if config:
        app.config.update(config)

    app.url_map.strict_slashes = False
    _configure_logging(app)

    # One proxy hop: nginx. Without this, rate limiting and the access log see
    # the proxy's address instead of the caller's.
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

    # One pool per distinct database named by a dataset.
    app.extensions["databases"] = {
        name: Database(
            name,
            _conninfo(name.upper(), name),
            statement_timeout_ms=app.config["STATEMENT_TIMEOUT_MS"],
            max_size=app.config["POOL_MAX_SIZE"],
        )
        for name in {d.database for d in DATASETS.values()}
    }

    _register_request_lifecycle(app)
    errors.register(app)

    from routes import bp

    app.register_blueprint(bp)
    _register_meta_routes(app)
    return app


def _register_request_lifecycle(app: Flask) -> None:
    @app.before_request
    def _start():
        # Honour an inbound request id so a trace survives across hops, but cap
        # its length and character set — it is echoed into responses and logs.
        inbound = (request.headers.get(REQUEST_ID_HEADER) or "")[:64]
        g.request_id = inbound if inbound.replace("-", "").isalnum() else uuid.uuid4().hex
        g.started = time.perf_counter()

    @app.after_request
    def _finish(response: Response) -> Response:
        duration_ms = round((time.perf_counter() - getattr(g, "started", time.perf_counter())) * 1000, 2)
        response.headers[REQUEST_ID_HEADER] = getattr(g, "request_id", "")
        response.headers["X-Response-Time-Ms"] = str(duration_ms)

        # A public read-only API: allow browser clients from anywhere. Safe
        # because there are no cookies, no credentials and no mutating verbs.
        response.headers.setdefault("Access-Control-Allow-Origin", "*")
        response.headers.setdefault("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        response.headers.setdefault("Access-Control-Allow-Headers", "Content-Type, If-None-Match")
        response.headers.setdefault("Access-Control-Expose-Headers",
                                    f"ETag, {REQUEST_ID_HEADER}, X-Response-Time-Ms, Retry-After")
        response.headers.setdefault("Access-Control-Max-Age", "86400")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")

        app.logger.info(
            "request",
            extra={
                "request_id": getattr(g, "request_id", None),
                "method": request.method,
                "path": request.full_path.rstrip("?"),
                "status": response.status_code,
                "duration_ms": duration_ms,
                "remote": request.remote_addr,
            },
        )
        return response


def _register_meta_routes(app: Flask) -> None:
    import swagger_ui_bundle

    @app.get("/")
    def index():
        """A JSON index. An API root should describe itself, not 404."""
        return jsonify(
            service="weather-data-api",
            version="1.0.0",
            documentation=f"{request.host_url.rstrip('/')}/docs",
            openapi=f"{request.host_url.rstrip('/')}/openapi.json",
            datasets={
                key: {
                    "title": d.title,
                    "description": d.description,
                    "endpoints": [
                        f"/v1/{key}/stations",
                        f"/v1/{key}/dates",
                        f"/v1/{key}/observations",
                        f"/v1/{key}/observations/station/{{station_id}}",
                        f"/v1/{key}/observations/date/{{date}}",
                    ],
                }
                for key, d in DATASETS.items()
            },
            operations=["/health", "/ready"],
        )

    @app.get("/openapi.json")
    def openapi():
        import spec

        return jsonify(spec.build(request.host_url.rstrip("/")))

    @app.get("/docs")
    def docs():
        """Swagger UI, served from the vendored bundle.

        Local files rather than a CDN, so the docs work regardless of outbound
        network and add no third-party dependency to a page describing a
        public API.
        """
        return Response(_DOCS_HTML, mimetype="text/html")

    @app.get("/docs/<path:filename>")
    def docs_asset(filename: str):
        return send_from_directory(str(swagger_ui_bundle.swagger_ui_path), filename)

    @app.get("/health")
    def health():
        """Liveness only — deliberately never touches Postgres, so a monitor can
        tell 'API down' apart from 'database down'."""
        return jsonify(status="ok", service="weather-data-api")

    @app.get("/ready")
    def ready():
        """Readiness, per dataset.

        Degraded rather than down when only one dataset is unreachable: the
        other's endpoints still serve correctly, and taking the whole API out of
        rotation for a partial fault would be self-inflicted downtime.
        """
        databases = app.extensions["databases"]
        checks = {}
        for name, database in databases.items():
            ok, err = database.healthy()
            checks[name] = {"ok": ok, **({"error": err} if err else {})}

        healthy = sum(1 for c in checks.values() if c["ok"])
        if healthy == len(checks):
            status, code = "ok", 200
        elif healthy:
            status, code = "degraded", 200
        else:
            status, code = "unavailable", 503

        response = jsonify(status=status, databases=checks)
        response.status_code = code
        # A readiness probe must never be cached.
        response.headers["Cache-Control"] = "no-store"
        if code == 503:
            response.headers["Retry-After"] = "30"
        return response


_DOCS_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Weather Data API — Reference</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="/docs/swagger-ui.css">
  <style>
    body { margin: 0; background: #fafafa; }
    .topbar { display: none; }
  </style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="/docs/swagger-ui-bundle.js"></script>
  <script src="/docs/swagger-ui-standalone-preset.js"></script>
  <script>
    window.ui = SwaggerUIBundle({
      url: "/openapi.json",
      dom_id: "#swagger-ui",
      deepLinking: true,
      tryItOutEnabled: true,
      defaultModelsExpandDepth: -1,
      presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
      layout: "StandaloneLayout"
    });
  </script>
</body>
</html>
"""


app = create_app()

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.getenv("PORT", "5100")), debug=True)
