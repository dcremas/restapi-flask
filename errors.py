"""Error responses, as RFC 9457 problem details.

Every failure leaves this API as `application/problem+json` with a correct HTTP
status. That is the whole point of this module: the previous implementation
returned **HTTP 200** with a plain-text body reading "Error, Observations not
found, 404." — so a client checking the status code saw success, and a client
parsing JSON got a bare string where it expected a list. Both were silent.

Shape (RFC 9457):

    {
      "type":     "https://api.dustincremascoli.com/problems/not-found",
      "title":    "Not Found",
      "status":   404,
      "detail":   "No station with id 72509014739.",
      "instance": "/v1/historical/observations/station/72509014739",
      "request_id": "..."
    }
"""

from __future__ import annotations

from flask import g, jsonify, request

PROBLEM_BASE = "https://api.dustincremascoli.com/problems"

CONTENT_TYPE = "application/problem+json"


class ApiError(Exception):
    """An error with a deliberate HTTP status and a client-safe message."""

    status = 500
    slug = "internal-error"
    title = "Internal Server Error"

    def __init__(self, detail: str = "", **extra) -> None:
        super().__init__(detail)
        self.detail = detail
        self.extra = extra


class BadRequest(ApiError):
    status, slug, title = 400, "bad-request", "Bad Request"


class NotFound(ApiError):
    status, slug, title = 404, "not-found", "Not Found"


class ServiceUnavailable(ApiError):
    status, slug, title = 503, "service-unavailable", "Service Unavailable"


def problem(status: int, title: str, slug: str, detail: str = "", **extra):
    """Build a problem+json response."""
    body = {
        "type": f"{PROBLEM_BASE}/{slug}",
        "title": title,
        "status": status,
        "instance": request.path,
    }
    if detail:
        body["detail"] = detail
    # Echoing the request id lets a caller quote it and have it found in the
    # logs, without exposing anything about the failure internals.
    rid = getattr(g, "request_id", None)
    if rid:
        body["request_id"] = rid
    body.update(extra)

    response = jsonify(body)
    response.status_code = status
    response.mimetype = CONTENT_TYPE
    return response


def register(app) -> None:
    """Install handlers so no failure can escape as HTML."""
    from db import DatabaseUnavailable

    @app.errorhandler(ApiError)
    def _api_error(exc: ApiError):
        return problem(exc.status, exc.title, exc.slug, exc.detail, **exc.extra)

    @app.errorhandler(DatabaseUnavailable)
    def _db_down(exc: DatabaseUnavailable):
        # 503, not 500: the request was valid and retrying is the right move.
        app.logger.error("dataset %s unavailable", exc)
        resp = problem(
            503, "Service Unavailable", "service-unavailable",
            "The data store backing this endpoint is temporarily unreachable. "
            "Retry shortly.",
        )
        resp.headers["Retry-After"] = "30"
        return resp

    @app.errorhandler(404)
    def _not_found(_e):
        return problem(
            404, "Not Found", "not-found",
            "No such endpoint. See /docs for the available routes.",
        )

    @app.errorhandler(405)
    def _method_not_allowed(_e):
        return problem(
            405, "Method Not Allowed", "method-not-allowed",
            "This API is read-only; every endpoint accepts GET.",
        )

    @app.errorhandler(429)
    def _too_many(_e):
        resp = problem(
            429, "Too Many Requests", "rate-limited",
            "Rate limit exceeded. Retry after a short pause.",
        )
        resp.headers["Retry-After"] = "10"
        return resp

    @app.errorhandler(500)
    def _internal(_e):
        return problem(
            500, "Internal Server Error", "internal-error",
            "An unexpected error occurred. The request id identifies it in our logs.",
        )

    @app.errorhandler(Exception)
    def _unhandled(exc: Exception):
        # Last line of defence. Log the whole traceback, tell the client nothing
        # about it — an exception message can carry SQL or connection details.
        app.logger.exception("unhandled error: %s", exc)
        return problem(
            500, "Internal Server Error", "internal-error",
            "An unexpected error occurred. The request id identifies it in our logs.",
        )
