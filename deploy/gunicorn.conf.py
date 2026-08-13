"""gunicorn configuration for the API.

Sized for a t3.medium that also runs two websites, Postgres and the Bokeh apps —
so this is deliberately modest. Two sync workers with threads, rather than many
processes: the work is almost entirely waiting on Postgres, and each worker holds
its own connection pool (POOL_MAX_SIZE per database), so worker count multiplies
database connections.
"""

import os

bind = "unix:/run/restapi/restapi.sock"
# umask 007 so the socket is group-writable and nginx (group nginx) can open it.
umask = 0o007

workers = int(os.getenv("GUNICORN_WORKERS", "2"))
threads = int(os.getenv("GUNICORN_THREADS", "4"))
worker_class = "gthread"

# Longer than the 8s statement_timeout, so a slow query is killed by Postgres
# with a clean 503 rather than by gunicorn killing the whole worker.
timeout = 30
graceful_timeout = 30
keepalive = 5

# Recycle workers periodically: bounds the impact of any slow leak, and the
# jitter stops every worker restarting in the same instant.
max_requests = 2000
max_requests_jitter = 200

# Logging is configured in the app as JSON on stdout; journald captures it.
accesslog = None
errorlog = "-"
loglevel = os.getenv("LOG_LEVEL", "info").lower()


def worker_exit(server, worker):
    """Close connection pools on the way out.

    Without this, pools are finalized during interpreter shutdown, where psycopg
    cannot join its maintenance threads — producing a traceback on every worker
    recycle and leaving Postgres to time the connections out itself.
    """
    try:
        from app import app

        for database in app.extensions.get("databases", {}).values():
            database.close()
    except Exception:
        pass
