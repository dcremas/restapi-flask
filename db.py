"""Database access.

One connection pool per dataset. Pools rather than a single long-lived
connection because the previous implementation opened one connection at import
time and never re-established it: a Postgres restart — or any dropped socket —
broke every subsequent request permanently until the process was restarted, and
nothing in the app noticed. A pool with `check` re-validates a connection before
handing it out, so a database bounce costs one request instead of the service.

Every connection is configured read-only with a statement timeout. The role is
SELECT-only as well (see deploy/sql/roles.sql); this is the second lock on the
same door, and the timeout is what stops one expensive query against the 8M-row
observations table from pinning a worker indefinitely.
"""

from __future__ import annotations

import logging
import threading

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

log = logging.getLogger(__name__)


class DatabaseUnavailable(RuntimeError):
    """Raised when a pool cannot produce a usable connection."""


class Database:
    """A lazily-opened pool for one logical dataset."""

    def __init__(
        self,
        name: str,
        conninfo: str,
        *,
        statement_timeout_ms: int,
        min_size: int = 1,
        max_size: int = 4,
        connect_timeout: float = 5.0,
    ) -> None:
        self.name = name
        self._conninfo = conninfo
        self._statement_timeout_ms = statement_timeout_ms
        self._min_size = min_size
        self._max_size = max_size
        self._connect_timeout = connect_timeout
        self._pool: ConnectionPool | None = None
        self._lock = threading.Lock()

    # -- lifecycle ---------------------------------------------------------
    def _configure(self, conn: Connection) -> None:
        """Applied to every new connection in the pool."""
        conn.autocommit = True
        with conn.cursor() as cur:
            # A read-only session makes a stray write impossible even if the
            # role's grants were ever widened by mistake.
            cur.execute("SET default_transaction_read_only = on")
            cur.execute(f"SET statement_timeout = {self._statement_timeout_ms}")
            # Never let a query wait on someone else's lock; fail fast instead.
            cur.execute("SET lock_timeout = 2000")
            cur.execute("SET idle_in_transaction_session_timeout = 10000")
            cur.execute("SET application_name = 'restapi'")

    @property
    def pool(self) -> ConnectionPool:
        """The pool, opened on first use.

        `open=False` plus lazy creation means the process starts even when
        Postgres is down — the public /health probe stays green and only the
        routes that need this dataset fail, which is what lets one dataset
        outlive an outage in the other.
        """
        if self._pool is None:
            with self._lock:
                if self._pool is None:
                    self._pool = ConnectionPool(
                        conninfo=self._conninfo,
                        min_size=self._min_size,
                        max_size=self._max_size,
                        timeout=self._connect_timeout,
                        max_idle=300.0,
                        # Recycle rather than trust: a connection idle across a
                        # database restart looks fine until it is used.
                        max_lifetime=1800.0,
                        check=ConnectionPool.check_connection,
                        configure=self._configure,
                        kwargs={"connect_timeout": int(self._connect_timeout)},
                        name=f"pool-{self.name}",
                        open=False,
                    )
                    self._pool.open()
        return self._pool

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None

    # -- querying ----------------------------------------------------------
    def rows(self, sql: str, params: tuple | dict | None = None) -> list[dict]:
        """Run a read query and return a list of dicts.

        Any driver-level failure becomes DatabaseUnavailable, which the error
        handlers turn into a 503 with a Retry-After — never a 500, because the
        request itself was fine.
        """
        try:
            with self.pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(sql, params)
                    return cur.fetchall()
        except Exception as exc:
            log.warning("query against %s failed: %s", self.name, exc)
            raise DatabaseUnavailable(self.name) from exc

    def healthy(self) -> tuple[bool, str | None]:
        """Cheap readiness probe. Returns (ok, error message)."""
        try:
            with self.pool.connection(timeout=2.0) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            return True, None
        except Exception as exc:
            # str(exc) only — a psycopg error can carry the conninfo, and that
            # would put the database password in an HTTP response.
            return False, exc.__class__.__name__
