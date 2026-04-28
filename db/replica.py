"""
Versioned file-level replication for DuckDB files.

Used for market.duckdb, paper.duckdb (frequent sync, 5s debounce)
and backtest.duckdb (infrequent sync, on completion).

Engine-side: ReplicaSync (writes replicas).
Dashboard-side: ReplicaConsumer (reads replicas).

Sync flow:
  1. Engine writes to source DB
  2. After write -> mark_dirty() + maybe_sync(source_conn)
  3. Sync worker (runs synchronously when source_conn provided, else background thread):
     a. CHECKPOINT via source_conn (or secondary connection if no source_conn)
     b. Copy source DB -> replica_v{N}.duckdb.tmp (via COPY FROM DATABASE or ATTACH)
     c. Rename .tmp -> replica_v{N}.duckdb
     d. Write pointer file (atomic: write-to-tmp, rename)
  4. Dashboard reads pointer, detects version change, reconnects
  5. Old versions cleaned up (keep latest 2)

Windows note: DuckDB uses exclusive file locking on Windows. Opening the same
file from two connections in the same process fails. Callers MUST pass
source_conn to maybe_sync() so the copy uses the existing connection
(COPY FROM DATABASE) rather than opening a second one (ATTACH).
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


class ReplicaSync:
    """Manages versioned file-level replication for DuckDB files.

    Used for paper.duckdb (frequent sync, 5s debounce)
    and backtest.duckdb (infrequent sync, on completion).
    """

    MAX_REPLICA_VERSIONS = 2
    REPLACE_ATTEMPTS = 20
    REPLACE_RETRY_SLEEP_SEC = 0.1

    def __init__(
        self,
        db_path: Path,
        replica_dir: Path,
        min_interval_sec: float = 5.0,
    ):
        self.db_path = db_path
        self.replica_dir = replica_dir
        self.replica_dir.mkdir(parents=True, exist_ok=True)
        self.latest_pointer = replica_dir / f"{db_path.stem}_replica_latest"
        self.min_interval = min_interval_sec
        self._last_sync_time: float = 0
        self._sync_lock = threading.Lock()
        self._syncing = False
        self._current_version = 0
        self._current_version = self._detect_current_version()
        self._dirty_gen = 0
        self._synced_gen = 0

    def mark_dirty(self) -> None:
        """Called after any write operation. Increments generation."""
        self._dirty_gen += 1

    def maybe_sync(
        self,
        source_conn: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        """Debounced sync. Only copies if dirty AND >min_interval since last sync.

        When source_conn is provided the copy runs synchronously on the
        calling thread because DuckDB connections are not thread-safe.
        On Windows this is required — a secondary ATTACH of a file that
        is already open in the same process raises IOException.
        """
        should_start = False
        with self._sync_lock:
            if self._dirty_gen <= self._synced_gen:
                return
            if time.time() - self._last_sync_time < self.min_interval:
                return
            if self._syncing:
                return
            self._syncing = True
            should_start = True
        if should_start:
            if source_conn is not None:
                self._sync_worker(source_conn=source_conn)
            else:
                threading.Thread(target=self._sync_worker, daemon=True).start()

    def force_sync(
        self,
        source_conn: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        """Forced sync (backtest completion, manual request).

        Runs synchronously so callers can publish a finished snapshot before
        returning to the UI or CLI. Waits up to 60 seconds for an
        in-progress sync to finish.
        """
        for _ in range(1200):
            with self._sync_lock:
                if not self._syncing:
                    self._syncing = True
                    break
            time.sleep(0.05)
        else:
            raise RuntimeError(
                f"Replica sync busy for {self.db_path.stem} — timeout waiting for prior sync"
            )
        self._sync_worker(source_conn=source_conn, raise_on_error=True)

    def get_current_version(self) -> int:
        return self._current_version

    def _detect_current_version(self) -> int:
        if self.latest_pointer.exists():
            try:
                content = self.latest_pointer.read_text().strip()
                return int(content.replace("v", ""))
            except (ValueError, OSError):
                pass
        return self._current_version

    def _resolve_source_db_name(self, source_conn: duckdb.DuckDBPyConnection) -> str:
        """Return the attached database name for the live source connection."""
        try:
            target_path = self.db_path.resolve()
        except OSError:
            target_path = self.db_path
        try:
            rows = source_conn.execute("PRAGMA database_list").fetchall()
        except Exception:
            rows = []
        for row in rows:
            if len(row) < 3:
                continue
            db_name = row[1]
            db_file = row[2]
            if not db_name or not db_file:
                continue
            try:
                if Path(str(db_file)).resolve() == target_path:
                    return str(db_name)
            except OSError:
                if Path(str(db_file)) == target_path:
                    return str(db_name)
        # Fallback: the primary database name is usually the first non-system entry.
        for row in rows:
            if len(row) >= 2 and row[1] not in ("system", "temp"):
                return str(row[1])
        return self.db_path.stem

    @staticmethod
    def _quote_identifier(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def _replace_with_retry(self, source: Path, target: Path) -> None:
        """Replace a replica file, tolerating transient Windows reader locks."""
        last_error: OSError | None = None
        for attempt in range(1, self.REPLACE_ATTEMPTS + 1):
            try:
                source.replace(target)
                return
            except OSError as exc:
                last_error = exc
                if attempt >= self.REPLACE_ATTEMPTS:
                    break
                time.sleep(self.REPLACE_RETRY_SLEEP_SEC)
        assert last_error is not None
        raise last_error

    def _sync_worker(
        self,
        source_conn: duckdb.DuckDBPyConnection | None = None,
        *,
        raise_on_error: bool = False,
    ) -> None:
        gen_at_start = self._dirty_gen
        try:
            source_db_name = self.db_path.stem
            if source_conn is not None:
                checkpoint_conn = source_conn
                source_db_name = self._resolve_source_db_name(source_conn)
                checkpoint_cleanup = False
            else:
                # Open a secondary connection only when the caller did not hand us
                # the live writer connection. This path is still used by ad hoc
                # maintenance scripts.
                checkpoint_conn = duckdb.connect(str(self.db_path))
                checkpoint_cleanup = True
            try:
                for attempt in range(3):
                    try:
                        checkpoint_conn.execute("CHECKPOINT")
                        break
                    except Exception:
                        if attempt < 2:
                            time.sleep(0.1 * (attempt + 1))
                            continue
                        raise
            finally:
                if checkpoint_cleanup:
                    checkpoint_conn.close()

            # Versioned copy
            version = self._current_version + 1
            replica_name = f"{self.db_path.stem}_replica_v{version}.duckdb"
            tmp_path = self.replica_dir / f"{replica_name}.tmp"
            final_path = self.replica_dir / replica_name

            tmp_path.unlink(missing_ok=True)
            if source_conn is not None:
                source_ident = self._quote_identifier(source_db_name)
                source_conn.execute(f"ATTACH '{tmp_path.as_posix()}' AS dstdb")
                try:
                    source_conn.execute(f"COPY FROM DATABASE {source_ident} TO dstdb")
                finally:
                    try:
                        source_conn.execute("DETACH dstdb")
                    except Exception:
                        pass
            else:
                copy_conn = duckdb.connect()
                try:
                    copy_conn.execute(f"ATTACH '{tmp_path.as_posix()}' AS dstdb")
                    copy_conn.execute(f"ATTACH '{self.db_path.as_posix()}' AS srcdb")
                    copy_conn.execute("COPY FROM DATABASE srcdb TO dstdb")
                    copy_conn.execute("DETACH srcdb")
                    copy_conn.execute("DETACH dstdb")
                finally:
                    copy_conn.close()
            self._replace_with_retry(tmp_path, final_path)

            # Atomic pointer write (write-to-tmp, rename)
            pointer_tmp = self.latest_pointer.with_suffix(".latest.tmp")
            pointer_tmp.write_text(f"v{version}")
            self._replace_with_retry(pointer_tmp, self.latest_pointer)

            with self._sync_lock:
                self._current_version = version
                self._synced_gen = gen_at_start
                self._last_sync_time = time.time()

            self._cleanup_old_versions(version)
        except Exception:
            logger.exception("Replica sync failed for %s", self.db_path.stem)
            if raise_on_error:
                raise
        finally:
            with self._sync_lock:
                self._syncing = False

    def _cleanup_old_versions(self, current: int) -> None:
        cutoff = current - self.MAX_REPLICA_VERSIONS
        stem_prefix = f"{self.db_path.stem}_replica_v"
        for f in self.replica_dir.glob(f"{stem_prefix}*.duckdb"):
            try:
                ver_str = f.stem.split("_v")[1].split(".")[0]
                ver = int(ver_str)
                if ver <= cutoff:
                    f.unlink(missing_ok=True)
            except (ValueError, IndexError, OSError):
                pass
