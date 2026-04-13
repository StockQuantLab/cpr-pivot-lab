"""
Dashboard-side reader for versioned DuckDB replicas.

Reads versioned replica files created by ReplicaSync and reconnects
on version change. Handles stale/failure states gracefully.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


class ReplicaConsumer:
    """Dashboard-side: reads versioned replicas and reconnects on version change."""

    def __init__(self, replica_dir: Path, db_stem: str):
        self.replica_dir = replica_dir
        self.db_stem = db_stem
        self.pointer_file = replica_dir / f"{db_stem}_replica_latest"
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._current_version = 0
        self._lock = threading.Lock()

    def get_connection(self) -> duckdb.DuckDBPyConnection | None:
        version = self._read_version()
        if version != self._current_version and version > 0:
            with self._lock:
                # Re-check version under lock to prevent double reconnect
                version = self._read_version()
                if version != self._current_version and version > 0:
                    self._reconnect(version)
        return self._conn

    def get_replica_path(self) -> Path | None:
        version = self._read_version()
        if version <= 0:
            # Pointer file missing — scan disk for the latest replica version
            version = self._scan_latest_version()
            if version <= 0:
                return None
        replica_path = self.replica_dir / f"{self.db_stem}_replica_v{version}.duckdb"
        return replica_path if replica_path.exists() else None

    def get_version(self) -> int:
        return self._read_version()

    def get_stale_seconds(self) -> float:
        if self._current_version == 0:
            return float("inf")
        replica_path = self.replica_dir / f"{self.db_stem}_replica_v{self._current_version}.duckdb"
        if not replica_path.exists():
            return float("inf")
        return time.time() - replica_path.stat().st_mtime

    def _read_version(self) -> int:
        if self.pointer_file.exists():
            try:
                content = self.pointer_file.read_text().strip()
                return int(content.replace("v", ""))
            except (ValueError, OSError):
                pass
        return self._current_version

    def _scan_latest_version(self) -> int:
        """Scan replica_dir for the highest versioned replica file on disk.

        Used when the pointer file is missing (first startup, crash recovery).
        """
        prefix = f"{self.db_stem}_replica_v"
        max_ver = 0
        try:
            for f in self.replica_dir.glob(f"{prefix}*.duckdb"):
                try:
                    ver_str = f.stem.split("_v")[1].split(".")[0]
                    ver = int(ver_str)
                    if ver > max_ver:
                        max_ver = ver
                except (ValueError, IndexError):
                    pass
        except OSError:
            pass
        if max_ver > 0:
            self._current_version = max_ver
            logger.info(
                "Pointer file missing for %s — found replica v%d on disk",
                self.db_stem,
                max_ver,
            )
        return max_ver

    def _reconnect(self, version: int) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
        replica_path = self.replica_dir / f"{self.db_stem}_replica_v{version}.duckdb"
        if replica_path.exists():
            self._conn = duckdb.connect(str(replica_path), read_only=True)
            self._current_version = version
