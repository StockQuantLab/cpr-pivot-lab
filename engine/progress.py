"""
Progress tracking for CPR backtest engine.

Provides tqdm-based progress bars with:
- Multi-symbol progress (overall)
- Per-symbol progress (days processed)
- ETA calculation
- Performance metrics (candles/sec, trades/sec)

Example output:
    Overall Progress:  15%|███▎    | 270/1800 [05:30<31:30, 0.82symbol/s]
      RELIANCE: 67%|██████▋▌| 169/252 [00:08<00:04, 18.2day/s]
"""

from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tqdm.std import tqdm as tqdm_type
else:
    tqdm_type = Any

try:
    from tqdm import tqdm as _tqdm_impl

    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    _tqdm_impl = None


# Fallback dummy tqdm if not installed
class _DummyTqdm:
    """No-op tqdm replacement when tqdm is not available."""

    def __init__(self, *args: Any, **kwargs: Any):
        self.n = 0
        self.total = kwargs.get("total", 0)
        self._desc = kwargs.get("desc", "")

    def update(self, n: int = 1) -> None:
        self.n += n

    def set_postfix_str(self, s: str) -> None:
        pass

    def write(self, s: str) -> None:
        """No-op write to match tqdm API."""
        print(s)

    def close(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def _tqdm(*args: Any, **kwargs: Any) -> _DummyTqdm | tqdm_type:
    """Return tqdm if available, otherwise dummy class."""
    if _tqdm_impl is not None:
        return _tqdm_impl(*args, **kwargs)
    return _DummyTqdm(*args, **kwargs)


class BacktestProgress:
    """
    Tracks progress across multiple symbols with nested progress bars.

    Usage:
        progress = BacktestProgress(total_symbols=10, verbose=True)

        for symbol in symbols:
            with progress.symbol_context(symbol, total_days=252) as day_prog:
                for trade_date in trade_dates:
                    # ... backtest work ...
                    day_prog.update(1)
    """

    def __init__(self, total_symbols: int, verbose: bool = True):
        self.verbose = verbose and TQDM_AVAILABLE
        self.total_symbols = total_symbols
        self.start_time = time.time()

        if self.verbose:
            self.overall_bar = _tqdm(
                total=total_symbols,
                desc="Overall Progress",
                unit="sym",
                position=0,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
            )
            self.symbol_bar: _DummyTqdm | tqdm_type = _tqdm()
        else:
            self.overall_bar = None
            self.symbol_bar = None

        self.symbol_results: list[dict[str, Any]] = []

    def symbol_context(self, symbol: str, total_days: int):
        """Context manager for per-symbol progress tracking."""
        return _SymbolProgress(self, symbol, total_days)

    def update_symbol(self, symbol: str, trades_count: int, elapsed: float) -> None:
        """Update overall progress after symbol completes."""
        if self.overall_bar is not None:
            self.overall_bar.update(1)
            if trades_count > 0:
                self.overall_bar.set_postfix_str(f"{symbol}: {trades_count} trades, {elapsed:.1f}s")

        self.symbol_results.append(
            {
                "symbol": symbol,
                "trades": trades_count,
                "elapsed": elapsed,
            }
        )

    def log_stage(self, label: str, **stats: object) -> None:
        """Print a [stage] status line, routing through tqdm.write() when active."""
        parts = [label] + [f"{k}={v}" for k, v in stats.items()]
        msg = "[stage] " + " | ".join(parts)
        if self.overall_bar is not None:
            self.overall_bar.write(msg)
        else:
            print(msg)

    def close(self) -> None:
        """Clean up progress bars."""
        if self.overall_bar is not None:
            self.overall_bar.close()
        if self.symbol_bar is not None:
            self.symbol_bar.close()

    def print_summary(self) -> None:
        """Print performance summary."""
        if not self.symbol_results:
            return

        total_trades = sum(r["trades"] for r in self.symbol_results)
        total_time = sum(r["elapsed"] for r in self.symbol_results)

        print(f"\n{'=' * 60}")
        print("Backtest Performance Summary")
        print(f"{'=' * 60}")
        print(f"Total symbols: {len(self.symbol_results)}")
        print(f"Total trades:  {total_trades}")
        print(f"Total time:    {total_time:.1f}s ({total_time / 60:.1f} minutes)")
        if len(self.symbol_results) > 0:
            print(f"Avg time/sym:  {total_time / len(self.symbol_results):.1f}s")

        # Show slowest symbols
        if len(self.symbol_results) > 1:
            sorted_results = sorted(self.symbol_results, key=lambda x: x["elapsed"], reverse=True)
            print("\nSlowest 5 symbols:")
            for r in sorted_results[:5]:
                print(f"  {r['symbol']:<12} {r['trades']:3d} trades, {r['elapsed']:.1f}s")
        print(f"{'=' * 60}\n")


class _SymbolProgress:
    """Helper for per-symbol progress tracking."""

    def __init__(self, parent: BacktestProgress, symbol: str, total_days: int):
        self.parent = parent
        self.symbol = symbol
        self.total_days = total_days
        self.start_time = time.time()
        self._trades_count = 0

    def __enter__(self):
        if self.parent.verbose and self.total_days > 5:  # Only show bar for 5+ days
            self.parent.symbol_bar = _tqdm(
                total=self.total_days,
                desc=f"  {self.symbol}",
                unit="day",
                position=1,
                leave=False,
                bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
            )
        return self

    def __exit__(self, *args):
        elapsed = time.time() - self.start_time

        if self.parent.symbol_bar:
            self.parent.symbol_bar.close()
            self.parent.symbol_bar = None

        self.parent.update_symbol(self.symbol, self._trades_count, elapsed)

    def set_trades_count(self, count: int) -> None:
        """Set number of trades found for this symbol."""
        self._trades_count = count

    def update(self, n: int = 1) -> None:
        """Update day progress."""
        if self.parent.symbol_bar:
            self.parent.symbol_bar.update(n)


def log_symbol_start(symbol: str, total_days: int) -> None:
    """Log the start of backtesting a symbol (used when tqdm not available)."""
    if not TQDM_AVAILABLE:
        print(f"[{symbol}] Backtesting {total_days} setup days...")


def log_symbol_complete(symbol: str, trades_count: int, elapsed: float) -> None:
    """Log completion of a symbol (used when tqdm not available)."""
    if not TQDM_AVAILABLE:
        status = f"{trades_count} trades" if trades_count > 0 else "No trades"
        print(f"[{symbol}] {status} in {elapsed:.1f}s")


def append_progress_event(path: str, row: dict[str, object]) -> None:
    """Append one NDJSON progress event (best-effort)."""
    payload = {"ts_utc": dt.datetime.now(dt.UTC).isoformat(), **row}
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, sort_keys=True))
        f.write("\n")
