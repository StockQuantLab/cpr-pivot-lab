"""Historical fake-feed replay helpers for paper trading."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from itertools import groupby
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

from db.duckdb import get_db
from db.paper_db import get_dashboard_paper_db, get_paper_db
from engine import paper_session_driver as paper_session_driver
from engine.bar_orchestrator import (
    SessionPositionTracker,
)
from engine.constants import parse_iso_date
from engine.cpr_atr_strategy import DayPack
from engine.paper_runtime import (
    PaperRuntimeState,
    SymbolRuntimeState,
    build_backtest_params,
    build_summary_feed_state,
    enforce_session_risk_controls,
    evaluate_candle,
    execute_entry,
    force_paper_db_sync,
    get_session_positions,
    load_setup_row,
)
from scripts.paper_archive import archive_completed_session
from scripts.paper_feed_audit import record_closed_candles

logger = logging.getLogger(__name__)

_REPLAY_YIELD_SYMBOL_CHUNK = 32


def _pdb():
    return get_paper_db()


async def get_session(session_id: str) -> Any:
    return _pdb().get_session(session_id)


async def update_session_state(session_id: str, **kwargs: Any) -> Any:
    return _pdb().update_session(session_id, **kwargs)


async def upsert_feed_state(**kwargs: Any) -> Any:
    return _pdb().upsert_feed_state(**kwargs)


async def get_feed_state(session_id: str) -> Any:
    return _pdb().get_feed_state(session_id)


IST = ZoneInfo("Asia/Kolkata")


@dataclass(slots=True)
class ReplayDayPack:
    symbol: str
    trade_date: str
    day_pack: DayPack


def _validate_iso_date(value: str | None) -> str | None:
    if value is None:
        return None
    return parse_iso_date(value)


def _minute_to_time_str(minute_of_day: int | float | str) -> str:
    total = int(minute_of_day)
    return f"{total // 60:02d}:{total % 60:02d}"


def _combine_bar_ts(trade_date: str, time_str: str) -> datetime:
    trade_day = date.fromisoformat(str(trade_date)[:10])
    candle_time = time.fromisoformat(time_str)
    return datetime.combine(trade_day, candle_time, tzinfo=IST)


def _runtime_setup_status(runtime_state: PaperRuntimeState, symbol: str) -> str:
    state = runtime_state.symbols.get(symbol)
    if state is None or state.setup_row is None:
        return "pending"
    direction = str(state.setup_row.get("direction") or "").upper()
    return "candidate" if direction in {"LONG", "SHORT"} else "rejected"


def _has_column(db: Any, table: str, column: str) -> bool:
    checker = getattr(db, "_table_has_column", None)
    if not callable(checker):
        return False
    try:
        return bool(checker(table, column))
    except Exception:
        return False


def _resolve_pack_time_mode(db: Any) -> str:
    return "minute_arr" if _has_column(db, "intraday_day_pack", "minute_arr") else "time_arr"


def _resolve_rvol_select(db: Any) -> str:
    return (
        "p.rvol_baseline_arr"
        if _has_column(db, "intraday_day_pack", "rvol_baseline_arr")
        else "NULL::DOUBLE[] AS rvol_baseline_arr"
    )


def _build_replay_query(
    *,
    symbols: list[str],
    start_date: str | None,
    end_date: str | None,
    pack_time_mode: str,
    rvol_select: str,
) -> tuple[str, dict[str, object]]:
    pack_time_select = (
        "p.minute_arr AS pack_time_arr"
        if pack_time_mode == "minute_arr"
        else "p.time_arr AS pack_time_arr"
    )
    where_clauses = ["list_contains($symbols, p.symbol)"]
    params: dict[str, object] = {"symbols": list(symbols)}
    if start_date:
        where_clauses.append("p.trade_date >= $start_date::DATE")
        params["start_date"] = _validate_iso_date(start_date)
    if end_date:
        where_clauses.append("p.trade_date <= $end_date::DATE")
        params["end_date"] = _validate_iso_date(end_date)

    query = f"""
        SELECT
            p.symbol,
            p.trade_date::VARCHAR AS trade_date,
            {pack_time_select},
            p.open_arr,
            p.high_arr,
            p.low_arr,
            p.close_arr,
            p.volume_arr,
            {rvol_select}
        FROM intraday_day_pack p
        WHERE {" AND ".join(where_clauses)}
        ORDER BY p.symbol, p.trade_date
    """
    return query, params


def _normalize_pack_times(raw_times: list[Any], pack_time_mode: str) -> list[str] | None:
    if not raw_times:
        return None
    if pack_time_mode == "minute_arr":
        try:
            return [_minute_to_time_str(t) for t in raw_times]
        except (TypeError, ValueError):
            return None
    return [str(t) for t in raw_times]


def _build_replay_day(
    *,
    symbol: object,
    trade_date: object,
    raw_times: list[Any],
    opens_raw: list[Any],
    highs_raw: list[Any],
    lows_raw: list[Any],
    closes_raw: list[Any],
    volumes_raw: list[Any],
    raw_baselines: list[Any],
    pack_time_mode: str,
) -> ReplayDayPack | None:
    times = _normalize_pack_times(raw_times, pack_time_mode)
    if times is None:
        return None

    opens = list(opens_raw or [])
    highs = list(highs_raw or [])
    lows = list(lows_raw or [])
    closes = list(closes_raw or [])
    volumes = list(volumes_raw or [])
    if not (len(times) == len(opens) == len(highs) == len(lows) == len(closes) == len(volumes)):
        return None

    baselines: list[float | None] | None = None
    if raw_baselines:
        baselines = [float(v) if v is not None else None for v in raw_baselines]

    return ReplayDayPack(
        symbol=str(symbol),
        trade_date=str(trade_date)[:10],
        day_pack=DayPack(
            time_str=times,
            opens=[float(x) for x in opens],
            highs=[float(x) for x in highs],
            lows=[float(x) for x in lows],
            closes=[float(x) for x in closes],
            volumes=[float(x) for x in volumes],
            rvol_baseline=baselines,
        ),
    )


def load_replay_day_packs(
    *,
    symbols: list[str],
    start_date: str | None,
    end_date: str | None,
) -> list[ReplayDayPack]:
    db = get_db()
    pack_time_mode = _resolve_pack_time_mode(db)
    query, params = _build_replay_query(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        pack_time_mode=pack_time_mode,
        rvol_select=_resolve_rvol_select(db),
    )

    pack_df = db.con.execute(query, params).pl()
    if pack_df.is_empty():
        return []

    symbols_col = pack_df["symbol"].to_list()
    dates_col = pack_df["trade_date"].to_list()
    pack_time_arrs = pack_df["pack_time_arr"].to_list()
    open_arrs = pack_df["open_arr"].to_list()
    high_arrs = pack_df["high_arr"].to_list()
    low_arrs = pack_df["low_arr"].to_list()
    close_arrs = pack_df["close_arr"].to_list()
    volume_arrs = pack_df["volume_arr"].to_list()
    rvol_arrs = pack_df["rvol_baseline_arr"].to_list()

    replay_days: list[ReplayDayPack] = []
    for idx in range(pack_df.height):
        replay_day = _build_replay_day(
            symbol=symbols_col[idx],
            trade_date=dates_col[idx],
            raw_times=pack_time_arrs[idx],
            opens_raw=open_arrs[idx],
            highs_raw=high_arrs[idx],
            lows_raw=low_arrs[idx],
            closes_raw=close_arrs[idx],
            volumes_raw=volume_arrs[idx],
            raw_baselines=list(rvol_arrs[idx] or []),
            pack_time_mode=pack_time_mode,
        )
        if replay_day is not None:
            replay_days.append(replay_day)

    return replay_days


def _replay_empty_result(
    session_id: str, replay_symbols: list[str], leave_active: bool
) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "symbols": replay_symbols,
        "days_replayed": 0,
        "bars_replayed": 0,
        "last_bar_ts": None,
        "completed": not leave_active,
        "message": "No intraday_day_pack rows matched the replay filters.",
    }


async def _complete_empty_replay(
    *,
    session_id: str,
    replay_symbols: list[str],
    leave_active: bool,
    notes: str | None,
) -> dict[str, Any]:
    result = _replay_empty_result(session_id, replay_symbols, leave_active)
    if not leave_active:
        await update_session_state(session_id, status="COMPLETED", notes=notes)
    return result


async def _process_replay_bar_major(
    *,
    session_id: str,
    session: Any,
    date_items: list[ReplayDayPack],
    stale_timeout: int,
    runtime_state: PaperRuntimeState,
    symbol_last_prices: dict[str, float],
    tracker: SessionPositionTracker,
    params: Any,
    log_candle_progress: bool = False,
) -> dict[str, Any]:
    """Process one trade date using canonical bar times + shared orchestration."""
    active_days = sorted(
        [d for d in date_items if d.day_pack.time_str],
        key=lambda item: item.symbol,
    )
    if not active_days:
        return {
            "triggered": False,
            "bars_replayed": 0,
            "last_bar_ts": None,
            "min_bars_per_symbol": 0,
            "max_bars_per_symbol": 0,
            "avg_bars_per_symbol": 0.0,
            "symbols_at_max_bars": 0,
        }

    base_days = list(active_days)
    bar_lengths = [len(d.day_pack.time_str) for d in base_days]
    all_bar_times = sorted({bar_time for d in base_days for bar_time in d.day_pack.time_str})
    total_bars = len(all_bar_times)
    progress_symbol = base_days[0].symbol
    last_bar_ts: datetime | None = None
    bars_replayed = 0
    last_close: float | None = None
    triggered = False
    last_processed_bar_index = 0
    stage_b_applied = False

    for bar_idx, bar_time in enumerate(all_bar_times, 1):
        last_processed_bar_index = bar_idx
        bar_candles: list[SimpleNamespace] = []
        for day in active_days:
            pack = day.day_pack
            candle_idx = pack._idx_by_time.get(bar_time)
            if candle_idx is None:
                continue
            candle_ts = _combine_bar_ts(day.trade_date, bar_time)
            last_close = float(pack.closes[candle_idx])
            last_bar_ts = candle_ts
            bars_replayed += 1
            symbol_last_prices[day.symbol] = last_close
            bar_candles.append(
                SimpleNamespace(
                    symbol=day.symbol,
                    bar_end=candle_ts,
                    open=float(pack.opens[candle_idx]),
                    high=float(pack.highs[candle_idx]),
                    low=float(pack.lows[candle_idx]),
                    close=last_close,
                    volume=float(pack.volumes[candle_idx]),
                )
            )

        if log_candle_progress and bar_candles:
            logger.info(
                "Replay candle session_id=%s trade_date=%s candle=%s bar=%d/%d",
                session_id,
                bar_candles[0].bar_end.date().isoformat(),
                bar_time,
                bar_idx,
                total_bars,
            )

        driver_result = await paper_session_driver.process_closed_bar_group(
            session_id=session_id,
            session=session,
            bar_candles=bar_candles,
            runtime_state=runtime_state,
            tracker=tracker,
            params=params,
            active_symbols=[d.symbol for d in active_days],
            strategy=str(session.strategy or "CPR_LEVELS"),
            direction_filter=str(getattr(params, "direction_filter", "BOTH") or "BOTH"),
            stage_b_applied=stage_b_applied,
            symbol_last_prices=symbol_last_prices,
            last_price=last_close,
            feed_source="replay",
            transport="replay",
            feed_audit_writer=record_closed_candles,
            evaluate_candle_fn=evaluate_candle,
            execute_entry_fn=execute_entry,
            enforce_risk_controls=enforce_session_risk_controls,
            build_feed_state=build_summary_feed_state,
        )

        active_symbols_after = set(driver_result["active_symbols"])
        active_days = [d for d in active_days if d.symbol in active_symbols_after]
        stage_b_applied = bool(driver_result["stage_b_applied"])
        last_close = driver_result["last_price"]

        if driver_result["triggered"]:
            triggered = True
            break

        if not active_days and not tracker.open_symbols():
            break

        await asyncio.sleep(0)

    # Write feed state for the last bar using the last symbol's data.
    if last_bar_ts is not None:
        await upsert_feed_state(
            session_id=session_id,
            status="OK",
            last_event_ts=last_bar_ts,
            last_bar_ts=last_bar_ts,
            last_price=last_close,
            stale_reason=None,
            raw_state={
                "mode": "historical_replay",
                "session_id": session_id,
                "trade_date": base_days[0].trade_date,
                "bar_index": max(0, last_processed_bar_index - 1),
                "bar_total": total_bars,
                "symbol_last_prices": {**symbol_last_prices},
                "strategy": session.strategy,
                "setup_prefetch": {
                    "skipped": runtime_state.skipped_setup_rows,
                    "invalid": runtime_state.invalid_setup_rows,
                },
            },
        )
        await update_session_state(
            session_id,
            latest_candle_ts=last_bar_ts,
            stale_feed_at=(
                last_bar_ts + timedelta(seconds=stale_timeout) if stale_timeout > 0 else None
            ),
        )
    return {
        "triggered": triggered,
        "bars_replayed": bars_replayed,
        "last_bar_ts": last_bar_ts,
        "min_bars_per_symbol": min(bar_lengths),
        "max_bars_per_symbol": max(bar_lengths),
        "avg_bars_per_symbol": (bars_replayed / len(base_days)) if base_days else 0.0,
        "symbols_at_max_bars": sum(1 for n in bar_lengths if n == total_bars),
        "active_symbols_remaining": len(active_days),
        "progress_symbol": progress_symbol,
    }


async def _finalize_replay_session(
    *,
    session_id: str,
    leave_active: bool,
    last_bar_ts: datetime | None,
    stale_timeout: int,
    notes: str | None,
) -> None:
    if leave_active:
        await update_session_state(
            session_id,
            latest_candle_ts=last_bar_ts,
            stale_feed_at=(
                last_bar_ts + timedelta(seconds=stale_timeout)
                if last_bar_ts is not None and stale_timeout > 0
                else None
            ),
            notes=notes,
        )
        return
    await update_session_state(
        session_id,
        status="COMPLETED",
        latest_candle_ts=last_bar_ts,
        clear_stale_feed_at=True,
        notes=notes,
    )


async def _prepare_replay_request(
    *,
    session_id: str,
    symbols: list[str] | None,
    start_date: str | None,
    end_date: str | None,
    notes: str | None,
) -> tuple[Any, list[str], str | None, str | None] | dict[str, Any]:
    session = await get_session(session_id)
    if session is None:
        return {"session_id": session_id, "error": "session not found"}

    replay_symbols = [s.strip() for s in symbols or session.symbols if s and s.strip()]
    if not replay_symbols:
        return {"session_id": session_id, "error": "no symbols available for replay"}

    normalized_start = _validate_iso_date(start_date)
    normalized_end = _validate_iso_date(end_date)
    if normalized_start and normalized_end and normalized_start > normalized_end:
        return {"session_id": session_id, "error": "start_date must be <= end_date"}

    if session.status != "ACTIVE":
        session = await update_session_state(session_id, status="ACTIVE", notes=notes)

    return session, replay_symbols, normalized_start, normalized_end


async def replay_session(
    *,
    session_id: str,
    symbols: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    leave_active: bool = False,
    notes: str | None = None,
    preloaded_days: list[ReplayDayPack] | None = None,
) -> dict[str, Any]:
    prepared = await _prepare_replay_request(
        session_id=session_id,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        notes=notes,
    )
    if isinstance(prepared, dict):
        return prepared
    session, replay_symbols, normalized_start, normalized_end = prepared

    if preloaded_days is not None:
        replay_symbols_set = set(replay_symbols)
        replay_days = [d for d in preloaded_days if d.symbol in replay_symbols_set]
    else:
        replay_days = load_replay_day_packs(
            symbols=replay_symbols,
            start_date=normalized_start,
            end_date=normalized_end,
        )
    if not replay_days:
        return await _complete_empty_replay(
            session_id=session_id,
            replay_symbols=replay_symbols,
            leave_active=leave_active,
            notes=notes,
        )

    stale_timeout = max(0, int(session.stale_feed_timeout_sec))
    last_bar_ts: datetime | None = None
    bars_replayed = 0
    runtime_state = PaperRuntimeState(
        allow_live_setup_fallback=False, bar_end_offset=timedelta(minutes=5)
    )
    symbol_last_prices: dict[str, float] = {}
    params = build_backtest_params(session)
    tracker = SessionPositionTracker(
        max_positions=int(getattr(session, "max_positions", 1) or 1),
        portfolio_value=float(getattr(params, "portfolio_value", 0.0) or 0.0),
        max_position_pct=float(getattr(params, "max_position_pct", 0.0) or 0.0),
    )
    tracker.seed_open_positions(await get_session_positions(session_id, statuses=["OPEN"]))

    # Group day packs by trade date so all symbols at the same date
    # are processed in bar-major order (all symbols per bar, then next bar).
    # This ensures max_positions reflects true concurrent open positions.
    # Risk controls are deferred to end-of-date to avoid flatten_time
    # short-circuiting earlier bars.
    logger.info(
        "Replay start session_id=%s strategy=%s symbols=%d dates=%d leave_active=%s",
        session_id,
        session.strategy,
        len(replay_symbols),
        len({d.trade_date for d in replay_days}),
        leave_active,
    )
    sorted_days = sorted(replay_days, key=lambda d: (d.trade_date, d.symbol))
    grouped_days = [(dt, list(grp)) for dt, grp in groupby(sorted_days, key=lambda d: d.trade_date)]
    total_dates = len(grouped_days)
    for date_idx, (_trade_date, date_items) in enumerate(grouped_days, 1):
        total_syms = len(date_items)

        # Prefetch setup rows for this date (same pattern as paper_live.py).
        # This ensures all setup rows are available from the first bar (09:15),
        # avoiding lazy-load timing issues where setup_row might not be available
        # on early bars. Without prefetch, evaluate_candle loads setup_row on the
        # first candle per symbol, which is typically 09:15 but may differ.
        date_symbols = [d.symbol for d in date_items]
        for symbol in date_symbols:
            state = runtime_state.symbols.setdefault(symbol, SymbolRuntimeState())
            if state.setup_row is not None and state.trade_date == _trade_date:
                continue  # Already loaded for this date
            setup_row = load_setup_row(
                symbol,
                _trade_date,
                live_candles=state.candles,
                or_minutes=params.or_minutes,
                allow_live_fallback=False,
                regime_index_symbol=getattr(params, "regime_index_symbol", ""),
                regime_snapshot_minutes=int(getattr(params, "regime_snapshot_minutes", 30) or 30),
            )
            if setup_row is None:
                runtime_state.skipped_setup_rows += 1
                continue
            tc = float(setup_row.get("tc") or 0.0)
            bc = float(setup_row.get("bc") or 0.0)
            atr = float(setup_row.get("atr") or 0.0)
            if tc <= 0.0 or bc <= 0.0 or atr <= 0.0:
                runtime_state.invalid_setup_rows += 1
                continue
            setup_row.setdefault("setup_source", "market_day_state")
            state.setup_row = setup_row
            # Set trade_date so _reset_symbol_state_for_trade_date becomes a no-op
            # when evaluate_candle runs — preventing the setup_row from being cleared.
            state.trade_date = _trade_date

        logger.info(
            "Replay date start session_id=%s trade_date=%s date_index=%d/%d symbols=%d",
            session_id,
            _trade_date,
            date_idx,
            total_dates,
            total_syms,
        )
        print(
            f"[replay] {_trade_date} — {total_syms} symbols (date {date_idx}/{total_dates})",
            flush=True,
        )
        replay_result = await _process_replay_bar_major(
            session_id=session_id,
            session=session,
            date_items=date_items,
            stale_timeout=stale_timeout,
            runtime_state=runtime_state,
            symbol_last_prices=symbol_last_prices,
            tracker=tracker,
            params=params,
            log_candle_progress=True,
        )
        last_bar_ts = replay_result["last_bar_ts"] or last_bar_ts
        bars_replayed += int(replay_result["bars_replayed"])
        avg_bars = float(replay_result.get("avg_bars_per_symbol") or 0.0)
        min_bars = int(replay_result.get("min_bars_per_symbol") or 0)
        max_bars = int(replay_result.get("max_bars_per_symbol") or 0)
        symbols_at_max = int(replay_result.get("symbols_at_max_bars") or 0)
        print(
            f"[replay]   {_trade_date} - {total_syms} symbols x avg {avg_bars:.2f} bars"
            f" (min={min_bars}, max={max_bars}, full={symbols_at_max}) done",
            flush=True,
        )
        if bool(replay_result.get("triggered")):
            # EOD flatten on the last date still completes the replay — only
            # an early-date loss-limit halt should leave the session STOPPING.
            if date_idx < total_dates:
                leave_active = True
            else:
                leave_active = False
            logger.info(
                "Replay risk control triggered session_id=%s trade_date=%s leave_active=%s last_bar_ts=%s",
                session_id,
                _trade_date,
                leave_active,
                last_bar_ts,
            )
            break

    logger.info(
        "Replay finalize begin session_id=%s leave_active=%s last_bar_ts=%s bars_replayed=%d",
        session_id,
        leave_active,
        last_bar_ts,
        bars_replayed,
    )
    await paper_session_driver.complete_session(
        session_id=session_id,
        complete_on_exit=not leave_active,
        last_bar_ts=last_bar_ts,
        stale_timeout=stale_timeout,
        notes=notes,
        update_session_state=update_session_state,
    )

    # Ensure the final session state (COMPLETED/STOPPING) reaches the replica.
    # The _finalize_replay_session write often falls within the replica's 5s
    # debounce window from the last position-update sync, so a normal
    # maybe_sync() would skip it.
    if not leave_active:
        force_paper_db_sync(_pdb())

    feed_state = await get_feed_state(session_id)
    final_session = await get_session(session_id)
    archive_payload = None
    if final_session and final_session.status == "COMPLETED":
        logger.info("Replay archive begin session_id=%s", session_id)
        archive_result = archive_completed_session(
            session_id,
            paper_db=get_dashboard_paper_db(),
        )
        archive_payload = (
            await archive_result if asyncio.iscoroutine(archive_result) else archive_result
        )
        logger.info(
            "Replay archive done session_id=%s rows=%s",
            session_id,
            archive_payload.get("rows") if isinstance(archive_payload, dict) else None,
        )
    logger.info(
        "Replay done session_id=%s final_status=%s bars_replayed=%d last_bar_ts=%s",
        session_id,
        final_session.status if final_session else None,
        bars_replayed,
        last_bar_ts,
    )
    return {
        "session_id": session_id,
        "strategy": session.strategy,
        "final_status": final_session.status if final_session else None,
        "symbols": replay_symbols,
        "start_date": normalized_start,
        "end_date": normalized_end,
        "days_replayed": len(replay_days),
        "bars_replayed": bars_replayed,
        "last_bar_ts": last_bar_ts,
        "feed_state": asdict(feed_state) if feed_state else None,
        "completed": not leave_active,
        "archive": archive_payload,
    }
