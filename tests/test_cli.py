"""Tests for engine/run_backtest.py — CLI argument parsing."""

import subprocess
import sys

import polars as pl
import pytest

import engine.command_lock as command_lock_module
import engine.run_backtest as run_backtest
import scripts.gold_pipeline as gold_pipeline
import scripts.run_campaign as run_campaign


class TestCLIArgParsing:
    """Test that CLI arg parsing works correctly."""

    def test_help_output(self):
        """--help should show CPR_LEVELS as default strategy."""
        result = subprocess.run(
            [sys.executable, "-m", "engine.run_backtest", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "CPR_LEVELS" in result.stdout
        assert "FBR" in result.stdout
        assert "VIRGIN_CPR" not in result.stdout

    def test_no_orb_in_choices(self):
        """ORB should not be in strategy choices."""
        result = subprocess.run(
            [sys.executable, "-m", "engine.run_backtest", "--help"],
            capture_output=True,
            text=True,
        )
        # ORB should not appear as a strategy choice
        # Check the strategy choices line specifically
        lines = [line for line in result.stdout.split("\n") if "--strategy" in line]
        for line in lines:
            assert "ORB" not in line.split("CPR")[0]  # Avoid matching "CPR_LEVELS" substring

    def test_no_cpr_fade_in_choices(self):
        """CPR_FADE should not be in strategy choices."""
        result = subprocess.run(
            [sys.executable, "-m", "engine.run_backtest", "--help"],
            capture_output=True,
            text=True,
        )
        assert "CPR_FADE" not in result.stdout

    def test_cpr_shift_flag_present(self):
        """--cpr-shift should be a valid flag."""
        result = subprocess.run(
            [sys.executable, "-m", "engine.run_backtest", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--cpr-shift" in result.stdout

    def test_universe_name_flag_present(self):
        """--universe-name should be available for fixed gold universe runs."""
        result = subprocess.run(
            [sys.executable, "-m", "engine.run_backtest", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--universe-name" in result.stdout

    def test_progress_file_flag_present(self):
        """--progress-file should be available for heartbeat logging."""
        result = subprocess.run(
            [sys.executable, "-m", "engine.run_backtest", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--progress-file" in result.stdout

    def test_quiet_flag_present(self):
        """--quiet should be available for compact/non-noisy output."""
        result = subprocess.run(
            [sys.executable, "-m", "engine.run_backtest", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--quiet" in result.stdout

    def test_chunk_flags_present(self):
        """Chunked/resumable execution flags should be exposed in CLI."""
        result = subprocess.run(
            [sys.executable, "-m", "engine.run_backtest", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--chunk-by" in result.stdout

    def test_runtime_batch_size_flag_present(self):
        """--runtime-batch-size should be exposed for chunked runtime fetch/sim."""
        result = subprocess.run(
            [sys.executable, "-m", "engine.run_backtest", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--runtime-batch-size" in result.stdout

    def test_duckdb_tuning_flags_present(self):
        """Backtest should expose DuckDB runtime tuning flags."""
        result = subprocess.run(
            [sys.executable, "-m", "engine.run_backtest", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--duckdb-threads" in result.stdout
        assert "--duckdb-max-memory" in result.stdout

    def test_cpr_soft_filter_flags_present(self):
        """Soft CPR entry filters should be exposed in CLI."""
        result = subprocess.run(
            [sys.executable, "-m", "engine.run_backtest", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--cpr-hold-confirm" in result.stdout
        assert "--cpr-min-close-atr" in result.stdout
        assert "--entry-window-end" in result.stdout
        assert "--long-max-gap-pct" in result.stdout

    def test_rejects_conflicting_cpr_confirm_flags(self):
        """Hard and soft confirm flags must be mutually exclusive."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "engine.run_backtest",
                "--start",
                "2023-01-01",
                "--end",
                "2023-12-31",
                "--cpr-confirm-entry",
                "--cpr-hold-confirm",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "Use either --cpr-confirm-entry or --cpr-hold-confirm" in result.stderr

    def test_rejects_invalid_runtime_batch_size(self):
        """Runtime batch size must be >= 1."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "engine.run_backtest",
                "--start",
                "2023-01-01",
                "--end",
                "2023-12-31",
                "--runtime-batch-size",
                "0",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "--runtime-batch-size must be >= 1" in result.stderr

    def test_gold_benchmark_progress_flag_present(self):
        """pivot-gold benchmark should expose --progress-file."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.gold_pipeline", "benchmark", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--progress-file" in result.stdout
        assert "Strategy to benchmark" in result.stdout

    def test_no_rejection_wick_flag(self):
        """--rejection-wick-pct should be removed."""
        result = subprocess.run(
            [sys.executable, "-m", "engine.run_backtest", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--rejection-wick-pct" not in result.stdout

    def test_no_cpr_approach_flag(self):
        """--cpr-approach-atr should be removed."""
        result = subprocess.run(
            [sys.executable, "-m", "engine.run_backtest", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--cpr-approach-atr" not in result.stdout

    def test_build_tables_scope_flags_present(self):
        """pivot-build should expose scoped symbol rebuild flags."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.build_tables", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--symbols" in result.stdout
        assert "--universe-name" in result.stdout
        assert "--full-history" in result.stdout
        assert "--staged-full-rebuild" in result.stdout
        assert "--duckdb-max-memory" in result.stdout
        assert "--refresh-since" in result.stdout
        assert "--allow-full-pack-rebuild" in result.stdout
        assert "--allow-full-history-rebuild" in result.stdout

    def test_build_tables_rejects_mixed_symbol_scope_flags(self):
        """pivot-build should reject --symbols + --universe-name together."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.build_tables",
                "--table",
                "pack",
                "--symbols",
                "SBIN",
                "--universe-name",
                "gold_51",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert (
            "--universe-name cannot be combined with --symbols, --symbols-file, or --missing."
            in result.stderr
        )

    def test_build_tables_full_force_requires_full_history_flag(self):
        """pivot-build full force should require explicit full-history confirmation."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.build_tables", "--force"],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "Full-history runtime rebuild requires --full-history." in result.stderr

    def test_build_tables_full_force_requires_staged_rebuild(self):
        """pivot-build full force should reject non-resumable full rebuilds."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.build_tables", "--force", "--full-history"],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "Non-resumable full-history rebuilds are disabled." in result.stderr

    def test_build_tables_staged_full_rebuild_requires_explicit_ack(self):
        """pivot-build staged full-history rebuild should require an explicit ack flag."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.build_tables",
                "--force",
                "--full-history",
                "--staged-full-rebuild",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert (
            "Full-history staged rebuilds are expensive and should be deliberate." in result.stderr
        )

    def test_build_tables_pack_force_requires_explicit_full_pack_ack(self):
        """pivot-build pack force should require explicit full pack rebuild confirmation."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.build_tables", "--table", "pack", "--force"],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert (
            "Full-universe `--table pack --force` rebuilds are destructive and expensive."
            in result.stderr
        )

    def test_build_tables_refresh_since_alias_is_accepted(self):
        """pivot-build should advertise --refresh-since in help (accepted alias for --since)."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.build_tables", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "--refresh-since" in result.stdout

    def test_build_tables_refresh_date_is_accepted(self):
        """pivot-build should advertise --refresh-date for exact-day refreshes."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.build_tables", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "--refresh-date" in result.stdout

    def test_build_tables_refresh_date_rejects_force(self):
        """pivot-build should reject exact-day refreshes combined with --force."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.build_tables",
                "--refresh-date",
                "2026-03-30",
                "--force",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "--refresh-date is a bounded incremental refresh" in result.stderr

    def test_parity_check_help_present(self):
        """pivot-parity-check CLI should be available."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.parity_check", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--expected-run-id" in result.stdout
        assert "--actual-run-id" in result.stdout

    def test_kite_ingest_help_present(self):
        """pivot-kite-ingest CLI should expose ingestion and checkpoint flags."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.kite_ingest", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--refresh-instruments" in result.stdout
        assert "--5min" in result.stdout
        assert "--resume" in result.stdout
        assert "--save-raw" in result.stdout
        assert "--update-features" in result.stdout

    def test_kite_token_help_present(self):
        """pivot-kite-token CLI should expose request-token exchange options."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.kite_token", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--request-token" in result.stdout
        assert "--apply-doppler" in result.stdout

    def test_kite_get_token_alias_present(self):
        """pivot-kite-get-token CLI should alias the token exchange helper."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.kite_get_token", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--request-token" in result.stdout
        assert "--apply-doppler" in result.stdout

    def test_clean_artifacts_help_present(self):
        """pivot-clean CLI should expose dry-run and data/progress options."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.clean_artifacts", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--dry-run" in result.stdout
        assert "--include-data-progress" in result.stdout

    def test_reset_run_history_help_present(self):
        """pivot-reset-history CLI should expose a guarded apply flag."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.reset_run_history", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "--apply" in result.stdout
        assert "--duckdb-only" in result.stdout

    def test_run_campaign_uses_quality_combo_for_cpr(self):
        """Campaign CPR leg should use the promoted shared-portfolio quality profile."""
        cpr_run = next(
            run for run in run_campaign.DEFAULT_RUN_ORDER if run.strategy == "CPR_LEVELS"
        )

        assert cpr_run.label == "cpr_quality_combo"
        assert cpr_run.extra_args == (
            "--cpr-min-close-atr",
            "0.5",
            "--min-price",
            "50",
            "--narrowing-filter",
            "--skip-rvol",
        )

    def test_run_campaign_help_present(self):
        """pivot-campaign CLI should expose execution workflow controls."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.run_campaign", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--full-universe" in result.stdout
        assert "--include-vcpr" not in result.stdout
        assert "--clean-before" in result.stdout
        assert "--clean-after" in result.stdout
        assert "--ensure-runtime-coverage" in result.stdout
        assert "--pack-batch-size" in result.stdout

    def test_run_campaign_dry_run(self):
        """pivot-campaign dry run should print command plan without execution."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.run_campaign",
                "--start",
                "2015-01-01",
                "--end",
                "2015-01-31",
                "--dry-run",
                "--no-ensure-runtime-coverage",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "Strategy order: FBR -> CPR_LEVELS" in result.stdout
        assert "Campaign complete." in result.stdout

    def test_run_campaign_closes_parent_db_before_spawn(self, monkeypatch):
        """pivot-campaign should drop the parent DuckDB handle before child execution."""
        events: list[object] = []

        monkeypatch.setattr(run_campaign, "close_db", lambda: events.append("close_db"))

        class _Completed:
            returncode = 7

        def _fake_run(cmd, check=False):
            events.append(("spawn", list(cmd), check))
            return _Completed()

        monkeypatch.setattr(run_campaign.subprocess, "run", _fake_run)

        code = run_campaign._run_subprocess(["uv", "run", "pivot-backtest"], dry_run=False)

        assert code == 7
        assert events[0] == "close_db"
        assert events[1] == ("spawn", ["uv", "run", "pivot-backtest"], False)

    def test_run_backtest_main_smoke_without_vcpr_wiring(self, monkeypatch, capsys):
        """pivot-backtest main should execute a real parse/build path without VCPR args."""
        calls: dict[str, object] = {}

        class _FakeDB:
            pass

        class _LockCtx:
            def __enter__(self):
                calls["lock_enter"] = True
                return self

            def __exit__(self, exc_type, exc, tb):
                calls["lock_exit"] = True
                return False

        class _FakeResult:
            run_id = "run-1"
            df = pl.DataFrame()

            def save_to_db(self, db):
                calls["save_to_db"] = db
                return 0

            def summary(self):
                return "summary"

        class _FakeBT:
            def __init__(self, params, db):
                calls["params"] = params
                calls["db"] = db

            def _make_run_id(self, symbols, start, end):
                calls["make_run_id"] = (symbols, start, end)
                return "umbrella-1"

            def run(self, **kwargs):
                calls["run_kwargs"] = kwargs
                return _FakeResult()

        monkeypatch.setattr(run_backtest, "get_db", lambda: _FakeDB())
        monkeypatch.setattr(run_backtest, "CPRATRBacktest", _FakeBT)
        monkeypatch.setattr(
            command_lock_module,
            "acquire_command_lock",
            lambda *args, **kwargs: _LockCtx(),
        )
        monkeypatch.setattr(
            run_backtest.sys,
            "argv",
            [
                "engine.run_backtest",
                "--symbol",
                "SBIN",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-02",
                "--strategy",
                "FBR",
                "--quiet",
            ],
        )

        run_backtest.main()

        out = capsys.readouterr().out
        assert "Compact summary: run_id=" in out
        assert calls["lock_enter"] is True
        assert calls["lock_exit"] is True
        assert calls["params"].strategy == "FBR"
        assert calls["run_kwargs"]["symbols"] == ["SBIN"]
        assert calls["make_run_id"] == (["SBIN"], "2025-01-01", "2025-01-02")

    def test_run_backtest_single_window_emits_heartbeat(self, monkeypatch):
        """Single-window runs should emit explicit progress heartbeats."""
        calls: dict[str, object] = {}
        progress_lines: list[tuple[float, str, str]] = []

        class _FakeDB:
            pass

        class _LockCtx:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class _FakeResult:
            run_id = "run-1"
            df = pl.DataFrame()

            def save_to_db(self, db):
                return 0

            def summary(self):
                return "summary"

        class _FakeBT:
            def __init__(self, params, db):
                calls["params"] = params
                calls["db"] = db

            def _make_run_id(self, symbols, start, end):
                return "umbrella-1"

            def run(self, **kwargs):
                calls["run_kwargs"] = kwargs
                progress_hook = kwargs.get("progress_hook")
                if progress_hook:
                    progress_hook({"event": "run_start", "total_symbols": 2})
                    progress_hook({"event": "symbol_done", "symbol": "SBIN"})
                    progress_hook({"event": "symbol_done", "symbol": "TCS"})
                return _FakeResult()

        monkeypatch.setattr(run_backtest, "get_db", lambda: _FakeDB())
        monkeypatch.setattr(run_backtest, "CPRATRBacktest", _FakeBT)
        monkeypatch.setattr(
            run_backtest,
            "_progress_line",
            lambda percent, event, message: progress_lines.append((percent, event, message)),
        )
        monkeypatch.setattr(
            command_lock_module,
            "acquire_command_lock",
            lambda *args, **kwargs: _LockCtx(),
        )
        monkeypatch.setattr(
            run_backtest.sys,
            "argv",
            [
                "engine.run_backtest",
                "--symbols",
                "SBIN,TCS",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-02",
                "--strategy",
                "FBR",
            ],
        )

        run_backtest.main()

        assert calls["run_kwargs"]["progress_hook"] is not None
        assert progress_lines
        assert any(line[1] == "symbol_done" for line in progress_lines)
        assert any(line[1] == "symbol_done" and line[0] == 100.0 for line in progress_lines)

    def test_run_backtest_progress_file_starts_before_symbol_resolution(self, monkeypatch):
        """Progress file should be created before expensive symbol lookup work starts."""
        events: list[str] = []

        class _FakeDB:
            def get_available_symbols(self):
                events.append("db_lookup")
                return ["SBIN"]

            def refresh_data_quality_issues(self):
                events.append("dq_refresh")
                return {}

        class _LockCtx:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class _FakeResult:
            run_id = "run-1"
            df = pl.DataFrame()

            def save_to_db(self, db):
                return 0

            def summary(self):
                return "summary"

        class _FakeBT:
            def __init__(self, params, db):
                pass

            def _make_run_id(self, symbols, start, end):
                return "umbrella-1"

            def run(self, **kwargs):
                return _FakeResult()

        def _append_progress(path, row):
            events.append(row["event"])

        monkeypatch.setattr(run_backtest, "get_db", lambda: _FakeDB())
        monkeypatch.setattr(run_backtest, "CPRATRBacktest", _FakeBT)
        monkeypatch.setattr(run_backtest, "append_progress_event", _append_progress)
        monkeypatch.setattr(
            command_lock_module,
            "acquire_command_lock",
            lambda *args, **kwargs: _LockCtx(),
        )
        monkeypatch.setattr(
            run_backtest.sys,
            "argv",
            [
                "engine.run_backtest",
                "--all",
                "--universe-size",
                "0",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-02",
                "--strategy",
                "CPR_LEVELS",
                "--progress-file",
                ".tmp_logs/test.ndjson",
            ],
        )

        run_backtest.main()

        assert events[0] == "cli_invoked"
        assert "db_lookup" in events
        assert events.index("cli_invoked") < events.index("db_lookup")

    def test_combine_chunk_results_rewrites_run_id(self, monkeypatch):
        """Aggregated chunk results must share the saved aggregate run_id."""

        class _FakeUUID:
            hex = "abc123def4567890"

        monkeypatch.setattr(run_backtest.uuid, "uuid4", lambda: _FakeUUID())

        chunk_1 = run_backtest.BacktestResult(
            run_id="chunk-1",
            params=run_backtest.BacktestParams(),
            _loaded_df=pl.DataFrame(
                {"run_id": ["chunk-1"], "symbol": ["SBIN"], "profit_loss": [1.0]}
            ),
        )
        chunk_2 = run_backtest.BacktestResult(
            run_id="chunk-2",
            params=run_backtest.BacktestParams(),
            _loaded_df=pl.DataFrame(
                {"run_id": ["chunk-2"], "symbol": ["TCS"], "profit_loss": [2.0]}
            ),
        )

        result = run_backtest._combine_chunk_results(
            chunk_results=[chunk_1, chunk_2],
            params=run_backtest.BacktestParams(),
            symbols=["SBIN", "TCS"],
            start_date="2025-01-01",
            end_date="2025-01-02",
        )

        assert result.run_id == "abc123def456"
        assert result.df["run_id"].unique().to_list() == ["abc123def456"]

    def test_run_backtest_saves_with_unique_run_id(self, monkeypatch):
        """Each --save execution should persist under a unique run_id."""
        calls: dict[str, object] = {}

        class _FakeDB:
            class _FakeCon:
                def execute(self, *args, **kwargs):
                    return None

                def register(self, *args, **kwargs):
                    return None

                def unregister(self, *args, **kwargs):
                    return None

            con = _FakeCon()

            def store_run_metadata(self, **kwargs):
                calls["stored_run_metadata"] = kwargs

            def store_backtest_results(
                self,
                results_df,
                execution_mode=None,
                transactional: bool = True,
            ):
                calls["stored_backtest_results"] = {
                    "execution_mode": execution_mode,
                    "row_count": results_df.height,
                }
                return results_df.height

        class _LockCtx:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class _FakeResult:
            def __init__(self, run_id: str):
                self.run_id = run_id
                self.df = pl.DataFrame()

            def save_to_db(self, db):
                calls["saved_run_id"] = self.run_id
                db.store_run_metadata(run_id=self.run_id)
                db.store_backtest_results(self.df, execution_mode="BACKTEST")
                return 0

            def summary(self):
                return "summary"

        class _FakeBT:
            def __init__(self, params, db):
                calls["params"] = params
                calls["db"] = db

            def _make_run_id(self, symbols, start, end):
                calls["make_run_id"] = (symbols, start, end)
                return "unique-run-id-1"

            def run(self, **kwargs):
                calls["run_kwargs"] = kwargs
                return _FakeResult(kwargs["run_id"])

        monkeypatch.setattr(run_backtest, "get_db", lambda: _FakeDB())
        monkeypatch.setattr(run_backtest, "get_backtest_db", lambda: _FakeDB())
        monkeypatch.setattr(run_backtest, "CPRATRBacktest", _FakeBT)
        monkeypatch.setattr(
            command_lock_module,
            "acquire_command_lock",
            lambda *args, **kwargs: _LockCtx(),
        )
        monkeypatch.setattr(
            run_backtest.sys,
            "argv",
            [
                "engine.run_backtest",
                "--symbol",
                "SBIN",
                "--start",
                "2025-01-01",
                "--end",
                "2025-01-02",
                "--strategy",
                "FBR",
                "--save",
                "--quiet",
            ],
        )

        run_backtest.main()

        assert "use_cache" not in calls["run_kwargs"]
        assert calls["run_kwargs"]["run_id"] == "unique-run-id-1"
        # _combine_chunk_results generates its own run_id for single-chunk too
        assert calls["stored_run_metadata"]["run_id"]
        assert calls["stored_backtest_results"]["row_count"] == 0
        assert calls["db"] is not calls["run_kwargs"]

    def test_gold_status_show_symbols_uses_preview_limit(self, monkeypatch, capsys):
        """pivot-gold status should not crash on symbol previews."""

        class _FakeDB:
            def list_universes(self):
                return [
                    {
                        "name": "gold_51",
                        "symbol_count": 2,
                        "start_date": "",
                        "end_date": "",
                        "source": "",
                    }
                ]

            def get_universe_symbols(self, _name):
                return ["SBIN", "RELIANCE"]

        monkeypatch.setattr(gold_pipeline, "get_db", lambda: _FakeDB())

        gold_pipeline.cmd_status(type("Args", (), {"name": None, "show_symbols": True})())

        out = capsys.readouterr().out
        assert "gold_51" in out
        assert "SBIN" in out

    def test_gold_prepare_missing_coverage_uses_preview_limit(self, monkeypatch, capsys):
        """pivot-gold prepare should report missing runtime coverage without crashing."""

        class _FakeDB:
            def refresh_data_quality_issues(self):
                return {"missing_5min": 0, "active_issues": 0}

            def get_liquid_symbols(self, *_args, **_kwargs):
                return ["SBIN"]

            def upsert_universe(self, *_args, **_kwargs):
                return 1

            def _table_exists(self, _table):
                return False

        monkeypatch.setattr(gold_pipeline, "get_db", lambda: _FakeDB())

        args = type(
            "Args",
            (),
            {
                "name": "gold_51",
                "start": "2025-01-01",
                "end": "2025-01-31",
                "universe_size": 1,
                "min_price": 0.0,
                "source": "liquidity_rank",
                "notes": "",
            },
        )()

        gold_pipeline.cmd_prepare(args)

        out = capsys.readouterr().out
        assert "Universe persisted" in out
        assert "Missing market_day_state symbols" in out

    def test_yes_full_run_flag_in_help(self):
        """--yes-full-run should appear in backtest --help output."""
        result = subprocess.run(
            [sys.executable, "-m", "engine.run_backtest", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--yes-full-run" in result.stdout

    def test_yes_full_run_guard_rejects_large_universe(self, monkeypatch):
        """Running with >100 resolved symbols without --yes-full-run must fail."""
        mock_db = type(
            "DB",
            (),
            {
                "get_liquid_symbols": staticmethod(lambda *a, **kw: ["S"] * 101),
                "get_available_symbols": staticmethod(lambda: ["S"] * 101),
                "get_universe_symbols": staticmethod(lambda n: None),
                "refresh_data_quality_issues": staticmethod(lambda: {}),
            },
        )()

        monkeypatch.setattr(run_backtest, "get_db", lambda: mock_db)
        monkeypatch.setattr(
            run_backtest.sys,
            "argv",
            [
                "engine.run_backtest",
                "--all",
                "--start",
                "2023-01-01",
                "--end",
                "2023-01-31",
            ],
        )

        with pytest.raises(SystemExit):
            run_backtest.main()

    def test_min_sl_atr_ratio_validation(self):
        """--min-sl-atr-ratio must be positive and <= --max-sl-atr-ratio."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "engine.run_backtest",
                "--symbol",
                "SBIN",
                "--start",
                "2023-01-01",
                "--end",
                "2023-01-31",
                "--min-sl-atr-ratio",
                "-1",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "--min-sl-atr-ratio must be > 0" in result.stderr

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "engine.run_backtest",
                "--symbol",
                "SBIN",
                "--start",
                "2023-01-01",
                "--end",
                "2023-01-31",
                "--min-sl-atr-ratio",
                "5.0",
                "--max-sl-atr-ratio",
                "2.0",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "must be <=" in result.stderr

    def test_risk_pct_validation(self):
        """--risk-pct must be > 0."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "engine.run_backtest",
                "--symbol",
                "SBIN",
                "--start",
                "2023-01-01",
                "--end",
                "2023-01-31",
                "--risk-pct",
                "0",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "--risk-pct must be > 0" in result.stderr

    def test_cpr_max_width_pct_validation(self):
        """--cpr-max-width-pct must be >= 0."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "engine.run_backtest",
                "--symbol",
                "SBIN",
                "--start",
                "2023-01-01",
                "--end",
                "2023-01-31",
                "--cpr-max-width-pct",
                "-1",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0
        assert "--cpr-max-width-pct must be >= 0" in result.stderr

    def test_new_cli_flags_in_help(self):
        """New hidden-param flags should appear in --help output."""
        result = subprocess.run(
            [sys.executable, "-m", "engine.run_backtest", "--help"],
            capture_output=True,
            text=True,
        )
        assert "--cpr-max-width-pct" in result.stdout
        assert "--min-sl-atr-ratio" in result.stdout
        assert "--risk-pct" in result.stdout
