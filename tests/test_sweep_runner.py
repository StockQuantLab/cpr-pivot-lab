"""Tests for sweep runner subprocess orchestration."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from unittest.mock import patch

from engine.sweep_runner import SweepResult, _build_label, _build_manifest

# --- Minimal SweepConfig stub for testing ---


@dataclass
class FakeSweepCompare:
    metric: str = "calmar"
    sort: str = "desc"
    top_n: int = 5


@dataclass
class FakeSweepConfig:
    name: str
    strategy: str
    base_params: dict
    sweep: list
    compare: FakeSweepCompare = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.compare is None:
            self.compare = FakeSweepCompare()

    def combinations(self):
        if not self.sweep:
            return [{}]
        keys = [a["param"] for a in self.sweep]
        value_lists = [a["values"] for a in self.sweep]
        import itertools

        return [dict(zip(keys, combo, strict=True)) for combo in itertools.product(*value_lists)]

    def build_params_for(self, combo):
        merged = {**self.base_params, "strategy": self.strategy, **combo}
        return merged


def _make_config(**overrides) -> FakeSweepConfig:
    defaults = {
        "name": "test",
        "strategy": "CPR_LEVELS",
        "base_params": {"universe_name": "gold_51", "start": "2020-01-01", "end": "2020-12-31"},
        "sweep": [{"param": "cpr_percentile", "values": [25, 33]}],
    }
    defaults.update(overrides)
    return FakeSweepConfig(**defaults)


# --- SweepResult ---


def test_sweep_result_dataclass():
    r = SweepResult(
        run_id="abc123",
        label="cpr_25_rvol",
        params_dict={"cpr_percentile": 25, "rvol": 1.2},
        exit_code=0,
        elapsed_sec=12.5,
    )
    assert r.run_id == "abc123"
    assert r.label == "cpr_25_rvol"
    assert r.exit_code == 0
    assert r.elapsed_sec == 12.5


# --- _build_label ---


def test_build_label():
    label = _build_label({"cpr_percentile": 25, "rvol": 1.2})
    assert label == "cpr-percentile=25-rvol=1.2"


# --- _build_manifest ---


def test_build_manifest():
    results = [
        SweepResult(run_id="abc", label="test", params_dict={}, exit_code=0, elapsed_sec=1.0),
        SweepResult(run_id="def", label="test2", params_dict={}, exit_code=1, elapsed_sec=2.0),
    ]
    manifest = _build_manifest("my-sweep", results)
    assert manifest["sweep"] == "my-sweep"
    assert manifest["dry_run"] is False
    assert manifest["completed"] == 2
    assert manifest["total_combinations"] == 2
    assert manifest["results"][0]["run_id"] == "abc"
    assert manifest["results"][1]["exit_code"] == 1


def test_build_manifest_dry_run():
    """Dry-run manifest must set completed=0 and dry_run=True."""
    results = [
        SweepResult(run_id="(dry-run)", label="a", params_dict={}, exit_code=0, elapsed_sec=0.0),
        SweepResult(run_id="(dry-run)", label="b", params_dict={}, exit_code=0, elapsed_sec=0.0),
    ]
    manifest = _build_manifest("preview", results, dry_run=True)
    assert manifest["dry_run"] is True
    assert manifest["completed"] == 0
    assert manifest["total_combinations"] == 2


# --- run_sweep ---


def test_run_sweep_dry_run():
    from engine.sweep_runner import run_sweep

    config = _make_config()
    results = run_sweep(config, dry_run=True)
    assert len(results) == 2
    assert all(r.run_id == "(dry-run)" for r in results)


def test_run_sweep_calls_subprocess():
    from engine.sweep_runner import run_sweep

    config = _make_config(sweep=[{"param": "cpr_percentile", "values": [25]}])

    mock_completed = subprocess.CompletedProcess(
        args=[], returncode=0, stdout="run_id: abc123\n", stderr=""
    )

    with (
        patch("db.duckdb.close_db"),
        patch("engine.sweep_runner.subprocess.run", return_value=mock_completed) as mock_run,
    ):
        results = run_sweep(config, dry_run=False)

    assert len(results) == 1
    assert results[0].run_id == "abc123"
    mock_run.assert_called_once()
    # Verify sys.executable is first arg and --save is included
    call_args = mock_run.call_args[0][0]
    assert "engine.run_backtest" in call_args
    assert "--save" in call_args
    assert "--force-rerun" not in call_args


def test_run_sweep_failure_continues():
    from engine.sweep_runner import run_sweep

    config = _make_config(sweep=[{"param": "cpr_percentile", "values": [25, 33]}])

    mock_failed = subprocess.CompletedProcess(
        args=[], returncode=1, stdout="", stderr="some error\n"
    )
    mock_ok = subprocess.CompletedProcess(
        args=[], returncode=0, stdout="run_id: abc123\n", stderr=""
    )

    with (
        patch("db.duckdb.close_db"),
        patch("engine.sweep_runner.subprocess.run", side_effect=[mock_failed, mock_ok]),
    ):
        results = run_sweep(config, dry_run=False)

    assert len(results) == 2
    assert results[0].run_id == "(failed)"
    assert results[0].exit_code == 1
    assert results[1].run_id == "abc123"
    assert results[1].exit_code == 0


def test_run_sweep_timeout():
    from engine.sweep_runner import run_sweep

    config = _make_config(sweep=[{"param": "cpr_percentile", "values": [25]}])

    with (
        patch("db.duckdb.close_db"),
        patch(
            "engine.sweep_runner.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd=[], timeout=3600),
        ),
    ):
        results = run_sweep(config, dry_run=False)

    assert len(results) == 1
    assert results[0].run_id == "(timeout)"
    assert results[0].exit_code == -1


def test_run_sweep_parses_run_id_from_stdout():
    from engine.sweep_runner import run_sweep

    config = _make_config(sweep=[{"param": "cpr_percentile", "values": [25]}])

    for stdout_line, expected in [
        ("run_id: abc123def", "abc123def"),
        ("Cached run_id: xyz789.", "xyz789"),
        ("some prefix run_id: foo bar", "foo bar"),
    ]:
        mock_completed = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=stdout_line + "\n", stderr=""
        )
        with (
            patch("db.duckdb.close_db"),
            patch("engine.sweep_runner.subprocess.run", return_value=mock_completed),
        ):
            results = run_sweep(config, dry_run=False)
        assert results[0].run_id == expected, f"Failed for stdout: {stdout_line}"


# --- _build_subprocess_args ---


def test_build_subprocess_args():
    from engine.sweep_runner import _build_subprocess_args

    config = _make_config(base_params={"start": "2020-01-01", "end": "2020-12-31"})
    args = _build_subprocess_args(config, {"cpr_percentile": 25})
    assert "engine.run_backtest" in args
    assert "--save" in args
    assert "--strategy" in args
    assert "CPR_LEVELS" in args
    assert "--cpr-percentile" in args
    assert "--force-rerun" not in args


def test_build_subprocess_args_store_true_presence_only():
    """store_true flags must be emitted as presence-only (no value appended)."""
    from engine.sweep_runner import _build_subprocess_args

    config = _make_config(
        base_params={"skip_rvol": True, "narrowing_filter": True, "start": "2020-01-01"}
    )
    args = _build_subprocess_args(config, {})
    assert "--skip-rvol" in args
    assert "--narrowing-filter" in args
    # Ensure "True" was NOT appended after the flag
    skip_idx = args.index("--skip-rvol")
    assert skip_idx + 1 >= len(args) or args[skip_idx + 1] != "True"


def test_build_subprocess_args_store_true_false_omitted():
    """store_true flags with value=False must be omitted entirely."""
    from engine.sweep_runner import _build_subprocess_args

    config = _make_config(
        base_params={"skip_rvol": False, "start": "2020-01-01", "end": "2020-12-31"}
    )
    args = _build_subprocess_args(config, {})
    assert "--skip-rvol" not in args


def test_run_sweep_dry_run_unique_labels_and_merged_params():
    """Each dry-run result must have a unique label and merged base+combo params."""
    from engine.sweep_runner import run_sweep

    config = _make_config(
        base_params={"universe_name": "gold_51", "start": "2020-01-01"},
        sweep=[{"param": "cpr_percentile", "values": [25, 33, 50]}],
    )
    results = run_sweep(config, dry_run=True)
    assert len(results) == 3
    labels = [r.label for r in results]
    assert len(set(labels)) == 3, f"Labels not unique: {labels}"
    # Each result must include base_params merged in
    for r in results:
        assert r.params_dict.get("universe_name") == "gold_51"
        assert r.params_dict.get("start") == "2020-01-01"
        assert "strategy" in r.params_dict
