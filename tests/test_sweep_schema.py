"""Tests for sweep YAML schema validation."""

from __future__ import annotations

import textwrap

import pytest

from engine.sweep_schema import (
    SweepAxis,
    SweepConfig,
    load_sweep_config,
)


def _make_valid_config(**overrides) -> dict:
    base = {
        "name": "test",
        "strategy": "CPR_LEVELS",
        "base_params": {"universe_name": "gold_51"},
        "sweep": [{"param": "cpr_percentile", "values": [25, 33]}],
    }
    base.update(overrides)
    return base


def test_sweep_axis_single():
    axis = SweepAxis(param="cpr_percentile", values=[0.02, 0.04])
    assert axis.param == "cpr_percentile"
    assert len(axis.combinations()) == 2


def test_sweep_axis_cartesian():
    axes = [
        SweepAxis(param="cpr_percentile", values=[0.02, 0.04]),
        SweepAxis(param="rvol", values=[0.8, 1.0]),
    ]
    combos = SweepConfig._cartesian(axes)
    assert len(combos) == 4
    assert combos[0] == {"cpr_percentile": 0.02, "rvol": 0.8}


def test_sweep_config_from_dict():
    cfg = SweepConfig.from_dict(_make_valid_config())
    assert cfg.name == "test"
    assert cfg.strategy == "CPR_LEVELS"
    assert len(cfg.combinations()) == 2


def test_load_sweep_config_from_yaml(tmp_path):
    yaml_file = tmp_path / "sweep.yaml"
    yaml_file.write_text(
        textwrap.dedent("""\
        name: test
        strategy: CPR_LEVELS
        base_params:
          universe_name: gold_51
        sweep:
          - param: cpr_percentile
            values: [25, 33]
    """)
    )
    cfg = load_sweep_config(yaml_file)
    assert cfg.name == "test"
    assert len(cfg.combinations()) == 2


def test_load_sweep_config_rejects_non_mapping(tmp_path):
    yaml_file = tmp_path / "bad.yaml"
    yaml_file.write_text("- not a dict\n")
    with pytest.raises(ValueError, match="YAML root must be a mapping"):
        load_sweep_config(yaml_file)


def test_invalid_param_rejected():
    with pytest.raises(ValueError, match="not a valid backtest param"):
        SweepConfig.from_dict(_make_valid_config(base_params={"nonexistent": 42}))


def test_control_flag_rejected():
    for flag in ("save", "quiet", "progress_file", "chunk_by"):
        with pytest.raises(ValueError, match="control flag"):
            SweepConfig.from_dict(_make_valid_config(base_params={flag: True}))


def test_duplicate_axis_rejected():
    raw = _make_valid_config(
        sweep=[
            {"param": "cpr_percentile", "values": [25]},
            {"param": "cpr_percentile", "values": [33]},
        ],
    )
    with pytest.raises(ValueError, match="Duplicate sweep axis"):
        SweepConfig.from_dict(raw)


def test_empty_values_rejected():
    raw = _make_valid_config(sweep=[{"param": "cpr_percentile", "values": []}])
    with pytest.raises(ValueError, match="empty values"):
        SweepConfig.from_dict(raw)


def test_unknown_strategy_rejected():
    with pytest.raises(ValueError, match="Unknown strategy"):
        SweepConfig.from_dict(_make_valid_config(strategy="NONEXISTENT"))


def test_unknown_compare_metric_rejected():
    with pytest.raises(ValueError, match="Unknown compare metric"):
        SweepConfig.from_dict(_make_valid_config(compare={"metric": "nonexistent"}))


def test_invalid_compare_sort_rejected():
    with pytest.raises(ValueError, match="Sort must be"):
        SweepConfig.from_dict(_make_valid_config(compare={"sort": "random"}))


def test_base_params_validated():
    with pytest.raises(ValueError, match="not a valid backtest param"):
        SweepConfig.from_dict(_make_valid_config(base_params={"fake_param": 99}))


def test_cartesian_empty_axes():
    cfg = SweepConfig.from_dict(_make_valid_config(sweep=[]))
    assert cfg.combinations() == [{}]


def test_build_params_for_merges_base_and_combo():
    cfg = SweepConfig.from_dict(
        _make_valid_config(
            base_params={"start": "2020-01-01", "end": "2020-12-31"},
        )
    )
    merged = cfg.build_params_for({"cpr_percentile": 25})
    assert merged["strategy"] == "CPR_LEVELS"
    assert merged["start"] == "2020-01-01"
    assert merged["cpr_percentile"] == 25
