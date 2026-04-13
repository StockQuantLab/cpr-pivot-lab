from __future__ import annotations

import pytest

from engine.command_lock import acquire_command_lock


def test_acquire_command_lock_rejects_second_owner(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    with acquire_command_lock("runtime-writer", detail="runtime writer"):
        with pytest.raises(SystemExit, match="runtime writer is already running"):
            with acquire_command_lock("runtime-writer", detail="runtime writer"):
                pass
