import shutil
import uuid
from pathlib import Path

import pytest


@pytest.fixture
def tmp_path():
    """Provide an isolated temporary directory as a Path for tests.

    Uses a local temporary directory factory to avoid environment-specific
    fixture cleanup issues on some Windows setups where pytest's default tmp_path
    base directory cannot be cleaned between function-scoped tests.
    """

    base_dir = Path(__file__).resolve().parent.parent / ".tmp_pytest"
    base_dir.mkdir(parents=True, exist_ok=True)
    path = base_dir / f"pytest-{uuid.uuid4().hex}"
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)
