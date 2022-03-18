from pathlib import Path

import py.path
import pytest


@pytest.fixture
def tmpdir(tmpdir: py.path.local) -> Path:
    return Path(tmpdir)
