from pathlib import Path

import pytest


@pytest.fixture
def tmpdir(tmpdir) -> Path:
    return Path(tmpdir)
