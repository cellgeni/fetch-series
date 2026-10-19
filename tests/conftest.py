"""Shared test setup.

The library itself never reads ``.env`` -- that is the caller's job -- so the
integration tests load it here. Tests still run without a key, just against the
lower unauthenticated NCBI rate limit.
"""

from collections.abc import Callable
from pathlib import Path

import pytest
from dotenv import load_dotenv

load_dotenv()

LFS_POINTER = b"version https://git-lfs"


def lfs_object_missing(path: Path) -> bool:
    """Whether ``path`` is absent *or* only a Git LFS pointer.

    A checkout without ``git lfs pull`` has the file, just not its content: a
    three-line pointer. ``exists()`` is true for it, so a test guarded only by
    ``exists()`` parses the pointer as data and fails, instead of skipping.
    """
    if not path.exists():
        return True
    with path.open("rb") as fh:
        return fh.read(len(LFS_POINTER)) == LFS_POINTER


@pytest.fixture
def require_lfs() -> Callable[[Path], None]:
    """Skip the calling test unless the Git LFS object at a path is fetched."""

    def require(path: Path) -> None:
        if lfs_object_missing(path):
            pytest.skip(f"{path} not present (Git LFS object not fetched)")

    return require
