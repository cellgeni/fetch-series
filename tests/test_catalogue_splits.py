"""The committed route splits against the recorded verdicts they were cut from.

Slow, and needs the Git LFS objects under ``data/``: CI builds the docs from
``route_splits.json`` precisely because it has neither.
"""

from __future__ import annotations

import pytest

from fetch_series.catalogue import CATALOGUE, load_splits
from fetch_series.catalogue.splits import compute, cross_archive_pairs
from fetch_series.graph import REGISTRY

pytestmark = pytest.mark.slow


def test_the_committed_splits_are_the_exports():
    """A stale route_splits.json would draw yesterday's routes on today's nodes."""
    splits, _ = compute(CATALOGUE, REGISTRY)
    key = lambda s: (s.route, s.source, s.target)  # noqa: E731
    assert sorted(splits, key=key) == sorted(load_splits(), key=key)


def test_no_recorded_verdict_crosses_insdc_archives():
    """Why the catalogue declares no SRP -> ERR: no verdict has ever contained one."""
    assert not cross_archive_pairs()
