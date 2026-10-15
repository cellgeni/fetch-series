"""Every RouteEvidence block, checked against the survey it claims to summarise.

The counts in ``graph.py`` rank the routes, and the route graph page draws them.
Both are only as good as the transcription from the survey, so this reads the
exported verdicts in ``data/surveys/`` and recounts them: queried, failed, empty
and distinct results. Marked ``slow`` and skipped without the Git LFS objects,
which CI does not fetch.

Two routes have no export (``run->geo_sample:ena_alias`` and
``run->ae_experiment:ena_alias`` were surveyed after the last ``fetch survey
export``), and the six 2026-05 script routes predate the harness. Those are
skipped by name of the missing file, not silently passed.
"""

import csv
import gzip
import sys
from collections import Counter
from functools import cache
from pathlib import Path

import pytest

from fetch_series.cli import _slug
from fetch_series.graph import REGISTRY, Route

pytestmark = pytest.mark.slow

SURVEYS = Path("data/surveys")
LFS_POINTER = b"version https://git-lfs"


def _is_lfs_pointer(path: Path) -> bool:
    with path.open("rb") as fh:
        return fh.read(len(LFS_POINTER)) == LFS_POINTER


@cache
def _corpus_dirs() -> dict[str, Path]:
    """Map each exported corpus name to its directory, through its manifest.

    Directory names are slugs, and the slug is lossy (``:`` and ``_`` collide),
    so the manifest's verbatim corpus name is the only reliable key.
    """
    dirs: dict[str, Path] = {}
    for manifest in SURVEYS.glob("*/manifest.txt"):
        if _is_lfs_pointer(manifest):
            continue
        for line in manifest.read_text().splitlines():
            if line.startswith("corpus:"):
                dirs[line.split(":", 1)[1].strip()] = manifest.parent
    return dirs


@pytest.mark.parametrize("route", list(REGISTRY), ids=lambda r: r.id)
def test_evidence_matches_the_exported_survey(route: Route):
    evidence = route.evidence
    if evidence is None:
        pytest.skip("unmeasured")
    if not _corpus_dirs():
        pytest.skip(f"{SURVEYS} not present (Git LFS objects not fetched)")
    directory = _corpus_dirs().get(evidence.corpus)
    export = directory / f"{_slug(route.id)}.csv.gz" if directory else None
    if export is None or not export.exists():
        pytest.skip(f"no export of {route.id} on {evidence.corpus}")

    # The `results` column of a census row can exceed csv's default field limit.
    csv.field_size_limit(sys.maxsize)
    outcomes: Counter[str] = Counter()
    results: set[str] = set()
    with gzip.open(export, "rt", newline="") as fh:
        for row in csv.DictReader(fh):
            outcomes[row["outcome"]] += 1
            if row["results"]:
                results.update(row["results"].split(";"))

    assert sum(outcomes.values()) == evidence.accessions_queried
    assert outcomes["failed"] == evidence.accessions_failed
    assert outcomes["empty"] == evidence.accessions_empty
    assert len(results) == evidence.unique_results
