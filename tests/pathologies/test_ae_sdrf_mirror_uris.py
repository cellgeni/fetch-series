"""Regression test for a known EBI ArrayExpress defect.

**This suite is inverted.** A failure here means the archive fixed something.

Knowledge base: docs/pathologies/ae-sdrf-points-at-decommissioned-mirror.md
"""

from __future__ import annotations

import pytest

from fetch_series.providers import biostudies
from fetch_series.survey import Limits, SurveyClient

pytestmark = pytest.mark.pathology

# The pair that makes the defect visible. Both point every Comment[FASTQ_URI] at
# the decommissioned mirror; only one registers the files it names.
REGISTERED = "E-MTAB-9221"
UNREGISTERED = "E-MTAB-8060"


async def _study(accession: str) -> tuple[list[list[str]], set[str]]:
    async with SurveyClient(limits=Limits(rps=3.0, concurrency=2, attempts=3)) as client:
        rows = await biostudies.sdrf_raw_rows(client, accession, timeout=60)
        names = biostudies.registered_names(
            await biostudies.registered_files(client, accession, timeout=60)
        )
    return rows, names


def _mirror_uris(rows: list[list[str]]) -> list[str]:
    return [
        uri
        for row in rows
        for uri in biostudies.FASTQ_URI.findall("\t".join(f for f in row if f))
        if biostudies.AE_MIRROR.search(uri)
    ]


@pytest.mark.parametrize("accession", [REGISTERED, UNREGISTERED])
def test_the_sdrf_still_points_at_the_decommissioned_mirror(accession: str, run_or_skip) -> None:
    rows, _ = run_or_skip(_study(accession))
    assert rows, f"{accession}'s SDRF could not be read"
    assert _mirror_uris(rows), (
        f"{accession} no longer references /pub/databases/microarray/. The URIs appear "
        "to have been rewritten -- update the knowledge-base page, and if both studies "
        "are fixed, retire the registration guard in providers/biostudies.py."
    )


def test_the_two_studies_are_still_distinguishable_only_by_registration(run_or_skip) -> None:
    """The whole point: identical at the SDRF level, opposite in what they hold."""
    good_rows, good_names = run_or_skip(_study(REGISTERED))
    bad_rows, bad_names = run_or_skip(_study(UNREGISTERED))

    assert _mirror_uris(good_rows) and _mirror_uris(bad_rows), "the premise is gone"

    good_fastq = {n for n in good_names if ".fastq" in n or ".fq" in n}
    bad_fastq = {n for n in bad_names if ".fastq" in n or ".fq" in n}

    assert good_fastq, (
        f"{REGISTERED} no longer registers any fastq files. It was the counter-example; "
        "without it the guard cannot be shown to be discriminating rather than blanket."
    )
    assert not bad_fastq, (
        f"{UNREGISTERED} now registers {len(bad_fastq)} fastq files. Its data appears to "
        "have been restored -- update the knowledge-base page."
    )

    # And the guard still does the right thing with each.
    assert biostudies.sdrf_fastq_uris(good_rows, REGISTERED, good_names)
    assert biostudies.sdrf_fastq_uris(bad_rows, UNREGISTERED, bad_names) == {}
