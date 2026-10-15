"""BioStudies and ArrayExpress parsing.

Two of the three tests here are regressions for defects that halved a result
set without erroring: a repeated SDRF column read through a dict, and a file
listing read under the wrong key.
"""

from __future__ import annotations

from typing import Any, ClassVar

from fetch_series.providers.biostudies import (
    registered_names,
    sdrf_fastq_uris,
)

MIRROR = "ftp://ftp.ebi.ac.uk/pub/databases/microarray/data/experiment/MTAB/E-MTAB-9221"


class TestRepeatedSdrfColumns:
    """SDRF repeats Comment[FASTQ_URI] once per mate, by design."""

    ROWS: ClassVar[list[list[str]]] = [
        ["Source Name", "Comment[ENA_RUN]", "Comment[FASTQ_URI]", "Comment[FASTQ_URI]"],
        ["UPN1", "ERR4265292", f"{MIRROR}/UPN1_R1.fastq.gz", f"{MIRROR}/UPN1_R2.fastq.gz"],
    ]

    def test_both_mates_survive(self):
        """Read through csv.DictReader, the repeated key keeps only the last
        value: one URI per run instead of two, which for paired reads is
        exactly half the data and no error."""
        found = sdrf_fastq_uris(self.ROWS, "E-MTAB-9221", {"UPN1_R1.fastq.gz", "UPN1_R2.fastq.gz"})
        assert len(found["ERR4265292"]) == 2

    def test_a_row_with_no_run_accession_is_skipped(self):
        assert sdrf_fastq_uris([["x", f"{MIRROR}/a.fastq.gz"]], "E-MTAB-9221", None) == {}


class TestMirrorRegistrationGuard:
    """A URI on the decommissioned mirror is believed only if still registered."""

    ROWS: ClassVar[list[list[str]]] = [
        ["UPN1", "ERR4265292", f"{MIRROR}/UPN1_R1.fastq.gz"],
    ]

    def test_a_registered_file_is_kept_and_rewritten_to_biostudies(self):
        """E-MTAB-9221: the URI is a stale spelling of a real study file."""
        found = sdrf_fastq_uris(self.ROWS, "E-MTAB-9221", {"UPN1_R1.fastq.gz"})
        assert found["ERR4265292"] == [
            "https://www.ebi.ac.uk/biostudies/files/E-MTAB-9221/UPN1_R1.fastq.gz"
        ]

    def test_an_unregistered_file_is_dropped_so_the_run_falls_through(self):
        """E-MTAB-8060: the study registers only its idf and sdrf. Keeping the
        URI discarded the real ENA BAM for all 15 runs, silently."""
        assert sdrf_fastq_uris(self.ROWS, "E-MTAB-8060", set()) == {}

    def test_an_unreachable_file_list_keeps_every_uri(self):
        """None means the API could not be reached. An unreachable API must not
        be able to reroute a whole study to its BAMs."""
        found = sdrf_fastq_uris(self.ROWS, "E-MTAB-8060", None)
        assert found["ERR4265292"] == [f"{MIRROR}/UPN1_R1.fastq.gz"]

    def test_a_uri_off_the_mirror_is_never_second_guessed(self):
        rows = [["UPN1", "ERR1", "ftp://ftp.sra.ebi.ac.uk/vol1/fastq/ERR1/ERR1.fastq.gz"]]
        assert sdrf_fastq_uris(rows, "E-MTAB-1", set())["ERR1"] == [
            "ftp://ftp.sra.ebi.ac.uk/vol1/fastq/ERR1/ERR1.fastq.gz"
        ]


class TestRegisteredNames:
    ITEMS: ClassVar[list[dict[str, Any]]] = [
        {"path": "UPN2_S2_L001_R1_001.fastq.gz", "Name": "UPN2_S2_L001_R1_001.fastq.gz"},
        {"path": "sub/dir/other.fastq.gz"},
        {"Name": "only-a-name.txt"},
        {"Size": 5},
    ]

    def test_takes_the_bare_filename_from_either_key(self):
        assert registered_names(self.ITEMS) == {
            "UPN2_S2_L001_R1_001.fastq.gz",
            "other.fastq.gz",
            "only-a-name.txt",
        }

    def test_an_entry_with_neither_key_is_skipped_not_added_as_empty(self):
        """An empty string in the registered set would match a URI ending in a
        slash and quietly re-admit it."""
        assert "" not in registered_names(self.ITEMS)
