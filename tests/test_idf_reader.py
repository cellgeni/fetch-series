"""Reading an ArrayExpress IDF: every cell of a row, in either dialect.

Two readers each missed half of the file. ``SECONDARY`` read only the first cell
after ``Comment[SecondaryAccession]``, where an E-GEOD import puts its GEO
series, and so read no study from 453 of the 551 IDFs it then counted as
declaring none. ``idf_comments`` anchored on a bare ``Comment[`` and read nothing
from an IDF that quotes its cells. The rows below are the archive's own, as
served on 2026-10-28.
"""

from __future__ import annotations

from fetch_series import core
from fetch_series.providers.biostudies import (
    idf_comments,
    idf_rows,
    idf_secondary,
    idf_values,
    sdrf_files,
    sequence_data,
)

# E-GEOD-10571: bare cells, the SRA study in the second.
E_GEOD_10571 = (
    "Comment[ArrayExpressAccession]\tE-GEOD-10571\n"
    "Publication Status Term Accession Number\t\t\t\t\t\n"
    "Comment[SecondaryAccession]\tGSE10571\tSRP003805\t\t\t\n"
    "Comment[GEOLastUpdateDate]\t2012-03-19\n"
    "Comment[SequenceDataURI]\thttp://www.ebi.ac.uk/ena/data/view/SRR069023-SRR069025\t\t\t\t\n"
    "SDRF File\tE-GEOD-10571.sdrf.txt\n"
)

# E-ERAD-129: every cell quoted, a space in every tag but one, a trailing tab.
E_ERAD_129 = (
    '"MAGE-TAB Version"\t"1.1"\n'
    '"Person First Name"\t""\t\n'
    '"SDRF File"\t"E-ERAD-129.sdrf.txt"\t\n'
    '"Comment [Submitted Name]"\t"Gorilla  chimpanzee and bonobo transcriptome sequencing"\n'
    '"Comment [SecondaryAccession]"\t"ERP002094"\n'
    '"Comment [SequenceDataURI]"\t"http://www.ebi.ac.uk/ena/data/view/ERR218142-ERR218147"\n'
    '"Comment[ArrayExpressAccession]"\t"E-ERAD-129"\n'
)


class TestEveryCell:
    def test_the_study_in_the_second_cell_is_read(self):
        assert idf_secondary(E_GEOD_10571) == ["SRP003805"]

    def test_the_whole_row_is_kept_for_the_catalogue(self):
        """The GEO series is the catalogue's ``geo_series``; only the route filters it out."""
        assert idf_comments(E_GEOD_10571)["SecondaryAccession"] == ["GSE10571", "SRP003805"]

    def test_two_rows_still_read_as_before(self):
        """E-GEOD-60450 writes the series and the study in rows of their own."""
        text = (
            "Comment[SecondaryAccession]\tGSE60450\t\t\t\t\n"
            "Comment[SecondaryAccession]\tSRP045534\t\t\t\t\n"
        )
        assert idf_secondary(text) == ["SRP045534"]

    def test_a_study_in_the_first_cell_still_reads(self):
        assert idf_secondary("Comment[SecondaryAccession]\tERP111731\n") == ["ERP111731"]

    def test_a_row_naming_no_study_reads_none(self):
        """E-MTAB-6505 has no such row; a row holding only a series is the same answer."""
        assert idf_secondary("Comment[SecondaryAccession]\tGSE1\n") == []
        assert idf_secondary("Comment[ArrayExpressAccession]\tE-MTAB-6505\n") == []


class TestQuotedCells:
    def test_the_quoted_study_is_read(self):
        assert idf_secondary(E_ERAD_129) == ["ERP002094"]

    def test_every_quoted_comment_is_read(self):
        comments = idf_comments(E_ERAD_129)
        assert comments["SecondaryAccession"] == ["ERP002094"]
        assert comments["ArrayExpressAccession"] == ["E-ERAD-129"]
        assert comments["Submitted Name"] == [
            "Gorilla  chimpanzee and bonobo transcriptome sequencing"
        ]

    def test_its_run_range_reaches_the_range_reader(self):
        uri = idf_comments(E_ERAD_129)["SequenceDataURI"]
        assert sequence_data(uri).accessions == [f"ERR21814{n}" for n in range(2, 8)]

    def test_its_sdrf_file_is_named(self):
        assert sdrf_files(E_ERAD_129) == ["E-ERAD-129.sdrf.txt"]

    def test_an_empty_quoted_cell_is_no_value(self):
        assert dict(idf_rows(E_ERAD_129))["Person First Name"] == []

    def test_a_doubled_quote_inside_a_cell_is_one_quote(self):
        rows = idf_rows('"Investigation Title"\t"the ""Ss"" mouse"\n')
        assert rows == [("Investigation Title", ['the "Ss" mouse'])]

    def test_a_stray_quote_stays_on_its_own_line(self):
        """An unclosed quote in a description must not swallow the rows after it."""
        text = (
            'Experiment Description\t"We sequenced 4 lines\n'
            "Comment[SecondaryAccession]\tGSE1\tSRP2\n"
        )
        assert idf_secondary(text) == ["SRP2"]
        assert idf_values(text, "Experiment Description") == ['"We sequenced 4 lines']


class TestTheLegacyHelper:
    def test_core_reads_through_the_same_reader(self, monkeypatch):
        """``core.ae2secondary`` read the first cell only, and only ERP or EGA."""

        class Response:
            text = E_GEOD_10571

        monkeypatch.setattr(core, "_study_file", lambda series, name: Response())
        assert core.ae2secondary("E-GEOD-10571") == ["SRP003805"]
