"""BioStudies and ArrayExpress parsing.

Two of the three tests here are regressions for defects that halved a result
set without erroring: a repeated SDRF column read through a dict, and a file
listing read under the wrong key.
"""

from __future__ import annotations

from typing import Any, ClassVar

import httpx
import pytest

from fetch_series import core
from fetch_series.providers.biostudies import (
    fetch_idf,
    idf_secondary_accessions,
    is_directory_index,
    merge_sdrf_tables,
    registered_names,
    sdrf_characteristics,
    sdrf_fastq_uris,
    sdrf_files,
    sdrf_raw_rows,
    sdrf_rows,
    sdrf_values,
)
from fetch_series.survey.client import (
    Limits,
    ServedCutError,
    ServedDirectoryError,
    SurveyClient,
    is_retryable,
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


class TestMirrorFallback:
    """``/biostudies/files/`` answers 404 for files the study registers (E-MTAB-13459)."""

    FIRE = "https://ftp.ebi.ac.uk/biostudies/fire/E-MTAB-/459/E-MTAB-13459"

    def _handler(self, seen: list[str], *, front: int = 404, mirror: int = 200, link: Any = FIRE):
        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            seen.append(url)
            if url.startswith("https://www.ebi.ac.uk/biostudies/files/"):
                return httpx.Response(front, text="Comment[SecondaryAccession]\tERP1\n")
            if url.endswith("/info"):
                return httpx.Response(200, json={"httpLink": link})
            if url.startswith(self.FIRE):
                return httpx.Response(mirror, text="Source Name\tComment[ENA_RUN]\nA\tERR1\n")
            raise AssertionError(url)

        return handler

    @staticmethod
    async def _call(handler, fn):
        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http,
            SurveyClient(Limits(rps=0), client=http) as client,
        ):
            return await fn(client, "E-MTAB-13459", 10)

    async def test_a_404_is_read_from_the_mirror_the_study_names(self):
        seen: list[str] = []
        rows = await self._call(self._handler(seen), sdrf_raw_rows)
        assert rows == [["Source Name", "Comment[ENA_RUN]"], ["A", "ERR1"]]
        assert seen[-1] == f"{self.FIRE}/Files/E-MTAB-13459.sdrf.txt"

    async def test_the_front_door_is_asked_first(self):
        seen: list[str] = []
        text = await self._call(self._handler(seen, front=200), fetch_idf)
        assert "ERP1" in text
        assert len(seen) == 1

    async def test_a_404_on_the_mirror_is_the_verdict(self):
        with pytest.raises(httpx.HTTPStatusError) as caught:
            await self._call(self._handler([], mirror=404), fetch_idf)
        assert caught.value.response.status_code == 404

    async def test_no_mirror_named_keeps_the_front_doors_404(self):
        seen: list[str] = []
        with pytest.raises(httpx.HTTPStatusError) as caught:
            await self._call(self._handler(seen, link=None), fetch_idf)
        assert str(caught.value.request.url).startswith("https://www.ebi.ac.uk/biostudies/files/")
        assert not any(url.startswith(self.FIRE) for url in seen)

    async def test_any_other_status_is_not_rerouted(self):
        seen: list[str] = []
        with pytest.raises(httpx.HTTPStatusError):
            await self._call(self._handler(seen, front=500), fetch_idf)
        assert not any(url.endswith("/info") for url in seen)

    def test_the_sync_helper_falls_back_too(self, monkeypatch):
        """``core.ae2secondary`` read the same front door, with the same 404."""
        monkeypatch.setattr(core, "transport", httpx.MockTransport(self._handler([])))
        assert core._study_file("E-MTAB-13459", "E-MTAB-13459.sdrf.txt").text.startswith("Source")


class TestSplitSdrf:
    """E-GEOD-29264 has no ``<acc>.sdrf.txt``: its IDF names a hyb and a seq SDRF."""

    STUDY = "E-GEOD-29264"
    IDF = (
        "Investigation Title\tCircadian ChIP\n"
        "SDRF File\tE-GEOD-29264.hyb.sdrf.txt\tE-GEOD-29264.seq.sdrf.txt\t\n"
    )
    HYB = (
        "Source Name\tCharacteristics [organism]\tTerm Source REF\tProtocol REF\tTerm Source REF\t"
        "Array Design REF\tTerm Source REF\tArray Data File\n"
        "GSM1 1\tSynechococcus\tEFO\tP-1\tArrayExpress\tA-1\tArrayExpress\ta.CEL\n"
    )
    SEQ = (
        "Source Name\tCharacteristics [organism]\tTerm Source REF\tProtocol REF\tTerm Source REF\t"
        "Protocol REF\tTerm Source REF\tComment [ENA_RUN]\tComment [FASTQ_URI]\tComment [FASTQ_URI]\n"
        "GSM2 1\tSynechococcus\tEFO\tP-1\tArrayExpress\tP-2\tArrayExpress\tSRR1\tr1.fq.gz\tr2.fq.gz\n"
    )

    def test_the_idf_names_every_file(self):
        assert sdrf_files(self.IDF) == [
            "E-GEOD-29264.hyb.sdrf.txt",
            "E-GEOD-29264.seq.sdrf.txt",
        ]

    def test_an_idf_naming_none_names_none(self):
        assert sdrf_files("Investigation Title\tx\n") == []

    def test_identical_headers_append_rows(self):
        table = [["Source Name", "Comment[ENA_RUN]"], ["A", "ERR1"]]
        other = [["Source Name", "Comment[ENA_RUN]"], ["B", "ERR2"]]
        assert merge_sdrf_tables([table, other]) == [
            ["Source Name", "Comment[ENA_RUN]"],
            ["A", "ERR1"],
            ["B", "ERR2"],
        ]

    def test_different_headers_align_by_name_and_keep_both_mates(self):
        merged = merge_sdrf_tables(
            [[line.split("\t") for line in t.splitlines()] for t in (self.HYB, self.SEQ)]
        )
        header, hyb, seq = merged
        assert header.count("Comment [FASTQ_URI]") == 2
        assert sdrf_values(merged, ("Comment[FASTQ_URI]",))["Comment[FASTQ_URI]"] == [
            "r1.fq.gz",
            "r2.fq.gz",
        ]
        assert hyb[header.index("Array Data File")] == "a.CEL"
        assert seq[header.index("Array Data File")] == ""
        assert seq[header.index("Comment [ENA_RUN]")] == "SRR1"
        assert hyb[header.index("Comment [ENA_RUN]")] == ""

    def test_a_qualifier_stays_beside_what_it_qualifies(self):
        """Counted on its own, the third Term Source REF is the array design's in
        one file and a protocol's in the other. Counted within its column, the
        seq file's second protocol and its term source go in together."""
        header, *_ = merge_sdrf_tables(
            [[line.split("\t") for line in t.splitlines()] for t in (self.HYB, self.SEQ)]
        )
        design = header.index("Array Design REF")
        assert header[design + 1] == "Term Source REF"
        protocols = [i for i, name in enumerate(header) if name == "Protocol REF"]
        assert len(protocols) == 2
        assert all(header[i + 1] == "Term Source REF" for i in protocols)

    def test_a_unit_keeps_its_term_columns(self):
        """E-MTAB-10553: the term columns after a Unit qualify the unit. The
        second file's term source qualifies the age, so it goes before the unit,
        where binding gives it to the age and not to the unit."""
        one = [
            ["Source Name", "Characteristics[age]", "Unit[time unit]", "Term Source REF"],
            ["A", "84", "year", "UO"],
        ]
        two = [["Source Name", "Characteristics[age]", "Term Source REF"], ["B", "3", "EFO"]]
        merged = merge_sdrf_tables([one, two])
        assert merged == [
            [
                "Source Name",
                "Characteristics[age]",
                "Term Source REF",
                "Unit[time unit]",
                "Term Source REF",
            ],
            ["A", "84", "", "year", "UO"],
            ["B", "3", "EFO", "", ""],
        ]
        found = sdrf_characteristics(merged)
        assert found["A"][0].term_source is None
        assert found["B"][0].term_source == "EFO"

    def _handler(self, seen: list[str], *, idf: str | None = IDF, missing: str = ""):
        files = {
            f"{self.STUDY}.idf.txt": idf,
            f"{self.STUDY}.hyb.sdrf.txt": self.HYB,
            f"{self.STUDY}.seq.sdrf.txt": self.SEQ,
            missing: None,
        }

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            seen.append(url)
            if url.endswith("/info"):
                return httpx.Response(200, json={})
            text = files.get(url.rsplit("/", 1)[-1])
            if text is None:
                return httpx.Response(404, text="")
            return httpx.Response(200, text=text)

        return handler

    @classmethod
    async def _call(cls, handler, fn):
        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http,
            SurveyClient(Limits(rps=0), client=http) as client,
        ):
            return await fn(client, cls.STUDY, 10)

    async def test_a_404_reads_the_files_the_idf_names(self):
        seen: list[str] = []
        rows = await self._call(self._handler(seen), sdrf_raw_rows)
        assert len(rows) == 3
        assert sdrf_values(rows, ("Comment[ENA_RUN]",))["Comment[ENA_RUN]"] == ["SRR1"]
        assert seen[0].endswith(f"/{self.STUDY}.sdrf.txt")
        assert seen[-1].endswith(".seq.sdrf.txt")

    async def test_the_dict_reader_gets_them_too(self):
        rows = await self._call(self._handler([]), sdrf_rows)
        assert [row["Source Name"] for row in rows] == ["GSM1 1", "GSM2 1"]

    async def test_an_idf_naming_only_the_usual_file_keeps_its_404(self):
        idf = f"SDRF File\t{self.STUDY}.sdrf.txt\n"
        with pytest.raises(httpx.HTTPStatusError) as caught:
            await self._call(self._handler([], idf=idf), sdrf_raw_rows)
        assert str(caught.value.request.url).endswith(f"/{self.STUDY}.sdrf.txt")

    async def test_a_missing_idf_keeps_the_sdrfs_404(self):
        with pytest.raises(httpx.HTTPStatusError) as caught:
            await self._call(self._handler([], idf=None), sdrf_raw_rows)
        assert str(caught.value.request.url).endswith(f"/{self.STUDY}.sdrf.txt")

    async def test_a_named_file_that_is_missing_fails_the_study(self):
        """Half a study read as all of it would be a silent loss."""
        with pytest.raises(httpx.HTTPStatusError) as caught:
            await self._call(self._handler([], missing=f"{self.STUDY}.seq.sdrf.txt"), sdrf_raw_rows)
        assert str(caught.value.request.url).endswith(".seq.sdrf.txt")

    def test_the_sync_helper_reads_them_too(self, monkeypatch):
        monkeypatch.setattr(core, "transport", httpx.MockTransport(self._handler([])))
        assert "SRR1" in core._sdrf_text(self.STUDY)


class TestCutAt4096:
    """E-MTAB-9791's SDRF, registered at 30,836 bytes, was served as its first 4,096.

    HTTP 200, mid-row, with a Content-Length and an ETag that agreed with the
    cut (2026-10-28). Only a body of exactly that length asks for the registered
    size, so every other read costs what it did.
    """

    STUDY = "E-MTAB-9791"
    NAME = f"{STUDY}.sdrf.txt"
    HEADER = "Source Name\tComment[ENA_RUN]\n"

    def _handler(self, seen: list[str], *, served: int, registered: int | None):
        body = (self.HEADER + "A\tERR1\n" * served)[:served]

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            seen.append(url)
            if url.startswith("https://www.ebi.ac.uk/biostudies/files/"):
                return httpx.Response(200, text=body)
            if "/files?" in url:
                assert request.url.params["search"] == self.NAME
                items = [] if registered is None else [{"path": self.NAME, "Size": registered}]
                return httpx.Response(200, json={"items": items, "pagination": {"total": 2}})
            raise AssertionError(url)

        return handler

    @classmethod
    async def _call(cls, handler):
        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http,
            SurveyClient(Limits(rps=0), client=http) as client,
        ):
            return await sdrf_raw_rows(client, cls.STUDY, 10)

    async def test_a_cut_fails_the_read_and_is_not_retryable(self):
        """E-MTAB-9791's SDRF was cut on every request for at least 46 minutes."""
        with pytest.raises(ServedCutError, match="served 4,096 of its 30,836") as caught:
            await self._call(self._handler([], served=4096, registered=30_836))
        assert not is_retryable(caught.value)

    async def test_a_cut_is_asked_for_once(self):
        """A retry would wait 15 seconds to ask a throttling mirror, or a memo, again."""
        seen: list[str] = []
        async with (
            httpx.AsyncClient(
                transport=httpx.MockTransport(self._handler(seen, served=4096, registered=30_836))
            ) as http,
            SurveyClient(Limits(rps=0), client=http) as client,
        ):
            with pytest.raises(ServedCutError):
                await client.with_retry(lambda timeout: sdrf_raw_rows(client, self.STUDY, timeout))
        assert sum(url.startswith("https://www.ebi.ac.uk/biostudies/files/") for url in seen) == 1

    async def test_a_file_that_is_4096_bytes_is_read(self):
        """E-MTAB-7492's IDF is 4,096 bytes registered, served and in the legacy copy."""
        seen: list[str] = []
        rows = await self._call(self._handler(seen, served=4096, registered=4096))
        assert rows[0] == ["Source Name", "Comment[ENA_RUN]"]
        assert any("/files?" in url for url in seen)

    async def test_any_other_length_costs_no_request(self):
        """E-MTAB-11506's IDF is served whole at 6,326 bytes and registered at 6,747.

        A later version than the size BioStudies registers. Comparing every body
        with its registered size would fail it.
        """
        seen: list[str] = []
        await self._call(self._handler(seen, served=6326, registered=6747))
        assert not any("/files?" in url for url in seen)

    async def test_a_file_the_study_does_not_list_is_read(self):
        rows = await self._call(self._handler([], served=4096, registered=None))
        assert rows

    def test_the_sync_helper_checks_too(self, monkeypatch):
        handler = self._handler([], served=4096, registered=30_836)
        monkeypatch.setattr(core, "transport", httpx.MockTransport(handler))
        with pytest.raises(ServedCutError):
            core._sdrf_text(self.STUDY)


class TestDirectoryListing:
    """E-MTAB-11257's IDF was served as an Apache listing of its own path.

    HTTP 200 and 837 bytes on 2026-10-28, where the file is a 4,480-byte IDF
    naming ERP133806. ``idf_secondary`` read it as an IDF naming no study and
    recorded EMPTY. The listing below is the one Apache writes for an empty
    directory at that path, and it is 837 bytes, as the one served was.
    """

    STUDY = "E-MTAB-11257"
    NAME = f"{STUDY}.idf.txt"
    PATH = f"/biostudies/fire/E-MTAB-/257/{STUDY}/Files/{NAME}"
    LISTING = (
        '<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">\n'
        "<html>\n"
        " <head>\n"
        f"  <title>Index of {PATH}</title>\n"
        " </head>\n"
        " <body>\n"
        f"<h1>Index of {PATH}</h1>\n"
        "  <table>\n"
        '   <tr><th valign="top"><img src="/icons/blank.gif" alt="[ICO]"></th>'
        '<th><a href="?C=N;O=D">Name</a></th><th><a href="?C=M;O=A">Last modified</a></th>'
        '<th><a href="?C=S;O=A">Size</a></th><th><a href="?C=D;O=A">Description</a></th></tr>\n'
        '   <tr><th colspan="5"><hr></th></tr>\n'
        '<tr><td valign="top"><img src="/icons/back.gif" alt="[PARENTDIR]"></td>'
        f'<td><a href="/biostudies/fire/E-MTAB-/257/{STUDY}/Files/">Parent Directory</a></td>'
        '<td>&nbsp;</td><td align="right">  - </td><td>&nbsp;</td></tr>\n'
        '   <tr><th colspan="5"><hr></th></tr>\n'
        "</table>\n"
        "</body></html>\n"
    )
    IDF = "MAGE-TAB Version\t1.1\nComment[SecondaryAccession]\tERP133806\n"

    def _handler(self, body: str, seen: list[str] | None = None):
        def handler(request: httpx.Request) -> httpx.Response:
            if seen is not None:
                seen.append(str(request.url))
            assert (
                str(request.url)
                == f"https://www.ebi.ac.uk/biostudies/files/{self.STUDY}/{self.NAME}"
            )
            return httpx.Response(200, text=body)

        return handler

    @classmethod
    async def _call(cls, handler):
        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http,
            SurveyClient(Limits(rps=0), client=http) as client,
        ):
            return await idf_secondary_accessions(client, cls.STUDY, 10)

    def test_the_listing_is_the_size_that_was_served(self):
        assert len(self.LISTING.encode()) == 837

    async def test_a_listing_fails_the_read_and_is_not_retried(self):
        """It answered every one of 96 requests from 00:33 to 01:25 UTC; a retry waits 15 s."""
        seen: list[str] = []
        with pytest.raises(ServedDirectoryError, match="directory listing of 837 bytes") as caught:
            await self._call(self._handler(self.LISTING, seen))
        assert not is_retryable(caught.value)
        assert len(seen) == 1

    async def test_the_file_is_read_and_costs_no_extra_request(self):
        seen: list[str] = []
        assert await self._call(self._handler(self.IDF, seen)) == ["ERP133806"]
        assert len(seen) == 1

    def test_prose_about_an_index_is_not_a_listing(self):
        """An IDF may say "index"; only Apache's page title marks a listing."""
        idf = "Investigation Title\tIndex of / single-cell atlas <title>\n"
        assert not is_directory_index(idf.encode())
        assert is_directory_index(self.LISTING.encode())

    def test_the_sync_helper_checks_too(self, monkeypatch):
        monkeypatch.setattr(core, "transport", httpx.MockTransport(self._handler(self.LISTING)))
        with pytest.raises(ServedDirectoryError, match="directory listing"):
            core.ae2secondary(self.STUDY)
