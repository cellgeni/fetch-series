"""Assembling and verifying a run's files.

Verification is a HEAD, not a download: the published md5 cannot be checked
without fetching the bytes, and at file-layer scale that is not a survey, it is
a mirror. What a HEAD does catch is the two failures that matter most -- a link
into a decommissioned mirror, and a file replaced without its metadata being
updated.
"""

from __future__ import annotations

import httpx
import pytest

from fetch_series.files import FileKind, FileRecord
from fetch_series.filesets import Verdict, files_for_run, verify, verify_all


def _record(url: str = "https://example/x.fastq.gz", size: int | None = 100) -> FileRecord:
    return FileRecord(
        run="SRR1", url=url, name="x.fastq.gz", kind=FileKind.FASTQ, source="t", size=size
    )


class _Client:
    """A client whose HEAD answers from a script, one entry per URL."""

    def __init__(self, responses):
        self.responses = responses
        self.seen: list[str] = []

    async def head(self, url, timeout=None):
        self.seen.append(url)
        outcome = self.responses[url]
        if isinstance(outcome, Exception):
            raise outcome
        status, headers = outcome
        return httpx.Response(status_code=status, headers=headers)


class TestVerdict:
    async def test_a_matching_size_is_a_clean_verdict(self):
        client = _Client({"https://example/x.fastq.gz": (200, {"content-length": "100"})})
        verdict = await verify(_record(), client)
        assert verdict.resolves and verdict.size_matches is True
        assert "size matches" in verdict.describe()

    async def test_a_size_mismatch_is_reported_with_both_numbers(self):
        """A file replaced without its metadata being updated. The link works,
        so nothing but the size says anything is wrong."""
        client = _Client({"https://example/x.fastq.gz": (200, {"content-length": "37"})})
        verdict = await verify(_record(), client)
        assert verdict.resolves and verdict.size_matches is False
        assert "100" in verdict.describe() and "37" in verdict.describe()

    async def test_a_404_is_a_verdict_not_an_exception(self):
        """A 404 on a published download link is the answer being asked for."""
        client = _Client({"https://example/x.fastq.gz": (404, {})})
        verdict = await verify(_record(), client)
        assert not verdict.resolves
        assert "404" in verdict.describe()

    async def test_a_transport_failure_keeps_its_class_and_does_not_propagate(self):
        client = _Client({"https://example/x.fastq.gz": httpx.ConnectError("dead mirror")})
        verdict = await verify(_record(), client)
        assert not verdict.resolves
        assert verdict.error == "ConnectError"

    async def test_an_uncheckable_size_is_not_a_failed_check(self):
        """No Content-Length, or no published size: the check could not run.
        Calling that a mismatch would condemn every chunked response."""
        no_header = _Client({"https://example/x.fastq.gz": (200, {})})
        assert (await verify(_record(), no_header)).size_matches is None

        no_published = _Client({"https://example/y": (200, {"content-length": "5"})})
        assert (
            await verify(_record("https://example/y", size=None), no_published)
        ).size_matches is None

    async def test_verify_all_covers_every_record(self):
        client = _Client(
            {
                "https://example/a": (200, {"content-length": "1"}),
                "https://example/b": (500, {}),
            }
        )
        verdicts = await verify_all(
            [_record("https://example/a", 1), _record("https://example/b", 1)], client
        )
        assert [v.resolves for v in verdicts] == [True, False]
        assert sorted(client.seen) == ["https://example/a", "https://example/b"]


class TestFilesForRun:
    """One route failing must not take the routes that answered down with it."""

    async def test_a_failing_route_does_not_lose_the_others(self, monkeypatch):
        """SDL failing must not lose what ENA answered, or the other way round."""
        import fetch_series.filesets as filesets

        async def good(run, client, timeout, column=None):
            return [_record()]

        async def sdl_fails(run, client, timeout):
            raise RuntimeError("SDL refused")

        monkeypatch.setattr(filesets, "ena_file_records", good)
        monkeypatch.setattr(filesets, "sdl_file_records", sdl_fails)

        fileset = await files_for_run("SRR1", client=None)
        assert len(fileset) == 1
        assert fileset.sources == ("t",)

    async def test_sdl_can_be_skipped(self, monkeypatch):
        import fetch_series.filesets as filesets

        called = []

        async def ena(run, client, timeout, column=None):
            return []

        async def sdl(run, client, timeout):
            called.append(run)
            return []

        monkeypatch.setattr(filesets, "ena_file_records", ena)
        monkeypatch.setattr(filesets, "sdl_file_records", sdl)
        await files_for_run("SRR1", client=None, include_sdl=False)
        assert called == []


def test_verdict_describe_handles_a_record_with_no_status():
    assert "boom" in Verdict(_record(), status=None, served_bytes=None, error="boom").describe()


@pytest.mark.integration
async def test_published_links_resolve_and_match_their_published_size():
    """The worked example, end to end against live archives."""
    from fetch_series.survey.client import Limits, SurveyClient

    async with SurveyClient(Limits(rps=3, concurrency=3)) as client:
        fileset = await files_for_run("SRR25056225", client)
        verdicts = await verify_all(fileset.records, client)
    assert verdicts
    assert all(v.resolves for v in verdicts)
    assert all(v.size_matches is not False for v in verdicts)


class TestEnaColumnsCostOneRequest:
    """All three ENA file families come from one filereport row."""

    async def test_one_request_returns_every_family(self, monkeypatch):
        from fetch_series.routes import ena_file_records

        calls = []

        async def report(client, accession, timeout, fields=None, result="read_run"):
            calls.append(accession)
            return [
                {
                    "run_accession": "SRR1",
                    "library_layout": "PAIRED",
                    "fastq_ftp": "ftp/a/SRR1_1.fastq.gz;ftp/a/SRR1_2.fastq.gz",
                    "submitted_ftp": "ftp/a/mine.bam",
                    "sra_ftp": "ftp/a/SRR1.sra",
                }
            ]

        import fetch_series.routes as routes

        monkeypatch.setattr(routes.ena_portal, "read_run_report", report)
        records = await ena_file_records("SRR1", client=None, timeout=10.0)

        assert len(calls) == 1, "asking for three families must not cost three requests"
        assert {r.source for r in records} == {
            "run->file:ena_fastq",
            "run->file:ena_submitted",
            "run->file:ena_sra",
        }

    async def test_a_named_column_still_returns_only_that_family(self, monkeypatch):
        """The survey runs one declared route at a time and must not be handed
        another route's results under its own id."""
        import fetch_series.routes as routes
        from fetch_series.routes import ena_file_records

        async def report(client, accession, timeout, fields=None, result="read_run"):
            return [
                {
                    "run_accession": "SRR1",
                    "fastq_ftp": "ftp/a/SRR1_1.fastq.gz",
                    "submitted_ftp": "ftp/a/mine.bam",
                }
            ]

        monkeypatch.setattr(routes.ena_portal, "read_run_report", report)
        records = await ena_file_records("SRR1", client=None, timeout=10.0, column="submitted")
        assert {r.source for r in records} == {"run->file:ena_submitted"}
