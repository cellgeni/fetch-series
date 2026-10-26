"""EFetch serves at most 10,000 records a request: every caller pages.

Knowledge base: docs/pathologies/efetch-returns-fewer-runs-than-esearch-counts.md
"""

from __future__ import annotations

import pytest

from fetch_series.providers.eutils import EFETCH_PAGE, efetch_pages
from fetch_series.routes import _runs_from_runinfo

HEADER = "Run,Experiment\n"


class _Response:
    def __init__(self, text):
        self.text = text


class _Client:
    """Answers a history page with its retstart, so the pages can be told apart."""

    api_key = None

    def __init__(self):
        self.asked: list[dict] = []

    async def get(self, url, params=None, timeout=None):
        self.asked.append(params or {})
        start = (params or {}).get("retstart", 0)
        return _Response(f"{HEADER}SRR{start},SRX{start}\n")

    async def post(self, url, data=None, timeout=None):
        self.asked.append(data or {})
        return _Response(HEADER)


async def test_a_history_is_paged_to_its_count():
    """PRJEB14362: 46,085 experiments, 9,939 runs from one unpaged EFetch."""
    client = _Client()
    pages = await efetch_pages(client, "sra", 10.0, webenv="W", query_key="1", count=46_085)
    starts = [p["retstart"] for p in client.asked]
    assert starts == list(range(0, 46_085, EFETCH_PAGE))
    assert all(p["retmax"] == EFETCH_PAGE <= 10_000 for p in client.asked)
    assert len(pages) == len(starts)


async def test_a_uid_list_is_sliced():
    client = _Client()
    await efetch_pages(client, "sra", 10.0, uids=[str(i) for i in range(12_001)])
    sizes = [len(p["id"].split(",")) for p in client.asked]
    assert sizes == [EFETCH_PAGE, EFETCH_PAGE, 2_001]


async def test_a_history_needs_its_count():
    with pytest.raises(ValueError, match="count"):
        await efetch_pages(_Client(), "sra", 10.0, webenv="W", query_key="1")


def test_runs_are_gathered_from_every_page_once():
    pages = [f"{HEADER}SRR1,SRX1\nSRR2,SRX2\n", f"{HEADER}SRR2,SRX2\nSRR3,SRX3\n"]
    assert _runs_from_runinfo(pages) == ["SRR1", "SRR2", "SRR3"]


def test_a_capped_sra_be_history_is_recognised():
    """PRJEB82511: 10,000 rows of 5,000 distinct runs for a 7,263-run project."""
    from fetch_series.providers.sra_be import history_was_capped

    capped = [{"Run": f"ERR{i % 5000}"} for i in range(10_000)]
    assert history_was_capped(capped)
    assert history_was_capped([{"Run": "ERR1"}, {"Run": "ERR1"}])
    assert not history_was_capped([{"Run": f"ERR{i}"} for i in range(3_819)])


async def test_a_capped_history_falls_back_to_the_project(monkeypatch):
    import fetch_series.routes as routes

    async def history(accession, client, timeout, *, via_elink):
        return "W", "1", 7_263

    async def capped(client, webenv, query_key, timeout):
        return [{"Run": f"ERR{i % 5000}"} for i in range(10_000)]

    async def whole(client, accession, timeout):
        return [{"Run": f"ERR{i}"} for i in range(7_263)]

    monkeypatch.setattr(routes, "_bioproject_sra_history", history)
    monkeypatch.setattr(routes.sra_be, "runinfo_by_history", capped)
    monkeypatch.setattr(routes.sra_be, "runinfo_by_accession", whole)
    route = routes.IMPLEMENTATIONS["bioproject->run:sra_be_direct_cgi"]
    assert len(await route("PRJEB82511", None, 10.0)) == 7_263


async def test_a_short_fallback_is_a_failure_not_the_project(monkeypatch):
    """PRJEB82511's by-accession answer was once an empty 200: never record that as no runs."""
    import fetch_series.routes as routes
    from fetch_series.survey.client import MalformedResponseError, is_retryable

    async def history(accession, client, timeout, *, via_elink):
        return "W", "1", 7_263

    async def capped(client, webenv, query_key, timeout):
        return [{"Run": f"ERR{i % 5000}"} for i in range(10_000)]

    async def empty(client, accession, timeout):
        return []

    monkeypatch.setattr(routes, "_bioproject_sra_history", history)
    monkeypatch.setattr(routes.sra_be, "runinfo_by_history", capped)
    monkeypatch.setattr(routes.sra_be, "runinfo_by_accession", empty)
    route = routes.IMPLEMENTATIONS["bioproject->run:sra_be_direct_cgi"]
    with pytest.raises(MalformedResponseError) as raised:
        await route("PRJEB82511", None, 10.0)
    assert is_retryable(raised.value)


def test_a_runinfo_cut_off_mid_row_is_a_failure():
    """PRJEB14362's by-accession answer ended in a 6-field row, with HTTP 200."""
    from fetch_series.providers.sra_be import parse_runinfo
    from fetch_series.survey.client import MalformedResponseError

    whole = "Run,ReleaseDate,Experiment\nERR1,2018-01-01,ERX1\nERR2,,ERX2\n"
    assert [r["Run"] for r in parse_runinfo(whole)] == ["ERR1", "ERR2"]
    with pytest.raises(MalformedResponseError):
        parse_runinfo(whole + "ERR3,2018")


async def test_an_empty_answer_to_a_counted_history_is_a_failure(monkeypatch):
    """PRJNA772116 was recorded with no runs from an empty 200 under load."""
    import fetch_series.routes as routes
    from fetch_series.survey.client import MalformedResponseError

    async def history(accession, client, timeout, *, via_elink):
        return "W", "1", 95

    async def empty(client, webenv, query_key, timeout):
        return []

    monkeypatch.setattr(routes, "_bioproject_sra_history", history)
    monkeypatch.setattr(routes.sra_be, "runinfo_by_history", empty)
    route = routes.IMPLEMENTATIONS["bioproject->run:sra_be_elink"]
    with pytest.raises(MalformedResponseError):
        await route("PRJNA772116", None, 10.0)
