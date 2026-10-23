"""ELink's backend failures arrive inside an HTTP 200 and must not read as "no links".

Knowledge base: docs/pathologies/megalink-backend-flakiness.md
"""

from __future__ import annotations

import pytest

from fetch_series.providers.eutils import elink_uids
from fetch_series.survey.client import MalformedResponseError, is_retryable


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _Client:
    api_key = None

    def __init__(self, payload):
        self.payload = payload

    async def get(self, url, params=None, timeout=None):
        return _Response(self.payload)


async def test_links_are_returned():
    payload = {
        "linksets": [
            {"dbfrom": "bioproject", "linksetdbs": [{"dbto": "biosample", "links": ["2", "1"]}]}
        ]
    }
    assert await elink_uids(_Client(payload), "bioproject", "biosample", ["9"], 10.0) == ["1", "2"]


async def test_a_project_with_no_links_is_empty():
    payload = {"linksets": [{"dbfrom": "bioproject", "ids": ["9"]}]}
    assert await elink_uids(_Client(payload), "bioproject", "biosample", ["9"], 10.0) == []


@pytest.mark.parametrize(
    "message",
    [
        "callMLink: Query failed on MegaLink server",
        "Couldn't resolve #pmquerysrv-mz?dbaf=biosample, the address table is empty.",
    ],
)
async def test_a_backend_message_is_a_retryable_failure_not_an_empty_answer(message):
    """The 2026-05-11 surveys logged 5,107 of these, in bursts, all inside HTTP 200s."""
    payload = {"linksets": [{"dbfrom": "bioproject", "ids": ["9"], "ERROR": message}]}
    with pytest.raises(MalformedResponseError, match="backend error") as raised:
        await elink_uids(_Client(payload), "bioproject", "biosample", ["9"], 10.0)
    assert is_retryable(raised.value)
