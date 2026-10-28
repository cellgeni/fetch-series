"""An IDF's ``Comment[SequenceDataURI]``: ranges of runs, read as the runs they name.

Reading the accessions in the text found a range's two ends: E-GEOD-29264's
SRR208051-SRR208059 is nine runs, and ``ae_experiment->run:biostudies_idf``
returned two. Over sample:3000@arrayexpress-ena that route found 16,873 runs
where the SDRF route found 112,561 over the same studies. Each value here is
copied from the IDF named, read on 2026-10-28.
"""

from __future__ import annotations

import httpx
import pytest

from fetch_series.accession import EntityType
from fetch_series.providers.biostudies import SEQUENCE_DATA_MAX_RANGE, sequence_data
from fetch_series.results import Outcome
from fetch_series.routes.catalogue import RouteFailed, answer, answer_route
from fetch_series.survey.client import Limits, SurveyClient

VIEW = "http://www.ebi.ac.uk/ena/data/view/"


class TestSequenceData:
    def test_a_range_is_every_run_in_it(self):
        """E-GEOD-29264: nine runs, of which the old reading found the two ends."""
        found = sequence_data([f"{VIEW}SRR208051-SRR208059"])
        assert found.accessions == [f"SRR20805{n}" for n in range(1, 10)]
        assert found.unread == []

    def test_one_run_is_a_range_of_one(self):
        """E-GEOD-24226 writes its single run as SRR066638-SRR066638."""
        assert sequence_data([f"{VIEW}SRR066638-SRR066638"]).accessions == ["SRR066638"]

    def test_a_zero_padded_number_keeps_its_width(self):
        assert sequence_data([f"{VIEW}SRR066638-SRR066640"]).accessions == [
            "SRR066638",
            "SRR066639",
            "SRR066640",
        ]

    def test_a_bare_accession_is_one_run(self):
        """E-GEOD-60493 writes SRR1553289 with no range at all."""
        assert sequence_data([f"{VIEW}SRR1553289"]).accessions == ["SRR1553289"]

    def test_a_comma_list_of_ranges(self):
        """E-GEOD-25133: 33 runs in one range and one in another."""
        found = sequence_data([f"{VIEW}SRR121550-SRR121582,SRR125319-SRR125319"])
        assert len(found.accessions) == 34
        assert found.accessions[-1] == "SRR125319"

    def test_every_cell_is_read_and_an_overlap_counted_once(self):
        """E-MTAB-6064 writes ten cells of overlapping pairs over nine runs."""
        cells = [
            f"{VIEW}ERR2124541-ERR2124541",
            *(f"{VIEW}ERR212454{n}-ERR212454{n + 1}" for n in range(1, 9)),
            f"{VIEW}ERR2124549-ERR2124549",
        ]
        assert sequence_data(cells).accessions == [f"ERR212454{n}" for n in range(1, 10)]

    def test_a_range_across_a_digit_boundary_ascends(self):
        """Not seen in the 3,000; the case the width rule must not refuse."""
        assert sequence_data([f"{VIEW}SRR999999-SRR1000001"]).accessions == [
            "SRR999999",
            "SRR1000000",
            "SRR1000001",
        ]

    def test_a_range_in_string_order_is_reported_not_read(self):
        """E-GEOD-47691: its first and last runs in string order, not a range."""
        found = sequence_data([f"{VIEW}SRR1204762-SRR886300", f"{VIEW}SRR946214-SRR946218"])
        assert found.unread == ["SRR1204762-SRR886300: it descends"]
        # The provider still reads the part it can; the caller decides.
        assert found.accessions == [f"SRR94621{n}" for n in range(4, 9)]

    def test_a_range_past_the_bound_is_reported_not_cut_short(self):
        """E-MTAB-4556: 118,813 accessions between the only two runs its SDRF lists."""
        found = sequence_data([f"{VIEW}ERR1432629-ERR1432629", f"{VIEW}ERR1313817-ERR1432629"])
        assert found.unread == ["ERR1313817-ERR1432629: it spans 118,813 accessions"]
        assert found.accessions == ["ERR1432629"]

    def test_the_bound_itself_is_expanded(self):
        last = 1_000_000 + SEQUENCE_DATA_MAX_RANGE - 1
        assert len(sequence_data([f"ERR1000000-ERR{last}"]).accessions) == SEQUENCE_DATA_MAX_RANGE
        assert sequence_data([f"ERR1000000-ERR{last + 1}"]).unread

    def test_ends_with_different_prefixes_are_reported(self):
        found = sequence_data([f"{VIEW}ERR208051-SRR208059"])
        assert found.unread == ["ERR208051-SRR208059: its ends have different prefixes"]

    def test_what_is_not_an_accession_is_reported(self):
        assert sequence_data([f"{VIEW}not-a-run"]).unread == [
            "not-a-run: not a range of one kind of accession"
        ]
        assert sequence_data([f"{VIEW}SRR208051x"]).unread == ["SRR208051x: not an accession"]

    def test_a_study_or_experiments_are_read_as_what_they_are(self):
        """E-ERAD-324 names a study; E-TABM-722 a list of experiments."""
        assert sequence_data([f"{VIEW}ERP007026"]).accessions == ["ERP007026"]
        found = sequence_data([f"{VIEW}ERX000498-ERX000505,ERX000523"])
        assert found.accessions[0] == "ERX000498" and len(found.accessions) == 9

    def test_a_link_naming_nothing_is_nothing(self):
        """E-GEOD-58432's field as BioStudies served it, cut off at 4,096 bytes."""
        found = sequence_data(["http://www.ebi.ac.uk/"])
        assert found.accessions == [] and found.unread == []


def _idf(*lines: str) -> str:
    return "".join(f"{line}\n" for line in lines)


E_GEOD_29264 = _idf(
    "Comment[SecondaryAccession]\tGSE29264\t\t\t",
    "Comment[SecondaryAccession]\tSRP006795\t\t\t",
    f"Comment[SequenceDataURI]\t{VIEW}SRR208051-SRR208059\t\t\t",
)
E_GEOD_47691 = _idf(
    "Comment[SecondaryAccession]\tGSE47691",
    "Comment[SecondaryAccession]\tSRP024266",
    f"Comment[SequenceDataURI]\t{VIEW}SRR1204762-SRR886300",
    f"Comment[SequenceDataURI]\t{VIEW}SRR946214-SRR946218",
)


async def _answer(text: str, accession: str, targets: list[EntityType]):
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url).endswith(f"/{accession}/{accession}.idf.txt")
        return httpx.Response(200, text=text)

    async with (
        httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http,
        SurveyClient(Limits(rps=0), client=http) as client,
    ):
        return await answer("biostudies_idf", accession, targets, {"biostudies": client})


class TestTheRoute:
    async def test_e_geod_29264_answers_nine_runs(self):
        verdicts = await _answer(E_GEOD_29264, "E-GEOD-29264", [EntityType.RUN])
        assert verdicts[EntityType.RUN].outcome is Outcome.RESOLVED
        assert verdicts[EntityType.RUN].values == tuple(f"SRR20805{n}" for n in range(1, 10))

    async def test_an_unread_range_fails_the_run_link_and_nothing_else(self):
        """E-GEOD-47691's readable range alone is 5 of the 22 runs its SDRF lists."""
        verdicts = await _answer(
            E_GEOD_47691, "E-GEOD-47691", [EntityType.RUN, EntityType.GEO_SERIES]
        )
        run = verdicts[EntityType.RUN]
        assert run.outcome is Outcome.FAILED and run.values == ()
        assert run.error == "UnreadableValue: SRR1204762-SRR886300: it descends"
        assert verdicts[EntityType.GEO_SERIES].values == ("GSE47691",)

    async def test_the_resolver_sees_a_failure_not_a_short_answer(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, text=E_GEOD_47691)

        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http,
            SurveyClient(Limits(rps=0), client=http) as client,
        ):
            with pytest.raises(RouteFailed, match="it descends"):
                await answer_route(
                    "biostudies_idf",
                    EntityType.AE_EXPERIMENT,
                    EntityType.RUN,
                    "E-GEOD-47691",
                    client,
                    10,
                )

    async def test_a_study_in_the_field_is_no_run(self):
        """E-ERAD-324 names its study, ERP007026, where other IDFs name runs."""
        text = _idf(f"Comment[SequenceDataURI]\t{VIEW}ERP007026")
        verdicts = await _answer(text, "E-ERAD-324", [EntityType.RUN])
        assert verdicts[EntityType.RUN].outcome is Outcome.EMPTY
