"""Tests for the route implementations.

The SOFT parser is tested offline against fixture text, because its failure
modes are parsing failures that need no network to reproduce.
"""

from __future__ import annotations

import pytest

from fetch_series.graph import REGISTRY
from fetch_series.providers.geo import parse_soft_family
from fetch_series.routes import IMPLEMENTATIONS

# Two samples, where the second declares no SRA relation at all. This is the
# GSE135325 / GSE137444 shape: GEO records the BioSample and nothing else.
SOFT_WITH_GAP = """\
^SERIES = GSE135325
!Series_title = An example
!Series_relation = BioProject: https://www.ncbi.nlm.nih.gov/bioproject/PRJNA560000
^SAMPLE = GSM4001
!Sample_title = first
!Sample_relation = BioSample: https://www.ncbi.nlm.nih.gov/biosample/SAMN12345678
!Sample_relation = SRA: https://www.ncbi.nlm.nih.gov/sra?term=SRX6647870
^SAMPLE = GSM4002
!Sample_title = second, BioSample only
!Sample_relation = BioSample: https://www.ncbi.nlm.nih.gov/biosample/SAMN12345679
^SAMPLE = GSM4003
!Sample_title = third
!Sample_relation = SRA: https://www.ncbi.nlm.nih.gov/sra?term=SRX6647872
"""

SUPERSERIES = """\
^SERIES = GSE100000
!Series_relation = SuperSeries of: GSE100001
!Series_relation = SuperSeries of: GSE100002
^SAMPLE = GSM5001
"""


class TestSoftParsing:
    def test_extracts_the_bioproject(self):
        family = parse_soft_family("GSE135325", SOFT_WITH_GAP)
        assert family.bioprojects == ["PRJNA560000"]

    def test_lists_every_sample_including_ones_with_no_relations(self):
        family = parse_soft_family("GSE135325", SOFT_WITH_GAP)
        assert family.samples == ["GSM4001", "GSM4002", "GSM4003"]

    def test_a_missing_sra_relation_does_not_leak_into_the_next_sample(self):
        """The bug this parser is shaped to avoid.

        Accumulating fields until all are set leaves the previous sample's
        values in place, so GSM4003's SRX would be attributed to GSM4002 -- or
        GSM4002 dropped entirely. Both are silent and both corrupt the mapping.
        """
        relations = parse_soft_family("GSE135325", SOFT_WITH_GAP).sample_relations
        assert relations["GSM4001"] == {
            "experiment": "SRX6647870",
            "biosample": "SAMN12345678",
        }
        # Present in the mapping, with only the relation GEO actually recorded.
        assert relations["GSM4002"] == {"biosample": "SAMN12345679"}
        assert relations["GSM4003"] == {"experiment": "SRX6647872"}

    def test_superseries_subseries_are_collected(self):
        """A SuperSeries carries no BioProject of its own, only its members do."""
        family = parse_soft_family("GSE100000", SUPERSERIES)
        assert family.bioprojects == []
        assert family.subseries == ["GSE100001", "GSE100002"]

    def test_empty_file_yields_an_empty_family(self):
        family = parse_soft_family("GSE1", "")
        assert family.samples == []
        assert family.bioprojects == []


class TestRegistryConsistency:
    def test_every_implementation_has_a_declaration(self):
        """An implemented route with no declaration has no evidence, no cost and
        no knowledge-base page, so nothing can rank or justify it."""
        declared = {route.id for route in REGISTRY}
        assert set(IMPLEMENTATIONS) <= declared, sorted(set(IMPLEMENTATIONS) - declared)

    def test_declared_route_ids_describe_their_edge(self):
        """The id encodes source->target:provider so a log line is readable."""
        for route in REGISTRY:
            assert "->" in route.id and ":" in route.id, route.id
            edge, _, _provider = route.id.partition(":")
            source, _, target = edge.partition("->")
            assert source and target

    @pytest.mark.parametrize("route_id", sorted(IMPLEMENTATIONS))
    def test_implementations_are_async(self, route_id: str):
        import inspect

        assert inspect.iscoroutinefunction(IMPLEMENTATIONS[route_id])
