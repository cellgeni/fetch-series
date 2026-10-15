"""The metadata layer's entry point: harvest, propose, ground, record.

Four tiers, run in order, each recording what it did on every claim it touches:

============  ===========================================  ==================
tier          module                                       ``method``
============  ===========================================  ==================
0 harvest     :mod:`fetch_series.metadata.harvest`          ``archive``
1 normalise   :mod:`fetch_series.metadata.rules`            ``rule``
2 ground      :mod:`fetch_series.metadata.ground`           ``ontology``
3 adjudicate  *(stage 2)*                                   ``llm``
============  ===========================================  ==================

Tiers 0-2 need no LLM key. That is not a fallback mode -- it is the layer, and
the model is an addition to it whose contribution has to be measured against
what is here before it earns a place.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from fetch_series.accession import Accession
from fetch_series.metadata import SampleMetadata
from fetch_series.metadata.ground import Grounder, GroundingStats
from fetch_series.metadata.harvest import Harvest, harvest
from fetch_series.metadata.rules import Proposal, propose, unmapped_keys
from fetch_series.survey.client import SurveyClient


@dataclass(slots=True)
class Collection:
    """Everything the layer concluded about one accession."""

    accession: str
    records: list[SampleMetadata] = field(default_factory=list)
    #: The deterministic proposals, kept beside the records because they carry
    #: what tier 1 *declined* to claim -- the vocabulary backlog, and in stage
    #: 2 the residual the model is asked about.
    proposals: list[Proposal] = field(default_factory=list)
    #: Study-scoped free text, held once. Never assigned to a sample; used to
    #: corroborate an abbreviation and, in stage 2, as digest evidence.
    study_text: dict[str, str] = field(default_factory=dict)
    stats: GroundingStats = field(default_factory=GroundingStats)

    @property
    def review_queue(self) -> list[tuple[str, str]]:
        """``(sample, reason)`` for every claim a human should look at.

        Ranked by how many samples share the reason, because a decision that
        settles 800 samples is worth a reviewer's minute and one that settles a
        single sample usually is not. That ordering is the whole point of
        having a queue rather than a flag.
        """
        pending = [
            (record.sample, claim.review)
            for record in self.records
            for claim in record.review_queue
            if claim.review
        ]
        shared: dict[str, int] = {}
        for _, reason in pending:
            shared[reason] = shared.get(reason, 0) + 1
        return sorted(pending, key=lambda pair: (-shared[pair[1]], pair[1], pair[0]))

    @property
    def conflicts(self) -> dict[str, dict[str, dict[str, tuple[str, ...]]]]:
        """Per sample, the fields whose sources disagree."""
        return {r.sample: r.conflicts for r in self.records if r.conflicts}

    def unmapped_keys(self) -> dict[str, int]:
        """Raw keys no rule mapped, by how many samples carry them."""
        return unmapped_keys(self.proposals)


async def collect(
    accession: Accession,
    client: SurveyClient,
    timeout: float = 120.0,
    *,
    limit: int | None = None,
    ground: bool = True,
) -> Collection:
    """Run tiers 0-2 over ``accession``.

    ``ground=False`` stops after tier 1. Not a debugging convenience: it is how
    the survey measures what grounding adds, and the two must be the same code
    path or the measurement is of something else.
    """
    harvests = await harvest(accession, client, timeout, limit=limit)
    proposals = propose(harvests)
    by_sample: dict[str, Harvest] = {h.sample: h for h in harvests}

    study_text: dict[str, str] = {}
    for one in harvests:
        study_text.update(one.study_text)

    grounder = Grounder(
        client=client, timeout=min(timeout, 60.0), study_text=" ".join(study_text.values())
    )
    collection = Collection(
        accession=accession.value,
        proposals=proposals,
        study_text=study_text,
        stats=grounder.stats,
    )
    for proposal in proposals:
        source = by_sample.get(proposal.sample)
        record = SampleMetadata(
            sample=proposal.sample,
            aliases=source.aliases if source else (),
            taxon=source.taxon if source else None,
            consulted=frozenset(source.consulted) if source else frozenset(),
        )
        claims = (
            await grounder.ground_all(proposal.claims, taxon=record.taxon)
            if ground
            else list(proposal.claims)
        )
        for claim in claims:
            record.add(claim)
        collection.records.append(record)
    return collection
