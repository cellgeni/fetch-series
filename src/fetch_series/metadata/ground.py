"""Tier 2: attach an ontology term to a value, or decline to.

Three steps, in this order, and the order is the finding.

1. **OLS exact match on the whole value.** Label or synonym, re-checked
   locally. ``PBMC`` lands on ``CL:2000001`` here with no synonym table of
   ours.
2. **OLS exact match on the value's parts**, longest first.
3. **ZOOMA**, constrained to the field's ontologies, with the submitter's own
   key as the property type.

## Why ZOOMA is last and not first

``Brain, prefrontal cortex`` -- a real BioSample ``tissue`` value, SAMN12011225
-- has no exact OLS match, so an implementation that reached for ZOOMA at the
first miss would ask it. Constrained to UBERON, ZOOMA returns
``UBERON:0001870`` *frontal cortex* at confidence ``GOOD``, with the correct
``UBERON:0000451`` *prefrontal cortex* ranked second. Splitting the value first
and matching ``prefrontal cortex`` exactly gets the right term.

So the cheap deterministic step is tried on parts before the approximate step is
tried at all, and every term records which step produced it, so the gold corpus
can report accuracy per step instead of for the layer as a whole.

## A ZOOMA match is never asserted, whatever its confidence

ZOOMA is the approximate step, and approximate here does not mean "slightly
off". Asked about the real ENA value ``CD45+ cells from a fresh kidney tumor
sample`` (GSE111360), constrained to CL, it returns **``CL:0001074``
CD34-positive, CD56-positive, CD117-positive common innate lymphoid
precursor**. CD45 is a pan-leukocyte marker; that term is a rare precursor
population. It is not a near miss.

So every ZOOMA grounding carries a review reason. Tier 2 *asserts* only what an
exact label or synonym match supports; ZOOMA's contribution is a candidate with
a reviewer attached. The term is still recorded, because a wrong candidate a
human can see is worth more than a blank, and because the review queue is the
output this layer exists to produce.

The label is also re-resolved through OLS rather than taken from ZOOMA's own
response. ZOOMA echoes back the *queried* string as the annotated property, so
recording that as the term's label printed ``CL:0001074 (CD45+ cells from a
fresh kidney tumor sample)`` -- a CURIE labelled with the prose it does not
mean, which hid exactly the error above.

## An abbreviation matched by synonym is never asserted

``disease = ASD`` is a real BioSample value on SAMN12011225, in a study whose
title reads *"post-mortem brain tissue from Autism Spectrum Disorder
patients"*. OLS exact-matches ``ASD`` to **``MONDO:0006664`` atrial septal
defect**, because that is the only MONDO term carrying ``ASD`` as a synonym --
autism spectrum disorder does not carry it. There is exactly one hit, so
nothing about the lookup looks ambiguous, and the answer is a different organ
system.

``PBMC`` -> ``CL:2000001`` is the same shape -- a short all-caps token matched
on a synonym -- and is correct. **Nothing internal to the lookup distinguishes
them.** So neither is asserted on its own: an abbreviation grounded by synonym
carries a review reason unless the study's own text contains the term's label,
which for ``ASD`` in that study it does not and for a genuine autism annotation
it would.

The term is still attached, because the value is usually right and dropping it
would lose more than it protects. It is simply never treated as settled.

## Declining is a result

A value that no step grounds keeps its normalised string and gets no term. It
is not passed to a fuzzy matcher and it is not dropped: it goes to
``SampleMetadata.ungrounded``, which ordered by frequency is the list of
synonyms worth declaring next. A wrong CURIE is worse than no CURIE, because
the CURIE is what a search index trusts.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field, replace

from fetch_series.metadata import Claim, Method, Term
from fetch_series.metadata.fields import by_name
from fetch_series.metadata.vocab import split_values
from fetch_series.providers import ols, zooma
from fetch_series.survey.client import SurveyClient, is_retryable

logger = logging.getLogger(__name__)

#: Knowledge-base slugs for defects this *step* exhibits, as opposed to any one
#: source. Surfaced by ``fetch metadata --sources`` and cited in the review
#: reason on every approximate grounding, so the behaviour is warned about
#: wherever it can affect a value rather than only documented.
GROUNDING_PATHOLOGIES: tuple[str, ...] = ("zooma-filter-forces-a-term",)


@dataclass(slots=True)
class GroundingStats:
    """How each step contributed, for the survey rather than for the caller.

    Counted per step because "tier 2 grounds 71% of tissue values" is not a
    reviewable claim: the three steps have different failure modes, and only
    ZOOMA's are the confident-but-wrong kind.
    """

    #: Claims offered for grounding. Counts claims, where the step counters
    #: below count *lookups* -- the memo means one lookup serves many claims,
    #: and conflating the two made a summary read "5/20 grounded, 0
    #: ungrounded", which is three mutually contradictory numbers.
    attempted: int = 0
    grounded_claims: int = 0
    lookups: int = 0
    ols_whole: int = 0
    ols_parts: int = 0
    curated_curie: int = 0
    zooma: int = 0
    ungrounded: int = 0
    requests: int = 0
    failed: int = 0
    #: Groundings flagged for review because an abbreviation matched by
    #: synonym and nothing corroborated the expansion.
    unverified_abbreviations: int = 0
    #: Groundings flagged because only the approximate step could answer.
    approximate: int = 0

    @property
    def grounded_lookups(self) -> int:
        """Distinct values grounded, which is what the step counters add to."""
        return self.ols_whole + self.ols_parts + self.curated_curie + self.zooma

    def summary(self) -> str:
        if not self.attempted:
            return "nothing to ground"
        pct = 100.0 * self.grounded_claims / self.attempted
        return (
            f"claims: {self.grounded_claims}/{self.attempted} grounded ({pct:.1f}%), "
            f"{self.attempted - self.grounded_claims} not; "
            f"lookups: {self.grounded_lookups}/{self.lookups} answered "
            f"({self.curated_curie} curated, {self.ols_whole} OLS-whole, "
            f"{self.ols_parts} OLS-parts, {self.zooma} ZOOMA, {self.ungrounded} none); "
            f"{self.requests} requests, {self.failed} failed; flagged for review: "
            f"{self.unverified_abbreviations} abbreviations, {self.approximate} approximate"
        )


@dataclass(slots=True)
class Grounder:
    """Grounds values, memoising every lookup.

    The memo is not an optimisation detail. A study of 800 samples from one
    tissue asks the same question 800 times, and a corpus-scale run asks it
    hundreds of thousands of times; without it this layer would be the slowest
    thing in the project and the heaviest user of two free EBI services.
    """

    client: SurveyClient
    timeout: float = 30.0
    #: The study's own free text, used only to corroborate an abbreviation's
    #: expansion. Never to assign a value -- that is the study-scope rule.
    study_text: str = ""
    stats: GroundingStats = field(default_factory=GroundingStats)
    _memo: dict[tuple[str, str, tuple[str, ...]], tuple[Term, ...]] = field(default_factory=dict)

    async def ground(
        self, claim: Claim, *, taxon: str | None = None, property_type: str | None = None
    ) -> list[Claim]:
        """Ground one claim, returning one claim per term found.

        Returns a **list** because a value can name more than one thing:
        ``lung, lymph node`` is two tissues and collapsing it to one would
        discard half a mixed sample's description. Where nothing grounds, the
        original claim comes back unchanged -- never an empty list, which would
        silently delete the value.
        """
        spec = by_name(claim.field)
        if not spec.grounded or claim.grounded:
            return [claim]
        ontologies = spec.permitted_ontologies(taxon)
        if not ontologies:
            return [claim]

        self.stats.attempted += 1
        key = (claim.field, claim.value, ontologies)
        if key not in self._memo:
            self.stats.lookups += 1
            self._memo[key] = await self._lookup(claim, ontologies, property_type)
            if not self._memo[key]:
                self.stats.ungrounded += 1
        terms = self._memo[key]
        if not terms:
            return [claim]
        self.stats.grounded_claims += 1
        return [
            replace(
                claim,
                # The ontology label becomes the value: it is the spelling a
                # search index and every other dataset will use, and the
                # submitter's own string is preserved in `raw`.
                value=term.label,
                raw=claim.raw or claim.evidence,
                term=term,
                method=Method.ONTOLOGY,
                review=self._review_reason(claim.value, term),
            )
            for term in terms
        ]

    def _review_reason(self, value: str, term: Term) -> str | None:
        """Why this grounding needs a human, if it does."""
        if term.via.startswith("zooma:"):
            self.stats.approximate += 1
            return (
                f"{value!r} has no exact ontology match; {term.curie} ({term.label!r}) is a "
                f"ZOOMA curated annotation at confidence {term.via.split(':', 1)[1]}, which is "
                f"approximate -- asked about 'CD45+ cells from a fresh kidney tumor sample' it "
                f"returned a rare innate lymphoid precursor"
            )
        if term.via != "ols:synonym" or not _looks_like_abbreviation(value):
            return None
        if term.label and term.label.casefold() in self.study_text.casefold():
            # The study spells the expansion out, so the abbreviation means
            # what the term says it means.
            return None
        self.stats.unverified_abbreviations += 1
        return (
            f"{value!r} is an abbreviation matched to {term.curie} "
            f"({term.label!r}) by synonym, and the study text does not spell that expansion "
            f"out; an abbreviation can be a synonym of a term in the right ontology and the "
            f"wrong disease -- ASD matched atrial septal defect in an autism study"
        )

    async def _lookup(
        self, claim: Claim, ontologies: tuple[str, ...], property_type: str | None
    ) -> tuple[Term, ...]:
        # A term the archive itself supplied -- an ArrayExpress curator's
        # `Term Accession Number` -- still gets validated. An unresolvable
        # CURIE is not a grounding, however authoritative its source.
        try:
            whole = await self._ols(claim.value, ontologies)
            if whole:
                self.stats.ols_whole += 1
                return whole

            for part in split_values(claim.value):
                found = await self._ols(part, ontologies)
                if found:
                    self.stats.ols_parts += 1
                    return found

            hits = await zooma.annotate(
                self.client,
                claim.value,
                ontologies,
                self.timeout,
                property_type=property_type,
            )
            self.stats.requests += 1
            if hits:
                top = hits[0]
                # The label comes from OLS, not from ZOOMA. ZOOMA's
                # `annotatedProperty.propertyValue` is the string that was
                # *queried*, so using it labelled CL:0001074 with the prose it
                # does not mean.
                resolved = await ols.lookup_curie(self.client, top.curie, self.timeout)
                self.stats.requests += 1
                if resolved is None:
                    # A CURIE that does not resolve is not a grounding, however
                    # confident the annotation that produced it.
                    return ()
                self.stats.zooma += 1
                # The confidence band is recorded in `via` and acted on
                # nowhere. See providers/zooma.py: `GOOD` was the band on two
                # measured wrong answers and `HIGH` on a third, so the bands
                # are not ordered by correctness in anything measured yet.
                return (
                    Term(
                        curie=top.curie,
                        label=resolved.label,
                        ontology=top.prefix,
                        via=f"zooma:{top.confidence}",
                    ),
                )
        except Exception as exc:
            self.stats.failed += 1
            kind = "unavailable" if is_retryable(exc) else "failed"
            logger.warning(
                "grounding %s=%r %s: %s: %s",
                claim.field,
                claim.value,
                kind,
                type(exc).__name__,
                exc,
            )
        return ()

    async def _ols(self, value: str, ontologies: tuple[str, ...]) -> tuple[Term, ...]:
        found = await ols.exact_match(self.client, value, ontologies, self.timeout)
        self.stats.requests += 1
        return tuple(
            Term(curie=t.curie, label=t.label, ontology=t.ontology, via=f"ols:{t.on}")
            for t in found
        )

    async def validate_curie(self, curie: str) -> Term | None:
        """Check a CURIE this project did not derive, and return it as a term."""
        found = await ols.lookup_curie(self.client, curie, self.timeout)
        self.stats.requests += 1
        if found is None:
            return None
        self.stats.curated_curie += 1
        return Term(curie=found.curie, label=found.label, ontology=found.ontology, via="ols:curie")

    async def ground_all(self, claims: Sequence[Claim], *, taxon: str | None = None) -> list[Claim]:
        """Ground every claim, in order, keeping ungrounded ones in place."""
        out: list[Claim] = []
        for claim in claims:
            # The submitter's own key is the best available property type for
            # ZOOMA: it narrows to annotations curators made for the same kind
            # of field, which is the one lever here that reduces wrong answers
            # rather than relocating them.
            property_type = _property_type(claim)
            out.extend(await self.ground(claim, taxon=taxon, property_type=property_type))
        return out


def _looks_like_abbreviation(value: str) -> bool:
    """Whether a value is short enough and capitalised enough to be an acronym.

    Shape only, because meaning is exactly what is unavailable here. Bounded at
    six characters and two capitals: ``ASD``, ``PBMC``, ``COPD``, ``T2D`` are
    in; ``male``, ``liver``, ``CD4+ T cell`` are out -- the last by having
    whitespace, which no acronym does.
    """
    token = value.strip()
    if not token or len(token) > 6 or any(c.isspace() for c in token):
        return False
    letters = [c for c in token if c.isalpha()]
    capitals = [c for c in letters if c.isupper()]
    return len(capitals) >= 2 and len(capitals) >= 0.75 * len(letters)


def _property_type(claim: Claim) -> str | None:
    """The attribute key a value appeared under, from the claim's source.

    ``ae:characteristics[organism part]`` -> ``organism part``. Parsed back out
    of the source rather than carried separately, so a claim stays a flat
    record that can be serialised and compared without a side table.
    """
    source = claim.source
    if "[" in source and source.endswith("]"):
        return source[source.index("[") + 1 : -1]
    return source.split(":", 1)[-1] if ":" in source else None
