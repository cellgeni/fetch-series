"""What this project concluded about one sample's metadata, and on what grounds.

The unit is the **sample**, not the run. A run's tissue is its sample's tissue,
and putting descriptive fields on ``RunRecord`` would duplicate every value once
per run -- which is how the 2026-05 predecessor counted 824,216 "runs" of which
69,923 were duplicates. Runs are joined to this record through the existing
relation table, not by copying values into it.

## Why the merge rule here is uniqueness, not first-wins

``RunRecord.set()`` is first-wins, and that is justified: routes are offered in
the order ``RouteRegistry.ranked()`` computes from measured evidence, so the
first answer is the best-evidenced one.

**No such measurement exists for metadata sources yet.** Nobody has surveyed
whether BioSample's ``tissue`` attribute agrees with GEO's
``!Sample_characteristics_ch1`` more often than the reverse, so there is no
order, so first-wins would be picking by declaration order and calling it
evidence -- the precise failure the governing rule exists to prevent.

Until :mod:`fetch_series.metadata.evidence` carries a survey, a single-valued
field is therefore asserted only when every source that answered names the same
value, and a disagreement is reported rather than resolved. That is the same
rule :attr:`~fetch_series.assays.AssayCall.library_type` follows, for the same
reason. When the evidence lands, ``resolve()`` takes the measured order and
first-wins becomes justified; the claims are all kept either way, so promoting
the rule needs no re-harvest.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum

from fetch_series.entities import MISSING
from fetch_series.metadata.fields import FIELD_NAMES, by_name


class Method(StrEnum):
    """Which tier produced a claim.

    Recorded on every claim so the tiers stay separable. The whole point of
    building the deterministic tiers first is to be able to say what the model
    adds, and that is unanswerable if a stored value cannot say where it came
    from.
    """

    #: The archive served it as a structured field or attribute pair.
    ARCHIVE = "archive"
    #: A deterministic rule derived it from the harvest.
    RULE = "rule"
    #: An ontology service grounded a normalised value to a term.
    ONTOLOGY = "ontology"
    #: A model adjudicated it against the digest.
    LLM = "llm"
    #: A human decided it. Outlives a re-parse.
    MANUAL = "manual"


@dataclass(frozen=True, slots=True)
class Term:
    """An ontology term a value was grounded to."""

    curie: str
    label: str
    ontology: str
    #: Which service answered, and how: ``ols:exact``, ``zooma:HIGH``. Kept
    #: because the two are not interchangeable -- an exact OLS label match and a
    #: ZOOMA curated annotation carry different weight, and until the gold
    #: corpus says how much, a caller has to be able to tell them apart.
    via: str

    def __str__(self) -> str:
        return f"{self.curie} ({self.label})"


@dataclass(frozen=True, slots=True)
class Claim:
    """One field, one value, and everything needed to argue with it."""

    field: str
    #: The normalised value. For a grounded field this is the ontology label
    #: where grounding succeeded, and the normalised submitter string where it
    #: did not -- so a value is always present even when a term is not.
    value: str
    method: Method
    #: The harvest field it came from: ``geo:characteristics[tissue]``,
    #: ``biosample:attribute[tissue]``, ``ena:tissue_type``,
    #: ``ae:Characteristics[organism part]``.
    source: str
    #: Verbatim text from the harvest supporting the claim. For tiers 0-1 this
    #: is the submitter's own string; for tier 3 it is checked by substring
    #: match against the digest, and an unverifiable claim is dropped.
    evidence: str
    #: The submitter's spelling before normalisation, where it differed.
    raw: str | None = None
    term: Term | None = None
    #: Why a human should look at this claim, where one should. Not a
    #: confidence score: a stated reason a reviewer can act on, which a number
    #: between 0 and 1 is not. A claim carrying this is still returned and
    #: still searchable -- withholding it would lose information that is
    #: usually right -- but it is never treated as settled.
    review: str | None = None

    @property
    def needs_review(self) -> bool:
        return self.review is not None

    @property
    def grounded(self) -> bool:
        return self.term is not None

    def explain(self) -> str:
        ground = f" -> {self.term}" if self.term else " [ungrounded]"
        raw = f" (raw: {self.raw!r})" if self.raw and self.raw != self.value else ""
        flag = f"\n      ?? REVIEW: {self.review}" if self.review else ""
        return (
            f"{self.field}={self.value}{ground} [{self.method}] "
            f"{self.source}: {self.evidence!r}{raw}{flag}"
        )


@dataclass(slots=True)
class SampleMetadata:
    """Every claim made about one sample, keyed by field.

    Nothing is discarded. A field with two disagreeing sources keeps both, and
    :meth:`asserted` declines to pick between them -- see the module docstring.
    """

    sample: str
    #: The other identities this sample is known by, so a claim harvested
    #: against a GSM and one harvested against a BioSample land on one record.
    aliases: tuple[str, ...] = ()
    #: NCBI taxon id, where known. Not a claim: it selects which ontology
    #: ``development_stage`` grounds against, so it has to be resolved before
    #: grounding rather than emerging from it.
    taxon: str | None = None
    #: Declared source ids that were actually consulted for this sample.
    #: A source absent from here said nothing because it was never asked, which
    #: is not the same claim as its having nothing to say.
    consulted: frozenset[str] = frozenset()
    #: field -> every claim offered for it, in the order offered.
    claims: dict[str, list[Claim]] = field(default_factory=dict)

    def add(self, claim: Claim) -> None:
        """Record a claim, unless the same source already said the same thing.

        The same value from a *different* source is kept, because two
        independent sources agreeing is itself a finding -- it is what made the
        GEO/ENA alias agreement (8,604 of 8,604) measurable at all.

        The same value from the *same* source is not. ENA's ``filereport`` is
        per **run**, and a sample with 880 runs offers its tissue 880 times:
        GSE109816 reported 2,688 grounded claims for a single sample before
        this check, which inflated every count derived from them and made the
        review queue unreadable. Repetition by an archive that answers at a
        finer granularity than the sample is not corroboration.
        """
        if claim.field not in FIELD_NAMES:
            raise KeyError(f"claim for undeclared field {claim.field!r}")
        if not claim.value or claim.value == MISSING:
            return
        existing = self.claims.setdefault(claim.field, [])
        if any(c.value == claim.value and c.source == claim.source for c in existing):
            return
        existing.append(claim)

    def values(self, name: str) -> tuple[str, ...]:
        """The distinct values offered for ``name``, in first-offered order."""
        seen: dict[str, None] = {}
        for claim in self.claims.get(name, ()):
            seen.setdefault(claim.value, None)
        return tuple(seen)

    def sources(self, name: str) -> tuple[str, ...]:
        """Every source that answered for ``name``, including agreeing ones."""
        return tuple(dict.fromkeys(claim.source for claim in self.claims.get(name, ())))

    def by_source(self, name: str) -> dict[str, frozenset[str]]:
        """What each source said about ``name``, as a set of values.

        Conflict is a property of two *sources* disagreeing, not of how many
        values a field allows. A submitter writing ``lung, lymph node`` in one
        characteristics line is describing a mixture; ENA saying ``lung`` where
        GEO says ``brain`` for the same sample is an archive disagreement. Both
        produce two values for the field, and collapsing the distinction would
        report the second as a mixture and hide it.
        """
        collected: dict[str, set[str]] = {}
        for claim in self.claims.get(name, ()):
            collected.setdefault(claim.source, set()).add(claim.value)
        return {source: frozenset(values) for source, values in collected.items()}

    def asserted(self, name: str) -> Claim | None:
        """The claim for ``name``, only where nothing disagrees with it.

        ``None`` for a multivalued field even when the sources agree: a single
        claim cannot stand for ``lung, lymph node``. Callers read
        :meth:`values` there and :meth:`ambiguous` for whether to trust it.
        """
        claims = self.claims.get(name, ())
        if not claims or self.ambiguous(name) or by_name(name).multivalued:
            return None
        if len(self.values(name)) != 1:
            # One source naming two values for a single-valued field. Not a
            # cross-archive conflict, but still not an assertion.
            return None
        # Prefer a grounded claim among equals: same value, more information.
        return next((c for c in claims if c.grounded), claims[0])

    def ambiguous(self, name: str) -> bool:
        """Whether two sources gave different answers for ``name``."""
        return len(set(self.by_source(name).values())) > 1

    @property
    def conflicts(self) -> dict[str, dict[str, tuple[str, ...]]]:
        """Fields where sources disagree, and what each source said.

        Naming the sources rather than only the values, because "ENA says lung,
        GEO says brain" is actionable and "lung or brain" is not -- and because
        which source is right is the question the gold corpus exists to answer.
        """
        return {
            name: {source: tuple(sorted(values)) for source, values in self.by_source(name).items()}
            for name in self.claims
            if self.ambiguous(name)
        }

    @property
    def ungrounded(self) -> dict[str, tuple[str, ...]]:
        """Values for grounded fields that no ontology term was found for.

        The vocabulary backlog. A value here is not a failure of the sample --
        it is a gap in the mapping, and counting them by frequency is how the
        next synonym worth declaring gets chosen.
        """
        out: dict[str, tuple[str, ...]] = {}
        for name, claims in self.claims.items():
            if not by_name(name).grounded:
                continue
            missing = tuple(dict.fromkeys(c.value for c in claims if not c.grounded))
            if missing:
                out[name] = missing
        return out

    @property
    def review_queue(self) -> tuple[Claim, ...]:
        """Claims a human should look at, in field order.

        Deliberately a list of claims and not a count. "12 fields need review"
        is not actionable; "disease=atrial septal defect, because ASD matched a
        synonym and nothing corroborates the expansion" is.
        """
        return tuple(
            claim
            for name in FIELD_NAMES
            for claim in self.claims.get(name, ())
            if claim.needs_review
        )

    def explain(self) -> list[str]:
        """Every claim, one line each, most-answered field first."""
        lines = [f"{self.sample}" + (f" (= {', '.join(self.aliases)})" if self.aliases else "")]
        for name in FIELD_NAMES:
            for claim in self.claims.get(name, ()):
                lines.append("  " + claim.explain())
            if self.ambiguous(name):
                said = "; ".join(
                    f"{source}={'|'.join(sorted(values))}"
                    for source, values in self.by_source(name).items()
                )
                lines.append(f"  !! {name} disagrees: {said}")
        return lines


def merge(records: Iterable[SampleMetadata]) -> Mapping[str, SampleMetadata]:
    """Index records by sample and by every alias, so joins find them either way.

    The same dict value under several keys, deliberately: a caller holding a GSM
    and a caller holding a BioSample are asking about one sample, and giving
    them two records is how the same value gets counted twice.
    """
    index: dict[str, SampleMetadata] = {}
    for record in records:
        for key in (record.sample, *record.aliases):
            index[key] = record
    return index
