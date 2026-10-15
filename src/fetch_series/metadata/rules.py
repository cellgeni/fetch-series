"""Tier 1: turn harvested pairs into claims, deterministically.

Rules do the whole job wherever the archive wrote a key. Across 198 datasets,
**77,530 characteristics were structured ``key: value`` and 5 were free text**,
so the residual a model could help with is not extraction -- it is the fields
nobody wrote down, and the grouping question nobody answers explicitly.

## Tier 1 reads keys. It does not read prose.

``5864_BA9`` is a sample title, and ``BA9`` is Brodmann area 9, which is a
tissue. Reading that requires knowing this study's naming convention, which is
pattern matching against one study and does not generalise -- so tier 1 leaves
it alone and carries the prose forward as evidence for tier 3.

The temptation is to add a regex for it. The reason not to is that it would be
indistinguishable, in the output, from a rule that generalises; and the whole
value of building the deterministic tiers first is being able to say what the
model adds. A rule that only works on one study inflates tier 1 and hides tier 3.

## A value shared by every sample is not thereby study-scoped

The opposite trap to the one :mod:`fetch_series.metadata.fields` guards. If 20
samples all carry ``tissue: lung``, every one of them *is* lung -- the value is
invariant and still a per-sample assertion, because the submitter wrote it once
per sample. Refusing it would discard correct metadata from every
single-tissue study, which is most of them.

What cannot assign a per-sample field is *prose shared across samples*: one
paragraph, written once, that the archive repeats. That is recorded as
:attr:`Proposal.invariant_prose` for tier 3 to respect rather than acted on
here, because tier 1 does not read prose at all.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from fetch_series.metadata import Claim, Method
from fetch_series.metadata.fields import Scope, by_name
from fetch_series.metadata.harvest import Harvest, HarvestedValue
from fetch_series.metadata.vocab import (
    canonical_field,
    normalise_value,
    resolve_with_context,
    why_unmapped,
    why_value_unmapped,
)

# Keys that identify rather than describe. Harvested because they are useful
# elsewhere -- `tax_id` selects the ontology `development_stage` grounds
# against -- but they are not claims about the material, and reporting them as
# unmapped would bury the keys that genuinely are.
IDENTITY_KEYS = frozenset({"tax_id", "run_accession", "sample_accession", "biomaterial_provider"})


@dataclass(frozen=True, slots=True)
class Unmapped:
    """A harvested value no rule turned into a claim, and why."""

    source: str
    key: str
    value: str
    reason: str

    def __str__(self) -> str:
        return f"{self.source} {self.key or '(prose)'}={self.value!r}: {self.reason}"


@dataclass(slots=True)
class Proposal:
    """The deterministic answer for one sample, and what it could not answer."""

    sample: str
    claims: list[Claim] = field(default_factory=list)
    #: Values deliberately not turned into claims. The vocabulary backlog and,
    #: in stage 2, the residual the model is asked about.
    unmapped: list[Unmapped] = field(default_factory=list)
    #: Sources whose prose is identical across every sample of the study, so it
    #: cannot assign anything per-sample however specific it reads.
    invariant_prose: tuple[str, ...] = ()

    def fields_answered(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(claim.field for claim in self.claims))


def _claim_for(value: HarvestedValue, name: str) -> Claim | list[Unmapped]:
    """One harvested pair as a claim, or the reasons it is not one."""
    if (reason := why_value_unmapped(name, value.value)) is not None:
        return [Unmapped(source=value.source, key=value.key, value=value.value, reason=reason)]
    normalised = normalise_value(name, value.value)
    if not normalised:
        return []
    # ARCHIVE where the archive's own key already was the field name, RULE
    # where a synonym or a value vocabulary had to be applied. The split is
    # what lets a survey say how much the synonym tables actually add over
    # taking the archives' spellings literally.
    rewritten = value.key.strip().lower() != name or normalised != value.value.strip()
    return Claim(
        field=name,
        value=normalised,
        method=Method.RULE if rewritten else Method.ARCHIVE,
        source=value.source,
        evidence=value.value,
        raw=value.value if normalised != value.value else None,
    )


def propose(harvests: Sequence[Harvest]) -> list[Proposal]:
    """Deterministic claims for every harvested sample.

    Takes the whole study, not one sample, because whether a prose field is
    shared across samples is only answerable over the set -- and because a
    caller handed one sample at a time would have no way to find out.
    """
    invariant = _invariant_prose(harvests)
    proposals: list[Proposal] = []
    for harvest in harvests:
        proposal = Proposal(sample=harvest.sample, invariant_prose=invariant)
        for value in harvest.values:
            if value.scope is Scope.STUDY:
                # Should not reach here -- `harvest` diverts study-scoped values
                # into study_text -- but a source added later might not, and
                # silently assigning one is the failure this guards.
                proposal.unmapped.append(
                    Unmapped(
                        source=value.source,
                        key=value.key,
                        value=value.value,
                        reason="study-scoped: describes the study, so it cannot assign a sample field",
                    )
                )
                continue
            if not value.key:
                proposal.unmapped.append(
                    Unmapped(
                        source=value.source,
                        key="",
                        value=value.value,
                        reason="prose: tier 1 reads keys only, carried forward as evidence",
                    )
                )
                continue
            if value.key.strip().lower() in IDENTITY_KEYS:
                continue
            name = canonical_field(value.key)
            if name is None:
                # A key whose meaning depends on the kind of record it is in.
                # Resolved from the archive's own declaration -- the BioSample
                # package, the harvest's taxon -- and left unmapped with the
                # ambiguity spelled out when neither says which.
                name, ambiguity = resolve_with_context(
                    value.key, package=value.context, taxon=harvest.taxon
                )
                if name is None:
                    proposal.unmapped.append(
                        Unmapped(
                            source=value.source,
                            key=value.key,
                            value=value.value,
                            reason=ambiguity
                            or why_unmapped(value.key)
                            or "no declared field for this key",
                        )
                    )
                    continue
            outcome = _claim_for(value, name)
            if isinstance(outcome, Claim):
                proposal.claims.append(outcome)
            else:
                proposal.unmapped.extend(outcome)
        proposals.append(proposal)
    return proposals


def _invariant_prose(harvests: Sequence[Harvest]) -> tuple[str, ...]:
    """Prose sources whose value is the same for every sample in the study.

    Requires more than one sample to mean anything: in a one-sample study every
    field is trivially invariant, and reporting them all would make the flag
    fire hardest where it is least informative.
    """
    if len(harvests) < 2:
        return ()
    seen: dict[str, set[str]] = {}
    for harvest in harvests:
        for value in harvest.values:
            if not value.key:
                seen.setdefault(value.source, set()).add(value.value)
    return tuple(sorted(source for source, values in seen.items() if len(values) == 1))


def unmapped_keys(proposals: Sequence[Proposal]) -> dict[str, int]:
    """Unmapped keys by how many samples carry them, commonest first.

    The vocabulary backlog, ordered by what it would be worth declaring. A key
    appearing on 800 samples earns a synonym entry; one appearing once does not,
    and of 116 keys observed in a 198-dataset corpus, 70 appeared exactly once.
    """
    counts: dict[str, int] = {}
    for proposal in proposals:
        for entry in proposal.unmapped:
            if entry.key:
                counts[entry.key] = counts.get(entry.key, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


def apply_to(record_claims: Sequence[Claim], name: str) -> tuple[str, ...]:
    """Distinct values claimed for ``name``, for callers holding bare claims."""
    by_name(name)  # raises on an undeclared field, as everywhere else
    return tuple(dict.fromkeys(c.value for c in record_claims if c.field == name))
