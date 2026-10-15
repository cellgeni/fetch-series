"""What can and cannot be measured about this layer, and the evidence for both.

The plan for this module was an evaluation corpus built from EBI curators'
annotations: ArrayExpress SDRFs carry curated ``Characteristics[...]`` and a
join key back to the INSDC sample, so the curator's term would be the label and
the submitter's prose the input. It was measured before it was committed to, and
**it does not exist.** Both branches are circular, for opposite reasons.

## The negative result

Sampled 120 studies from the committed ``arrayexpress-ena`` corpus (20,693
studies; seed 20260916, 2026-09-16). 119 SDRFs read, **119 carried mappable
Characteristics, and 0 carried an NCBI- or DDBJ-registered sample.**

* **``E-MTAB-*`` (9,802 studies)** are EBI submissions. Their ENA and BioSample
  records are generated from the same submission as the SDRF, so scoring
  ``ena:filereport`` against the SDRF measures whether EBI copied its own form
  consistently. E-MTAB-10553 duly returns **100% agreement on all 16
  (source, field) pairs**, which ranks nothing.
* **``E-GEOD-*`` (10,597 studies)** are ArrayExpress's imports of GEO series,
  keyed by GSM, and the sample *is* NCBI-registered -- the promising branch.
  But the import is mechanical. For GSM677627, GEO's
  ``developmental stage = late embryo 20dC 4.5 hrs post-early embryo`` appears
  in the SDRF as the identical string; ``genotype = wild type`` identical;
  ``Sex: Hermaphrodite`` arrives lowercased. The only substantive change is to
  a **key**: GEO's ``strain`` becomes ``strain or line``.

Comparing a value against a copy of itself is not a measurement. So value
accuracy for this layer is **not measured**, and this module does not pretend
otherwise -- the same asymmetry
:doc:`the assay layer </assays>` states when it reports 90.9% recall and
declines to report precision.

## What is measured instead

Two things, both real, neither of them accuracy:

1. **Key-mapping agreement** (:func:`key_verdicts`). The one thing the E-GEOD
   import genuinely contributes is EBI's judgement that ``strain or line``
   names the same property as ``strain``. Over 10,597 studies that is a large
   labelled set for :data:`~fetch_series.metadata.vocab.CANONICAL_KEYS`, and it
   is the part of this layer most likely to be wrong.
2. **Cross-archive agreement** (:func:`agreement`). Where two sources both
   state a field for one sample, how often do they agree? This is *consistency*,
   not correctness -- two archives can agree and both be wrong -- and it is
   what ranks sources, exactly as disagreement between identity routes is what
   found the pathologies in this knowledge base.

Accuracy would need human adjudication. The right shape for that is a small
hand-adjudicated set used as a **gate** rather than as evidence, on the model of
``hard-cases``: a route must produce a verdict for all of them before it earns a
bigger run, and none of those verdicts is recorded as ``RouteEvidence``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from enum import StrEnum

from fetch_series.metadata import SampleMetadata
from fetch_series.metadata.evidence import ExtractorEvidence, source_of
from fetch_series.metadata.fields import FIELD_NAMES
from fetch_series.metadata.vocab import canonical_field, normalise_value
from fetch_series.providers.biostudies import SdrfCharacteristic

#: The source that provides the labels, and therefore cannot be scored.
LABEL_SOURCE = "ae:characteristics"

#: Sample-identifier prefixes whose record was created by someone other than
#: EBI, so that an ArrayExpress annotation of it is a second description rather
#: than the same one. ``GSM`` is here because ``E-GEOD-*`` SDRFs are keyed by
#: GEO sample accession -- though see the module docstring for why that
#: independence turns out to hold for keys and not for values.
INDEPENDENT_PREFIXES = ("SAMN", "SAMD", "GSM")


def is_independent(sample: str) -> bool:
    """Whether this sample's record was created independently of its label."""
    return sample.upper().startswith(INDEPENDENT_PREFIXES)


class Outcome(StrEnum):
    """What one source did on one labelled field of one sample.

    Four states, not two, for the same reason a route verdict has three:
    silence is not error. A source that never claims a disease is not wrong
    about disease, and averaging its silence into an accuracy figure would
    make the sparsest source look the least reliable.
    """

    AGREE = "agree"
    DISAGREE = "disagree"
    SILENT = "silent"
    #: Answered, but neither side grounded, so the strings are all there is to
    #: compare. Counted separately because a string comparison of free text is
    #: a weaker verdict than a CURIE match.
    AGREE_UNGROUNDED = "agree_ungrounded"


@dataclass(frozen=True, slots=True)
class GoldLabel:
    """One curated value for one field of one sample."""

    sample: str
    field: str
    value: str
    curie: str | None
    #: The SDRF column key, verbatim, so a disagreement can be read back
    #: against the curator's own spelling.
    key: str


@dataclass(frozen=True, slots=True)
class Verdict:
    """One source's answer on one labelled field, and how it compared."""

    sample: str
    field: str
    source: str
    gold: str
    got: str | None
    outcome: Outcome
    #: ``curie`` or ``label`` -- which comparison produced the outcome.
    compared_on: str = "label"
    #: Whether the label was created independently of the record being scored.
    #: False for an EBI-registered sample, whose ENA and BioSample records come
    #: from the same submission as the SDRF. See the module docstring.
    independent: bool = True


def labels_from_sdrf(
    curated: Mapping[str, Sequence[SdrfCharacteristic]],
) -> list[GoldLabel]:
    """Gold labels from one study's SDRF characteristics.

    Pure, so the whole label side is testable from literals. Only columns whose
    key maps to a declared field become labels -- ``Characteristics[individual]``
    maps to ``donor_id`` and is kept; ``Characteristics[clinical history]`` maps
    to nothing and is dropped rather than compared against a field it does not
    describe.
    """
    labels: list[GoldLabel] = []
    for sample, characteristics in curated.items():
        for characteristic in characteristics:
            name = canonical_field(characteristic.key)
            if name is None or name not in FIELD_NAMES:
                continue
            value = normalise_value(name, characteristic.value)
            if not value:
                continue
            label = GoldLabel(
                sample=sample,
                field=name,
                value=value,
                curie=characteristic.curie,
                key=characteristic.key,
            )
            if label not in labels:
                labels.append(label)
    return labels


def _comparable(text: str) -> str:
    return " ".join(text.split()).casefold()


def score(
    records: Iterable[SampleMetadata],
    labels: Sequence[GoldLabel],
) -> list[Verdict]:
    """Compare what each source claimed against the curated label.

    Compares CURIEs where both sides carry one and strings otherwise, and says
    which in :attr:`Verdict.compared_on`. Comparing a grounded label against a
    curator's raw word would fail spuriously -- this project grounds ``Brain,
    prefrontal cortex`` to *prefrontal cortex* and a curator writing ``brain``
    is not thereby wrong -- so grounding is applied to neither side here and
    resolved by CURIE where available.
    """
    by_sample: dict[str, SampleMetadata] = {}
    for record in records:
        for key in (record.sample, *record.aliases):
            by_sample[key] = record

    verdicts: list[Verdict] = []
    for label in labels:
        independent = is_independent(label.sample)
        found_record = by_sample.get(label.sample)
        if found_record is None:
            continue
        claims = found_record.claims.get(label.field, ())
        # Group the sample's claims by declared source, so each source gets one
        # verdict per labelled field rather than one per claim -- otherwise a
        # source that happened to state a value twice would count twice.
        by_source: dict[str, list[str]] = {}
        curies: dict[str, set[str]] = {}
        for claim in claims:
            declared = source_of(claim.source)
            if declared is None or declared.id == LABEL_SOURCE:
                continue
            # De-duplicated: ENA's filereport is per *run*, so a sample with
            # twelve runs offers the same value twelve times, and joining them
            # produced `adult|adult|adult|...` in the verdict.
            values = by_source.setdefault(declared.id, [])
            if claim.value not in values:
                values.append(claim.value)
            if claim.term is not None:
                curies.setdefault(declared.id, set()).add(claim.term.curie)

        for source in _scoreable_sources(records=by_sample, sample=label.sample):
            got = by_source.get(source)
            if not got:
                verdicts.append(
                    Verdict(
                        sample=label.sample,
                        field=label.field,
                        source=source,
                        gold=label.value,
                        got=None,
                        outcome=Outcome.SILENT,
                        independent=independent,
                    )
                )
                continue
            if label.curie and (found := curies.get(source)):
                agreed = label.curie in found
                verdicts.append(
                    Verdict(
                        sample=label.sample,
                        field=label.field,
                        source=source,
                        gold=label.curie,
                        got="|".join(sorted(found)),
                        outcome=Outcome.AGREE if agreed else Outcome.DISAGREE,
                        compared_on="curie",
                        independent=independent,
                    )
                )
                continue
            wanted = _comparable(label.value)
            agreed = any(_comparable(value) == wanted for value in got)
            verdicts.append(
                Verdict(
                    sample=label.sample,
                    field=label.field,
                    source=source,
                    gold=label.value,
                    got="|".join(got),
                    outcome=Outcome.AGREE_UNGROUNDED if agreed else Outcome.DISAGREE,
                    independent=independent,
                )
            )
    return verdicts


def _scoreable_sources(records: Mapping[str, SampleMetadata], sample: str) -> tuple[str, ...]:
    """Declared sources that were consulted about this sample.

    Read from ``SampleMetadata.consulted``, not inferred from which sources
    happened to produce a claim. The two differ exactly where it matters: a
    source that was asked and said nothing about this field is **silent**,
    which is informative, while a source that was never asked is absent, which
    is not. Inferring it from claims collapsed the two and reported no silences
    at all for a source that answered nothing anywhere -- so a sparse source
    looked complete instead of sparse.

    :doc:`what counts as evidence </decisions/what-counts-as-evidence>` is
    explicit that never-queried is a fourth verdict state.
    """
    record = records.get(sample)
    if record is None:
        return ()
    return tuple(sorted(record.consulted - {LABEL_SOURCE}))


def summarise(
    verdicts: Sequence[Verdict],
    corpus: str,
    surveyed_on: date,
    *,
    independent_only: bool = True,
) -> dict[tuple[str, str], ExtractorEvidence]:
    """Turn verdicts into paste-ready evidence, keyed ``(source, field)``.

    Scores only :attr:`Verdict.independent` verdicts by default. Including the
    rest measures whether EBI filled its own form consistently and returns
    agreement near 100% for every source -- see the module docstring. The
    count set aside goes into the notes rather than being dropped quietly.

    ``AGREE_UNGROUNDED`` counts as an agreement and is named in the notes,
    because a string match is a weaker verdict than a CURIE match and the
    difference should be visible in the declaration rather than only in the
    verdict file.
    """
    buckets: dict[tuple[str, str], list[Verdict]] = {}
    set_aside: dict[tuple[str, str], int] = {}
    for verdict in verdicts:
        key = (verdict.source, verdict.field)
        if independent_only and not verdict.independent:
            set_aside[key] = set_aside.get(key, 0) + 1
            continue
        buckets.setdefault(key, []).append(verdict)

    evidence: dict[tuple[str, str], ExtractorEvidence] = {}
    for (source, name), group in sorted(buckets.items()):
        agree = sum(1 for v in group if v.outcome is Outcome.AGREE)
        loose = sum(1 for v in group if v.outcome is Outcome.AGREE_UNGROUNDED)
        disagree = sum(1 for v in group if v.outcome is Outcome.DISAGREE)
        answered = agree + loose + disagree
        by_curie = sum(1 for v in group if v.compared_on == "curie")
        evidence[(source, name)] = ExtractorEvidence(
            corpus=corpus,
            surveyed_on=surveyed_on,
            field=name,
            samples_queried=len(group),
            samples_answered=answered,
            agreements=agree + loose,
            disagreements=disagree,
            notes=(
                f"{by_curie} of {answered} verdicts compared on CURIE, the rest on label; "
                f"{loose} agreements are label-only; "
                f"{set_aside.get((source, name), 0)} set aside as EBI-registered "
                f"(label not independent of record)"
            ),
        )
    return evidence


# ---------------------------------------------------------------------------
# Key-mapping agreement
#
# The one thing the E-GEOD import genuinely contributes. EBI read GEO's
# `strain` and wrote `strain or line`; that is a human judgement that the two
# name the same property, and there are 10,597 studies of them.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class KeyVerdict:
    """Whether this project maps a key to the field EBI mapped it to."""

    sample: str
    #: The key as the source archive wrote it.
    source_key: str
    #: The key EBI's SDRF column used for the same value.
    label_key: str
    source_field: str | None
    label_field: str | None

    @property
    def outcome(self) -> Outcome:
        if self.source_field is None:
            return Outcome.SILENT
        return Outcome.AGREE if self.source_field == self.label_field else Outcome.DISAGREE


def key_verdicts(
    harvested: Mapping[str, Sequence[tuple[str, str]]],
    curated: Mapping[str, Sequence[SdrfCharacteristic]],
) -> list[KeyVerdict]:
    """Compare key mappings, matched on the *value* the two archives share.

    The value is the join, not the key -- the whole question is whether two
    different keys mean the same thing, so keying on the key would beg it.
    Matching on an identical value is exactly right for the E-GEOD import,
    where the value was copied verbatim.

    ``harvested`` is ``{sample: [(raw_key, value)]}`` as the source archive
    wrote it; ``curated`` is the SDRF for the same samples.
    """
    verdicts: list[KeyVerdict] = []
    for sample, pairs in harvested.items():
        characteristics = curated.get(sample)
        if not characteristics:
            continue
        by_value: dict[str, SdrfCharacteristic] = {}
        for characteristic in characteristics:
            by_value.setdefault(_comparable(characteristic.value), characteristic)
        for raw_key, value in pairs:
            match = by_value.get(_comparable(value))
            if match is None:
                continue
            verdicts.append(
                KeyVerdict(
                    sample=sample,
                    source_key=raw_key,
                    label_key=match.key,
                    source_field=canonical_field(raw_key),
                    label_field=canonical_field(match.key),
                )
            )
    return verdicts


def unmapped_label_keys(verdicts: Sequence[KeyVerdict]) -> dict[str, int]:
    """SDRF keys EBI uses that this project maps to nothing, by frequency.

    The highest-value entry in the vocabulary backlog: EBI's column vocabulary
    is controlled, so a key appearing here often is a real property this field
    set either does not declare or does not recognise the spelling of.
    """
    counts: dict[str, int] = {}
    for verdict in verdicts:
        if verdict.label_field is None:
            counts[verdict.label_key] = counts.get(verdict.label_key, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


# ---------------------------------------------------------------------------
# Cross-archive agreement
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Agreement:
    """How often two sources gave the same answer for one field."""

    field: str
    left: str
    right: str
    agreed: int
    disagreed: int
    #: Samples where exactly one of the pair answered. Not a disagreement --
    #: it is a difference in coverage, and averaging the two together would
    #: make the most complete source look like the least reliable one.
    only_left: int
    only_right: int

    @property
    def rate(self) -> float:
        judged = self.agreed + self.disagreed
        return self.agreed / judged if judged else 0.0

    def __str__(self) -> str:
        return (
            f"{self.field}: {self.left} vs {self.right} -- "
            f"{self.agreed}/{self.agreed + self.disagreed} agree ({self.rate:.1%}), "
            f"{self.only_left} only {self.left}, {self.only_right} only {self.right}"
        )


def agreement(records: Iterable[SampleMetadata]) -> list[Agreement]:
    """Pairwise agreement between sources, per field, over these samples.

    Consistency, not correctness: two archives can agree and both be wrong,
    and one disagreeing with three others is suspicious rather than refuted.
    What it is good for is the same thing route disagreement was good for --
    every pathology in this knowledge base started as two sources that
    disagreed and a question about which.

    Pairs are ordered so each is counted once, and a pair where only one side
    answered is counted as coverage rather than as disagreement.
    """
    pairs = [
        (left, right)
        for index, left in enumerate(_ANSWERABLE)
        for right in _ANSWERABLE[index + 1 :]
    ]
    tally: dict[tuple[str, str, str], list[int]] = {}
    for record in records:
        for name in record.claims:
            by_source = _by_declared_source(record, name)
            for left, right in pairs:
                # Both must have been asked. A BioProject with no GEO series
                # was never asked what GEO says, and scoring that as GEO being
                # silent invents a true negative -- the fourth verdict state.
                if not {left, right} <= record.consulted:
                    continue
                said_left = by_source.get(left)
                said_right = by_source.get(right)
                if not said_left and not said_right:
                    continue
                counts = tally.setdefault((name, left, right), [0, 0, 0, 0])
                if said_left and said_right:
                    # Overlapping value sets count as agreement: a source
                    # naming `lung` where another names `lung, lymph node` is
                    # less complete, not contradictory.
                    counts[0 if said_left & said_right else 1] += 1
                elif said_left:
                    counts[2] += 1
                else:
                    counts[3] += 1

    return sorted(
        (
            Agreement(
                field=name,
                left=left,
                right=right,
                agreed=counts[0],
                disagreed=counts[1],
                only_left=counts[2],
                only_right=counts[3],
            )
            for (name, left, right), counts in tally.items()
        ),
        key=lambda a: (a.field, a.left, a.right),
    )


def _by_declared_source(record: SampleMetadata, name: str) -> dict[str, frozenset[str]]:
    """A field's values grouped by *declared* source, not by provenance string.

    ``SampleMetadata.by_source`` keys on the full provenance --
    ``ena:filereport[tissue_type]`` -- which is right for reporting a conflict
    to a human and wrong for comparing archives: ENA's ``tissue_type`` and
    BioSample's ``attribute[tissue]`` are two archives answering one question,
    and grouping them by their own column names put every pair in a bucket of
    its own and reported no agreement anywhere.
    """
    collected: dict[str, set[str]] = {}
    for claim in record.claims.get(name, ()):
        declared = source_of(claim.source)
        if declared is None:
            continue
        collected.setdefault(declared.id, set()).add(claim.value)
    return {source: frozenset(values) for source, values in collected.items()}


def _answerable() -> tuple[str, ...]:
    from fetch_series.metadata.evidence import SOURCES

    return tuple(s.id for s in SOURCES if s.id != LABEL_SOURCE)


_ANSWERABLE = _answerable()
