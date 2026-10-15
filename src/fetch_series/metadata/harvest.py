"""Tier 0: collect what the archives already say, without interpreting any of it.

Four sources, none of which is complete and no two of which agree on spelling:

* **BioSample** attribute pairs -- the richest, and carrying NCBI's own
  ``harmonized_name``. This project fetched the containing ESummary record and
  kept only the accession until now.
* **GEO** ``!Sample_characteristics_ch1``, already ``key: value``, plus the
  source name and the series-level prose.
* **ENA** ``filereport`` descriptive columns, which are structured but sparsely
  filled.
* **ArrayExpress** SDRF ``Characteristics[...]``, curated by a human against a
  controlled key vocabulary and sometimes carrying an ontology term outright.

Nothing here maps a key, normalises a value or grounds anything. A
:class:`HarvestedValue` is the submitter's own key and the submitter's own
string, plus where it came from and whether it describes a sample or a study.
Keeping this layer stupid is what makes the next two measurable: a claim can
always be traced back to a verbatim string an archive served.

## Scope is recorded here because it cannot be recovered later

``!Series_summary`` and ``study_title`` are one paragraph shared by every sample
in the study. Once such a value has been copied onto a sample it is
indistinguishable from something the submitter said about that sample, and
PRJNA434002 is the case that makes this concrete: its ``study_title`` names
Autism Spectrum Disorder for all 56 runs, controls included.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from fetch_series.accession import Accession, EntityType
from fetch_series.entities import RunRecord
from fetch_series.metadata.fields import Scope
from fetch_series.providers import biostudies, ena_portal, eutils, geo
from fetch_series.providers.biostudies import SdrfCharacteristic
from fetch_series.providers.eutils import BioSampleRecord
from fetch_series.providers.geo import SoftFamily
from fetch_series.relations import relations
from fetch_series.survey.client import SurveyClient, is_retryable

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class HarvestedValue:
    """One key/value pair, exactly as an archive served it."""

    #: Where it came from, always ``archive:surface[key]``:
    #: ``biosample:attribute[tissue]``, ``ena:filereport[tissue_type]``,
    #: ``ae:characteristics[organism part]``. Uniform because
    #: :func:`fetch_series.metadata.evidence.source_of` recovers the declared
    #: source by splitting on the bracket -- an inconsistent spelling here
    #: makes a claim unattributable and therefore unmeasurable.
    source: str
    #: The submitter's own key. Empty for a field that is not a pair, such as a
    #: title or a summary paragraph.
    key: str
    value: str
    scope: Scope = Scope.SAMPLE
    #: An ontology term the archive itself supplied. Only ArrayExpress does,
    #: through ``Term Accession Number``, and it still gets validated -- an
    #: unresolvable CURIE is not a grounding.
    curie: str | None = None
    #: Whether the archive vouched for the key's spelling. True for a BioSample
    #: ``harmonized_name`` and for an SDRF column, false for raw submitter text.
    curated_key: bool = False
    #: What kind of record this came from, where the archive declares it: a
    #: BioSample package, an ENA checklist. Some keys mean different things in
    #: different records -- ``isolate`` is the donor in a Human package and a
    #: strain in a Microbe one -- and this is the archive's own answer rather
    #: than an inference from the value.
    context: str | None = None


@dataclass(slots=True)
class Harvest:
    """Everything harvested for one sample, before interpretation."""

    sample: str
    aliases: tuple[str, ...] = ()
    taxon: str | None = None
    values: list[HarvestedValue] = field(default_factory=list)
    #: Study-scoped free text, held once rather than copied onto every sample.
    #: Keyed by source, e.g. ``geo:soft[series_summary]``.
    study_text: dict[str, str] = field(default_factory=dict)
    #: Declared source ids that were actually asked about this sample.
    #:
    #: Tracked because **never queried is not the same as empty**. A BioProject
    #: with no GEO series was never asked what GEO's characteristics say, and
    #: counting that as GEO having nothing to say would manufacture a true
    #: negative out of a request that was never made -- which
    #: `docs/decisions/what-counts-as-evidence.md` names as the worse of the
    #: two ways to collapse a verdict's states.
    consulted: set[str] = field(default_factory=set)

    def keys(self) -> tuple[str, ...]:
        """Every distinct raw key harvested, for vocabulary work."""
        return tuple(dict.fromkeys(v.key for v in self.values if v.key))


# --- pure converters -------------------------------------------------------
#
# Each takes one provider's output and returns HarvestedValues. No I/O, so the
# whole of tier 0's parsing is testable from string literals.


def from_biosample(record: BioSampleRecord) -> list[HarvestedValue]:
    """BioSample attributes and description fields."""
    found = [
        HarvestedValue(
            source=f"biosample:attribute[{value.raw_name}]",
            key=value.name,
            value=value.value,
            curated_key=value.harmonised,
            context=record.package,
        )
        for value in record.attributes
    ]
    # The title and organism are not attribute pairs, so they carry no key. The
    # title is often the only place a tissue appears -- "5893_PFC_Nova".
    if record.title:
        found.append(
            HarvestedValue(source="biosample:description[title]", key="", value=record.title)
        )
    if record.organism:
        found.append(
            HarvestedValue(
                source="biosample:description[organism]", key="organism", value=record.organism
            )
        )
    if record.paragraph:
        found.append(
            HarvestedValue(
                source="biosample:description[paragraph]", key="", value=record.paragraph
            )
        )
    return found


def from_soft_sample(family: SoftFamily, gsm: str) -> list[HarvestedValue]:
    """GEO's characteristics and descriptive prose for one sample."""
    found = [
        HarvestedValue(
            source=f"geo:characteristics[{key}]",
            key=key,
            value=value,
            # GEO imposes no vocabulary on these keys at all; the 116 distinct
            # spellings in a 198-dataset corpus are the consequence.
            curated_key=False,
        )
        for key, value in family.sample_characteristics.get(gsm, ())
    ]
    text = family.sample_text.get(gsm, {})
    for name in ("sample_source_name", "sample_title", "sample_description"):
        if value := text.get(name):
            found.append(HarvestedValue(source=f"geo:soft[{name}]", key="", value=value))
    if organism := text.get("sample_organism"):
        found.append(
            HarvestedValue(source="geo:soft[sample_organism]", key="organism", value=organism)
        )
    return found


def from_ena_row(row: Mapping[str, str]) -> list[HarvestedValue]:
    """ENA's descriptive filereport columns for one run.

    The column name *is* the key: ENA's schema names these fields, so unlike
    GEO's characteristics there is nothing to parse out of a value.
    """
    found: list[HarvestedValue] = []
    for column in ena_portal.METADATA_FIELDS:
        value = (row.get(column) or "").strip()
        if not value or column in {
            "run_accession",
            "sample_accession",
            "secondary_sample_accession",
        }:
            continue
        found.append(
            HarvestedValue(
                source=f"ena:filereport[{column}]",
                # A title or a protocol is prose, not a keyed attribute.
                key="" if column in _ENA_PROSE else column,
                value=value,
                scope=(Scope.STUDY if column in ena_portal.STUDY_SCOPED_FIELDS else Scope.SAMPLE),
                curated_key=column not in _ENA_PROSE,
            )
        )
    return found


_ENA_PROSE = frozenset(
    {
        "sample_title",
        "sample_description",
        "sample_alias",
        "description",
        "study_title",
        "experiment_title",
        "library_construction_protocol",
    }
)


def from_sdrf(characteristics: Sequence[SdrfCharacteristic]) -> list[HarvestedValue]:
    """ArrayExpress curated characteristics for one sample."""
    return [
        HarvestedValue(
            source=f"ae:{characteristic.kind}[{characteristic.key}]",
            key=characteristic.key,
            value=characteristic.value,
            curie=characteristic.curie,
            # The key comes from EBI's controlled column vocabulary, not from
            # the submitter. This is the one source whose keys need no synonym
            # table -- which is also why it is the label half of the gold set.
            curated_key=True,
        )
        for characteristic in characteristics
    ]


# --- orchestration ---------------------------------------------------------


def _sample_key(record: RunRecord) -> str | None:
    """The identity to file a run's metadata under, most joinable first.

    BioSample first because it is the only identifier all four archives share,
    so it is the key under which two sources can be compared at all.
    """
    return record.biosample or record.geo_sample or record.sample


async def harvest(
    accession: Accession,
    client: SurveyClient,
    timeout: float = 120.0,
    *,
    limit: int | None = None,
) -> list[Harvest]:
    """Harvest every source that can answer for ``accession``.

    The relation table supplies the identities, which is what makes one record
    per sample possible: without it, a BioSample attribute and a GEO
    characteristic about the same material land on two records that nothing
    joins.

    A source that fails is logged and skipped, not fatal. Tier 0's value is
    additive -- three sources answering where a fourth timed out is a usable
    harvest, and refusing to produce one would make the whole layer as fragile
    as its least reliable archive.
    """
    records = await relations(accession, client, timeout)
    if limit is not None:
        records = records[:limit]

    by_sample: dict[str, Harvest] = {}
    for record in records:
        key = _sample_key(record)
        if key is None:
            continue
        if key not in by_sample:
            aliases = {record.biosample, record.geo_sample, record.sample, record.run} - {
                None,
                key,
            }
            by_sample[key] = Harvest(sample=key, aliases=tuple(sorted(a for a in aliases if a)))

    # ENA's descriptive columns, one request keyed on something ENA accepts.
    #
    # Not on the input accession: `filereport` answers a GEO series with a 400,
    # and it answers `E-MTAB-10553` with one too. The relation table has
    # already resolved the INSDC identifiers, so the project or study
    # accession is in hand -- which is also the only key that returns every
    # sample in one request rather than one request per sample.
    keyed_on = _ena_key(records) or accession.value
    try:
        rows = await ena_portal.read_run_report(
            client, keyed_on, timeout, fields=ena_portal.METADATA_FIELDS
        )
    except Exception as exc:  # every source is optional; see _skipped
        _skipped(f"ena:filereport({keyed_on})", exc)
        rows = []
    for row in rows:
        key = row.get("sample_accession") or row.get("secondary_sample_accession") or ""
        target = by_sample.get(key.strip())
        if target is None:
            continue
        target.taxon = target.taxon or (row.get("tax_id") or "").strip() or None
        target.consulted.add("ena:filereport")
        _absorb(target, from_ena_row(row))

    # BioSample attributes for every sample the relation table named.
    biosamples = [r.biosample for r in records if r.biosample]
    if biosamples:
        try:
            # Named `entry`, not `record`: the enclosing loop binds `record`
            # to a RunRecord, and shadowing it here silently harvested the
            # wrong object until mypy said so.
            for entry in await eutils.fetch_biosamples(client, biosamples, timeout):
                if (target := by_sample.get(entry.accession)) is not None:
                    target.taxon = target.taxon or entry.taxon
                    target.aliases = tuple(
                        sorted({*target.aliases, *(value for _, value in entry.cross_links)})
                    )
                    target.consulted.update({"biosample:attribute", "biosample:description"})
                    _absorb(target, from_biosample(entry))
        except Exception as exc:
            _skipped("biosample:efetch", exc)

    # GEO's own record, for a series. Fetched again rather than threaded out of
    # `relations`, which does not return the family it parsed; the SOFT file is
    # the largest single download here, so widening `relations` to hand it back
    # is the obvious next efficiency and deliberately not done in the same
    # change as the M3 parity contract it would touch.
    series = next((r.geo_series for r in records if r.geo_series), None)
    if series:
        try:
            family = geo.parse_soft_family(
                series, await geo.fetch_soft_family(client, series, timeout)
            )
            study_text = {f"geo:soft[{name}]": value for name, value in family.series_text.items()}
            for record in records:
                if record.geo_sample and (target := by_sample.get(_sample_key(record) or "")):
                    target.consulted.update({"geo:characteristics", "geo:soft"})
                    _absorb(target, from_soft_sample(family, record.geo_sample))
                    target.study_text.update(study_text)
        except Exception as exc:
            _skipped(f"geo:soft_family({series})", exc)

    # ArrayExpress, where the study has an accession. Keyed on whatever the
    # SDRF names, so both the BioSample and the ENA sample find their row.
    ae = accession.value if accession.entity is EntityType.AE_EXPERIMENT else None
    ae = ae or next((r.ae_experiment for r in records if r.ae_experiment), None)
    if ae:
        try:
            rows_raw = await biostudies.sdrf_raw_rows(client, ae, timeout)
            curated = biostudies.sdrf_characteristics(rows_raw)
            for sample_key, characteristics in curated.items():
                target = by_sample.get(sample_key) or _by_alias(by_sample, sample_key)
                if target is not None:
                    target.consulted.add("ae:characteristics")
                    _absorb(target, from_sdrf(characteristics))
        except Exception as exc:
            _skipped(f"ae:sdrf({ae})", exc)

    return list(by_sample.values())


def _ena_key(records: Sequence[RunRecord]) -> str | None:
    """An accession ENA's filereport will accept for this whole set.

    Project first, then study: both return every run of the submission in one
    request. A run accession would work too and is deliberately not used -- it
    would need one request per run, which for GSE241770's 7,905 runs is the
    difference between one call and nearly eight thousand.
    """
    for attribute in ("bioproject", "study"):
        found = next((getattr(r, attribute) for r in records if getattr(r, attribute)), None)
        if found:
            return str(found)
    return None


def _absorb(target: Harvest, values: list[HarvestedValue]) -> None:
    """Add values, keeping study-scoped prose out of the per-sample list."""
    for value in values:
        if value.scope is Scope.STUDY:
            target.study_text.setdefault(value.source, value.value)
        else:
            target.values.append(value)


def _by_alias(index: Mapping[str, Harvest], key: str) -> Harvest | None:
    return next((h for h in index.values() if key in h.aliases), None)


def _skipped(source: str, exc: BaseException) -> None:
    """Log a source that did not answer, distinguishing transient from not.

    The distinction matters for the same reason it does in the pathology suite:
    a harvest missing a source because the archive was busy is not evidence
    that the archive holds nothing.
    """
    kind = "unavailable" if is_retryable(exc) else "failed"
    logger.warning("harvest source %s %s: %s: %s", source, kind, type(exc).__name__, exc)
