"""EBI ZOOMA: curated free-text-to-ontology annotations.

Free, no key. Where OLS answers "is this string a term", ZOOMA answers "has a
curator ever mapped a string like this to a term" -- and its corpus is curated
annotations from ArrayExpress and BioSamples, which is to say from exactly the
submitter prose this project is trying to read. That makes it the right tool for
the tail OLS cannot match exactly, and the wrong tool for anything else.

## It will always find you something, and that is the danger

Measured against the live API while writing this module:

* ``male`` unconstrained returns a **MIxS restroom-gender** term at confidence
  ``HIGH``.
* Constrained to UBERON it returns ``UBERON:0003101`` *male organism* at
  ``GOOD`` -- an anatomical entity where a phenotypic quality was asked for.
* ``tumour adjacent normal lung`` returns ``MONDO:0006049`` *lung cancer* at
  ``GOOD``, labelling a healthy control as diseased.

The filter does not prevent a wrong answer; it selects which wrong answer you
get. So two things are mandatory at the call site: the field must declare the
ontology that can actually express it (``sex`` declares PATO, not UBERON), and
the returned CURIE must be re-checked against that declaration.

## No confidence threshold ships here

``confidence`` is returned and recorded, and nothing in this module acts on it.
A cutoff -- "accept HIGH and GOOD" -- is a default, and the governing rule is
that defaults need a survey behind them. ``GOOD`` was the band on both wrong
answers above and ``HIGH`` was the band on the third, so the bands are not
ordered by correctness in any way this project has measured yet. Once the gold
corpus reports accuracy per band, a threshold can be declared with a number
beside it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from fetch_series.survey.client import SurveyClient

ANNOTATE = "https://www.ebi.ac.uk/spot/zooma/v2/api/services/annotate"

# Only these two IRI shapes have an unambiguous CURIE. ZOOMA also returns
# SNOMED (`http://snomed.info/id/404798000`), MeSH, and MIxS enum IRIs, none of
# which belong to an OBO ontology this project declares -- they are dropped
# rather than transliterated into a prefix that no ontology would recognise.
_OBO_PURL = re.compile(r"^https?://purl\.obolibrary\.org/obo/([A-Za-z]+)_(\w+)$")
_EFO_PURL = re.compile(r"^https?://www\.ebi\.ac\.uk/efo/(EFO)_(\w+)$")


def iri_to_curie(iri: str) -> str | None:
    """``…/obo/UBERON_0000451`` -> ``UBERON:0000451``; anything else -> None."""
    for pattern in (_OBO_PURL, _EFO_PURL):
        if found := pattern.match(iri.strip()):
            return f"{found.group(1).upper()}:{found.group(2)}"
    return None


@dataclass(frozen=True, slots=True)
class ZoomaHit:
    """One curated annotation ZOOMA offered."""

    curie: str
    confidence: str
    #: The property value the curator annotated, which may differ from the
    #: query -- ZOOMA matches approximately, and seeing what it actually
    #: matched is the only way to notice that it matched something else.
    matched_value: str
    source: str

    @property
    def prefix(self) -> str:
        return self.curie.split(":", 1)[0].lower()


def _hits(payload: Any, permitted: frozenset[str]) -> list[ZoomaHit]:
    if not isinstance(payload, list):
        return []
    found: list[ZoomaHit] = []
    seen: set[str] = set()
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        tags = [str(t) for t in (entry.get("semanticTags") or [])]
        # A combination annotation -- two tags for one property value, as in
        # "lung carcinoma" mapped to both a disease and an anatomy term -- says
        # nothing unambiguous about a single field, so it is skipped rather
        # than half-read.
        if len(tags) != 1:
            continue
        curie = iri_to_curie(tags[0])
        if curie is None or curie.split(":", 1)[0].lower() not in permitted or curie in seen:
            continue
        seen.add(curie)
        annotated = entry.get("annotatedProperty") or {}
        provenance = entry.get("provenance") or {}
        source = provenance.get("source") or {}
        found.append(
            ZoomaHit(
                curie=curie,
                confidence=str(entry.get("confidence") or "").strip() or "UNKNOWN",
                matched_value=str(annotated.get("propertyValue") or "").strip(),
                source=str(source.get("name") or "").strip(),
            )
        )
    return found


async def annotate(
    client: SurveyClient,
    value: str,
    ontologies: tuple[str, ...],
    timeout: float,
    *,
    property_type: str | None = None,
) -> list[ZoomaHit]:
    """Curated annotations for ``value``, restricted to ``ontologies``.

    ``property_type`` is the attribute key the value appeared under (``organism
    part``, ``sex``). Passing it narrows ZOOMA to annotations curators made for
    the same kind of field, which is the one lever here that reduces wrong
    answers rather than merely relocating them.

    Returns every hit, in ZOOMA's own order, with the confidence band attached
    and no cutoff applied -- see the module docstring.
    """
    if not value.strip() or not ontologies:
        return []
    response = await client.get(
        ANNOTATE,
        params={
            "propertyValue": value,
            "propertyType": property_type,
            # `required:[none]` asks for annotations from any datasource rather
            # than only curator-verified ones; the ontology list is what does
            # the restricting.
            "filter": f"required:[none],ontologies:[{','.join(ontologies)}]",
        },
        timeout=timeout,
    )
    return _hits(response.json(), frozenset(o.lower() for o in ontologies))
