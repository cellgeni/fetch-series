"""EBI Ontology Lookup Service (OLS4).

Free, no key, and the authority on whether a term exists at all. Used for two
things: turning a normalised submitter string into a CURIE by exact label or
synonym match, and checking that a CURIE someone else proposed is real.

## Neither of its filters does what its name suggests

Both were measured against the live API while this module was written, and both
findings are load-bearing.

**``exact=true`` does not return only exact matches.** ``q=male&ontology=pato&
exact=true`` returns **40** documents, the first being ``PATO:0000384 male`` and
the rest including ``PATO:0040049 male with DSD`` and ``PATO:0000890 male
sterile``. Taking the top hit happens to work here and is trusting a relevance
rank, which is not a measurement. So the match is re-checked locally: a document
is accepted only if its own label or one of its synonyms equals the query.

**``ontology=cl`` does not restrict results to CL.** The same request returns
``GO:0032943 mononuclear cell proliferation``, because CL imports GO. The
returned CURIE's prefix is therefore checked against the permitted set rather
than assumed from the parameter, and ``is_defining_ontology`` is required -- a
term looked up by CURIE comes back once per ontology that imports it
(``UBERON:0000451`` returns 8 documents, the first from ``ado``), and only one
of them is the ontology that defines it.

The upside measured at the same time: **synonyms are indexed**, so ``PBMC``
resolves to ``CL:2000001 peripheral blood mononuclear cell`` with no synonym
table of our own.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fetch_series.survey.client import SurveyClient

API = "https://www.ebi.ac.uk/ols4/api"

# Requested explicitly: the default document is large, and `synonym` and
# `is_defining_ontology` are both required to validate a hit rather than trust
# one. `rows` is small because an exact match that is not in the first handful
# is not an exact match.
_FIELDS = "obo_id,label,ontology_name,synonym,is_defining_ontology,is_obsolete"
_ROWS = 10


@dataclass(frozen=True, slots=True)
class OlsTerm:
    """One ontology term, and what about it matched."""

    curie: str
    label: str
    ontology: str
    #: The label or synonym string that matched the query, verbatim.
    matched: str
    #: ``label`` or ``synonym`` -- kept because a synonym hit is a weaker claim
    #: than a label hit and the gold corpus will say how much weaker.
    on: str

    @property
    def prefix(self) -> str:
        return self.curie.split(":", 1)[0].lower()


def _comparable(text: str) -> str:
    """Casefolded, whitespace-collapsed, for comparing a query to a label."""
    return " ".join(text.split()).casefold()


def _accept(doc: dict[str, Any], query: str, permitted: frozenset[str]) -> OlsTerm | None:
    """A document, only if it really is an exact hit in a permitted ontology."""
    curie = str(doc.get("obo_id") or "").strip()
    if not curie or ":" not in curie:
        return None
    if curie.split(":", 1)[0].lower() not in permitted:
        return None
    if doc.get("is_obsolete") or not doc.get("is_defining_ontology"):
        return None
    label = str(doc.get("label") or "").strip()
    wanted = _comparable(query)
    if _comparable(label) == wanted:
        matched, on = label, "label"
    else:
        synonyms = [str(s) for s in (doc.get("synonym") or [])]
        hit = next((s for s in synonyms if _comparable(s) == wanted), None)
        if hit is None:
            return None
        matched, on = hit, "synonym"
    return OlsTerm(
        curie=curie,
        label=label,
        ontology=str(doc.get("ontology_name") or "").strip(),
        matched=matched,
        on=on,
    )


async def exact_match(
    client: SurveyClient,
    value: str,
    ontologies: tuple[str, ...],
    timeout: float,
) -> list[OlsTerm]:
    """Terms whose label or a synonym is exactly ``value``, in a permitted ontology.

    Returns a list because a string can genuinely be a label in two ontologies,
    and picking one by relevance rank would be the invented default this project
    exists to avoid. An empty list means no exact match, which is a real answer:
    the value goes to the vocabulary backlog rather than to a fuzzy matcher.
    """
    if not value.strip() or not ontologies:
        return []
    response = await client.get(
        f"{API}/search",
        params={
            "q": value,
            "ontology": ",".join(ontologies),
            "exact": "true",
            "rows": _ROWS,
            "fieldList": _FIELDS,
        },
        timeout=timeout,
    )
    payload = response.json()
    docs = payload.get("response", {}).get("docs", []) if isinstance(payload, dict) else []
    permitted = frozenset(o.lower() for o in ontologies)
    found: list[OlsTerm] = []
    seen: set[str] = set()
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        term = _accept(doc, value, permitted)
        if term is not None and term.curie not in seen:
            seen.add(term.curie)
            found.append(term)
    return found


async def lookup_curie(client: SurveyClient, curie: str, timeout: float) -> OlsTerm | None:
    """The defining term for ``curie``, or ``None`` if there is none.

    Used to validate a CURIE this project did not derive: an ArrayExpress SDRF
    ``Term Accession Number``, or in stage 2 a model-proposed identifier. An
    unresolvable CURIE is not stored as a grounded value.
    """
    if ":" not in curie:
        return None
    response = await client.get(
        f"{API}/terms", params={"obo_id": curie, "size": 20}, timeout=timeout
    )
    payload = response.json()
    terms = payload.get("_embedded", {}).get("terms", []) if isinstance(payload, dict) else []
    for doc in terms:
        # Only the defining ontology's copy. The query returns one document per
        # ontology that imports the term, ordered by neither relevance nor
        # authority -- UBERON:0000451 comes back first from `ado`.
        if isinstance(doc, dict) and doc.get("is_defining_ontology") and not doc.get("is_obsolete"):
            return OlsTerm(
                curie=str(doc.get("obo_id") or curie),
                label=str(doc.get("label") or "").strip(),
                ontology=str(doc.get("ontology_name") or "").strip(),
                matched=str(doc.get("label") or "").strip(),
                on="curie",
            )
    return None
