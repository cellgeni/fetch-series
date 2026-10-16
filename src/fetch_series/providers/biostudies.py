"""EBI BioStudies and ArrayExpress.

ArrayExpress studies describe themselves in two files: an IDF of study-level
metadata and an SDRF table of samples. Neither is guaranteed to carry the link
you want, which is why the fallback chain matters -- E-MTAB-6505 declares no
secondary accession at all and is only reachable through its BioSamples.
"""

from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

from fetch_series.accession import try_parse
from fetch_series.survey.client import SurveyClient

FILES = "https://www.ebi.ac.uk/biostudies/files"
API = "https://www.ebi.ac.uk/biostudies/api/v1"

# Accepts ERP/SRP/DRP and EGA study accessions.
SECONDARY = re.compile(r"Comment\s*\[SecondaryAccession\]\s*((?:[SED]RP|EGA[SD])\w*\d+)")

# BioStudies pages silently: with no explicit limit it returns the first 25 of
# however many there are, which would have declared 15 of E-MTAB-9221's 40
# registered fastq files unregistered.
PAGE_SIZE = 500


async def idf_secondary_accessions(
    client: SurveyClient, accession: str, timeout: float
) -> list[str]:
    """Secondary study accessions declared in a study's IDF file."""
    response = await client.get(f"{FILES}/{accession}/{accession}.idf.txt", timeout=timeout)
    return sorted(set(SECONDARY.findall(response.text)))


async def fetch_idf(client: SurveyClient, accession: str, timeout: float) -> str:
    """A study's IDF file, as text."""
    response = await client.get(f"{FILES}/{accession}/{accession}.idf.txt", timeout=timeout)
    return response.text


_IDF_COMMENT = re.compile(r"^Comment\s*\[([^\]]+)\]\t(.*)$", re.MULTILINE)


def idf_comments(text: str) -> dict[str, list[str]]:
    """Every ``Comment[...]`` row of an IDF, by name, values in order.

    A name can repeat: E-GEOD-60450 declares ``Comment[SecondaryAccession]``
    twice, once for GSE60450 and once for SRP045534, so the rows are collected
    rather than overwritten.
    """
    found: dict[str, list[str]] = {}
    for match in _IDF_COMMENT.finditer(text):
        values = [value.strip() for value in match.group(2).split("\t") if value.strip()]
        found.setdefault(match.group(1).strip(), []).extend(values)
    return found


def _column_key(name: str) -> str:
    """``Comment [ENA_RUN]`` and ``Comment[ENA_RUN]`` are one column; both spellings occur."""
    return re.sub(r"\s+", "", name).lower()


def sdrf_values(
    rows: list[list[str]], columns: tuple[str, ...], *, source_name: str | None = None
) -> dict[str, list[str]]:
    """Distinct values of each named column, from raw SDRF rows.

    Every column of a repeated name is read, not the last: E-MTAB-9221 carries
    ``Comment[FASTQ_URI]`` once per mate. ``source_name`` keeps only the rows of
    one sample, which is how an ArrayExpress sample -- an SDRF row with no
    accession of its own -- is addressed.
    """
    if not rows:
        return {column: [] for column in columns}
    header, *data = rows
    keys = [_column_key(name) for name in header]
    wanted = {_column_key(column): column for column in columns}
    source_index = keys.index("sourcename") if "sourcename" in keys else None
    found: dict[str, set[str]] = {column: set() for column in columns}
    for row in data:
        if source_name is not None and (
            source_index is None or _cell(row, source_index) != source_name
        ):
            continue
        for index, key in enumerate(keys):
            if key in wanted and (value := _cell(row, index)):
                found[wanted[key]].add(value)
    return {column: sorted(values) for column, values in found.items()}


async def sdrf_raw_rows(client: SurveyClient, accession: str, timeout: float) -> list[list[str]]:
    """The SDRF as raw field lists, header included.

    SDRF repeats column names by design -- E-MTAB-9221 has two
    ``Comment[FASTQ_URI]`` columns, one per mate -- and ``csv.DictReader`` keeps
    only the last value for a repeated key. Reading that study through a dict
    silently returned one URI per run instead of two, which for paired reads is
    exactly half the data. Anything that reads a column which may repeat must
    use this, not ``sdrf_rows``.
    """
    response = await client.get(f"{FILES}/{accession}/{accession}.sdrf.txt", timeout=timeout)
    text = response.text.strip()
    if not text:
        return []
    return [line.split("\t") for line in text.splitlines()]


async def sdrf_rows(client: SurveyClient, accession: str, timeout: float) -> list[dict[str, Any]]:
    """The SDRF sample table as row dicts.

    Convenient, and lossy for any column name that repeats -- see
    :func:`sdrf_raw_rows`.
    """
    response = await client.get(f"{FILES}/{accession}/{accession}.sdrf.txt", timeout=timeout)
    text = response.text.strip()
    if not text:
        return []
    return list(csv.DictReader(io.StringIO(text), delimiter="\t"))


def sdrf_column(rows: list[dict[str, Any]], name: str) -> list[str]:
    """Distinct values of an SDRF column.

    ``csv.DictReader`` fills absent trailing columns with None, so the key can be
    present with a None value; short rows are common in hand-edited SDRFs.
    """
    values = {
        value.strip() for row in rows if (value := row.get(name)) is not None and value.strip()
    }
    return sorted(values)


async def registered_files(
    client: SurveyClient, accession: str, timeout: float
) -> list[dict[str, Any]]:
    """Every file a study actually registers with BioStudies, paged in full."""
    collected: list[dict[str, Any]] = []
    offset = 0
    while True:
        response = await client.get(
            f"{API}/studies/{accession}/files",
            params={"limit": PAGE_SIZE, "offset": offset},
            timeout=timeout,
        )
        payload = response.json()
        # The list is under `items`, and `files` is not an alias for it -- a
        # .get("files", []) returns an empty list and no error, which read as
        # "this study registers nothing". For the guard that uses this list,
        # registering nothing is the finding, so the bug was invisible: it
        # turned every study into E-MTAB-8060.
        if isinstance(payload, list):
            page = payload
            total = len(page)
        else:
            page = payload.get("items") or []
            total = (payload.get("pagination") or {}).get("total", len(page))
        collected.extend(page)
        offset += PAGE_SIZE
        if not page or offset >= total:
            break
    return collected


async def study(client: SurveyClient, accession: str, timeout: float) -> dict[str, Any]:
    """A study's BioStudies JSON record."""
    response = await client.get(f"{API}/studies/{accession}", timeout=timeout)
    payload = response.json()
    return payload if isinstance(payload, dict) else {}


def study_links(payload: dict[str, Any]) -> list[tuple[str, str | None]]:
    """``(url, Type)`` for every link in a study record, however deeply nested.

    Links sit in ``section.links`` and in every subsection, either singly or in
    groups. E-MTAB-5061 types its links (``ENA``, ``Biostudies``, ``DOI``); a
    Europe PMC record's links carry ``Type: null``, so a caller must read a
    link by what it says rather than by what it is labelled.
    """
    found: list[tuple[str, str | None]] = []

    def walk(section: dict[str, Any]) -> None:
        for group in section.get("links") or []:
            for item in group if isinstance(group, list) else [group]:
                if not isinstance(item, dict) or not item.get("url"):
                    continue
                kind = next(
                    (
                        a.get("value")
                        for a in item.get("attributes") or []
                        if a.get("name") == "Type"
                    ),
                    None,
                )
                found.append((str(item["url"]).strip(), kind))
        for sub in section.get("subsections") or []:
            for child in sub if isinstance(sub, list) else [sub]:
                if isinstance(child, dict):
                    walk(child)

    walk(payload.get("section") or {})
    return found


def registered_names(files: list[dict[str, Any]]) -> set[str]:
    """The bare filenames a study registers, for comparing against SDRF URIs."""
    return {
        str(entry.get("path") or entry.get("Name") or "").rsplit("/", 1)[-1]
        for entry in files
        if entry.get("path") or entry.get("Name")
    }


SEARCH = f"{API}/{{collection}}/search"
SEARCH_ALL = f"{API}/search"

# BioStudies caps pageSize; 100 is accepted everywhere and keeps a full
# enumeration of the ArrayExpress collection to a few hundred requests.
SEARCH_PAGE_SIZE = 100

# BioStudies serves at most this many hits per query, however they are paged,
# and answers anything past it with HTTP 500 -- not 400, and not a truncated
# page. A client that treats 5xx as transient retries a permanent condition
# forever. Enumerating a larger collection means partitioning the query.
SEARCH_WINDOW = 20_000

# Released-year is the one facet that partitions the whole ArrayExpress
# collection into windows small enough to page. 2001 is the first year with any
# content; the upper bound is open so a new year needs no code change.
FIRST_RELEASE_YEAR = 2001


async def search(
    client: SurveyClient,
    timeout: float,
    query: str | None = None,
    collection: str | None = "arrayexpress",
    facets: dict[str, str] | None = None,
    max_pages: int | None = None,
) -> list[str]:
    """Accessions matching a BioStudies search, paged to exhaustion.

    ``totalHits`` is authoritative and is used to stop, rather than trusting an
    empty page: BioStudies returns the first 25 hits and no warning when no
    explicit size is given, which is the pathology this project already
    documents for the files endpoint. ``collection=None`` searches every
    collection, Europe PMC's citation records among them.
    """
    params: dict[str, str | int] = {"pageSize": SEARCH_PAGE_SIZE}
    if query:
        params["query"] = query
    for key, value in (facets or {}).items():
        params[key] = value

    accessions: list[str] = []
    page = 1
    while True:
        url = SEARCH.format(collection=collection) if collection else SEARCH_ALL
        response = await client.get(url, params={**params, "page": page}, timeout=timeout)
        payload = response.json()
        hits = payload.get("hits") or []
        total = payload.get("totalHits", 0)
        if total > SEARCH_WINDOW and max_pages is None:
            raise ValueError(
                f"BioStudies reports {total:,} hits but serves only the first "
                f"{SEARCH_WINDOW:,}, then answers HTTP 500. Partition the query "
                f"(see search_by_year) rather than paging into the error."
            )
        accessions.extend(hit["accession"] for hit in hits if hit.get("accession"))
        if not hits or len(accessions) >= total:
            break
        page += 1
        if max_pages is not None and page > max_pages:
            break
    return accessions


async def search_by_year(
    client: SurveyClient,
    timeout: float,
    collection: str = "arrayexpress",
    facets: dict[str, str] | None = None,
    through_year: int | None = None,
) -> list[str]:
    """Enumerate a whole collection by partitioning on release year.

    Each year is well under the 20,000-hit window, so every partition pages
    cleanly. The partitions are disjoint and exhaustive by construction -- every
    study has exactly one release year -- so the union is the full collection
    and needs no deduplication beyond ordinary defensiveness.
    """
    last = through_year or date.today().year
    accessions: set[str] = set()
    for year in range(FIRST_RELEASE_YEAR, last + 1):
        accessions.update(
            await search(
                client,
                timeout,
                collection=collection,
                facets={**(facets or {}), "facet.released_year": str(year)},
            )
        )
    return sorted(accessions)


# The pre-BioStudies ArrayExpress mirror. A SDRF URI under this path is a stale
# spelling that means two different things depending on whether the study still
# registers the file, which is why it can never be taken at face value.
AE_MIRROR = re.compile(r"/pub/databases/(microarray|arrayexpress)/data/experiment/")

# Anything fastq-shaped, however the submitter spelled it.
FASTQ_URI = re.compile(r"(?:ftp|https?)://\S*\.f(?:ast)?q(?:\.gz)?\b", re.IGNORECASE)


def sdrf_fastq_uris(
    rows: list[list[str]], accession: str, registered: set[str] | None
) -> dict[str, list[str]]:
    """Submitter fastq URIs from an SDRF, keyed by the run they belong to.

    Takes raw field lists, not row dicts: SDRF repeats ``Comment[FASTQ_URI]``
    once per mate, and a dict keeps only the last. Columns are not standardised
    anyway, so URIs are found by shape rather than by name and a row is
    attributed to whatever run accession it mentions.

    A URI under the decommissioned pre-BioStudies mirror is believed only if
    the study still registers a file of that name, and rewritten to the
    BioStudies path when it does. E-MTAB-8060 and E-MTAB-9221 are
    indistinguishable at this point -- both point every fastq URI at the mirror
    and both also declare a BAM -- and yet 9221 registers all 40 of its fastqs
    while 8060 registers none of its 36. Taking 8060's URIs at face value
    discarded the real BAM for all 15 of its runs, silently.

    ``registered`` of None means the file list could not be fetched. Every URI
    is then kept as written: an unreachable API must not be able to reroute a
    whole study to its BAMs.
    """
    by_run: dict[str, list[str]] = {}
    for row in rows:
        line = "\t".join(field for field in row if field)
        uris = FASTQ_URI.findall(line)
        if not uris:
            continue
        runs = sorted(set(re.findall(r"\b[SED]RR\d+\b", line)))
        if not runs:
            continue
        kept: list[str] = []
        for uri in uris:
            if registered is None or not AE_MIRROR.search(uri):
                kept.append(uri)
                continue
            name = uri.rsplit("/", 1)[-1]
            if name in registered:
                kept.append(f"{FILES}/{accession}/{name}")
        for run in runs:
            by_run.setdefault(run, []).extend(kept)
    return {run: sorted(dict.fromkeys(uris)) for run, uris in by_run.items() if uris}


# ---------------------------------------------------------------------------
# SDRF characteristics
#
# ArrayExpress is the only one of these archives whose descriptive metadata was
# curated by a human against a controlled key vocabulary, and some of it carries
# the ontology term the curator chose. That makes it the label half of the
# evaluation corpus in `fetch_series.metadata.gold`, joined back to GEO/SRA free
# text through `Comment[BioSD_SAMPLE]` or `Comment[ENA_SAMPLE]`.
#
# The positional rule is the part that is easy to get wrong. `Term Source REF`
# and `Term Accession Number` qualify the **immediately preceding qualifiable
# column**, and that column is not always a characteristic. E-MTAB-10553's
# header runs:
#
#   5:Characteristics[age]  6:Unit[time unit]  7:Term Source REF  8:Term Accession Number
#
# and its first row runs `84`, `year`, `EFO`, `UO_0000036`. `UO_0000036` is the
# Units Ontology term for *year* -- it qualifies the unit, not the age. Binding
# term columns to the nearest `Characteristics[...]` instead of the nearest
# qualifiable column would record every such age as having an ontology term,
# and record the wrong one.
# ---------------------------------------------------------------------------

_QUALIFIABLE = re.compile(r"^\s*(characteristics|factor\s*value|unit)\s*\[(.+)\]\s*$", re.I)
_TERM_SOURCE = "term source ref"
_TERM_ACCESSION = "term accession number"
# A curator writes `UBERON_0002107`; some SDRFs carry the full PURL instead.
_ACCESSION_SHAPE = re.compile(r"([A-Za-z]+)[_:](\w+)\s*$")

# Columns that identify which sample a row is about, most joinable first.
SAMPLE_KEY_COLUMNS = ("comment[biosd_sample]", "comment[ena_sample]", "source name")


@dataclass(frozen=True, slots=True)
class SdrfCharacteristic:
    """One curated attribute of one sample, with its term where there is one."""

    key: str
    value: str
    #: ``characteristics`` or ``factor value``. A factor value is a
    #: characteristic the study varied deliberately, which makes it the better
    #: evidence for grouping -- and still a characteristic.
    kind: str
    term_source: str | None = None
    #: As written by the curator: ``UBERON_0002107``, or a full PURL.
    term_accession: str | None = None

    @property
    def curie(self) -> str | None:
        """``UBERON_0002107`` -> ``UBERON:0002107``, or ``None``.

        Returns ``None`` rather than a guess for anything that does not match
        the shape. A term accession that cannot be turned into a CURIE is not
        validated against an ontology, so storing it as one would assert a
        grounding that was never checked.
        """
        if not self.term_accession:
            return None
        if found := _ACCESSION_SHAPE.search(self.term_accession.strip()):
            return f"{found.group(1).upper()}:{found.group(2)}"
        return None


def _bind_term_columns(header: list[str]) -> dict[int, tuple[str, str, int | None, int | None]]:
    """Map each qualifiable column index to its key and its term columns.

    Only ``Characteristics`` and ``Factor Value`` columns are returned. A
    ``Unit`` column is tracked -- so that term columns following it are bound to
    it and therefore *not* offered to the characteristic before it -- and then
    dropped.
    """
    bound: dict[int, tuple[str, str, int | None, int | None]] = {}
    current: int | None = None
    for index, raw in enumerate(header):
        name = raw.strip().lower()
        if found := _QUALIFIABLE.match(raw):
            kind = " ".join(found.group(1).lower().split())
            key = found.group(2).strip()
            if kind == "unit":
                # Claims the following term columns away from the preceding
                # characteristic, which is the whole point of tracking it.
                current = None
            else:
                current = index
                bound[index] = (key, kind, None, None)
        elif name == _TERM_SOURCE and current is not None:
            key, kind, _, accession = bound[current]
            bound[current] = (key, kind, index, accession)
        elif name == _TERM_ACCESSION and current is not None:
            key, kind, source, _ = bound[current]
            bound[current] = (key, kind, source, index)
        elif name not in (_TERM_SOURCE, _TERM_ACCESSION):
            current = None
    return bound


def _sample_accession(key: str) -> str:
    """An accession from an SDRF sample key, where the key carries one.

    ``E-GEOD-*`` SDRFs have no ``Comment[BioSD_SAMPLE]`` column, so the key
    falls back to ``Source Name`` -- which for those studies is the GEO sample
    plus a suffix: ``GSM677627 1``. Returned as ``GSM677627``, because the
    whole point of the key is to join against the relation table and
    ``GSM677627 1`` joins to nothing.

    A key carrying no recognisable accession is returned unchanged rather than
    dropped: it is still the only identity that study offers, and a submitter
    name keys an SDRF to itself perfectly well.
    """
    head = key.split()[0] if key.split() else key
    parsed = try_parse(head)
    return parsed.value if parsed is not None else key


def _cell(row: list[str], index: int | None) -> str | None:
    """A cell by index, tolerating the short rows hand-edited SDRFs contain."""
    if index is None or index >= len(row):
        return None
    return row[index].strip() or None


def sdrf_characteristics(rows: list[list[str]]) -> dict[str, list[SdrfCharacteristic]]:
    """Curated characteristics per sample, from raw SDRF field lists.

    Takes ``sdrf_raw_rows`` output, never ``sdrf_rows``: the header repeats
    ``Term Source REF`` and ``Term Accession Number`` once per annotated column,
    and reading it through ``csv.DictReader`` keeps only the last of each --
    which silently strips the ontology terms from every characteristic but one.

    Keyed by the row's BioSample accession where the SDRF gives one, else its
    ENA sample, else the submitter's source name. One SDRF row is one
    *assay*, so several rows commonly describe one sample; their characteristics
    are merged and de-duplicated.
    """
    if not rows:
        return {}
    header, *data = rows
    bound = _bind_term_columns(header)
    key_columns = [
        (name, index)
        for name in SAMPLE_KEY_COLUMNS
        for index, raw in enumerate(header)
        if raw.strip().lower() == name
    ]

    collected: dict[str, list[SdrfCharacteristic]] = {}
    for row in data:
        sample = next(
            (value for _, index in key_columns if (value := _cell(row, index))),
            None,
        )
        if sample is None:
            continue
        sample = _sample_accession(sample)
        found = collected.setdefault(sample, [])
        for index, (key, kind, source_index, accession_index) in bound.items():
            value = _cell(row, index)
            if not value:
                continue
            characteristic = SdrfCharacteristic(
                key=key,
                value=value,
                kind=kind,
                term_source=_cell(row, source_index),
                term_accession=_cell(row, accession_index),
            )
            if characteristic not in found:
                found.append(characteristic)
    return collected
