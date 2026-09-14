"""Offline tests for the accession registry."""

import pytest

from fetch_series.accession import (
    RULES,
    Accession,
    Archive,
    EntityType,
    UnknownAccessionError,
    parse,
    parse_all,
    try_parse,
)


@pytest.mark.parametrize(
    ("raw", "entity", "archive"),
    [
        ("GSE236084", EntityType.GEO_SERIES, Archive.GEO),
        ("GSM1234567", EntityType.GEO_SAMPLE, Archive.GEO),
        ("GPL24676", EntityType.GEO_PLATFORM, Archive.GEO),
        ("PRJNA988806", EntityType.BIOPROJECT, Archive.NCBI),
        ("PRJEB42537", EntityType.BIOPROJECT, Archive.EBI),
        ("PRJDB12345", EntityType.BIOPROJECT, Archive.DDBJ),
        ("SAMN12345678", EntityType.BIOSAMPLE, Archive.NCBI),
        ("SAMEA5053920", EntityType.BIOSAMPLE, Archive.EBI),
        ("SAMD00123456", EntityType.BIOSAMPLE, Archive.DDBJ),
        ("SRP446371", EntityType.STUDY, Archive.NCBI),
        ("ERP126408", EntityType.STUDY, Archive.EBI),
        ("DRP000001", EntityType.STUDY, Archive.DDBJ),
        ("SRR25056225", EntityType.RUN, Archive.NCBI),
        ("ERR2861957", EntityType.RUN, Archive.EBI),
        ("DRR730719", EntityType.RUN, Archive.DDBJ),
        ("SRX1234567", EntityType.EXPERIMENT, Archive.NCBI),
        ("SRS9161836", EntityType.SAMPLE, Archive.NCBI),
        ("DRS188691", EntityType.SAMPLE, Archive.DDBJ),
        ("E-MTAB-6505", EntityType.AE_EXPERIMENT, Archive.ARRAYEXPRESS),
        ("E-GEOD-12345", EntityType.AE_EXPERIMENT, Archive.ARRAYEXPRESS),
        ("S-BSST123", EntityType.BIOSTUDY, Archive.BIOSTUDIES),
    ],
)
def test_parses_known_namespaces(raw: str, entity: EntityType, archive: Archive):
    accession = parse(raw)
    assert accession.value == raw
    assert accession.entity is entity
    assert accession.archive is archive


def test_insdc_prefix_letter_decides_the_archive():
    """S/E/D share a schema but are issued by three different archives."""
    assert parse("SRR1").archive is Archive.NCBI
    assert parse("ERR1").archive is Archive.EBI
    assert parse("DRR1").archive is Archive.DDBJ
    assert all(parse(a).entity is EntityType.RUN for a in ("SRR1", "ERR1", "DRR1"))


def test_normalises_case_and_whitespace():
    assert parse("  gse236084 ").value == "GSE236084"
    assert parse("e-mtab-6505").value == "E-MTAB-6505"


@pytest.mark.parametrize(
    "raw",
    [
        "-",  # the placeholder the survey table uses for a missing value
        "",
        "GSE",  # prefix with no number
        "GSE12a",  # trailing junk
        "xGSE12",  # leading junk
        "E-MTAB6505",  # ArrayExpress accessions are hyphenated
        "PRJNA",
        "NOTANACCESSION",
    ],
)
def test_rejects_non_accessions(raw: str):
    with pytest.raises(UnknownAccessionError):
        parse(raw)
    assert try_parse(raw) is None


def test_namespaces_do_not_overlap():
    """Two rules matching one accession would make parsing order-dependent."""
    samples = [
        "GSE1",
        "GSM1",
        "GPL1",
        "PRJNA1",
        "PRJEB1",
        "PRJDB1",
        "PRJEA1",
        "PRJDA1",
        "SAMN1",
        "SAMEA1",
        "SAMD1",
        "SRP1",
        "SRS1",
        "SRX1",
        "SRR1",
        "SRA1",
        "SRZ1",
        "ERP1",
        "ERS1",
        "ERX1",
        "ERR1",
        "ERA1",
        "DRP1",
        "DRS1",
        "DRX1",
        "DRR1",
        "E-MTAB-1",
        "S-BSST1",
    ]
    for value in samples:
        matching = [r.name for r in RULES if r.pattern.fullmatch(value)]
        assert len(matching) == 1, f"{value} matched {matching}"


def test_parse_all_separates_unrecognised_values():
    """`-` must be reported, not silently dropped: as a grep pattern it matches everything."""
    parsed, unknown = parse_all(["GSE1", "-", "SRR2", "junk"])
    assert [a.value for a in parsed] == ["GSE1", "SRR2"]
    assert unknown == ["-", "junk"]


@pytest.mark.parametrize(
    ("raw", "stem"),
    [
        ("GSE236084", "GSE236nnn"),
        ("GSE1000", "GSE1nnn"),
        ("GSE999", "GSEnnn"),
        ("GSE1", "GSEnnn"),
        ("GSM1234567", "GSM1234nnn"),
    ],
)
def test_geo_ftp_stem(raw: str, stem: str):
    """The masked directory GEO buckets its FTP tree by."""
    assert parse(raw).geo_ftp_stem == stem


def test_geo_ftp_stem_rejects_non_geo():
    with pytest.raises(ValueError, match="not a GEO accession"):
        _ = parse("SRR1").geo_ftp_stem


def test_is_insdc():
    assert parse("SRR1").is_insdc
    assert parse("ERP1").is_insdc
    assert not parse("GSE1").is_insdc
    assert not parse("PRJNA1").is_insdc
    assert not parse("SAMN1").is_insdc


def test_accession_is_hashable_and_frozen():
    """Accessions are dict keys and set members throughout the resolver."""
    a = parse("GSE1")
    assert {a, parse("GSE1")} == {a}
    with pytest.raises(AttributeError):
        a.value = "GSE2"  # type: ignore[misc]
    assert isinstance(a, Accession)
    assert str(a) == "GSE1"
