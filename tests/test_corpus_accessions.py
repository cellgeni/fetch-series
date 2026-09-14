"""The accession registry, checked against the real reprocessed corpus.

``data/All_10x.sample_table.tsv`` is 104,442 rows of accessions that actually
went through reprocessing, so it is the best available evidence that the
registry covers what the archives really emit. Marked ``slow`` and skipped when
the file is absent, because CI does not fetch Git LFS objects.
"""

from pathlib import Path

import pytest

from fetch_series.accession import try_parse

pytestmark = pytest.mark.slow

CORPUS = Path("data/All_10x.sample_table.tsv")
ACCESSION_COLUMNS = ("sample", "gse", "prj", "srs", "srx", "srr")
# `-` is the table's placeholder for a missing value. It must stay unparsed:
# it is not an identifier, and as a `grep -f` pattern it matches every line of
# a metadata table.
PLACEHOLDER = "-"


@pytest.fixture(scope="module")
def corpus_tokens() -> list[str]:
    if not CORPUS.exists():
        pytest.skip(f"{CORPUS} not present (Git LFS object not fetched)")
    tokens: list[str] = []
    with CORPUS.open() as fh:
        for line in fh:
            fields = line.rstrip("\n").split("\t")[: len(ACCESSION_COLUMNS)]
            for field in fields:
                tokens.extend(t.strip() for t in field.split(",") if t.strip())
    return tokens


def test_registry_covers_the_whole_corpus(corpus_tokens: list[str]):
    """Every token is either a known accession or the `-` placeholder."""
    unrecognised = {t for t in corpus_tokens if try_parse(t) is None}
    assert unrecognised <= {PLACEHOLDER}, f"registry does not cover: {sorted(unrecognised)[:20]}"


def test_corpus_is_the_size_we_think_it_is(corpus_tokens: list[str]):
    """A guard against silently swapping the corpus underneath the survey results."""
    recognised = [t for t in corpus_tokens if try_parse(t) is not None]
    assert len(recognised) == 792_610
