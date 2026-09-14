"""Offline tests for credential redaction.

These exist because the NCBI API key reached a public repository through log
files: retry warnings render the full request URL, and the survey logs were
committed next to their results.
"""

import logging

from fetch_series.logging_utils import REDACTED, configure_logging, redact, run_logfile

KEY = "0123456789abcdef0123456789abcdef1234"


def test_redacts_api_key_in_url():
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=sra&api_key={KEY}"
    out = redact(url)
    assert KEY not in out
    assert REDACTED in out
    # The rest of the URL has to survive, or the log line stops being useful.
    assert "db=sra" in out


def test_redacts_when_key_is_not_the_last_parameter():
    out = redact(f"esearch.fcgi?api_key={KEY}&term=PRJNA988806&retmode=json")
    assert KEY not in out
    assert "term=PRJNA988806" in out
    assert "retmode=json" in out


def test_redacts_case_insensitively_and_other_credentials():
    assert KEY not in redact(f"?API_KEY={KEY}")
    assert "sekrit" not in redact("?access_token=sekrit")
    assert "sekrit" not in redact("?password=sekrit")


def test_leaves_innocuous_text_alone():
    text = "Resolved PRJNA988806 -> GSE236084 in 412 ms"
    assert redact(text) == text


def test_formatter_redacts_log_records(tmp_path):
    logfile = tmp_path / "run.log"
    configure_logging(level=logging.INFO, logfile=logfile)
    logging.getLogger("test").warning("Retrying %s", f"https://x/e.fcgi?api_key={KEY}")
    logging.shutdown()

    contents = logfile.read_text()
    assert KEY not in contents
    assert REDACTED in contents


def test_formatter_redacts_tracebacks(tmp_path):
    """URLs leak through exception text too, not just the format string."""
    logfile = tmp_path / "run.log"
    configure_logging(level=logging.INFO, logfile=logfile)
    try:
        raise RuntimeError(f"connect failed for https://x/e.fcgi?api_key={KEY}")
    except RuntimeError:
        logging.getLogger("test").exception("request failed")
    logging.shutdown()

    assert KEY not in logfile.read_text()


def test_logfile_is_appended_not_truncated(tmp_path):
    """A re-run must not destroy the log documenting the previous results."""
    logfile = tmp_path / "run.log"
    for message in ("first run", "second run"):
        configure_logging(level=logging.INFO, logfile=logfile)
        logging.getLogger("test").warning(message)
        logging.shutdown()

    contents = logfile.read_text()
    assert "first run" in contents
    assert "second run" in contents


def test_run_logfile_is_per_route_and_timestamped(tmp_path):
    path = run_logfile("bioproject2sra", root=tmp_path)
    assert path.parent == tmp_path / "bioproject2sra"
    assert path.suffix == ".log"
    assert path.name.endswith("Z.log")
