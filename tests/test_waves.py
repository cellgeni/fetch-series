"""Every link is reported in exactly one declared wave."""

from __future__ import annotations

from fetch_series.catalogue import CATALOGUE
from fetch_series.catalogue.waves import WAVES, links_through, wave_of


def test_every_link_has_a_declared_wave():
    assert {wave_of(link) for link in CATALOGUE} <= set(WAVES)


def test_waves_are_cumulative():
    sizes = [len(links_through(w)) for w in sorted(WAVES)]
    assert sizes == sorted(sizes) and sizes[-1] == len(CATALOGUE.links)


def test_the_waves_are_the_plans():
    assert wave_of(CATALOGUE.link("run_ebi->sample_ebi:ena_filereport")) == 1
    assert wave_of(CATALOGUE.link("run_ncbi->metadata_ncbi:sra_be_runinfo_document")) == 2
    assert wave_of(CATALOGUE.link("ae_experiment->ae_sample:biostudies_sdrf")) == 4
    assert wave_of(CATALOGUE.link("run_ncbi->sra_normalized:sdl")) == 5
