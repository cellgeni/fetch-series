"""The resolved record: one row per run, with every cross-archive link on it.

Deliberately built after the surveys rather than before. Guessing at a field set
first would have produced a model shaped around what the archives *ought* to
return; these fields are the ones the censuses showed are actually available and
actually disagree.

The unit is the **run**, because a run is what carries files. Everything else --
GEO sample, experiment, INSDC sample, BioSample, study, project -- is a link
hanging off it, and any of them can be missing without the run being unusable.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields

# One row per run. Order is the column order of the exported table.
RELATION_COLUMNS = (
    "run",
    "experiment",
    "sample",
    "biosample",
    "study",
    "bioproject",
    "geo_sample",
    "geo_series",
    "ae_experiment",
    "species",
    "library_strategy",
)

# The table's placeholder for a value no route supplied. Spelled out rather than
# left empty because a run of tabs collapses in shell tooling and shifts every
# column after it -- the same reason fetch10xmeta writes "-".
MISSING = "-"


@dataclass(slots=True)
class RunRecord:
    """Everything known about one run, and where each part came from."""

    run: str
    experiment: str | None = None
    sample: str | None = None
    biosample: str | None = None
    study: str | None = None
    bioproject: str | None = None
    geo_sample: str | None = None
    geo_series: str | None = None
    ae_experiment: str | None = None
    species: str | None = None
    library_strategy: str | None = None
    # field name -> the route ids that supplied it, in the order they answered.
    provenance: dict[str, list[str]] = field(default_factory=dict)
    # Field names where two routes supplied different values.
    conflicts: dict[str, list[str]] = field(default_factory=dict)

    def set(self, name: str, value: str | None, route_id: str) -> None:
        """Record a value for ``name``, keeping the first and noting conflicts.

        First-wins rather than last-wins, because routes are offered in measured
        order: the first answer comes from the route the evidence ranks highest.
        A second, different answer is not discarded silently -- it is recorded in
        :attr:`conflicts`, which is the only honest thing to do when two archives
        disagree about the same field.
        """
        if not value or value == MISSING:
            return
        self.provenance.setdefault(name, []).append(route_id)
        current = getattr(self, name)
        if current is None:
            setattr(self, name, value)
        elif current != value and value not in self.conflicts.get(name, []):
            self.conflicts.setdefault(name, [current]).append(value)

    def as_row(self) -> list[str]:
        return [getattr(self, column) or MISSING for column in RELATION_COLUMNS]

    @property
    def is_complete_for_reprocessing(self) -> bool:
        """Whether this run can be turned into a samplesheet line.

        A run needs an identity to group by and a species to pick a reference.
        The sample link may come from GEO or from INSDC; either will do, but
        without one there is nothing to aggregate runs into a sample.
        """
        has_sample = any((self.geo_sample, self.sample, self.biosample))
        return bool(self.run and has_sample and self.species)

    @property
    def missing_for_reprocessing(self) -> list[str]:
        """Which required parts are absent, for reporting rather than guessing."""
        gaps = []
        if not any((self.geo_sample, self.sample, self.biosample)):
            gaps.append("sample")
        if not self.species:
            gaps.append("species")
        return gaps


def field_names() -> tuple[str, ...]:
    """The record's data fields, excluding provenance bookkeeping."""
    return tuple(f.name for f in fields(RunRecord) if f.name not in {"provenance", "conflicts"})
