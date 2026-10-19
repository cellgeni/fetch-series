"""Search node positions for the route graph page, so they can be reproduced.

    uv run fetch routes graph | uv run python scripts/route_graph_layout.py

Prints the ``ROW``, ``COLUMNS`` and ``POSITIONS`` constants that
``docs/javascripts/route-graph.js`` draws with. Two things are fixed, and only
what is left is searched:

* **Rows**, one per level, top to bottom: datasets, samples, libraries, runs,
  the records about the data, then read files, supplementary files and metadata
  documents. A record all three INSDC archives share -- a BioProject, a
  BioSample -- and BioSample metadata sit on a row of their own beside their
  level, over a band the page draws across the three INSDC columns. They are
  nodes of ordinary width, not the band itself: a node as wide as the span
  would sit across every line from the BioProject down, and a line passing
  under a node reads as though it started there.
* **Columns**, one per archive, left to right: GEO, NCBI, EBI, DDBJ,
  ArrayExpress and BioStudies. Every node stays inside its archive's column;
  a read format belongs to no archive, so the five share the INSDC span.

Within those bounds each node's x is searched, scored the way the first
hand-tuned layout was judged, in order of weight:

1. **No bundle through a node it does not belong to.** A line that passes
   under a third node reads as though it starts or ends there. A bundle of
   measured routes weighs three times a dotted untested one.
2. **Nodes in a row never overlap**, and keep room between them.
3. **Room for labels.** Every lane is named along its length, so a bundle's
   span must fit its longest name.
4. **Few crossings, then short lines.**

Two views are scored together, because the page draws both: the default, which
hides submissions, analyses and platforms, and the full one. The default
weighs more, since it is what a reader sees first.

Straight chords stand in for the drawn curves while positions are searched.
Then each bundle that still passes under a node it does not belong to is given
the least bow that clears *every* such node -- the page otherwise bows every
bundle the same slight amount -- and those bows are printed as ``BOWS``. A
bundle no bow clears keeps the page's curve: bending it to pass under fewer
nodes swept measured routes through unrelated columns, which read worse. A
bundle the default view draws is bent for that view. The ``through`` figures
reported count what each view actually draws, curves and all; the straight
chords understate it.

Letting BioProject and BioSample sit anywhere, rather than within the INSDC
columns, was tried with and without bows, on 2026-10-19: the search put both
back where they are, and found no fewer lines passing under nodes.

Pure Python and seeded, so the same graph gives the same positions.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import random
import sys
from collections.abc import Iterable
from dataclasses import dataclass

# Row y. The `_shared` rows hold the records and documents all three INSDC
# archives share, just above the level they belong to.
ROW = {
    "dataset_shared": 120,
    "dataset": 250,
    "sample_shared": 440,
    "sample": 570,
    "library": 760,
    "run": 940,
    "other": 1120,
    "file": 1320,
    "supplementary": 1500,
    "metadata": 1680,
    "metadata_shared": 1810,
}
SHARED_ROW = {"bioproject": "dataset_shared", "biosample": "sample_shared"}
SHARED_ROW_OF_METADATA = {"metadata_biosample": "metadata_shared"}

# Column bands, left edge to right edge.
COLUMN_WIDTH = 520
COLUMNS = ("geo", "ncbi", "ebi", "ddbj", "arrayexpress")
LEFT = 200
BAND = {
    column: (LEFT + i * COLUMN_WIDTH, LEFT + (i + 1) * COLUMN_WIDTH)
    for i, column in enumerate(COLUMNS)
}
BAND["insdc"] = (BAND["ncbi"][0], BAND["ddbj"][1])

NODE_HEIGHT = 54
LANE = 26  # between parallel lanes mid-way, as in route-graph.js
ARROW = 11
LABEL_CHAR = 7.2  # 12px monospace
NODE_CHAR = 8.6  # 15px semibold
SHAPE_PADDING = {
    "dataset": 44,
    "sample": 52,
    "library": 70,
    "run": 64,
    "other": 40,
    "file": 56,
    "supplementary": 56,
    "metadata": 48,
}
STEP = 10
GAP = 36  # between neighbours in a row

W_THROUGH = 1000.0
W_OVERLAP = 5000.0
W_LABEL = 2.0
W_CROSS = 3.0
W_LENGTH = 0.004
W_DEFAULT, W_FULL = 1.0, 0.5

# Bows a bundle may take, as a fraction of its length: how far its middle sits
# off the straight line, to the left of travel when positive. The page's own
# are 0.03 for a lone direction and at least 0.06 beside its reverse
# (route-graph.js); anything else here is there to clear a node.
BOW_CHOICES = (0.03, 0.06, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5)
SAMPLES = 24  # straight pieces a curve is tested as
ROW_SLACK = 90  # how far past the rows of its ends a curve may bow
CLEARANCE = 18  # room a bent curve leaves a node, against the 10 a straight one is held to


@dataclass(frozen=True, slots=True)
class Bundle:
    source: str
    target: str
    lanes: int
    label: int  # characters in the longest lane name
    other: bool  # touches a node only the full view shows
    measured: bool  # carries at least one route


def bundles(graph: dict) -> list[Bundle]:
    """One per drawn direction, as the page bundles them: routes by rank, then untested."""
    other = {n["id"] for n in graph["nodes"] if n["layer"] == "other"}
    lanes: dict[tuple[str, str], list[int]] = {}
    routes: set[tuple[str, str]] = set()
    for edge in graph["edges"]:
        if edge["source"] == edge["target"]:
            continue  # a self link is a loop above its node, not a span
        if edge["status"] == "measured":
            routes.add((edge["source"], edge["target"]))
            length = len(edge["route"].split(":", 1)[1])
        else:
            names = sorted({graph["mechanisms"][e["mechanism"]]["name"] for e in edge["links"]})
            shown = names[:2] + ([f"+{len(names) - 2}"] if len(names) > 2 else [])
            length = len(" · ".join(shown))
        lanes.setdefault((edge["source"], edge["target"]), []).append(length)
    return [
        Bundle(s, t, len(names), max(names), s in other or t in other, (s, t) in routes)
        for (s, t), names in sorted(lanes.items())
    ]


def row_of(node: dict) -> str:
    return SHARED_ROW.get(node["id"]) or SHARED_ROW_OF_METADATA.get(node["id"]) or node["layer"]


def widths(graph: dict) -> dict[str, float]:
    found = {}
    for n in graph["nodes"]:
        text = max(len(n["label"]), len(n.get("prefix") or "") * 0.85)
        found[n["id"]] = max(120.0, text * NODE_CHAR + SHAPE_PADDING[n["layer"]])
    return found


def _clip(p, q, box) -> bool:
    """Whether segment p-q meets the axis-aligned box (Liang-Barsky)."""
    (x0, y0), (x1, y1) = p, q
    left, top, right, bottom = box
    dx, dy = x1 - x0, y1 - y0
    lo, hi = 0.0, 1.0
    for edge_p, edge_q in ((-dx, x0 - left), (dx, right - x0), (-dy, y0 - top), (dy, bottom - y0)):
        if edge_p == 0:
            if edge_q < 0:
                return False
            continue
        r = edge_q / edge_p
        if edge_p < 0:
            lo = max(lo, r)
        else:
            hi = min(hi, r)
        if lo > hi:
            return False
    return True


def _cross(a, b, c, d) -> bool:
    def orient(p, q, r):
        return (q[0] - p[0]) * (r[1] - p[1]) - (q[1] - p[1]) * (r[0] - p[0])

    d1, d2 = orient(c, d, a), orient(c, d, b)
    d3, d4 = orient(a, b, c), orient(a, b, d)
    return d1 * d2 < 0 and d3 * d4 < 0


class Layout:
    def __init__(self, graph: dict) -> None:
        self.nodes = [n["id"] for n in graph["nodes"]]
        self.layer = {n["id"]: n["layer"] for n in graph["nodes"]}
        self.row = {n["id"]: row_of(n) for n in graph["nodes"]}
        self.width = widths(graph)
        self.fixed: set[str] = set()
        self.column = {n["id"]: n["column"] for n in graph["nodes"]}
        self.band = {}
        for n in graph["nodes"]:
            lo, hi = BAND[n["column"]]
            half = self.width[n["id"]] / 2
            self.band[n["id"]] = (lo + half + GAP / 2, hi - half - GAP / 2)
        self.bundles = bundles(graph)
        self.other = {n for n in self.nodes if self.layer[n] == "other"}
        self.incident: dict[str, list[int]] = {n: [] for n in self.nodes}
        for i, b in enumerate(self.bundles):
            self.incident[b.source].append(i)
            self.incident[b.target].append(i)
        self.row_mates = {
            n: [m for m in self.nodes if m != n and self.row[m] == self.row[n]] for n in self.nodes
        }
        self.movable = [n for n in self.nodes if n not in self.fixed]
        self.x: dict[str, float] = {}
        pairs = {(b.source, b.target) for b in self.bundles}
        self.reverse = [(b.target, b.source) in pairs for b in self.bundles]
        self.bow: dict[int, float] = {}  # the searched bows; the page's own otherwise

    def at(self, node: str) -> tuple[float, float]:
        return (self.x[node], ROW[self.row[node]])

    def drawn_at(self, node: str, default: bool) -> tuple[float, float]:
        """Where the page draws ``node``: the default view closes up the row it hides."""
        x, y = self.at(node)
        if default and y > ROW["other"]:
            y -= ROW["file"] - ROW["other"]
        return (x, y)

    def obstacles(self, i: int, default: bool) -> list[str]:
        """The nodes bundle ``i`` must not pass under, in one view."""
        b = self.bundles[i]
        return [
            n
            for n in self.nodes
            if n not in (b.source, b.target) and not (default and n in self.other)
        ]

    def _weight(self, *nodes: str) -> float:
        return W_FULL if any(n in self.other for n in nodes) else W_DEFAULT + W_FULL

    # -------------------------------------------------------------- terms

    def _box(
        self, i: int, node: str, room: float = 10, default: bool = False
    ) -> tuple[float, float, float, float]:
        """``node``, grown by half of bundle ``i``'s width and ``room``."""
        margin = (self.bundles[i].lanes - 1) / 2 * LANE + room
        x, y = self.drawn_at(node, default)
        half = self.width[node] / 2 + margin
        return (x - half, y - NODE_HEIGHT / 2 - margin, x + half, y + NODE_HEIGHT / 2 + margin)

    def _through_weight(self, i: int, node: str) -> float:
        b = self.bundles[i]
        weight = W_THROUGH * (1.0 if b.measured else 1 / 3)
        return weight * self._weight(b.source, b.target, node)

    def through(self, i: int, node: str) -> float:
        """Bundle ``i``, drawn straight, passing under ``node``."""
        b = self.bundles[i]
        hit = _clip(self.at(b.source), self.at(b.target), self._box(i, node))
        return self._through_weight(i, node) if hit else 0.0

    # -------------------------------------------------------------- bows

    def page_bow(self, i: int, default: bool = False) -> float:
        """The bow route-graph.js gives bundle ``i`` unless told otherwise."""
        b = self.bundles[i]
        if not self.reverse[i]:
            return 0.03
        chord = math.dist(self.drawn_at(b.source, default), self.drawn_at(b.target, default))
        return max(0.06, ((b.lanes - 1) / 2 * LANE + 20) / chord)

    def curve(self, i: int, bow: float, default: bool = False) -> list[tuple[float, float]]:
        """Bundle ``i`` as the page draws it with ``bow``: a quadratic, as points."""
        b = self.bundles[i]
        (x0, y0), (x1, y1) = self.drawn_at(b.source, default), self.drawn_at(b.target, default)
        chord = math.hypot(x1 - x0, y1 - y0)
        left = ((y1 - y0) / chord, -(x1 - x0) / chord)  # of travel, with y down
        cx = (x0 + x1) / 2 + left[0] * 2 * bow * chord
        cy = (y0 + y1) / 2 + left[1] * 2 * bow * chord
        points = []
        for k in range(SAMPLES + 1):
            u = k / SAMPLES
            a, m, c = (1 - u) ** 2, 2 * (1 - u) * u, u * u
            points.append((a * x0 + m * cx + c * x1, a * y0 + m * cy + c * y1))
        return points

    def curved_through(
        self, i: int, node: str, bow: float, room: float = 10, default: bool = False
    ) -> float:
        points = self.curve(i, bow, default)
        box = self._box(i, node, room, default)
        hit = any(_clip(p, q, box) for p, q in itertools.pairwise(points))
        return self._through_weight(i, node) if hit else 0.0

    def bow_of(self, i: int, default: bool = False) -> float:
        own = self.page_bow(i, default)
        if i not in self.bow:
            return own
        return max(own, self.bow[i]) if self.reverse[i] else self.bow[i]

    def bend(self) -> None:
        """Bend each bundle that passes under a node just enough to clear every one.

        Positions are fixed by now, so each bundle is decided alone, and only a
        bow that clears *every* node it does not touch is taken -- with room to
        spare, so it does not skim one either. A bow that merely passes under
        fewer would trade three short crossings for one long sweep through an
        unrelated archive's column, which reads worse; such a bundle keeps the
        page's own curve.

        A bundle beside its reverse bows only further to its own side, so the
        two never cross; a lone one may bow either way. A curve that would leave
        the columns, or stray past the rows of its own two ends, is not offered:
        one that dips through a row it does not reach reads as reaching it.

        A bundle the default view draws is bent for that view: its geometry,
        with the hidden row closed up, and only the nodes it shows. Bending
        round a submission nobody can see would put a kink in every reader's
        first view for the sake of the second.
        """
        left, right = BAND[COLUMNS[0]][0], BAND[COLUMNS[-1]][1]
        for i, b in enumerate(self.bundles):
            default = not b.other
            own = self.page_bow(i, default)
            others = self.obstacles(i, default)
            if not any(self.curved_through(i, n, own, default=default) for n in others):
                continue
            ends = (self.drawn_at(b.source, default)[1], self.drawn_at(b.target, default)[1])
            top, bottom = min(ends) - ROW_SLACK, max(ends) + ROW_SLACK
            sides = (1,) if self.reverse[i] else (1, -1)
            offered = {
                side * bow
                for bow in BOW_CHOICES
                for side in sides
                if not self.reverse[i] or bow > own
            }
            # Nearest the page's own bow first: the gentlest curve that clears.
            for bow in sorted(offered, key=lambda bow, own=own: (abs(bow - own), -bow)):
                points = self.curve(i, bow, default)
                if not all(left <= x <= right and top <= y <= bottom for x, y in points):
                    continue
                if not any(self.curved_through(i, n, bow, CLEARANCE, default) for n in others):
                    self.bow[i] = bow
                    break

    def span(self, i: int) -> float:
        """Label shortfall and length of one bundle."""
        b = self.bundles[i]
        (x0, y0), (x1, y1) = self.at(b.source), self.at(b.target)
        chord = math.hypot(x1 - x0, y1 - y0)
        if chord < 1.0:
            return W_OVERLAP  # two ends on one spot: no room for anything
        ux, uy = abs(x1 - x0) / chord, abs(y1 - y0) / chord

        def inside(node: str) -> float:
            hw, hh = self.width[node] / 2, NODE_HEIGHT / 2
            return min(hw / ux if ux else math.inf, hh / uy if uy else math.inf)

        room = chord - inside(b.source) - inside(b.target) - ARROW
        need = b.label * LABEL_CHAR + 24
        weight = self._weight(b.source, b.target)
        return weight * (W_LABEL * max(0.0, need - room) + W_LENGTH * chord * b.lanes)

    def crossing(self, i: int, j: int) -> float:
        a, b = self.bundles[i], self.bundles[j]
        if {a.source, a.target} & {b.source, b.target}:
            return 0.0
        hit = _cross(self.at(a.source), self.at(a.target), self.at(b.source), self.at(b.target))
        return W_CROSS * self._weight(a.source, a.target, b.source, b.target) if hit else 0.0

    def overlap(self, node: str, mate: str) -> float:
        room = (self.width[node] + self.width[mate]) / 2 + GAP
        return W_OVERLAP if abs(self.x[node] - self.x[mate]) < room else 0.0

    def touching(self, node: str) -> float:
        """Every term that involves ``node``, each once."""
        total = 0.0
        mine = self.incident[node]
        mine_set = set(mine)
        for i in mine:
            b = self.bundles[i]
            total += self.span(i)
            for other in self.nodes:
                if other not in (b.source, b.target):
                    total += self.through(i, other)
            for j in range(len(self.bundles)):
                if j not in mine_set:
                    total += self.crossing(i, j)
        for i in range(len(self.bundles)):
            if i not in mine_set:
                total += self.through(i, node)
        for mate in self.row_mates[node]:
            total += self.overlap(node, mate)
        return total

    def energy(self) -> float:
        total = sum(self.span(i) for i in range(len(self.bundles)))
        for i, b in enumerate(self.bundles):
            for node in self.nodes:
                if node not in (b.source, b.target):
                    total += self.through(i, node)
            for j in range(i + 1, len(self.bundles)):
                total += self.crossing(i, j)
        for n in self.nodes:
            for m in self.row_mates[n]:
                if n < m:
                    total += self.overlap(n, m)
        return total

    # ------------------------------------------------------------- search

    def clamp(self, node: str, x: float) -> float:
        lo, hi = self.band[node]
        if lo > hi:  # a node as wide as its band sits in its middle
            return round((lo + hi) / 2 / STEP) * STEP
        return min(hi, max(lo, round(x / STEP) * STEP))

    def move(self, node: str, x: float) -> float:
        """Move ``node`` to ``x`` and return the change in energy."""
        before = self.touching(node)
        self.x[node] = x
        return self.touching(node) - before

    def anneal(self, rng: random.Random, iterations: int) -> float:
        energy = self.energy()
        best, best_x = energy, dict(self.x)
        for k in range(iterations):
            temperature = 300.0 * (1 - k / iterations) + 0.5
            node = rng.choice(self.movable)
            old = dict(self.x)
            mates = [m for m in self.row_mates[node] if m in self.movable]
            if mates and rng.random() < 0.3:
                mate = rng.choice(mates)
                delta = self.move(node, self.clamp(node, old[mate])) + self.move(
                    mate, self.clamp(mate, old[node])
                )
            else:
                x = self.clamp(node, old[node] + rng.gauss(0, 120))
                if x == old[node]:
                    continue
                delta = self.move(node, x)
            if delta <= 0 or rng.random() < math.exp(-delta / temperature):
                energy += delta
                if energy < best - 1e-9:
                    best, best_x = energy, dict(self.x)
            else:
                self.x = old
        self.x = best_x
        return best

    def drawn_through(self, default: bool) -> dict[str, int]:
        """Bundles passing under a node they do not belong to, as one view draws them."""
        found = {"routes": 0, "untested": 0}
        for i, b in enumerate(self.bundles):
            if default and b.other:
                continue
            bow = self.bow_of(i, default)
            hits = sum(
                1
                for n in self.obstacles(i, default)
                if self.curved_through(i, n, bow, default=default)
            )
            found["routes" if b.measured else "untested"] += hits
        return found

    def report(self) -> dict[str, object]:
        """What a reader sees: bundles drawn with their bows, not as chords."""
        crossings = sum(
            1
            for i in range(len(self.bundles))
            for j in range(i + 1, len(self.bundles))
            if self.crossing(i, j)
        )
        overlaps = sum(
            1 for n in self.nodes for m in self.row_mates[n] if n < m and self.overlap(n, m)
        )
        return {
            "energy": round(self.energy(), 1),
            "through": self.drawn_through(default=True),
            "through_full": self.drawn_through(default=False),
            "bent": len(self.bow),
            "crossings": crossings,
            "overlaps": overlaps,
        }


def initial(layout: Layout, rng: random.Random) -> None:
    """Spread each row's nodes evenly across their bands, with a little jitter."""
    for node in layout.fixed:
        lo, hi = BAND["insdc"]
        layout.x[node] = (lo + hi) / 2
    cells: dict[tuple[str, str], list[str]] = {}
    for node in layout.movable:
        cells.setdefault((layout.row[node], layout.column[node]), []).append(node)
    for (_, column), members in cells.items():
        rng.shuffle(members)
        lo, hi = BAND[column]
        for k, node in enumerate(members, start=1):
            layout.x[node] = layout.clamp(node, lo + k * (hi - lo) / (len(members) + 1))


def search(graph: dict, seeds: Iterable[int], iterations: int) -> Layout:
    best: Layout | None = None
    best_energy = math.inf
    for seed in seeds:
        rng = random.Random(seed)
        layout = Layout(graph)
        initial(layout, rng)
        energy = layout.anneal(rng, iterations)
        layout.bend()
        print(f"seed {seed}: {layout.report()}", file=sys.stderr)
        if energy < best_energy:
            best, best_energy = layout, energy
    assert best is not None
    return best


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--seeds", type=int, default=4)
    parser.add_argument("--iterations", type=int, default=6000)
    args = parser.parse_args()
    graph = json.load(sys.stdin)
    layout = search(graph, range(args.seeds), args.iterations)
    print(
        f"// {layout.report()} -- scripts/route_graph_layout.py, "
        f"{args.seeds} seeds x {args.iterations}"
    )
    print("const ROW = { " + ", ".join(f"{row}: {y}" for row, y in ROW.items()) + " };")
    print(
        "const COLUMNS = { "
        + ", ".join(f"{c}: [{BAND[c][0]}, {BAND[c][1]}]" for c in COLUMNS)
        + " };"
    )
    print("const POSITIONS = {")
    for node in layout.nodes:
        print(f"  {node}: {{ x: {int(layout.x[node])}, y: ROW.{layout.row[node]} }},")
    print("};")
    print("const BOWS = {")
    for i, b in enumerate(layout.bundles):
        if i in layout.bow:
            print(f'  "{b.source}>{b.target}": {layout.bow[i]},')
    print("};")


if __name__ == "__main__":
    main()
