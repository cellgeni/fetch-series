"""Search node positions for the route graph page, so they can be reproduced.

    uv run fetch routes graph | uv run python scripts/route_graph_layout.py

Prints the ``ROW`` and ``POSITIONS`` constants that
``docs/javascripts/route-graph.js`` draws with. Rows are fixed -- one per level,
top to bottom -- so only each node's x is searched. A placement is scored the
way the first hand-tuned layout was judged, in order of weight:

1. **No bundle through a node it does not belong to.** A line that passes
   under a third node reads as though it starts or ends there. A bundle of
   measured routes weighs three times a dotted untested one: the first layout
   had none, and the routes are what the page is for.
2. **Nodes in a row never overlap**, and keep room between them.
3. **Room for labels.** Every lane is named along its length, so a bundle's
   span must fit its longest name.
4. **Archive order across a row**: GEO left, INSDC in the middle, ArrayExpress
   right, as the families are coloured.
5. **Few crossings, then short lines.**

Two views are scored together, because the page draws both: the default, which
hides submissions, analyses and platforms, and the full one. The default
weighs more, since it is what a reader sees first.

Straight chords stand in for the drawn curves, which bow only slightly. Pure
Python and seeded, so the same graph gives the same positions.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
from collections.abc import Iterable
from dataclasses import dataclass

ROW = {"dataset": 150, "sample": 410, "library": 650, "run": 870, "file": 1090, "other": 1310}
NODE_HEIGHT = 54
LANE = 26  # between parallel lanes mid-way, as in route-graph.js
ARROW = 11
LABEL_CHAR = 7.2  # 12px monospace
NODE_CHAR = 8.6  # 15px semibold
SHAPE_PADDING = {"dataset": 44, "sample": 52, "library": 70, "run": 64, "file": 56, "other": 40}
X_MIN, X_MAX, STEP = 250, 2500, 25
GAP = 70  # between neighbours in a row

# Left to right within a row. Files and the "other" records are placed by the
# archive whose record they are.
ARCHIVE_ORDER = {
    "geo_series": 0,
    "geo_sample": 0,
    "geo_files": 0,
    "geo_platform": 0,
    "ae_experiment": 2,
    "biostudy": 3,
    "ae_sample": 2,
    "ae_files": 2,
}

W_THROUGH = 1000.0
W_OVERLAP = 5000.0
W_LABEL = 2.0
W_ORDER = 150.0
W_CROSS = 3.0
W_LENGTH = 0.004
W_DEFAULT, W_FULL = 1.0, 0.5


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
            name = edge["id"].split(":", 1)[1]
            length = len(name)
        else:
            names = sorted({graph["mechanisms"][e["mechanism"]]["name"] for e in edge["links"]})
            shown = names[:2] + ([f"+{len(names) - 2}"] if len(names) > 2 else [])
            length = len(" · ".join(shown))
        lanes.setdefault((edge["source"], edge["target"]), []).append(length)
    return [
        Bundle(s, t, len(names), max(names), s in other or t in other, (s, t) in routes)
        for (s, t), names in sorted(lanes.items())
    ]


def widths(graph: dict) -> dict[str, float]:
    return {
        n["id"]: max(130.0, len(n["label"]) * NODE_CHAR + SHAPE_PADDING[n["layer"]])
        for n in graph["nodes"]
    }


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
        self.family = {n["id"]: n["family"] for n in graph["nodes"]}
        self.width = widths(graph)
        self.bundles = bundles(graph)
        self.other = {n for n in self.nodes if self.layer[n] == "other"}
        self.incident: dict[str, list[int]] = {n: [] for n in self.nodes}
        for i, b in enumerate(self.bundles):
            self.incident[b.source].append(i)
            self.incident[b.target].append(i)
        self.row_mates = {
            n: [m for m in self.nodes if m != n and self.layer[m] == self.layer[n]]
            for n in self.nodes
        }
        self.x: dict[str, float] = {}

    def at(self, node: str) -> tuple[float, float]:
        return (self.x[node], ROW[self.layer[node]])

    def _weight(self, *nodes: str) -> float:
        return W_FULL if any(n in self.other for n in nodes) else W_DEFAULT + W_FULL

    def _order(self, node: str) -> int:
        if node in ARCHIVE_ORDER:
            return ARCHIVE_ORDER[node]
        return {"geo": 0, "insdc": 1, "arrayexpress": 2}.get(self.family[node], 1)

    # -------------------------------------------------------------- terms

    def through(self, i: int, node: str) -> float:
        b = self.bundles[i]
        margin = (b.lanes - 1) / 2 * LANE + 10
        x, y = self.at(node)
        half = self.width[node] / 2 + margin
        box = (x - half, y - NODE_HEIGHT / 2 - margin, x + half, y + NODE_HEIGHT / 2 + margin)
        hit = _clip(self.at(b.source), self.at(b.target), box)
        weight = W_THROUGH * (1.0 if b.measured else 1 / 3)
        return weight * self._weight(b.source, b.target, node) if hit else 0.0

    def span(self, i: int) -> float:
        """Label shortfall and length of one bundle."""
        b = self.bundles[i]
        (x0, y0), (x1, y1) = self.at(b.source), self.at(b.target)
        chord = math.hypot(x1 - x0, y1 - y0) or 1.0
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

    def row(self, node: str, mate: str) -> float:
        score = 0.0
        if abs(self.x[node] - self.x[mate]) < (self.width[node] + self.width[mate]) / 2 + GAP:
            score += W_OVERLAP
        left, right = (node, mate) if self.x[node] < self.x[mate] else (mate, node)
        if self._order(left) > self._order(right):
            score += W_ORDER
        return score

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
            total += self.row(node, mate)
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
                    total += self.row(n, m)
        return total

    # ------------------------------------------------------------- search

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
            node = rng.choice(self.nodes)
            old = dict(self.x)
            if self.row_mates[node] and rng.random() < 0.3:
                mate = rng.choice(self.row_mates[node])
                delta = self.move(node, old[mate]) + self.move(mate, old[node])
            else:
                shift = round(rng.gauss(0, 140) / STEP) * STEP
                x = min(X_MAX, max(X_MIN, old[node] + shift))
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

    def report(self) -> dict[str, float]:
        through = {
            kind: sum(
                1
                for i, b in enumerate(self.bundles)
                for n in self.nodes
                if b.measured is (kind == "routes")
                and n not in (b.source, b.target)
                and self.through(i, n)
            )
            for kind in ("routes", "untested")
        }
        crossings = sum(
            1
            for i in range(len(self.bundles))
            for j in range(i + 1, len(self.bundles))
            if self.crossing(i, j)
        )
        return {"energy": round(self.energy(), 1), "through": through, "crossings": crossings}


def initial(layout: Layout, rng: random.Random) -> None:
    """Spread each row evenly, in archive order, with a little jitter."""
    for level in ROW:
        members = sorted(
            (n for n in layout.nodes if layout.layer[n] == level),
            key=lambda n: (layout._order(n), rng.random()),
        )
        span = (X_MAX - X_MIN) / (len(members) + 1)
        for k, node in enumerate(members, start=1):
            layout.x[node] = round((X_MIN + k * span) / STEP) * STEP


def search(graph: dict, seeds: Iterable[int], iterations: int) -> Layout:
    best: Layout | None = None
    best_energy = math.inf
    for seed in seeds:
        rng = random.Random(seed)
        layout = Layout(graph)
        initial(layout, rng)
        energy = layout.anneal(rng, iterations)
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
    print(f"// {layout.report()} -- scripts/route_graph_layout.py, {args.seeds} seeds x {args.iterations}")
    print("const ROW = { " + ", ".join(f"{level}: {y}" for level, y in ROW.items()) + " };")
    print("const POSITIONS = {")
    for node in layout.nodes:
        print(f"  {node}: {{ x: {int(layout.x[node])}, y: ROW.{layout.layer[node]} }},")
    print("};")


if __name__ == "__main__":
    main()
