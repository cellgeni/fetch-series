/* The route graph page (routes/graph.md).
 *
 * The data is embedded at build time by hooks/route_graph.py from
 * fetch_series.export.graph.route_graph(): layers, nodes, one edge per route
 * with its RouteEvidence and its rank, and every type path. Nothing that
 * involves a judgement -- ranking, path finding -- is recomputed here; this
 * file only draws.
 *
 * The drawing is plain SVG. Two channels say what a node is: its colour is the
 * archive family that issues it, its shape is the layer it sits at (dataset,
 * sample, library, run, file), and its position repeats both -- layers run top
 * to bottom, families left to right. Each route is its own line, named along
 * its length, and the line itself is split into what its survey found:
 * resolved, then empty, then failed, from source to target.
 *
 * Loaded on every page (extra_javascript) but does nothing unless the page
 * carries #route-graph. Material's instant navigation swaps pages without a
 * reload, so mounting hangs off document$.
 */
(() => {
  "use strict";

  const SVG_NS = "http://www.w3.org/2000/svg";

  // ------------------------------------------------------------- layout

  // Layers top to bottom, one row each. Across each row the positions were
  // searched rather than drawn by hand: every placement scored by how many
  // route bundles it drives through a node they do not belong to (none, here),
  // whether each span leaves room for its routes' names, then by crossings and
  // total line length, keeping GEO to the left and ArrayExpress to the right.
  // Libraries sit under GEO because that is where most routes to them start.
  const ROW = { dataset: 110, sample: 350, library: 570, run: 790, file: 1000, other: 1120 };
  const POSITIONS = {
    geo_series: { x: 525, y: ROW.dataset },
    bioproject: { x: 875, y: ROW.dataset },
    study: { x: 1225, y: ROW.dataset },
    ae_experiment: { x: 1625, y: ROW.dataset },
    geo_sample: { x: 350, y: ROW.sample },
    biosample: { x: 750, y: ROW.sample },
    sample: { x: 1000, y: ROW.sample },
    experiment: { x: 425, y: ROW.library },
    run: { x: 875, y: ROW.run },
    file: { x: 1100, y: ROW.file },
  };

  // How far a bundle bows from the straight line, as a fraction of its length.
  // Most directions take the default; these are tuned so that no bundle runs
  // through a node it does not belong to.
  const BOW = {};

  const NODE_HEIGHT = 54;
  const LANE = 26; // between parallel routes mid-way: room for each one's name
  const PORT = 9; // between parallel routes where they meet a node
  const ARROW = 11;
  const LABEL_FONT = 15;

  const number = new Intl.NumberFormat("en-GB");
  const percent = new Intl.NumberFormat("en-GB", { style: "percent", maximumFractionDigits: 1 });

  // ----------------------------------------------------------- helpers

  /* Build DOM without innerHTML: route notes and corpus names are data. */
  function el(tag, attrs = {}, ...children) {
    return fill(document.createElement(tag), attrs, children);
  }

  function svg(tag, attrs = {}, ...children) {
    return fill(document.createElementNS(SVG_NS, tag), attrs, children);
  }

  function fill(node, attrs, children) {
    for (const [key, value] of Object.entries(attrs)) {
      if (value === null || value === undefined || value === false) continue;
      if (key === "class") node.setAttribute("class", value);
      else if (key === "text") node.textContent = value;
      else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
      else node.setAttribute(key, value === true ? "" : value);
    }
    for (const child of children.flat()) {
      if (child === null || child === undefined || child === false) continue;
      node.append(child instanceof Node ? child : document.createTextNode(String(child)));
    }
    return node;
  }

  const code = (text) => el("code", { text });
  const variant = (route) => route.id.split(":").slice(1).join(":") || route.id;

  function share(evidence) {
    if (!evidence || evidence.resolved === null || !evidence.queried) return null;
    return evidence.resolved / evidence.queried;
  }

  /* The outcome parts of a route, in drawing order: what resolved, what came
   * back empty, what failed. Where the survey never separated resolved from
   * empty, the answered accessions are one "unknown" part. */
  function outcomeParts(evidence) {
    if (!evidence || !evidence.queried) return [];
    const parts =
      evidence.empty === null
        ? [
            ["unknown", evidence.queried - evidence.failed],
            ["failed", evidence.failed],
          ]
        : [
            ["resolved", evidence.resolved],
            ["empty", evidence.empty],
            ["failed", evidence.failed],
          ];
    return parts.filter(([, count]) => count > 0);
  }

  function outcomeBar(evidence, mini = false) {
    const bar = el("div", { class: mini ? "rg-bar rg-bar--mini" : "rg-bar", "aria-hidden": "true" });
    for (const [kind, count] of outcomeParts(evidence)) {
      bar.append(el("span", { class: `rg-seg rg-seg--${kind}`, style: `flex: ${count} 0 0` }));
    }
    return bar;
  }

  function outcomeText(evidence) {
    if (!evidence) return "unmeasured";
    if (evidence.empty === null) {
      return `${number.format(evidence.failed)} failed · resolved/empty split not recorded`;
    }
    return (
      `${number.format(evidence.resolved)} resolved · ${number.format(evidence.empty)} empty · ` +
      `${number.format(evidence.failed)} failed`
    );
  }

  // ----------------------------------------------------------- geometry

  const add = (a, b) => ({ x: a.x + b.x, y: a.y + b.y });
  const sub = (a, b) => ({ x: a.x - b.x, y: a.y - b.y });
  const scale = (a, k) => ({ x: a.x * k, y: a.y * k });
  const dist = (a, b) => Math.hypot(a.x - b.x, a.y - b.y);

  /* A point on the quadratic curve p0 -c- p1. */
  function at(q, t) {
    const u = 1 - t;
    return {
      x: u * u * q.p0.x + 2 * u * t * q.c.x + t * t * q.p1.x,
      y: u * u * q.p0.y + 2 * u * t * q.c.y + t * t * q.p1.y,
    };
  }

  /* The piece of a quadratic between parameters a and b, itself a quadratic. */
  function piece(q, a, b) {
    const blend = (s, t) => ({
      x: (1 - s) * (1 - t) * q.p0.x + ((1 - s) * t + s * (1 - t)) * q.c.x + s * t * q.p1.x,
      y: (1 - s) * (1 - t) * q.p0.y + ((1 - s) * t + s * (1 - t)) * q.c.y + s * t * q.p1.y,
    });
    return { p0: at(q, a), c: blend(a, b), p1: at(q, b) };
  }

  function length(q) {
    let total = 0;
    let previous = q.p0;
    for (let i = 1; i <= 48; i++) {
      const point = at(q, i / 48);
      total += dist(previous, point);
      previous = point;
    }
    return total;
  }

  /* Bisect for the parameter where `test` flips between lo and hi. */
  function bisect(lo, hi, test) {
    const inside = test(lo);
    for (let i = 0; i < 30; i++) {
      const mid = (lo + hi) / 2;
      if (test(mid) === inside) lo = mid;
      else hi = mid;
    }
    return (lo + hi) / 2;
  }

  const fmt = (p) => `${p.x.toFixed(1)},${p.y.toFixed(1)}`;
  const qPath = (q) => `M${fmt(q.p0)} Q${fmt(q.c)} ${fmt(q.p1)}`;
  const reversed = (q) => ({ p0: q.p1, c: q.c, p1: q.p0 });

  function roundedRect(x, y, w, h, r) {
    return (
      `M${x + r},${y} H${x + w - r} A${r},${r} 0 0 1 ${x + w},${y + r} V${y + h - r} ` +
      `A${r},${r} 0 0 1 ${x + w - r},${y + h} H${x + r} A${r},${r} 0 0 1 ${x},${y + h - r} ` +
      `V${y + r} A${r},${r} 0 0 1 ${x + r},${y} Z`
    );
  }

  /* One silhouette per layer, centred on the origin.
   *   dataset  a stack of sheets: a record that holds other records
   *   sample   a capsule: one specimen
   *   library  a hexagon: the prepared molecule
   *   run      a slanted bar: a pass through the sequencer
   *   file     a page with its corner folded */
  function shape(layer, w, h) {
    const x = w / 2;
    const y = h / 2;
    switch (layer) {
      case "dataset": {
        const lift = Math.max(3, h * 0.2);
        const r = Math.min(7, h * 0.15);
        return {
          back: [roundedRect(-x + 2 * lift, -y - 2 * lift, w, h, r), roundedRect(-x + lift, -y - lift, w, h, r)],
          body: roundedRect(-x, -y, w, h, r),
        };
      }
      case "sample":
        return { back: [], body: roundedRect(-x, -y, w, h, y) };
      case "library": {
        const k = y * 0.8;
        return { back: [], body: `M${-x},0 L${-x + k},${-y} H${x - k} L${x},0 L${x - k},${y} H${-x + k} Z` };
      }
      case "run": {
        const k = h * 0.3;
        return { back: [], body: `M${-x + k},${-y} H${x} L${x - k},${y} H${-x} Z` };
      }
      case "file": {
        const c = h * 0.3;
        return {
          back: [],
          body: `M${-x},${-y} H${x - c} L${x},${-y + c} V${y} H${-x} Z`,
          fold: `M${x - c},${-y} V${-y + c} H${x}`,
        };
      }
      default:
        return { back: [], body: roundedRect(-x, -y, w, h, 4) };
    }
  }

  const SHAPE_PADDING = { dataset: 44, sample: 52, library: 70, run: 64, file: 56, other: 40 };
  const FAMILY_LABELS = { insdc: "INSDC: NCBI, EBI and DDBJ", geo: "GEO", arrayexpress: "ArrayExpress", file: "Files" };

  // --------------------------------------------------------- the graph

  class RouteGraph {
    constructor(root, data) {
      this.root = root;
      this.data = data;
      this.layers = data.layers;
      this.nodes = new Map(data.nodes.map((n) => [n.id, n]));
      this.edges = new Map(data.edges.map((e) => [e.id, e]));
      this.providers = [...new Set(data.edges.map((e) => e.provider))].sort();
      this.state = {
        providers: new Set(this.providers),
        topOnly: false,
        selected: null, // { kind: "edge" | "node", id }
        path: null, // { from, to, index }
        sort: { key: "direction", ascending: true },
      };
      this.view = { x: 0, y: 0, k: 1 };
      this.viewMoved = false;

      this.buildDom();
      this.layout();
      this.draw();
      this.applyFilters();
      this.renderLegend();
      this.fit();

      this.resizeObserver = new ResizeObserver(() => {
        if (!this.viewMoved) this.fit();
      });
      this.resizeObserver.observe(this.stage);
    }

    destroy() {
      this.resizeObserver.disconnect();
      this.tooltip.remove();
    }

    /* ---------------------------------------------------------------- DOM */

    buildDom() {
      const typeOptions = (placeholder) => [
        el("option", { value: "", text: placeholder }),
        ...[...this.nodes.values()]
          .sort((a, b) => a.label.localeCompare(b.label))
          .map((n) => el("option", { value: n.id, text: n.label })),
      ];
      this.fromSelect = el("select", { "aria-label": "Path from", onchange: () => this.choosePath() }, typeOptions("from…"));
      this.toSelect = el("select", { "aria-label": "Path to", onchange: () => this.choosePath() }, typeOptions("to…"));

      const providers = el(
        "details",
        { class: "rg-dropdown" },
        el("summary", { class: "rg-button", text: "Providers" }),
        el(
          "div",
          { class: "rg-dropdown-body" },
          this.providers.map((provider) =>
            el(
              "label",
              {},
              el("input", {
                type: "checkbox",
                checked: true,
                onchange: (event) => {
                  if (event.target.checked) this.state.providers.add(provider);
                  else this.state.providers.delete(provider);
                  this.applyFilters();
                },
              }),
              code(provider),
            ),
          ),
        ),
      );

      const controls = el(
        "div",
        { class: "rg-controls" },
        el(
          "div",
          { class: "rg-path-finder" },
          el("span", { class: "rg-control-label", text: "Find a path" }),
          this.fromSelect,
          el("span", { "aria-hidden": "true", text: "→" }),
          this.toSelect,
        ),
        el(
          "label",
          {},
          el("input", {
            type: "checkbox",
            onchange: (event) => {
              this.state.topOnly = event.target.checked;
              this.applyFilters();
            },
          }),
          "Only the top-ranked route per direction",
        ),
        providers,
        el("button", { class: "rg-button", type: "button", text: "Clear selection", onclick: () => this.clear() }),
      );

      this.svg = svg("svg", { class: "rg-svg", role: "img", "aria-label": "Route graph; the table below lists the same routes" });
      // The texture for a resolved/empty split that was never recorded: the
      // same diagonal hatch the bars use.
      this.defs = svg(
        "defs",
        {},
        svg(
          "pattern",
          { id: "rg-hatch", width: 5, height: 5, patternUnits: "userSpaceOnUse", patternTransform: "rotate(45)" },
          svg("rect", { class: "rg-hatch-ink", width: 2.5, height: 5 }),
        ),
      );
      this.viewport = svg("g", { class: "rg-viewport" });
      this.svg.append(this.defs, this.viewport);

      this.panel = el("aside", { class: "rg-panel", "aria-live": "polite", hidden: true });
      const zoom = el(
        "div",
        { class: "rg-zoom" },
        el("button", { class: "rg-button", type: "button", "aria-label": "Zoom in", text: "+", onclick: () => this.zoomBy(1.25) }),
        el("button", { class: "rg-button", type: "button", "aria-label": "Zoom out", text: "−", onclick: () => this.zoomBy(0.8) }),
        el("button", { class: "rg-button", type: "button", text: "Fit", onclick: () => this.fit() }),
      );
      const hint = el("p", { class: "rg-hint", text: "Click a route or a type for details · drag to pan · pinch or ⌘/Ctrl + scroll to zoom" });
      this.stage = el("div", { class: "rg-stage" }, this.svg, this.panel, zoom, hint);
      this.legend = el("div", { class: "rg-legend" });
      this.tableWrap = el("div", { class: "rg-table-wrap" });
      this.tooltip = el("div", { class: "rg-tooltip rg-root", hidden: true });

      this.root.replaceChildren(controls, this.stage, this.legend, this.tableWrap);
      document.body.append(this.tooltip);
      this.bindViewport();
    }

    /* ------------------------------------------------------------- layout */

    layout() {
      const measure = document.createElement("canvas").getContext("2d");
      measure.font = `600 ${LABEL_FONT}px ${getComputedStyle(this.root).fontFamily}`;

      // A type the layout does not know yet goes at the end of its layer's row.
      const rowEnds = {};
      for (const [id, p] of Object.entries(POSITIONS)) {
        const layer = this.nodes.get(id)?.layer;
        if (layer) rowEnds[layer] = Math.max(rowEnds[layer] ?? 0, p.x);
      }

      this.place = new Map();
      for (const node of this.nodes.values()) {
        let position = POSITIONS[node.id];
        if (!position) {
          const x = (rowEnds[node.layer] ?? 0) + 300;
          rowEnds[node.layer] = x;
          position = { x, y: ROW[node.layer] ?? ROW.other };
        }
        const width = Math.max(130, Math.ceil(measure.measureText(node.label).width) + (SHAPE_PADDING[node.layer] ?? 40));
        this.place.set(node.id, { ...position, w: width, h: NODE_HEIGHT });
      }

      // Lanes: every route is one, grouped by direction and ordered by rank.
      const directions = new Map();
      for (const route of this.edges.values()) {
        const key = `${route.source}>${route.target}`;
        if (!directions.has(key)) directions.set(key, { key, source: route.source, target: route.target, routes: [] });
        directions.get(key).routes.push(route);
      }
      for (const bundle of directions.values()) {
        bundle.routes.sort((a, b) => a.rank - b.rank);
        const s = this.place.get(bundle.source);
        const t = this.place.get(bundle.target);
        bundle.chord = dist(s, t);
        const along = scale(sub(t, s), 1 / bundle.chord);
        bundle.left = { x: along.y, y: -along.x }; // left of travel, with y down
        // A direction with a reverse partner bows clear of it by at least half
        // its own width; a lone direction keeps a gentle curve.
        const halfWidth = ((bundle.routes.length - 1) / 2) * LANE;
        const reverse = directions.has(`${bundle.target}>${bundle.source}`);
        bundle.bow = BOW[bundle.key] ?? (reverse ? Math.max(0.06, (halfWidth + 20) / bundle.chord) : 0.03);
        bundle.middle = add(scale(add(s, t), 0.5), scale(bundle.left, bundle.bow * bundle.chord));
      }
      this.ports(directions);
      this.lanes = new Map();
      for (const bundle of directions.values()) {
        bundle.routes.forEach((route, i) => this.lanes.set(route.id, this.lane(bundle, i)));
      }

      const points = [...this.place.values()].flatMap((p) => [
        { x: p.x - p.w / 2, y: p.y - p.h / 2 - 16 },
        { x: p.x + p.w / 2 + 16, y: p.y + p.h / 2 },
      ]);
      for (const lane of this.lanes.values()) points.push(at(lane.curve, 0.5));
      const xs = points.map((p) => p.x);
      const ys = points.map((p) => p.y);
      const top = Math.min(...ys) - 40;
      this.bounds = { x: 0, y: top, width: Math.max(...xs) + 40, height: Math.max(...ys) + 40 - top };
    }

    /* Ports: each direction meeting a node gets its own stretch of the side it
     * arrives on, ordered by where its bundle runs, so bundles meet a node side
     * by side instead of piling onto its centre. */
    ports(directions) {
      const sideOf = (node, other) => {
        const dx = other.x - node.x;
        const dy = other.y - node.y;
        if (Math.abs(dy) * node.w >= Math.abs(dx) * node.h) return dy < 0 ? "top" : "bottom";
        return dx < 0 ? "left" : "right";
      };
      const sides = new Map();
      for (const bundle of directions.values()) {
        for (const [end, nodeId, otherId] of [
          ["start", bundle.source, bundle.target],
          ["end", bundle.target, bundle.source],
        ]) {
          const node = this.place.get(nodeId);
          const side = sideOf(node, this.place.get(otherId));
          const key = `${nodeId}:${side}`;
          if (!sides.has(key)) sides.set(key, { node, side, ends: [] });
          sides.get(key).ends.push({ bundle, end });
        }
      }
      for (const { node, side, ends } of sides.values()) {
        const vertical = side === "left" || side === "right";
        const axis = vertical ? { x: 0, y: 1 } : { x: 1, y: 0 };
        const usable = vertical ? node.h - 14 : node.w * 0.62;
        const lanes = ends.reduce((sum, e) => sum + e.bundle.routes.length, 0);
        const pitch = Math.min(PORT, usable / Math.max(1, lanes));
        // Order by where each bundle actually runs (its bowed middle), which
        // also keeps a direction and its reverse from crossing at the node.
        ends.sort((a, b) => (vertical ? a.bundle.middle.y - b.bundle.middle.y : a.bundle.middle.x - b.bundle.middle.x));
        let cursor = (-lanes * pitch) / 2;
        for (const { bundle, end } of ends) {
          const width = bundle.routes.length * pitch;
          bundle[end] = { anchor: add(node, scale(axis, cursor + width / 2)), axis, pitch };
          cursor += width;
        }
      }
    }

    /* The curve one route is drawn along, from its port on the source to its
     * port on the target, trimmed to the nodes' outlines and shortened to leave
     * room for its arrowhead. */
    lane(bundle, i) {
      const s = this.place.get(bundle.source);
      const t = this.place.get(bundle.target);
      const n = bundle.routes.length;
      const k = i - (n - 1) / 2;
      // Lane order at each port follows the lanes' order across the bundle, so
      // parallel routes never twist round each other.
      const at_ = (port) => add(port.anchor, scale(port.axis, Math.sign(port.axis.x * bundle.left.x + port.axis.y * bundle.left.y || 1) * k * port.pitch));
      const a = at_(bundle.start);
      const b = at_(bundle.end);
      const mid = scale(add(a, b), 0.5);
      const chordMid = scale(add(s, t), 0.5);
      const already = (mid.x - chordMid.x) * bundle.left.x + (mid.y - chordMid.y) * bundle.left.y;
      const offset = bundle.bow * bundle.chord + k * LANE;
      const q = { p0: a, p1: b, c: add(mid, scale(bundle.left, 2 * (offset - already))) };

      const inside = (node, pad) => (p) =>
        Math.abs(p.x - node.x) <= node.w / 2 + pad && Math.abs(p.y - node.y) <= node.h / 2 + pad;
      const inSource = inside(s, 6);
      const inTarget = inside(t, 5);
      const t0 = inSource(at(q, 0)) ? bisect(0, 0.5, (u) => inSource(at(q, u))) : 0;
      const t1 = inTarget(at(q, 1)) ? bisect(0.5, 1, (u) => inTarget(at(q, u))) : 1;
      const tip = at(q, t1);
      const t2 = bisect(t0, t1, (u) => dist(at(q, u), tip) > ARROW);
      const curve = piece(q, t0, t2);
      return { curve, tip, base: at(q, t2), length: length(curve) };
    }

    /* --------------------------------------------------------------- draw */

    draw() {
      const maxResults = Math.max(1, ...this.data.edges.map((e) => e.evidence?.unique_results ?? 0));
      const widthOf = (route) =>
        2 + (3.5 * Math.log10(1 + (route.evidence?.unique_results ?? 0))) / Math.log10(1 + maxResults);

      // Layer rows: a label in the gutter and a faint rule behind everything.
      const rows = svg("g", { class: "rg-rows", "aria-hidden": "true" });
      const used = new Set([...this.nodes.values()].map((n) => n.layer));
      for (const layer of this.layers) {
        if (!used.has(layer.id)) continue;
        const y = ROW[layer.id] ?? ROW.other;
        rows.append(
          svg("line", { class: "rg-row-rule", x1: 130, x2: this.bounds.width - 20, y1: y, y2: y }),
          svg("text", { class: "rg-row-label", x: 20, y, dy: "0.35em", text: layer.label.toUpperCase() }),
        );
      }

      const lanes = svg("g", { class: "rg-lanes" });
      const labels = svg("g", { class: "rg-lane-labels" });
      this.laneEls = new Map();
      this.labelEls = new Map();
      let index = 0;
      for (const route of this.edges.values()) {
        const lane = this.lanes.get(route.id);
        const width = widthOf(route);
        const d = qPath(lane.curve);
        const group = svg("g", { class: "rg-lane", "data-route": route.id });

        // The line, split along its length into what the survey found. A
        // non-zero part is never drawn shorter than 4px, so a handful of
        // failures in thirteen thousand still shows.
        const parts = outcomeParts(route.evidence);
        const total = parts.reduce((sum, [, count]) => sum + count, 0) || 1;
        const lengths = parts.map(([, count]) => Math.max(4, (lane.length * count) / total));
        const excess = lengths.reduce((a, b) => a + b, 0) - lane.length;
        if (excess > 0) lengths[lengths.indexOf(Math.max(...lengths))] -= excess;
        let start = 0;
        parts.forEach(([kind], i) => {
          const gap = i < parts.length - 1 ? 2 : 0;
          const dash = Math.max(0.5, lengths[i] - gap);
          group.append(
            svg("path", {
              class: `rg-line rg-line--${kind}`,
              d,
              "stroke-width": width.toFixed(2),
              "stroke-dasharray": `0 ${start.toFixed(1)} ${dash.toFixed(1)} ${(lane.length + 10).toFixed(1)}`,
            }),
          );
          start += lengths[i];
        });
        if (!parts.length) group.append(svg("path", { class: "rg-line rg-line--unknown", d, "stroke-width": width.toFixed(2) }));

        // Arrowhead, then the moving dots shown on a focused route.
        const dir = scale(sub(lane.tip, lane.base), 1 / ARROW);
        const across = { x: dir.y, y: -dir.x };
        const half = 4 + width * 0.6;
        const a = add(lane.base, scale(across, half));
        const b = add(lane.base, scale(across, -half));
        group.append(
          svg("path", { class: "rg-arrow", d: `M${fmt(lane.tip)} L${fmt(a)} L${fmt(b)} Z` }),
          svg("path", { class: "rg-flow", d, "stroke-width": Math.max(1.5, width - 1.5).toFixed(2) }),
          // A generous invisible stroke to hover and click.
          svg("path", { class: "rg-hit", d }),
        );

        // The route's name along its line, on the upper side, read left to
        // right whichever way the route runs -- and bottom to top on a steep
        // one, so a bundle's names all read the same way. Labels sit in their
        // own layer above every line so a crossing route cannot strike one out.
        const labelId = `rg-label-${index++}`;
        const heading = (Math.atan2(lane.curve.p1.y - lane.curve.p0.y, lane.curve.p1.x - lane.curve.p0.x) * 180) / Math.PI;
        const labelCurve = heading > 80 || heading < -100 ? reversed(lane.curve) : lane.curve;
        this.defs.append(svg("path", { id: labelId, d: qPath(labelCurve) }));
        const label = svg(
          "text",
          { class: "rg-lane-label", dy: (-(width / 2 + 5)).toFixed(1), "data-route": route.id },
          svg("textPath", { href: `#${labelId}`, startOffset: "50%", "text-anchor": "middle", text: variant(route) }),
        );
        labels.append(label);
        this.labelEls.set(route.id, label);

        for (const target of [group, label]) {
          target.addEventListener("pointerenter", (event) => this.hover("edge", route.id, event));
          target.addEventListener("pointermove", (event) => this.moveTooltip(event));
          target.addEventListener("pointerleave", () => this.unhover());
          target.addEventListener("click", (event) => {
            event.stopPropagation();
            this.select("edge", route.id);
          });
        }
        lanes.append(group);
        this.laneEls.set(route.id, group);
      }

      const nodes = svg("g", { class: "rg-nodes" });
      this.nodeEls = new Map();
      for (const node of this.nodes.values()) {
        const p = this.place.get(node.id);
        const outline = shape(node.layer, p.w, p.h);
        const group = svg(
          "g",
          {
            class: `rg-node rg-family-${node.family} rg-layer-${node.layer}`,
            transform: `translate(${p.x},${p.y})`,
            tabindex: 0,
            role: "button",
            "aria-label": `${node.label}: ${node.routes_out} routes out, ${node.routes_in} in`,
          },
          outline.back.map((d) => svg("path", { class: "rg-node-back", d })),
          svg("path", { class: "rg-node-body", d: outline.body }),
          outline.fold ? svg("path", { class: "rg-node-fold", d: outline.fold }) : null,
          svg("text", { class: "rg-node-label", "text-anchor": "middle", dy: "0.35em", text: node.label }),
        );
        group.addEventListener("pointerenter", (event) => this.hover("node", node.id, event));
        group.addEventListener("pointermove", (event) => this.moveTooltip(event));
        group.addEventListener("pointerleave", () => this.unhover());
        group.addEventListener("click", (event) => {
          event.stopPropagation();
          this.select("node", node.id);
        });
        group.addEventListener("keydown", (event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            this.select("node", node.id);
          }
        });
        nodes.append(group);
        this.nodeEls.set(node.id, group);
      }

      this.viewport.replaceChildren(rows, lanes, labels, nodes);
    }

    /* ------------------------------------------------------ zoom and pan */

    applyView() {
      const { x, y, k } = this.view;
      this.viewport.setAttribute("transform", `translate(${x.toFixed(1)},${y.toFixed(1)}) scale(${k.toFixed(4)})`);
    }

    fit() {
      const box = this.svg.getBoundingClientRect();
      if (!box.width || !box.height) return;
      const b = this.bounds;
      const k = Math.min(box.width / b.width, box.height / b.height);
      this.view = { k, x: (box.width - b.width * k) / 2 - b.x * k, y: (box.height - b.height * k) / 2 - b.y * k };
      this.viewMoved = false;
      this.applyView();
    }

    zoomBy(factor, cx, cy) {
      const box = this.svg.getBoundingClientRect();
      const px = cx ?? box.width / 2;
      const py = cy ?? box.height / 2;
      const k = Math.min(4, Math.max(0.2, this.view.k * factor));
      const ratio = k / this.view.k;
      this.view = { k, x: px - (px - this.view.x) * ratio, y: py - (py - this.view.y) * ratio };
      this.viewMoved = true;
      this.applyView();
    }

    bindViewport() {
      // Drag anywhere to pan. A drag that moved does not also count as a
      // click, which would clear the selection or select what it ended on.
      let drag = null;
      this.svg.addEventListener("pointerdown", (event) => {
        if (event.button !== 0) return;
        drag = { x: event.clientX, y: event.clientY, view: { ...this.view }, moved: false };
      });
      this.svg.addEventListener("pointermove", (event) => {
        if (!drag) return;
        const dx = event.clientX - drag.x;
        const dy = event.clientY - drag.y;
        if (!drag.moved && Math.hypot(dx, dy) < 4) return;
        if (!drag.moved) {
          this.svg.setPointerCapture(event.pointerId);
          this.unhover();
        }
        drag.moved = true;
        this.svg.classList.add("is-panning");
        this.view = { ...drag.view, x: drag.view.x + dx, y: drag.view.y + dy };
        this.viewMoved = true;
        this.applyView();
      });
      const end = (event) => {
        if (drag?.moved) {
          if (this.svg.hasPointerCapture(event.pointerId)) this.svg.releasePointerCapture(event.pointerId);
          this.svg.addEventListener("click", (e) => e.stopPropagation(), { capture: true, once: true });
        }
        drag = null;
        this.svg.classList.remove("is-panning");
      };
      this.svg.addEventListener("pointerup", end);
      this.svg.addEventListener("pointercancel", end);
      this.svg.addEventListener("click", () => this.clear());

      // Plain scrolling scrolls the page; pinching or ctrl/cmd + scroll zooms.
      this.svg.addEventListener(
        "wheel",
        (event) => {
          if (!event.ctrlKey && !event.metaKey) return;
          event.preventDefault();
          const box = this.svg.getBoundingClientRect();
          this.zoomBy(Math.exp(-event.deltaY * 0.002), event.clientX - box.left, event.clientY - box.top);
        },
        { passive: false },
      );
    }

    /* ------------------------------------------------------------ tooltip */

    hover(kind, id, event) {
      if (this.svg.classList.contains("is-panning")) return;
      this.unhover();
      if (kind === "edge") {
        const route = this.edges.get(id);
        const evidence = route.evidence;
        this.tooltip.replaceChildren(
          el("div", { class: "rg-tooltip-title", text: route.id }),
          el("div", { class: "rg-tooltip-values", text: outcomeText(evidence) }),
          outcomeBar(evidence),
          evidence
            ? el("div", {}, `of ${number.format(evidence.queried)} queried · ${number.format(evidence.unique_results ?? 0)} distinct results`)
            : null,
          route.of > 1 ? el("div", {}, `Rank ${route.rank} of ${route.of} for this direction`) : null,
        );
        this.mark(id);
      } else {
        const node = this.nodes.get(id);
        this.tooltip.replaceChildren(
          el("div", { class: "rg-tooltip-values", text: node.label }),
          el("div", {}, `${node.routes_out} routes out · ${node.routes_in} in`),
        );
        for (const route of this.edges.values()) {
          if (route.source === id || route.target === id) this.mark(route.id);
        }
        this.nodeEls.get(id).classList.add("is-hover");
      }
      this.svg.classList.add("has-hover");
      this.tooltip.hidden = false;
      this.moveTooltip(event);
    }

    mark(routeId) {
      this.laneEls.get(routeId).classList.add("is-hover");
      this.labelEls.get(routeId).classList.add("is-hover");
      const route = this.edges.get(routeId);
      this.nodeEls.get(route.source).classList.add("is-hover");
      this.nodeEls.get(route.target).classList.add("is-hover");
    }

    unhover() {
      this.svg.classList.remove("has-hover");
      for (const element of this.svg.querySelectorAll(".is-hover")) element.classList.remove("is-hover");
      this.tooltip.hidden = true;
    }

    moveTooltip(pointer) {
      if (!pointer || this.tooltip.hidden) return;
      const box = this.tooltip.getBoundingClientRect();
      const x = pointer.clientX + 16 + box.width > window.innerWidth ? pointer.clientX - box.width - 16 : pointer.clientX + 16;
      const y = pointer.clientY + 16 + box.height > window.innerHeight ? pointer.clientY - box.height - 16 : pointer.clientY + 16;
      this.tooltip.style.left = `${Math.max(4, x)}px`;
      this.tooltip.style.top = `${Math.max(4, y)}px`;
    }

    /* ------------------------------------------------- filters & emphasis */

    visible(route) {
      return this.state.providers.has(route.provider) && (!this.state.topOnly || route.rank === 1);
    }

    applyFilters() {
      for (const [id, group] of this.laneEls) {
        const hidden = !this.visible(this.edges.get(id));
        group.classList.toggle("is-hidden", hidden);
        this.labelEls.get(id).classList.toggle("is-hidden", hidden);
      }
      this.emphasise();
      this.renderPanel();
      this.renderTable();
    }

    /* The best visible route for one hop: lowest rank, i.e. what the resolver
     * would try first, among the routes the filters leave on screen. */
    hopRoute(source, target) {
      return (
        [...this.edges.values()]
          .filter((r) => r.source === source && r.target === target && this.visible(r))
          .sort((a, b) => a.rank - b.rank)[0] ?? null
      );
    }

    emphasis() {
      const { selected, path } = this.state;
      if (path && path.index !== null) {
        const steps = this.paths(path.from, path.to)[path.index];
        const edges = [];
        for (let i = 0; i < steps.length - 1; i++) {
          const route = this.hopRoute(steps[i], steps[i + 1]);
          if (route) edges.push(route.id);
        }
        return { nodes: new Set(steps), edges: new Set(edges) };
      }
      if (selected?.kind === "edge") {
        const route = this.edges.get(selected.id);
        return { nodes: new Set([route.source, route.target]), edges: new Set([route.id]) };
      }
      if (selected?.kind === "node") {
        const nodes = new Set([selected.id]);
        const edges = new Set();
        for (const route of this.edges.values()) {
          if (!this.visible(route)) continue;
          if (route.source === selected.id || route.target === selected.id) {
            edges.add(route.id);
            nodes.add(route.source);
            nodes.add(route.target);
          }
        }
        return { nodes, edges };
      }
      return null;
    }

    emphasise() {
      const focus = this.emphasis();
      this.svg.classList.toggle("has-focus", Boolean(focus));
      for (const [id, group] of this.laneEls) {
        const on = Boolean(focus?.edges.has(id));
        group.classList.toggle("is-focus", on);
        this.labelEls.get(id).classList.toggle("is-focus", on);
      }
      for (const [id, group] of this.nodeEls) group.classList.toggle("is-focus", Boolean(focus?.nodes.has(id)));
    }

    select(kind, id) {
      this.state.selected = { kind, id };
      this.state.path = null;
      this.fromSelect.value = "";
      this.toSelect.value = "";
      this.refresh();
    }

    clear() {
      this.state.selected = null;
      this.state.path = null;
      this.fromSelect.value = "";
      this.toSelect.value = "";
      this.refresh();
    }

    refresh() {
      this.emphasise();
      this.renderPanel();
      this.highlightRow();
    }

    paths(from, to) {
      return this.data.paths[from]?.[to] ?? [];
    }

    choosePath() {
      const from = this.fromSelect.value;
      const to = this.toSelect.value;
      this.state.selected = null;
      this.state.path = from && to ? { from, to, index: this.paths(from, to).length ? 0 : null } : null;
      this.refresh();
    }

    /* -------------------------------------------------------------- panel */

    routeItem(route) {
      const value = share(route.evidence);
      return el(
        "button",
        { class: "rg-item", type: "button", onclick: () => this.select("edge", route.id) },
        code(variant(route)),
        route.of > 1 ? el("span", { class: "rg-muted", text: ` · rank ${route.rank} of ${route.of}` }) : null,
        el(
          "span",
          { class: "rg-item-meta" },
          outcomeBar(route.evidence, true),
          value === null ? "split not recorded" : `${percent.format(value)} resolved`,
          route.evidence?.failed ? ` · ${number.format(route.evidence.failed)} failed` : "",
        ),
      );
    }

    renderPanel() {
      const { selected, path } = this.state;
      let parts = null;
      if (path) parts = this.pathPanel(path);
      else if (selected?.kind === "edge") parts = this.edgePanel(this.edges.get(selected.id));
      else if (selected?.kind === "node") parts = this.nodePanel(this.nodes.get(selected.id));
      this.panel.hidden = !parts;
      if (!parts) return;
      const close = el("button", { class: "rg-panel-close", type: "button", "aria-label": "Close", text: "×", onclick: () => this.clear() });
      // replaceChildren would print a skipped optional part as the text "null".
      this.panel.replaceChildren(close, ...parts.filter(Boolean));
      this.panel.scrollTop = 0;
    }

    edgePanel(route) {
      const evidence = route.evidence;
      const source = this.nodes.get(route.source).label;
      const target = this.nodes.get(route.target).label;
      const parts = [
        el("p", { class: "rg-panel-kicker", text: `${source} → ${target}` }),
        el("h3", {}, code(variant(route))),
        el("p", { text: route.summary }),
        el(
          "p",
          { class: "rg-muted" },
          route.of > 1
            ? `Rank ${route.rank} of ${route.of} for this direction, as the resolver tries them.`
            : "The only route for this direction.",
        ),
      ];
      if (!evidence) {
        parts.push(el("p", { class: "rg-muted", text: "No survey has measured this route yet." }));
      } else {
        const counts =
          evidence.empty === null
            ? [
                [evidence.queried - evidence.failed, "answered", "unknown"],
                [evidence.failed, "failed", "failed"],
              ]
            : [
                [evidence.resolved, "resolved", "resolved"],
                [evidence.empty, "empty", "empty"],
                [evidence.failed, "failed", "failed"],
              ];
        parts.push(
          el(
            "div",
            { class: "rg-counts" },
            counts.map(([count, label, kind]) =>
              el(
                "div",
                { class: "rg-count" },
                el("strong", { text: number.format(count) }),
                el("span", {}, el("span", { class: `rg-swatch rg-swatch--${kind}` }), label),
              ),
            ),
          ),
          outcomeBar(evidence),
          evidence.empty === null
            ? el("p", { class: "rg-muted" }, "This survey did not separate resolved from empty, so they are shown together.")
            : null,
          el(
            "dl",
            {},
            el("dt", { text: "Queried" }),
            el("dd", { text: number.format(evidence.queried) }),
            el("dt", { text: "Distinct results" }),
            el("dd", { text: number.format(evidence.unique_results ?? 0) }),
            el("dt", { text: "Failure rate" }),
            el("dd", { text: percent.format(evidence.failure_rate) }),
            el("dt", { text: "Corpus" }),
            el("dd", {}, code(evidence.corpus)),
            el("dt", { text: "Surveyed" }),
            el("dd", { text: evidence.surveyed_on }),
            el("dt", { text: "Provider" }),
            el("dd", {}, code(route.provider)),
            el("dt", { text: "Cost" }),
            el(
              "dd",
              {},
              `${route.cost.requests} request${route.cost.requests === 1 ? "" : "s"} per accession, ` +
                `${route.cost.rate_limit_rps} req/s` +
                (route.cost.paginates ? ", paginates" : ""),
            ),
            route.source_archives.length ? el("dt", { text: "Accepts" }) : null,
            route.source_archives.length ? el("dd", { text: `${route.source_archives.join(", ")} accessions only` }) : null,
          ),
          evidence.notes ? el("p", { text: evidence.notes }) : null,
        );
      }
      parts.push(
        route.proves_data_exists ? el("span", { class: "rg-badge", text: "results are confirmed data" }) : null,
        route.implemented ? null : el("span", { class: "rg-badge", text: "declared, not implemented" }),
      );
      if (route.pathologies.length) {
        parts.push(
          el("h4", { text: "Known pathologies" }),
          el(
            "ul",
            {},
            route.pathologies.map((p) =>
              el(
                "li",
                {},
                p.url ? el("a", { href: p.url, text: p.slug }) : code(p.slug),
                p.url ? null : el("span", { class: "rg-muted" }, " (not yet written up)"),
              ),
            ),
          ),
        );
      }
      parts.push(el("p", {}, el("a", { href: route.kb_url }, "Read the route page →")));
      return parts;
    }

    nodePanel(node) {
      const group = (routes, other) => {
        const byOther = new Map();
        for (const route of routes) {
          if (!byOther.has(route[other])) byOther.set(route[other], []);
          byOther.get(route[other]).push(route);
        }
        return [...byOther.entries()]
          .sort(([a], [b]) => this.nodes.get(a).label.localeCompare(this.nodes.get(b).label))
          .map(([key, list]) =>
            el(
              "li",
              {},
              el("strong", { text: this.nodes.get(key).label }),
              list.sort((a, b) => a.rank - b.rank).map((route) => this.routeItem(route)),
            ),
          );
      };
      const routes = [...this.edges.values()].filter((r) => this.visible(r));
      const out = routes.filter((r) => r.source === node.id);
      const into = routes.filter((r) => r.target === node.id);
      const layer = this.layers.find((l) => l.id === node.layer)?.label ?? node.layer;
      return [
        el("p", { class: "rg-panel-kicker", text: `${layer} · ${FAMILY_LABELS[node.family] ?? node.family}` }),
        el("h3", { text: node.label }),
        node.accessions.length
          ? el("dl", {}, node.accessions.flatMap((a) => [el("dt", {}, code(a.pattern)), el("dd", { text: a.description })]))
          : el("p", { class: "rg-muted", text: "Reached through routes, never parsed from an identifier." }),
        el("h4", { text: `From ${node.label}` }),
        out.length ? el("ul", {}, group(out, "target")) : el("p", { class: "rg-muted", text: "None shown." }),
        el("h4", { text: `To ${node.label}` }),
        into.length ? el("ul", {}, group(into, "source")) : el("p", { class: "rg-muted", text: "None shown." }),
      ];
    }

    pathPanel(path) {
      const from = this.nodes.get(path.from).label;
      const to = this.nodes.get(path.to).label;
      const found = this.paths(path.from, path.to);
      const parts = [el("p", { class: "rg-panel-kicker", text: "Paths" }), el("h3", { text: `${from} → ${to}` })];
      if (!found.length) {
        parts.push(el("p", { text: "No declared route connects these, within four hops." }));
        return parts;
      }
      parts.push(
        el(
          "p",
          { class: "rg-muted" },
          `${found.length} type path${found.length === 1 ? "" : "s"}, shortest first. ` +
            "Each hop uses the best-ranked route the filters leave visible.",
        ),
      );
      found.forEach((steps, index) => {
        const hops = [];
        for (let i = 0; i < steps.length - 1; i++) hops.push(this.hopRoute(steps[i], steps[i + 1]));
        parts.push(
          el(
            "button",
            {
              class: "rg-item",
              type: "button",
              "aria-pressed": index === path.index ? "true" : "false",
              onclick: () => {
                this.state.path = { ...path, index };
                this.emphasise();
                this.renderPanel();
              },
            },
            steps.map((s) => this.nodes.get(s).label).join(" → "),
            el(
              "span",
              { class: "rg-item-meta" },
              hops.every(Boolean) ? hops.map(variant).join(" · ") : "a hop has no route under the current filters",
            ),
          ),
        );
      });
      return parts;
    }

    /* ------------------------------------------------------------- legend */

    renderLegend() {
      const icon = (layer) => {
        const outline = shape(layer, 30, 16);
        return svg(
          "svg",
          { class: "rg-legend-shape", viewBox: "-20 -18 42 30", "aria-hidden": "true" },
          svg(
            "g",
            { class: "rg-node rg-family-neutral" },
            outline.back.map((d) => svg("path", { class: "rg-node-back", d })),
            svg("path", { class: "rg-node-body", d: outline.body }),
            outline.fold ? svg("path", { class: "rg-node-fold", d: outline.fold }) : null,
          ),
        );
      };
      const line = (kind) =>
        svg(
          "svg",
          { class: "rg-legend-line", viewBox: "0 0 30 8", "aria-hidden": "true" },
          svg("path", { class: `rg-line rg-line--${kind}`, d: "M1,4 H29", "stroke-width": 3 }),
        );
      const item = (key, label) => el("span", { class: "rg-legend-item" }, key, label);
      const order = Object.keys(FAMILY_LABELS);
      const families = [...new Set(this.data.nodes.map((n) => n.family))].sort(
        (x, y) => (order.indexOf(x) + 1 || 99) - (order.indexOf(y) + 1 || 99),
      );
      const usedLayers = new Set(this.data.nodes.map((n) => n.layer));

      this.legend.replaceChildren(
        el(
          "div",
          { class: "rg-legend-group" },
          el("span", { class: "rg-legend-title", text: "Shape is the layer" }),
          this.layers.filter((l) => usedLayers.has(l.id)).map((l) => item(icon(l.id), l.label.toLowerCase())),
        ),
        el(
          "div",
          { class: "rg-legend-group" },
          el("span", { class: "rg-legend-title", text: "Colour is the archive" }),
          families.map((f) => item(el("span", { class: `rg-swatch rg-family-${f}` }), FAMILY_LABELS[f] ?? f)),
        ),
        el(
          "div",
          { class: "rg-legend-group" },
          el("span", { class: "rg-legend-title", text: "Each route's line, from source to target" }),
          item(line("resolved"), "resolved"),
          item(line("empty"), "empty"),
          item(line("failed"), "failed"),
          item(line("unknown"), "resolved or empty, not recorded"),
          el("span", { class: "rg-legend-note", text: "Width is distinct results, log scale." }),
        ),
      );
    }

    /* -------------------------------------------------------------- table */

    columns() {
      const ev = (r) => r.evidence ?? {};
      const label = (id) => this.nodes.get(id).label;
      return [
        { key: "direction", title: "Direction", value: (r) => `${label(r.source)} → ${label(r.target)}` },
        { key: "route", title: "Route", value: (r) => r.id },
        { key: "rank", title: "Rank", numeric: true, value: (r) => r.rank },
        { key: "corpus", title: "Corpus", value: (r) => ev(r).corpus ?? "" },
        { key: "surveyed", title: "Surveyed", value: (r) => ev(r).surveyed_on ?? "" },
        { key: "queried", title: "Queried", numeric: true, value: (r) => ev(r).queried ?? -1 },
        { key: "resolved", title: "Resolved", numeric: true, value: (r) => ev(r).resolved ?? -1 },
        { key: "empty", title: "Empty", numeric: true, value: (r) => ev(r).empty ?? -1 },
        { key: "failed", title: "Failed", numeric: true, value: (r) => ev(r).failed ?? -1 },
        { key: "results", title: "Distinct results", numeric: true, value: (r) => ev(r).unique_results ?? -1 },
        { key: "share", title: "Outcomes", value: (r) => share(r.evidence) ?? -1 },
      ];
    }

    renderTable() {
      const columns = this.columns();
      const { key, ascending } = this.state.sort;
      const column = columns.find((c) => c.key === key);
      const rows = [...this.edges.values()].filter((r) => this.visible(r));
      rows.sort((a, b) => {
        const x = column.value(a);
        const y = column.value(b);
        const order = typeof x === "number" ? x - y : String(x).localeCompare(String(y));
        // Ties fall back to direction then rank, so the order is stable.
        const tie = columns[0].value(a).localeCompare(columns[0].value(b)) || a.rank - b.rank;
        return (ascending ? order : -order) || tie;
      });

      const head = el(
        "tr",
        {},
        columns.map((c) =>
          el(
            "th",
            { class: c.numeric ? "rg-num" : null, "aria-sort": c.key === key ? (ascending ? "ascending" : "descending") : null },
            el("button", {
              type: "button",
              text: c.title,
              onclick: () => {
                this.state.sort = { key: c.key, ascending: c.key === key ? !ascending : true };
                this.renderTable();
              },
            }),
          ),
        ),
      );

      const cell = (value) =>
        value === null || value === undefined
          ? el("td", { class: "rg-num rg-muted", text: "not recorded" })
          : el("td", { class: "rg-num", text: number.format(value) });

      const body = rows.map((route) => {
        const evidence = route.evidence ?? {};
        return el(
          "tr",
          { "data-route": route.id },
          el("td", { text: columns[0].value(route) }),
          el(
            "td",
            {},
            el(
              "button",
              {
                class: "rg-route-button",
                type: "button",
                onclick: () => {
                  this.select("edge", route.id);
                  this.stage.scrollIntoView({ behavior: "smooth", block: "nearest" });
                },
              },
              code(route.id),
            ),
          ),
          el("td", { class: "rg-num rg-nowrap", text: `${route.rank} of ${route.of}` }),
          el("td", { class: "rg-corpus", title: evidence.corpus ?? "", text: evidence.corpus ?? "unmeasured" }),
          el("td", { class: "rg-nowrap", text: evidence.surveyed_on ?? "" }),
          cell(evidence.queried),
          cell(evidence.resolved),
          cell(evidence.empty),
          cell(evidence.failed),
          cell(evidence.unique_results),
          el("td", {}, outcomeBar(route.evidence, true)),
        );
      });

      this.tableWrap.replaceChildren(el("table", { class: "rg-table" }, el("thead", {}, head), el("tbody", {}, body)));
      this.highlightRow();
    }

    highlightRow() {
      const id = this.state.selected?.kind === "edge" ? this.state.selected.id : null;
      for (const row of this.tableWrap.querySelectorAll("tr[data-route]")) {
        row.classList.toggle("rg-row--selected", row.dataset.route === id);
      }
    }
  }

  /* ------------------------------------------------------------- mounting */

  let current = null;

  function mount() {
    if (current) {
      current.destroy();
      current = null;
    }
    const root = document.getElementById("route-graph");
    const source = document.getElementById("route-graph-data");
    if (!root || !source) return;
    try {
      current = new RouteGraph(root, JSON.parse(source.textContent));
    } catch (error) {
      root.replaceChildren(
        el(
          "p",
          { class: "rg-error" },
          `The graph could not be drawn (${error.message}). The same numbers are in `,
          el("a", { href: "../", text: "the route matrix" }),
          ".",
        ),
      );
      throw error;
    }
  }

  if (window.document$) window.document$.subscribe(mount);
  else document.addEventListener("DOMContentLoaded", mount);
})();
