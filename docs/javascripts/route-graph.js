/* The route graph page (routes/graph.md).
 *
 * The data is embedded at build time by hooks/route_graph.py from
 * fetch_series.export.graph.route_graph(): the catalogue's nodes, one
 * *measured* edge per declared route with its RouteEvidence and rank, one
 * *untested* edge per node pair the catalogue links and no route measures,
 * the unconfirmed links that are listed but never drawn, and the paths over
 * each. Nothing that involves a judgement -- ranking, path finding, gaps -- is
 * recomputed here; this file only draws.
 *
 * Two channels say what a node is: its colour is the archive family that
 * issues it, its shape is the level it sits at, and its position repeats both
 * -- levels run top to bottom, families left to right. A measured route is a
 * solid line, its width the accessions it resolved and its ink the share of
 * queried accessions that resolved. An untested link is a thin dotted line with
 * a hollow arrowhead: a probe found it answering once, which proves it exists
 * and measures nothing.
 *
 * Loaded on every page (extra_javascript) but does nothing unless the page
 * carries #route-graph. Material's instant navigation swaps pages without a
 * reload, so mounting hangs off document$.
 */
(() => {
  "use strict";

  const SVG_NS = "http://www.w3.org/2000/svg";

  // ------------------------------------------------------------- layout

  // Levels top to bottom, one row each; x positions searched, not drawn by
  // hand, by scripts/route_graph_layout.py over `fetch routes graph`. Rerun it
  // when nodes or links change and paste its output here.
  // LAYOUT-BEGIN
  // {'energy': 14744.1, 'through': {'routes': 0, 'untested': 23}, 'crossings': 1238} -- scripts/route_graph_layout.py, 6 seeds x 12000
  const ROW = { dataset: 150, sample: 410, library: 650, run: 870, file: 1090, other: 1310 };
  const POSITIONS = {
    bioproject: { x: 975, y: ROW.dataset },
    study: { x: 1600, y: ROW.dataset },
    geo_series: { x: 625, y: ROW.dataset },
    ae_experiment: { x: 2200, y: ROW.dataset },
    biostudy: { x: 2500, y: ROW.dataset },
    biosample: { x: 1450, y: ROW.sample },
    sample: { x: 1875, y: ROW.sample },
    geo_sample: { x: 250, y: ROW.sample },
    ae_sample: { x: 2450, y: ROW.sample },
    experiment: { x: 250, y: ROW.library },
    run: { x: 1625, y: ROW.run },
    ena_files: { x: 2100, y: ROW.file },
    sra_files: { x: 650, y: ROW.file },
    ae_files: { x: 2500, y: ROW.file },
    geo_files: { x: 250, y: ROW.file },
    submission: { x: 1475, y: ROW.other },
    analysis: { x: 1825, y: ROW.other },
    geo_platform: { x: 1200, y: ROW.other },
  };
  // LAYOUT-END

  const NODE_HEIGHT = 54;
  const LANE = 26; // between parallel lanes mid-way: room for each one's name
  const PORT = 9; // between parallel lanes where they meet a node
  const ARROW = 11;
  const LABEL_FONT = 15;
  const LOOP_RISE = 78;
  const DETAIL_ZOOM = 0.9;

  // Lower bounds of the resolved-share bins, matching --rg-ramp-0 .. -4.
  const SHARE_BINS = [0, 0.25, 0.5, 0.75, 0.95];
  const SHARE_LABELS = ["under 25%", "25–50%", "50–75%", "75–95%", "95% or more"];

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
    for (const child of children.flat(Infinity)) {
      if (child === null || child === undefined || child === false) continue;
      node.append(child instanceof Node ? child : document.createTextNode(String(child)));
    }
    return node;
  }

  const code = (text) => el("code", { text });
  const variant = (route) => route.id.split(":").slice(1).join(":") || route.id;
  const measured = (edge) => edge.status === "measured";

  function share(evidence) {
    if (!evidence || evidence.resolved === null || !evidence.queried) return null;
    return evidence.resolved / evidence.queried;
  }

  function shareBin(value) {
    let bin = 0;
    SHARE_BINS.forEach((lower, i) => {
      if (value >= lower) bin = i;
    });
    return bin;
  }

  /* What the line's width stands for: accessions resolved. Where the survey
   * never split resolved from empty, the answered count is the most that can
   * be said, and the line is hatched to say so. */
  function resolvedCount(evidence) {
    if (!evidence) return 0;
    return evidence.resolved ?? evidence.queried - evidence.failed;
  }

  /* The outcome parts of a route, in drawing order. Where the survey never
   * separated resolved from empty, the answered accessions are one part. */
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

  /* One silhouette per level, centred on the origin.
   *   dataset  a stack of sheets: a record that holds other records
   *   sample   a capsule: one specimen
   *   library  a hexagon: the prepared molecule
   *   run      a slanted bar: a pass through the sequencer
   *   file     a page with its corner folded
   *   other    a plain card: a record about the data, not a level of it */
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
  const FAMILY_LABELS = { insdc: "INSDC: NCBI, EBI and DDBJ", geo: "GEO", arrayexpress: "ArrayExpress and BioStudies", file: "Files" };

  // --------------------------------------------------------- the graph

  class RouteGraph {
    constructor(root, data) {
      this.root = root;
      this.data = data;
      this.layers = data.layers;
      this.nodes = new Map(data.nodes.map((n) => [n.id, n]));
      this.allEdges = new Map(data.edges.map((e) => [e.id, e]));
      this.mechanisms = data.mechanisms;
      this.unconfirmed = new Map(data.unconfirmed.map((u) => [`${u.source}>${u.target}`, u]));
      this.providers = [...new Set(data.edges.filter(measured).map((e) => e.provider))].sort();
      const measuredEdges = data.edges.filter(measured);
      this.maxResolved = Math.max(1, ...measuredEdges.map((e) => resolvedCount(e.evidence)));
      this.state = {
        view: "all", // "all" | "measured"
        showOther: false,
        providers: new Set(this.providers),
        topOnly: false,
        includeUntested: false,
        selected: null, // { kind: "edge" | "node" | "pair", id, source, target }
        path: null, // { from, to, index }
        sort: { key: "direction", ascending: true },
      };
      this.view = { x: 0, y: 0, k: 1 };
      this.viewMoved = false;

      this.buildDom();
      this.rebuild();
      this.renderLegend();
      this.renderMatrix();

      this.resizeObserver = new ResizeObserver(() => {
        if (!this.viewMoved) this.fit();
      });
      this.resizeObserver.observe(this.stage);
    }

    destroy() {
      this.resizeObserver.disconnect();
      this.tooltip.remove();
    }

    /* Re-lay and redraw for the current view: which edges and nodes exist on
     * the canvas changes the bundles, so lanes are recomputed, not hidden. */
    rebuild() {
      this.edges = new Map([...this.allEdges].filter(([, e]) => this.inView(e)));
      this.layout();
      this.draw();
      this.applyFilters();
      this.fit();
    }

    /* ---------------------------------------------------------- visibility */

    nodeShown(id) {
      return this.state.showOther || this.nodes.get(id).layer !== "other";
    }

    /* On the canvas at all, under the two view toggles. */
    inView(edge) {
      if (!measured(edge) && this.state.view === "measured") return false;
      return this.nodeShown(edge.source) && this.nodeShown(edge.target);
    }

    /* On the canvas and not filtered out. Providers and rank apply to routes,
     * which are the only edges that have either. */
    visible(edge) {
      if (!this.edges.has(edge.id)) return false;
      if (!measured(edge)) return true;
      return this.state.providers.has(edge.provider) && (!this.state.topOnly || edge.rank === 1);
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
        el("summary", { class: "rg-button", text: "Route providers" }),
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

      const viewChoice = (value, label) =>
        el(
          "label",
          {},
          el("input", {
            type: "radio",
            name: "rg-view",
            value,
            checked: this.state.view === value,
            onchange: () => this.setView(value),
          }),
          el("span", { text: label }),
        );
      this.viewControl = el(
        "fieldset",
        { class: "rg-segmented" },
        el("legend", { class: "rg-visually-hidden", text: "Show" }),
        viewChoice("measured", "Measured routes"),
        viewChoice("all", "All known links"),
      );
      this.otherToggle = el("input", {
        type: "checkbox",
        onchange: (event) => this.setShowOther(event.target.checked),
      });
      this.untestedPaths = el("input", {
        type: "checkbox",
        onchange: (event) => {
          this.state.includeUntested = event.target.checked;
          this.choosePath();
        },
      });

      const controls = el(
        "div",
        { class: "rg-controls" },
        this.viewControl,
        el("label", {}, this.otherToggle, "Show submissions, analyses, platforms"),
        el(
          "div",
          { class: "rg-path-finder" },
          el("span", { class: "rg-control-label", text: "Find a path" }),
          this.fromSelect,
          el("span", { "aria-hidden": "true", text: "→" }),
          this.toSelect,
          el("label", {}, this.untestedPaths, "include untested"),
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

      this.svg = svg("svg", { class: "rg-svg", role: "img", "aria-label": "Route graph; the matrix and table below list the same links" });
      // The texture for a resolved/empty split that was never recorded.
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
      const hint = el("p", { class: "rg-hint", text: "Click a line or a type for details · drag to pan · pinch or ⌘/Ctrl + scroll to zoom" });
      this.stage = el("div", { class: "rg-stage" }, this.svg, this.panel, zoom, hint);
      this.legend = el("div", { class: "rg-legend" });
      this.matrixWrap = el("section", { class: "rg-matrix-section", "aria-label": "Reachability matrix" });
      this.tableWrap = el("div", { class: "rg-table-wrap" });
      this.tooltip = el("div", { class: "rg-tooltip rg-root", hidden: true });

      this.root.replaceChildren(controls, this.stage, this.legend, this.matrixWrap, this.tableWrap);
      document.body.append(this.tooltip);
      this.bindViewport();
    }

    setView(value) {
      this.state.view = value;
      for (const input of this.viewControl.querySelectorAll("input")) input.checked = input.value === value;
      this.dropHiddenSelection();
      this.rebuild();
    }

    setShowOther(on) {
      this.state.showOther = on;
      this.otherToggle.checked = on;
      this.dropHiddenSelection();
      this.rebuild();
      this.renderMatrix();
    }

    /* A selection the new view cannot show is cleared rather than left
     * highlighting nothing. */
    dropHiddenSelection() {
      const s = this.state.selected;
      if (!s) return;
      const nodes = s.kind === "node" ? [s.id] : s.kind === "pair" ? [s.source, s.target] : [];
      const edge = s.kind === "edge" ? this.allEdges.get(s.id) : null;
      const hidden =
        nodes.some((id) => !this.nodeShown(id)) || (edge && !(this.nodeShown(edge.source) && this.nodeShown(edge.target)));
      if (hidden || (edge && !measured(edge) && this.state.view === "measured")) this.state.selected = null;
    }

    /* ------------------------------------------------------------- layout */

    layout() {
      const measure = document.createElement("canvas").getContext("2d");
      measure.font = `600 ${LABEL_FONT}px ${getComputedStyle(this.root).fontFamily}`;

      // A type the layout does not know yet goes at the end of its row.
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

      // Lanes: every edge is one, grouped by direction. Routes go first, in
      // the order the resolver tries them; the untested lane goes last.
      const directions = new Map();
      for (const edge of this.edges.values()) {
        const key = `${edge.source}>${edge.target}`;
        if (!directions.has(key)) directions.set(key, { key, source: edge.source, target: edge.target, routes: [] });
        directions.get(key).routes.push(edge);
      }
      const order = (e) => (measured(e) ? e.rank : 999);
      this.lanes = new Map();
      const spans = [];
      for (const bundle of directions.values()) {
        bundle.routes.sort((a, b) => order(a) - order(b));
        if (bundle.source === bundle.target) {
          bundle.routes.forEach((edge, i) => this.lanes.set(edge.id, this.loop(bundle.source, i)));
          continue;
        }
        const s = this.place.get(bundle.source);
        const t = this.place.get(bundle.target);
        bundle.chord = dist(s, t);
        const along = scale(sub(t, s), 1 / bundle.chord);
        bundle.left = { x: along.y, y: -along.x }; // left of travel, with y down
        // A direction with a reverse partner bows clear of it by at least half
        // its own width; a lone direction keeps a gentle curve.
        const halfWidth = ((bundle.routes.length - 1) / 2) * LANE;
        const reverse = directions.has(`${bundle.target}>${bundle.source}`);
        bundle.bow = reverse ? Math.max(0.06, (halfWidth + 20) / bundle.chord) : 0.03;
        bundle.middle = add(scale(add(s, t), 0.5), scale(bundle.left, bundle.bow * bundle.chord));
        spans.push(bundle);
      }
      this.ports(spans);
      for (const bundle of spans) {
        bundle.routes.forEach((edge, i) => this.lanes.set(edge.id, this.lane(bundle, i)));
      }
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
      for (const bundle of directions) {
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

    /* The curve one lane is drawn along, from its port on the source to its
     * port on the target, trimmed to the nodes' outlines and shortened to leave
     * room for its arrowhead. */
    lane(bundle, i) {
      const s = this.place.get(bundle.source);
      const t = this.place.get(bundle.target);
      const n = bundle.routes.length;
      const k = i - (n - 1) / 2;
      // Lane order at each port follows the lanes' order across the bundle, so
      // parallel lanes never twist round each other.
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
      // Names read left to right whichever way a lane runs, and bottom to top
      // on a steep one, so a bundle's names all read the same way.
      const heading = (Math.atan2(curve.p1.y - curve.p0.y, curve.p1.x - curve.p0.x) * 180) / Math.PI;
      const labelCurve = heading > 80 || heading < -100 ? reversed(curve) : curve;
      return { d: qPath(curve), labelD: qPath(labelCurve), tip, base: at(q, t2), mid: at(curve, 0.5) };
    }

    /* A self link -- a SubSeries, an umbrella project -- as a loop over the
     * top of its node, entering where it left. */
    loop(nodeId, i) {
      const p = this.place.get(nodeId);
      // Clear of the sheets stacked behind a dataset.
      const top = p.y - p.h / 2 - (this.nodes.get(nodeId).layer === "dataset" ? 12 : 2);
      const spread = p.w * (0.2 + 0.08 * i);
      const rise = LOOP_RISE + 18 * i;
      const p0 = { x: p.x - spread, y: top };
      const tip = { x: p.x + spread, y: top };
      const c1 = { x: p.x - spread * 2.1, y: top - rise };
      const c2 = { x: p.x + spread * 2.1, y: top - rise };
      const into = scale(sub(tip, c2), 1 / dist(tip, c2));
      const base = sub(tip, scale(into, ARROW));
      const d = `M${fmt(p0)} C${fmt(c1)} ${fmt(c2)} ${fmt(base)}`;
      return { d, labelD: d, tip, base, mid: { x: p.x, y: top - rise * 0.75 } };
    }

    /* --------------------------------------------------------------- draw */

    widthOf(edge) {
      if (!measured(edge)) return 1.6;
      return 2 + (4.5 * Math.log10(1 + resolvedCount(edge.evidence))) / Math.log10(1 + this.maxResolved);
    }

    inkClass(edge) {
      if (!measured(edge)) return "rg-line--untested";
      const value = share(edge.evidence);
      return value === null ? "rg-line--unknown" : `rg-line--share rg-share-${shareBin(value)}`;
    }

    laneName(edge) {
      if (measured(edge)) return variant(edge);
      const names = [...new Set(edge.links.map((l) => this.mechanisms[l.mechanism].name))];
      return names.slice(0, 2).join(" · ") + (names.length > 2 ? ` · +${names.length - 2}` : "");
    }

    draw() {
      // Level rows: a label in the gutter and a faint rule behind everything.
      const rows = svg("g", { class: "rg-rows", "aria-hidden": "true" });
      const shownNodes = [...this.nodes.values()].filter((n) => this.nodeShown(n.id));
      const used = new Set(shownNodes.map((n) => n.layer));
      const right = Math.max(...shownNodes.map((n) => this.place.get(n.id).x + this.place.get(n.id).w / 2)) + 40;
      for (const layer of this.layers) {
        if (!used.has(layer.id)) continue;
        const y = ROW[layer.id] ?? ROW.other;
        rows.append(
          svg("line", { class: "rg-row-rule", x1: 130, x2: right, y1: y, y2: y }),
          svg("text", { class: "rg-row-label", x: 20, y, dy: "0.35em", text: layer.label.toUpperCase() }),
        );
      }

      // Untested lanes first, so a measured route is never drawn under one.
      const lanes = svg("g", { class: "rg-lanes" });
      const labels = svg("g", { class: "rg-lane-labels" });
      this.laneEls = new Map();
      this.labelEls = new Map();
      for (const old of this.defs.querySelectorAll("path[id^='rg-label-']")) old.remove();
      const ordered = [...this.edges.values()].sort((a, b) => Number(measured(a)) - Number(measured(b)));
      ordered.forEach((edge, index) => {
        const lane = this.lanes.get(edge.id);
        const width = this.widthOf(edge);
        const group = svg("g", { class: `rg-lane ${measured(edge) ? "rg-lane--measured" : "rg-lane--untested"}`, "data-edge": edge.id });
        group.append(svg("path", { class: `rg-line ${this.inkClass(edge)}`, d: lane.d, "stroke-width": width.toFixed(2) }));

        // Arrowhead: filled in the line's ink for a route, hollow for an
        // untested link -- a link that exists, not a quantity that flows.
        const dir = scale(sub(lane.tip, lane.base), 1 / ARROW);
        const across = { x: dir.y, y: -dir.x };
        const half = measured(edge) ? 4 + width * 0.6 : 4;
        const a = add(lane.base, scale(across, half));
        const b = add(lane.base, scale(across, -half));
        group.append(
          svg("path", { class: `rg-arrow ${measured(edge) ? this.inkClass(edge) : "rg-arrow--hollow"}`, d: `M${fmt(lane.tip)} L${fmt(a)} L${fmt(b)} Z` }),
          svg("path", { class: "rg-flow", d: lane.d, "stroke-width": Math.max(1.5, width - 1.5).toFixed(2) }),
          // A generous invisible stroke to hover and click.
          svg("path", { class: "rg-hit", d: lane.d }),
        );

        // The lane's name along it, on the upper side. Labels sit in their own
        // layer above every line so a crossing lane cannot strike one out.
        const labelId = `rg-label-${index}`;
        this.defs.append(svg("path", { id: labelId, d: lane.labelD }));
        const label = svg(
          "text",
          {
            class: `rg-lane-label ${measured(edge) ? "" : "rg-lane-label--untested"}`,
            dy: (-(width / 2 + 5)).toFixed(1),
            "data-edge": edge.id,
          },
          svg("textPath", { href: `#${labelId}`, startOffset: "50%", "text-anchor": "middle", text: this.laneName(edge) }),
        );
        labels.append(label);
        this.labelEls.set(edge.id, label);

        for (const target of [group, label]) {
          target.addEventListener("pointerenter", (event) => this.hover("edge", edge.id, event));
          target.addEventListener("pointermove", (event) => this.moveTooltip(event));
          target.addEventListener("pointerleave", () => this.unhover());
          target.addEventListener("click", (event) => {
            event.stopPropagation();
            this.selectEdge(edge.id);
          });
        }
        lanes.append(group);
        this.laneEls.set(edge.id, group);
      });

      const nodes = svg("g", { class: "rg-nodes" });
      this.nodeEls = new Map();
      for (const node of shownNodes) {
        const p = this.place.get(node.id);
        const outline = shape(node.layer, p.w, p.h);
        const group = svg(
          "g",
          {
            class: `rg-node rg-family-${node.family} rg-layer-${node.layer}`,
            transform: `translate(${p.x},${p.y})`,
            tabindex: 0,
            role: "button",
            "aria-label": `${node.label}: ${node.routes_out} routes out, ${node.untested_out} untested links out`,
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
      // An untested lane's mechanisms are named only once there is room to
      // read them: at the zoom that fits a hundred dotted lanes on screen,
      // their names are noise over the routes'. Hover or focus names one.
      this.svg.classList.toggle("is-detailed", k >= DETAIL_ZOOM);
    }

    fit(resized = false) {
      const box = this.svg.getBoundingClientRect();
      if (!box.width || !box.height) return;
      // Everything drawn, measured in the drawing's own units.
      this.viewport.removeAttribute("transform");
      const b = this.viewport.getBBox();
      const pad = 24;
      // The hint and the zoom buttons sit over the bottom of the stage; the
      // drawing fits above them rather than under them.
      const reserve = 52;
      // On a wide screen the stage takes the drawing's own proportions, so a
      // fit leaves no empty band above and below it; CSS bounds the height,
      // and a narrow screen keeps the taller stage to zoom into.
      // Once only: where CSS clamps the height, it never comes out as asked.
      if (!resized && box.width >= 900) {
        const wanted = Math.round((box.width * (b.height + 2 * pad)) / (b.width + 2 * pad)) + reserve;
        if (Math.abs(wanted - box.height) > 4) {
          this.stage.style.height = `${wanted}px`;
          this.fit(true);
          return;
        }
      }
      const room = box.height - reserve;
      const k = Math.min(box.width / (b.width + 2 * pad), room / (b.height + 2 * pad));
      this.view = {
        k,
        x: (box.width - b.width * k) / 2 - b.x * k,
        y: (room - b.height * k) / 2 - b.y * k,
      };
      this.viewMoved = false;
      this.applyView();
    }

    zoomBy(factor, cx, cy) {
      const box = this.svg.getBoundingClientRect();
      const px = cx ?? box.width / 2;
      const py = cy ?? box.height / 2;
      const k = Math.min(4, Math.max(0.15, this.view.k * factor));
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

    label(id) {
      return this.nodes.get(id).label;
    }

    probeLine(entry) {
      const probe = entry.probe;
      const name = this.mechanisms[entry.mechanism].name;
      if (!probe) return `${name}: not probed`;
      if (probe.outcome === "failed") return `${name}: probe of ${probe.accession} failed`;
      if (!probe.found_count) return `${name}: ${probe.accession} found nothing`;
      return `${name}: ${probe.accession} → ${number.format(probe.found_count)} (${probe.found[0]}${probe.found_count > 1 ? ", …" : ""})`;
    }

    hover(kind, id, event) {
      if (this.svg.classList.contains("is-panning")) return;
      this.unhover();
      if (kind === "edge") {
        const edge = this.edges.get(id);
        if (measured(edge)) {
          const evidence = edge.evidence;
          this.tooltip.replaceChildren(
            el("div", { class: "rg-tooltip-title", text: edge.id }),
            el("div", { class: "rg-tooltip-values", text: outcomeText(evidence) }),
            outcomeBar(evidence),
            evidence
              ? el(
                  "div",
                  {},
                  `${share(evidence) === null ? "split not recorded" : `${percent.format(share(evidence))} resolved`} of ` +
                    `${number.format(evidence.queried)} queried · ${number.format(evidence.unique_results ?? 0)} distinct results`,
                )
              : null,
            edge.of > 1 ? el("div", {}, `Rank ${edge.rank} of ${edge.of} for this direction`) : null,
          );
        } else {
          this.tooltip.replaceChildren(
            el("div", { class: "rg-tooltip-title", text: `${this.label(edge.source)} → ${this.label(edge.target)}` }),
            el("div", { class: "rg-tooltip-values", text: `Untested · ${edge.links.length} mechanism${edge.links.length === 1 ? "" : "s"}` }),
            el("div", { class: "rg-muted", text: "Confirmed by one probe each; nothing measured." }),
            el("ul", { class: "rg-tooltip-list" }, edge.links.map((entry) => el("li", { text: this.probeLine(entry) }))),
          );
        }
        this.mark(id);
      } else {
        const node = this.nodes.get(id);
        this.tooltip.replaceChildren(
          el("div", { class: "rg-tooltip-values", text: node.label }),
          el("div", {}, `${node.routes_out} routes out · ${node.routes_in} in`),
          el("div", {}, `${node.untested_out} untested links out · ${node.untested_in} in`),
        );
        for (const edge of this.edges.values()) {
          if (edge.source === id || edge.target === id) this.mark(edge.id);
        }
        this.nodeEls.get(id).classList.add("is-hover");
      }
      this.svg.classList.add("has-hover");
      this.tooltip.hidden = false;
      this.moveTooltip(event);
    }

    mark(edgeId) {
      this.laneEls.get(edgeId)?.classList.add("is-hover");
      this.labelEls.get(edgeId)?.classList.add("is-hover");
      const edge = this.edges.get(edgeId);
      this.nodeEls.get(edge.source)?.classList.add("is-hover");
      this.nodeEls.get(edge.target)?.classList.add("is-hover");
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

    /* The edge one hop of a path is drawn along: the best visible route,
     * which is what the resolver would try first; else, where untested hops
     * are allowed, the untested link. */
    hopEdge(source, target) {
      const candidates = [...this.edges.values()].filter((e) => e.source === source && e.target === target && this.visible(e));
      const routes = candidates.filter(measured).sort((a, b) => a.rank - b.rank);
      if (routes.length) return routes[0];
      return this.state.includeUntested ? candidates.find((e) => !measured(e)) ?? null : null;
    }

    emphasis() {
      const { selected, path } = this.state;
      if (path && path.index !== null) {
        const steps = this.paths(path.from, path.to)[path.index];
        const edges = [];
        for (let i = 0; i < steps.length - 1; i++) {
          const edge = this.hopEdge(steps[i], steps[i + 1]);
          if (edge) edges.push(edge.id);
        }
        return { nodes: new Set(steps), edges: new Set(edges) };
      }
      if (selected?.kind === "edge") {
        const edge = this.allEdges.get(selected.id);
        return { nodes: new Set([edge.source, edge.target]), edges: new Set([edge.id]) };
      }
      if (selected?.kind === "pair") {
        const edges = [...this.edges.values()].filter((e) => e.source === selected.source && e.target === selected.target && this.visible(e));
        return { nodes: new Set([selected.source, selected.target]), edges: new Set(edges.map((e) => e.id)) };
      }
      if (selected?.kind === "node") {
        const nodes = new Set([selected.id]);
        const edges = new Set();
        for (const edge of this.edges.values()) {
          if (!this.visible(edge)) continue;
          if (edge.source === selected.id || edge.target === selected.id) {
            edges.add(edge.id);
            nodes.add(edge.source);
            nodes.add(edge.target);
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

    /* A route opens its own panel; an untested lane is its node pair. */
    selectEdge(id) {
      const edge = this.allEdges.get(id);
      if (measured(edge)) this.select("edge", id);
      else this.selectPair(edge.source, edge.target);
    }

    select(kind, id) {
      this.state.selected = { kind, id };
      this.resetPath();
      this.refresh();
    }

    /* Select a node pair, switching the view so it can be seen: a pair that
     * only untested links reach needs "All known links", and one touching a
     * submission, analysis or platform needs those shown. */
    selectPair(source, target) {
      const needsOther = !this.nodeShown(source) || !this.nodeShown(target);
      const hasMeasured = [...this.allEdges.values()].some((e) => measured(e) && e.source === source && e.target === target);
      if (needsOther) this.setShowOther(true);
      if (!hasMeasured && this.state.view === "measured") this.setView("all");
      this.state.selected = { kind: "pair", source, target };
      this.resetPath();
      this.refresh();
    }

    resetPath() {
      this.state.path = null;
      this.fromSelect.value = "";
      this.toSelect.value = "";
    }

    clear() {
      this.state.selected = null;
      this.resetPath();
      this.refresh();
    }

    refresh() {
      this.emphasise();
      this.renderPanel();
      this.highlightRow();
      this.highlightCell();
    }

    /* Measured paths come from the registry's own search; with untested
     * links included, the catalogue's shortest paths and those one hop longer.
     * A path through a hidden node is not offered. */
    paths(from, to) {
      const source = this.state.includeUntested ? this.data.paths_all : this.data.paths_measured;
      return (source[from]?.[to] ?? []).filter((steps) => steps.every((id) => this.nodeShown(id)));
    }

    choosePath() {
      const from = this.fromSelect.value;
      const to = this.toSelect.value;
      if (this.state.includeUntested && this.state.view === "measured") this.setView("all");
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

    /* One mechanism of a link: the request, the field, what the docs say, and
     * the probe -- the whole case for the link existing. */
    mechanismBlock(entry) {
      const m = this.mechanisms[entry.mechanism];
      const probe = entry.probe;
      let probeLine = null;
      if (probe) {
        const found = probe.found_count
          ? [`found ${number.format(probe.found_count)}: `, ...probe.found.slice(0, 3).flatMap((v, i) => [i ? ", " : "", code(v)]), probe.found_count > 3 ? ", …" : ""]
          : [probe.outcome === "failed" ? `failed: ${probe.error}` : "found nothing"];
        probeLine = el("p", { class: "rg-probe" }, el("strong", { text: "Probe " }), code(probe.accession), ` on ${probe.checked_on} — `, ...found);
      }
      return el(
        "li",
        { class: "rg-mechanism" },
        el("div", {}, el("strong", { text: m.name }), el("span", { class: "rg-muted", text: ` · ${m.api}` })),
        el("div", { class: "rg-endpoint" }, code(m.endpoint)),
        el("div", {}, el("span", { class: "rg-muted", text: "Read from " }), code(entry.field)),
        m.via.length ? el("div", { class: "rg-muted", text: `Through ${m.via.map((v) => this.label(v)).join(", ")}, unseen by the caller.` }) : null,
        el(
          "div",
          {},
          m.basis === "documented"
            ? el("a", { href: m.citation_url, rel: "noopener", text: "Documented" })
            : el("span", { class: "rg-badge", text: "live-only" }),
          m.basis === "documented" ? "" : el("span", { class: "rg-muted", text: " no documentation names this field; the probe is the evidence" }),
        ),
        probeLine,
        m.note ? el("p", { class: "rg-muted", text: m.note }) : null,
      );
    }

    renderPanel() {
      const { selected, path } = this.state;
      let parts = null;
      if (path) parts = this.pathPanel(path);
      else if (selected?.kind === "edge") parts = this.edgePanel(this.allEdges.get(selected.id));
      else if (selected?.kind === "pair") parts = this.pairPanel(selected.source, selected.target);
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
      const parts = [
        el("p", { class: "rg-panel-kicker", text: `Measured route · ${this.label(route.source)} → ${this.label(route.target)}` }),
        el("h3", {}, code(variant(route))),
        el("p", { text: route.summary }),
        el(
          "p",
          { class: "rg-muted" },
          route.of > 1 ? `Rank ${route.rank} of ${route.of} for this direction, as the resolver tries them.` : "The only route for this direction.",
        ),
      ];
      if (evidence) {
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
              el("div", { class: "rg-count" }, el("strong", { text: number.format(count) }), el("span", {}, el("span", { class: `rg-swatch rg-swatch--${kind}` }), label)),
            ),
          ),
          outcomeBar(evidence),
          evidence.empty === null ? el("p", { class: "rg-muted" }, "This survey did not separate resolved from empty, so they are shown together.") : null,
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
            el("dd", {}, `${route.cost.requests} request${route.cost.requests === 1 ? "" : "s"} per accession, ${route.cost.rate_limit_rps} req/s` + (route.cost.paginates ? ", paginates" : "")),
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
            route.pathologies.map((p) => el("li", {}, p.url ? el("a", { href: p.url, text: p.slug }) : code(p.slug), p.url ? null : el("span", { class: "rg-muted" }, " (not yet written up)"))),
          ),
        );
      }
      parts.push(el("h4", { text: "Mechanism" }), el("ul", { class: "rg-mechanisms" }, this.mechanismBlock(route.link)));
      parts.push(el("p", {}, el("a", { href: route.kb_url }, "Read the route page →")));
      return parts;
    }

    pairPanel(source, target) {
      const routes = [...this.allEdges.values()].filter((e) => measured(e) && e.source === source && e.target === target).sort((a, b) => a.rank - b.rank);
      const untested = [...this.allEdges.values()].find((e) => !measured(e) && e.source === source && e.target === target);
      const unconfirmed = this.unconfirmed.get(`${source}>${target}`);
      const parts = [
        el("p", { class: "rg-panel-kicker", text: source === target ? "Self link" : "Node pair" }),
        el("h3", { text: source === target ? `${this.label(source)} → another ${this.label(source)}` : `${this.label(source)} → ${this.label(target)}` }),
      ];
      if (!routes.length && !untested && !unconfirmed) {
        parts.push(el("p", { text: "No archive API this catalogue knows links these in one request." }));
        return parts;
      }
      if (routes.length) parts.push(el("h4", { text: `Measured routes (${routes.length})` }), el("ul", {}, routes.map((r) => el("li", {}, this.routeItem(r)))));
      if (untested) {
        parts.push(
          el("h4", { text: `Untested (${untested.links.length})` }),
          el("p", { class: "rg-muted", text: "No survey has measured these. Each was confirmed by one live request, which proves the link exists and measures nothing." }),
          el("ul", { class: "rg-mechanisms" }, untested.links.map((entry) => this.mechanismBlock(entry))),
        );
      }
      if (unconfirmed) {
        parts.push(
          el("h4", { text: `Documented, not confirmed (${unconfirmed.links.length})` }),
          el("p", { class: "rg-muted", text: "Declared, and the probe found nothing, so these are not drawn." }),
          el("ul", { class: "rg-mechanisms" }, unconfirmed.links.map((entry) => this.mechanismBlock(entry))),
        );
      }
      return parts;
    }

    nodePanel(node) {
      const layer = this.layers.find((l) => l.id === node.layer)?.label ?? node.layer;
      const reach = new Map();
      for (const edge of this.allEdges.values()) {
        if (edge.source !== node.id) continue;
        if (!reach.has(edge.target)) reach.set(edge.target, { routes: [], untested: null });
        if (measured(edge)) reach.get(edge.target).routes.push(edge);
        else reach.get(edge.target).untested = edge;
      }
      const reachItem = (target, { routes, untested }) => {
        const best = routes.sort((a, b) => a.rank - b.rank)[0];
        const value = best ? share(best.evidence) : null;
        const status = routes.length
          ? `${routes.length} route${routes.length === 1 ? "" : "s"}` + (best ? `, best ${value === null ? "split not recorded" : `${percent.format(value)} resolved`}` : "")
          : "";
        const names = untested ? [...new Set(untested.links.map((l) => this.mechanisms[l.mechanism].name))] : [];
        return el(
          "li",
          {},
          el(
            "button",
            { class: "rg-item", type: "button", onclick: () => this.selectPair(node.id, target) },
            el("strong", { text: target === node.id ? `another ${this.label(target)}` : this.label(target) }),
            routes.length ? el("span", { class: "rg-item-meta" }, el("span", { class: "rg-chip rg-chip--measured", text: "measured" }), status) : null,
            untested ? el("span", { class: "rg-item-meta" }, el("span", { class: "rg-chip rg-chip--untested", text: "untested" }), names.join(" · ")) : null,
          ),
        );
      };
      const order = (id) => this.data.nodes.findIndex((n) => n.id === id);
      const reached = [...reach.entries()].sort(([a], [b]) => order(a) - order(b));
      const documented = [...this.unconfirmed.values()].filter((u) => u.source === node.id && !reach.has(u.target));
      const gaps = (this.data.unreachable[node.id] ?? []).filter((t) => !documented.some((u) => u.target === t));
      const parts = [
        el("p", { class: "rg-panel-kicker", text: `${layer} · ${node.database} · ${FAMILY_LABELS[node.family] ?? node.family}` }),
        el("h3", { text: node.label }),
        node.note ? el("p", { text: node.note }) : null,
        node.accessions.length
          ? el("dl", {}, node.accessions.flatMap((a) => [el("dt", {}, code(a.pattern)), el("dd", { text: a.description })]))
          : el("p", { class: "rg-muted", text: node.entity === "file" ? "Reached through links, never parsed from an identifier." : "Has no accession of its own." }),
      ];
      if (node.layer === "file") {
        parts.push(el("p", { class: "rg-muted", text: "A file is where links end: nothing links onward from it." }));
      } else {
        parts.push(
          el("h4", { text: `Reaches in one hop (${reached.length})` }),
          reached.length ? el("ul", {}, reached.map(([target, entry]) => reachItem(target, entry))) : el("p", { class: "rg-muted", text: "Nothing." }),
          documented.length ? el("h4", { text: "Documented, not confirmed" }) : null,
          documented.length
            ? el("ul", {}, documented.map((u) => el("li", {}, el("button", { class: "rg-item", type: "button", onclick: () => this.selectPair(node.id, u.target) }, this.label(u.target)))))
            : null,
          el("h4", { text: `No one-hop link to (${gaps.length})` }),
          gaps.length ? el("p", { text: gaps.map((t) => this.label(t)).join(" · ") }) : el("p", { class: "rg-muted", text: "Every other node is one hop away." }),
        );
      }
      const into = [...this.allEdges.values()].filter((e) => e.target === node.id && measured(e));
      if (into.length) {
        parts.push(el("h4", { text: `Measured routes into ${node.label} (${into.length})` }), el("ul", {}, into.sort((a, b) => this.label(a.source).localeCompare(this.label(b.source)) || a.rank - b.rank).map((r) => el("li", {}, el("span", { class: "rg-muted", text: `from ${this.label(r.source)} ` }), this.routeItem(r)))));
      }
      return parts;
    }

    pathPanel(path) {
      const found = this.paths(path.from, path.to);
      const parts = [el("p", { class: "rg-panel-kicker", text: this.state.includeUntested ? "Paths, untested links included" : "Paths over measured routes" }), el("h3", { text: `${this.label(path.from)} → ${this.label(path.to)}` })];
      if (!found.length) {
        parts.push(el("p", { text: this.state.includeUntested ? "No known link connects these within four hops." : "No measured route connects these within four hops. Try including untested links." }));
        return parts;
      }
      parts.push(
        el(
          "p",
          { class: "rg-muted" },
          `${found.length} path${found.length === 1 ? "" : "s"}, shortest first. ` + "Each hop uses the best-ranked route the filters leave visible" + (this.state.includeUntested ? ", else an untested link." : "."),
        ),
      );
      found.forEach((steps, index) => {
        const hops = [];
        for (let i = 0; i < steps.length - 1; i++) hops.push(this.hopEdge(steps[i], steps[i + 1]));
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
            steps.map((s) => this.label(s)).join(" → "),
            el(
              "span",
              { class: "rg-item-meta" },
              hops.every(Boolean) ? hops.map((h) => (measured(h) ? variant(h) : `untested: ${this.laneName(h)}`)).join("  ·  ") : "a hop has no line under the current filters",
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
      const line = (cls, width = 3) =>
        svg("svg", { class: "rg-legend-line", viewBox: "0 0 30 8", "aria-hidden": "true" }, svg("path", { class: `rg-line ${cls}`, d: "M1,4 H29", "stroke-width": width }));
      const item = (key, label) => el("span", { class: "rg-legend-item" }, key, label);
      const order = Object.keys(FAMILY_LABELS);
      const families = [...new Set(this.data.nodes.map((n) => n.family))].sort((x, y) => (order.indexOf(x) + 1 || 99) - (order.indexOf(y) + 1 || 99));

      this.legend.replaceChildren(
        el("div", { class: "rg-legend-group" }, el("span", { class: "rg-legend-title", text: "Shape is the level" }), this.layers.map((l) => item(icon(l.id), l.label.toLowerCase()))),
        el("div", { class: "rg-legend-group" }, el("span", { class: "rg-legend-title", text: "Colour is the archive" }), families.map((f) => item(el("span", { class: `rg-swatch rg-family-${f}` }), FAMILY_LABELS[f] ?? f))),
        el(
          "div",
          { class: "rg-legend-group" },
          el("span", { class: "rg-legend-title", text: "A measured route's ink: share of queried accessions resolved" }),
          SHARE_LABELS.map((label, i) => item(line(`rg-line--share rg-share-${i}`), label)),
          item(line("rg-line--unknown"), "resolved/empty split not recorded"),
          el("span", { class: "rg-legend-note", text: "Width is accessions resolved, log scale." }),
        ),
        el(
          "div",
          { class: "rg-legend-group" },
          el("span", { class: "rg-legend-title", text: "An untested link" }),
          item(line("rg-line--untested", 1.6), "one per node pair, confirmed by a probe; measures nothing"),
        ),
      );
    }

    /* ------------------------------------------------------------- matrix */

    /* Every node against every node: filled by the best route's resolved
     * share where a route measures the pair, outlined where only untested
     * links do, dashed where a link is documented and its probe found
     * nothing, blank where no link exists. The gaps are the point. */
    renderMatrix() {
      const nodes = this.data.nodes.filter((n) => this.nodeShown(n.id));
      const cellFor = (source, target) => {
        const routes = [...this.allEdges.values()].filter((e) => measured(e) && e.source === source && e.target === target).sort((a, b) => a.rank - b.rank);
        const untested = [...this.allEdges.values()].some((e) => !measured(e) && e.source === source && e.target === target);
        const unconfirmed = this.unconfirmed.has(`${source}>${target}`);
        if (routes.length) {
          const value = share(routes[0].evidence);
          return {
            kind: value === null ? "unknown" : `share-${shareBin(value)}`,
            text: `${routes.length} measured route${routes.length === 1 ? "" : "s"}; best ${value === null ? "split not recorded" : `resolves ${percent.format(value)}`}` + (untested ? "; untested links too" : ""),
          };
        }
        if (untested) return { kind: "untested", text: "untested links only" };
        if (unconfirmed) return { kind: "unconfirmed", text: "documented, not confirmed" };
        return { kind: "none", text: "no one-hop link" };
      };

      const head = el("tr", {}, el("th", { class: "rg-matrix-corner", scope: "col" }, el("span", { text: "from ↓  to →" })), nodes.map((n) => el("th", { scope: "col", class: "rg-matrix-col" }, el("span", { text: n.label }))));
      const rows = nodes.map((source) =>
        el(
          "tr",
          {},
          el("th", { scope: "row", class: "rg-matrix-row" }, source.label),
          nodes.map((target) => {
            const cell = cellFor(source.id, target.id);
            const self = source.id === target.id;
            if (cell.kind === "none") return el("td", { class: self ? "rg-cell rg-cell--self" : "rg-cell", "aria-label": `${source.label} → ${target.label}: ${cell.text}` });
            return el(
              "td",
              { class: "rg-cell" },
              el("button", {
                type: "button",
                class: `rg-cell-mark rg-cell--${cell.kind}`,
                "data-pair": `${source.id}>${target.id}`,
                "aria-label": `${source.label} → ${self ? `another ${target.label}` : target.label}: ${cell.text}`,
                title: `${source.label} → ${self ? `another ${target.label}` : target.label}: ${cell.text}`,
                onclick: () => {
                  this.selectPair(source.id, target.id);
                  this.stage.scrollIntoView({ behavior: "smooth", block: "nearest" });
                },
              }),
            );
          }),
        ),
      );
      const key = (kind, label) => el("span", { class: "rg-legend-item" }, el("span", { class: `rg-cell-mark rg-cell-key rg-cell--${kind}`, "aria-hidden": "true" }), label);
      this.matrixWrap.replaceChildren(
        el("h2", { class: "rg-matrix-title", text: "Who reaches whom in one request" }),
        el(
          "div",
          { class: "rg-legend rg-matrix-legend" },
          key("share-4", "measured, filled by the best route's resolved share"),
          key("untested", "untested only"),
          key("unconfirmed", "documented, probe found nothing"),
          el("span", { class: "rg-legend-item" }, el("span", { class: "rg-cell-key rg-cell-key--blank", "aria-hidden": "true" }), "no link"),
        ),
        el("div", { class: "rg-matrix-wrap" }, el("table", { class: "rg-matrix" }, el("thead", {}, head), el("tbody", {}, rows))),
      );
      this.highlightCell();
    }

    highlightCell() {
      const s = this.state.selected;
      const pair = s?.kind === "pair" ? `${s.source}>${s.target}` : s?.kind === "edge" ? (() => { const e = this.allEdges.get(s.id); return `${e.source}>${e.target}`; })() : null;
      for (const cell of this.matrixWrap.querySelectorAll("[data-pair]")) cell.classList.toggle("is-selected", cell.dataset.pair === pair);
    }

    /* -------------------------------------------------------------- table */

    columns() {
      const ev = (r) => r.evidence ?? {};
      return [
        { key: "direction", title: "Direction", value: (r) => `${this.label(r.source)} → ${this.label(r.target)}` },
        { key: "status", title: "Status", value: (r) => r.status },
        { key: "route", title: "Route or mechanisms", value: (r) => (measured(r) ? r.id : this.laneName(r)) },
        { key: "rank", title: "Rank", numeric: true, value: (r) => (measured(r) ? r.rank : 999) },
        { key: "corpus", title: "Corpus or probe", value: (r) => (measured(r) ? ev(r).corpus ?? "" : r.links[0].probe?.accession ?? "") },
        { key: "surveyed", title: "Date", value: (r) => (measured(r) ? ev(r).surveyed_on ?? "" : r.links[0].probe?.checked_on ?? "") },
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
        // Ties fall back to direction, routes before untested, then rank.
        const tie = columns[0].value(a).localeCompare(columns[0].value(b)) || columns[3].value(a) - columns[3].value(b);
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

      const cell = (value, untested) =>
        untested
          ? el("td", { class: "rg-num rg-muted", text: "—" })
          : value === null || value === undefined
            ? el("td", { class: "rg-num rg-muted", text: "not recorded" })
            : el("td", { class: "rg-num", text: number.format(value) });

      const body = rows.map((edge) => {
        const evidence = edge.evidence ?? {};
        const untested = !measured(edge);
        const probes = untested ? edge.links.map((l) => l.probe).filter(Boolean) : [];
        return el(
          "tr",
          { "data-edge": edge.id },
          el("td", { text: columns[0].value(edge) }),
          el("td", {}, el("span", { class: `rg-chip rg-chip--${edge.status}`, text: edge.status })),
          el(
            "td",
            {},
            el(
              "button",
              {
                class: "rg-route-button",
                type: "button",
                onclick: () => {
                  this.selectEdge(edge.id);
                  this.stage.scrollIntoView({ behavior: "smooth", block: "nearest" });
                },
              },
              untested ? edge.links.map((l, i) => [i ? " · " : "", code(this.mechanisms[l.mechanism].name)]) : code(edge.id),
            ),
          ),
          el("td", { class: "rg-num rg-nowrap", text: untested ? "—" : `${edge.rank} of ${edge.of}` }),
          untested
            ? el("td", { class: "rg-corpus", title: [...new Set(probes.map((p) => p.accession))].join(", "), text: `probe ${[...new Set(probes.map((p) => p.accession))].join(", ")}` })
            : el("td", { class: "rg-corpus", title: evidence.corpus ?? "", text: evidence.corpus ?? "unmeasured" }),
          el("td", { class: "rg-nowrap", text: untested ? probes[0]?.checked_on ?? "" : evidence.surveyed_on ?? "" }),
          cell(evidence.queried, untested),
          cell(evidence.resolved, untested),
          cell(evidence.empty, untested),
          cell(evidence.failed, untested),
          cell(evidence.unique_results, untested),
          el("td", {}, untested ? el("span", { class: "rg-muted", text: "not measured" }) : outcomeBar(edge.evidence, true)),
        );
      });

      this.tableWrap.replaceChildren(el("table", { class: "rg-table" }, el("thead", {}, head), el("tbody", {}, body)));
      this.highlightRow();
    }

    highlightRow() {
      const s = this.state.selected;
      for (const row of this.tableWrap.querySelectorAll("tr[data-edge]")) {
        const edge = this.allEdges.get(row.dataset.edge);
        const on = s?.kind === "edge" ? row.dataset.edge === s.id : s?.kind === "pair" ? edge.source === s.source && edge.target === s.target : false;
        row.classList.toggle("rg-row--selected", on);
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
        el("p", { class: "rg-error" }, `The graph could not be drawn (${error.message}). The same numbers are in `, el("a", { href: "../", text: "the route matrix" }), "."),
      );
      throw error;
    }
  }

  if (window.document$) window.document$.subscribe(mount);
  else document.addEventListener("DOMContentLoaded", mount);
})();
