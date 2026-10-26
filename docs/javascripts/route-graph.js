/* The route graph page (routes/graph.md).
 *
 * The data is written beside the page at build time, as graph.json, by
 * hooks/route_graph.py from fetch_series.export.graph.route_graph(), and
 * fetched here: the catalogue's nodes, one
 * *measured* edge per declared route with its RouteEvidence and rank, one
 * *surveyed* edge per node pair a link survey found usable, one *untested*
 * edge per node pair the catalogue links and nothing has measured, the links
 * that are listed but never drawn, and the paths over each. Nothing that involves a judgement -- ranking, path finding, gaps -- is
 * recomputed here; this file only draws.
 *
 * Two channels say what a node is: its colour is the archive that issues it,
 * its shape is the level it sits at, and its position repeats both -- levels
 * run top to bottom, archives left to right in columns: GEO, NCBI, EBI, DDBJ,
 * ArrayExpress. A record all three INSDC archives hold (a BioProject, a
 * BioSample) sits on a row of its own above its level, in their columns.
 *
 * A measured route is drawn once per node pair it covers, from its split: the
 * route's own verdicts restricted to that source and target. Its ink is the
 * share of the source's queried accessions that resolved, its width how many
 * of them reached this target. A surveyed link is a hollow solid line, one
 * width, off the ink ramp: a link survey's seeded draw found it usable, which
 * gives it counts and never a rank -- a draw of 300 is far too coarse for the
 * ranking's 1% band. An untested link is a thin dotted line with a hollow
 * arrowhead: a probe found it answering once -- and, into a file or a
 * document, found something usable -- which proves it exists and measures
 * nothing.
 *
 * Loaded on every page (extra_javascript) but does nothing unless the page
 * carries #route-graph. Material's instant navigation swaps pages without a
 * reload, so mounting hangs off document$. It also re-creates every <script>
 * in the new page to run it, which is why the data is not embedded in one: a
 * JSON block was run as JavaScript and never arrived.
 */
(() => {
  "use strict";

  const SVG_NS = "http://www.w3.org/2000/svg";

  // ------------------------------------------------------------- layout

  // Levels top to bottom, one row each, and archives left to right, one
  // column each; x positions within a node's row and column searched, not
  // drawn by hand, by scripts/route_graph_layout.py over `fetch routes graph`.
  // BOWS is how far a bundle bends to clear the nodes it would otherwise
  // pass under, as a fraction of its length; every other bundle keeps the
  // page's own gentle curve. Rerun it when nodes or links change and paste
  // its output here.
  // LAYOUT-BEGIN
  // {'energy': 96629.6, 'through': {'routes': 23, 'untested': 28}, 'through_full': {'routes': 35, 'untested': 110}, 'bent': 83, 'crossings': 5085, 'overlaps': 0} -- scripts/route_graph_layout.py, 3 seeds x 8000
  const ROW = { dataset_shared: 120, dataset: 250, sample_shared: 440, sample: 570, library: 760, run: 940, other: 1120, file: 1320, supplementary: 1500, metadata: 1680, metadata_shared: 1810 };
  const COLUMNS = { geo: [200, 720], ncbi: [720, 1240], ebi: [1240, 1760], ddbj: [1760, 2280], arrayexpress: [2280, 2800] };
  const POSITIONS = {
    bioproject: { x: 1540, y: ROW.dataset_shared },
    study_ncbi: { x: 1070, y: ROW.dataset },
    study_ebi: { x: 1681, y: ROW.dataset },
    study_ddbj: { x: 1930, y: ROW.dataset },
    geo_series: { x: 420, y: ROW.dataset },
    ae_experiment: { x: 2418, y: ROW.dataset },
    biostudy: { x: 2691, y: ROW.dataset },
    biosample: { x: 1190, y: ROW.sample_shared },
    sample_ncbi: { x: 807, y: ROW.sample },
    sample_ebi: { x: 1673, y: ROW.sample },
    sample_ddbj: { x: 2170, y: ROW.sample },
    geo_sample: { x: 287, y: ROW.sample },
    ae_sample: { x: 2674, y: ROW.sample },
    experiment_ncbi: { x: 1110, y: ROW.library },
    experiment_ebi: { x: 1590, y: ROW.library },
    experiment_ddbj: { x: 2060, y: ROW.library },
    run_ncbi: { x: 800, y: ROW.run },
    run_ebi: { x: 1320, y: ROW.run },
    run_ddbj: { x: 2199, y: ROW.run },
    submission_ncbi: { x: 1080, y: ROW.other },
    submission_ebi: { x: 1640, y: ROW.other },
    submission_ddbj: { x: 1960, y: ROW.other },
    analysis_ncbi: { x: 809, y: ROW.other },
    analysis_ebi: { x: 1440, y: ROW.other },
    analysis_ddbj: { x: 2190, y: ROW.other },
    geo_platform: { x: 560, y: ROW.other },
    fastq: { x: 1040, y: ROW.file },
    bam: { x: 1870, y: ROW.file },
    cram: { x: 2180, y: ROW.file },
    sra_normalized: { x: 1280, y: ROW.file },
    sra_lite: { x: 810, y: ROW.file },
    geo_supplementary: { x: 480, y: ROW.supplementary },
    ae_supplementary: { x: 2620, y: ROW.supplementary },
    metadata_ncbi: { x: 1020, y: ROW.metadata },
    metadata_ebi: { x: 1470, y: ROW.metadata },
    metadata_ddbj: { x: 1875, y: ROW.metadata },
    metadata_geo: { x: 315, y: ROW.metadata },
    metadata_ae: { x: 2412, y: ROW.metadata },
    metadata_biosample: { x: 2100, y: ROW.metadata_shared },
  };
  const BOWS = {
    "ae_experiment>experiment_ncbi": -0.06,
    "ae_experiment>fastq": -0.15,
    "ae_experiment>run_ebi": 0.3,
    "ae_sample>biosample": -0.03,
    "ae_sample>experiment_ebi": 0.15,
    "ae_sample>metadata_ae": -0.03,
    "ae_sample>run_ebi": 0.1,
    "ae_sample>run_ncbi": 0.1,
    "ae_sample>sample_ebi": 0.06,
    "analysis_ddbj>study_ddbj": 0.2,
    "analysis_ebi>biosample": 0.5,
    "analysis_ebi>study_ebi": 0.1,
    "analysis_ncbi>bioproject": -0.15,
    "analysis_ncbi>study_ncbi": -0.06,
    "bioproject>bam": -0.3,
    "bioproject>sample_ddbj": 0.3,
    "bioproject>sample_ebi": 0.5,
    "bioproject>sample_ncbi": 0.2,
    "bioproject>sra_lite": 0.15,
    "biosample>bam": -0.06,
    "biosample>cram": 0.06,
    "biosample>experiment_ddbj": 0.15,
    "biosample>fastq": 0.2,
    "biosample>sra_normalized": -0.4,
    "biosample>study_ddbj": 0.2,
    "biosample>submission_ncbi": 0.15,
    "biostudy>bioproject": -0.1,
    "experiment_ddbj>metadata_ebi": -0.03,
    "experiment_ebi>fastq": -0.15,
    "experiment_ebi>metadata_ncbi": 0.15,
    "experiment_ebi>study_ebi": 0.1,
    "experiment_ncbi>geo_series": 0.15,
    "experiment_ncbi>metadata_ddbj": 0.15,
    "experiment_ncbi>metadata_ebi": 0.3,
    "experiment_ncbi>metadata_ncbi": 0.1,
    "geo_sample>bioproject": 0.1,
    "geo_sample>fastq": 0.15,
    "geo_sample>submission_ncbi": 0.1,
    "geo_series>experiment_ncbi": 0.15,
    "geo_series>sra_lite": -0.06,
    "geo_series>sra_normalized": -0.06,
    "run_ddbj>bioproject": 0.5,
    "run_ddbj>biosample": 0.25,
    "run_ddbj>fastq": -0.06,
    "run_ddbj>metadata_ddbj": -0.03,
    "run_ddbj>metadata_ncbi": -0.06,
    "run_ddbj>study_ddbj": 0.4,
    "run_ebi>metadata_ebi": 0.1,
    "run_ebi>metadata_ncbi": 0.5,
    "run_ebi>sample_ebi": 0.1,
    "run_ncbi>bioproject": 0.15,
    "run_ncbi>metadata_ncbi": 0.1,
    "run_ncbi>study_ncbi": 0.4,
    "sample_ddbj>bioproject": 0.25,
    "sample_ddbj>biosample": 0.15,
    "sample_ddbj>sra_normalized": -0.03,
    "sample_ebi>analysis_ebi": 0.25,
    "sample_ebi>fastq": -0.06,
    "sample_ebi>submission_ebi": 0.1,
    "sample_ncbi>sra_lite": 0.2,
    "sample_ncbi>sra_normalized": -0.03,
    "study_ddbj>fastq": -0.06,
    "study_ddbj>run_ddbj": 0.3,
    "study_ddbj>sra_normalized": 0.15,
    "study_ebi>analysis_ebi": 0.25,
    "study_ebi>cram": -0.06,
    "study_ebi>experiment_ebi": 0.4,
    "study_ebi>run_ebi": 0.5,
    "study_ebi>submission_ebi": 0.15,
    "study_ncbi>experiment_ncbi": 0.5,
    "study_ncbi>fastq": -0.1,
    "study_ncbi>sra_lite": 0.06,
    "study_ncbi>sra_normalized": -0.25,
    "submission_ddbj>bioproject": 0.4,
    "submission_ddbj>biosample": 0.1,
    "submission_ddbj>sample_ddbj": 0.3,
    "submission_ebi>ae_experiment": 0.1,
    "submission_ebi>bioproject": 0.15,
    "submission_ebi>fastq": 0.25,
    "submission_ebi>study_ebi": 0.25,
    "submission_ncbi>bioproject": 0.3,
    "submission_ncbi>biosample": 0.25,
    "submission_ncbi>study_ncbi": 0.1,
  };
  // LAYOUT-END

  // Nodes the layout spans across the INSDC columns carry their own width.
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
  const variant = (route) => route.route.split(":").slice(1).join(":") || route.route;
  // Three drawn kinds. Only a measured edge is a route: it alone has a rank,
  // a provider and a split. A surveyed edge has counts per link and corpus and
  // no rank; an untested edge has a probe per link and no counts.
  const measured = (edge) => edge.status === "measured";
  const isSurveyed = (edge) => edge.status === "surveyed";
  const isUntested = (edge) => edge.status === "untested";
  const kindOf = (edge) => (measured(edge) ? "measured" : isSurveyed(edge) ? "surveyed" : "untested");
  // Back to front: an untested lane is never drawn over a surveyed one, nor a
  // surveyed one over a route.
  const DEPTH = { untested: 0, surveyed: 1, measured: 2 };

  /* A surveyed link's splits, flattened: one per mechanism and corpus, in
   * mechanism name order then corpus order -- the order the export gives,
   * which is never yield order. */
  const surveySplits = (edge) => edge.links.flatMap((link) => (link.splits ?? []).map((split) => ({ ...split, mechanism: link.mechanism })));

  /* The 95% interval half-width on a yield over n draws, at its widest (p =
   * 0.5), in percentage points: ±5.7 at 300. */
  const halfWidth = (n) => (n ? 98 / Math.sqrt(n) : null);

  /* Resolved among queried, for a route's evidence or for one of its splits:
   * the same fields, the second restricted to one source node. */
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

  /* What a route line's width stands for: how many of its source's queried
   * accessions had a result on this target -- an ERR among a BioProject's
   * runs, a BAM among a run's files. */
  function reachedCount(split) {
    return split ? split.reached : 0;
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
   *   dataset        a stack of sheets: a record that holds other records
   *   sample         a capsule: one specimen
   *   library        a hexagon: the prepared molecule
   *   run            a slanted bar: a pass through the sequencer
   *   other          a plain card: a record about the data, not a level of it
   *   file           a page with its corner folded: read files, by format
   *   supplementary  the same page: a submitter's processed files
   *   metadata       a card with a ruled header: a document describing records */
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
      case "file":
      case "supplementary": {
        const c = h * 0.3;
        return {
          back: [],
          body: `M${-x},${-y} H${x - c} L${x},${-y + c} V${y} H${-x} Z`,
          fold: `M${x - c},${-y} V${-y + c} H${x}`,
        };
      }
      case "metadata": {
        // A form: a card with a ruled header.
        const r = Math.min(5, h * 0.12);
        return { back: [], body: roundedRect(-x, -y, w, h, r), fold: `M${-x},${-y + h * 0.3} H${x}` };
      }
      default:
        return { back: [], body: roundedRect(-x, -y, w, h, 4) };
    }
  }

  const SHAPE_PADDING = { dataset: 44, sample: 52, library: 70, run: 64, other: 40, file: 56, supplementary: 56, metadata: 48 };
  const FAMILY_LABELS = {
    geo: "GEO",
    ncbi: "NCBI",
    ebi: "EBI",
    ddbj: "DDBJ",
    arrayexpress: "ArrayExpress and BioStudies",
    shared: "Held by all three INSDC archives",
    file: "Read files, by format",
  };
  const COLUMN_LABELS = { geo: "GEO", ncbi: "NCBI", ebi: "EBI", ddbj: "DDBJ", arrayexpress: "ArrayExpress" };
  const ORIGIN_SHORT = { submitted: "submitted", "archive-generated": "generated", "archive format": "SRA object", mixed: "mixed" };

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
      this.measuredNone = new Map(data.measured_none.map((u) => [`${u.source}>${u.target}`, u]));
      this.surveyedNone = new Map((data.surveyed_none ?? []).map((u) => [`${u.source}>${u.target}`, u]));
      this.providers = [...new Set(data.edges.filter(measured).map((e) => e.provider))].sort();
      const measuredEdges = data.edges.filter(measured);
      this.maxReached = Math.max(1, ...measuredEdges.map((e) => reachedCount(e.split)));
      this.state = {
        view: "all", // "all" | "surveyed" (measured + surveyed) | "measured"
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

    /* Whether the current view draws this kind of edge: "measured" routes
     * alone, "surveyed" adds the link surveys, "all" adds the untested links. */
    kindShown(edge, view = this.state.view) {
      if (view === "measured") return measured(edge);
      if (view === "surveyed") return !isUntested(edge);
      return true;
    }

    /* On the canvas at all, under the two view toggles. */
    inView(edge) {
      if (!this.kindShown(edge)) return false;
      return this.nodeShown(edge.source) && this.nodeShown(edge.target);
    }

    /* On the canvas and not filtered out. Providers and rank apply to routes,
     * which are the only edges that have either: a surveyed link has neither a
     * provider nor a rank, so "only the top-ranked route" leaves it drawn. */
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
        viewChoice("surveyed", "Measured + surveyed"),
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
     * highlighting nothing. A path is kept, and starts again from its first:
     * which paths exist depends on the view (measured, or measured and
     * surveyed), so the one that was picked may not be offered any more. */
    dropHiddenSelection() {
      const path = this.state.path;
      if (path) path.index = this.paths(path.from, path.to).length ? 0 : null;
      const s = this.state.selected;
      if (!s) return;
      const nodes = s.kind === "node" ? [s.id] : s.kind === "pair" ? [s.source, s.target] : [];
      const edge = s.kind === "edge" ? this.allEdges.get(s.id) : null;
      const hidden =
        nodes.some((id) => !this.nodeShown(id)) || (edge && !(this.nodeShown(edge.source) && this.nodeShown(edge.target)));
      if (hidden || (edge && !this.kindShown(edge))) this.state.selected = null;
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
        // A node as wide as its label or its prefix line, whichever is wider;
        // a node the layout spans across the INSDC columns keeps its span.
        const text = Math.max(measure.measureText(node.label).width, node.prefix ? node.prefix.length * 7.4 : 0);
        const width = position.w ?? Math.max(120, Math.ceil(text) + (SHAPE_PADDING[node.layer] ?? 40));
        this.place.set(node.id, { x: position.x, y: this.yOf(position.y), w: width, h: NODE_HEIGHT });
      }

      // Lanes: every edge is one, grouped by direction. Routes go first, in
      // the order the resolver tries them; then the surveyed lane, which has
      // no rank and so no place among them; the untested lane goes last.
      const directions = new Map();
      for (const edge of this.edges.values()) {
        const key = `${edge.source}>${edge.target}`;
        if (!directions.has(key)) directions.set(key, { key, source: edge.source, target: edge.target, routes: [] });
        directions.get(key).routes.push(edge);
      }
      const order = (e) => (measured(e) ? e.rank : isSurveyed(e) ? 998 : 999);
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
        // Unless the layout search found it passes under a node it does not
        // belong to, and gave it the least bow that clears it (BOWS). Beside
        // its reverse it only ever bows further out, never across it.
        const own = reverse ? Math.max(0.06, (halfWidth + 20) / bundle.chord) : 0.03;
        const searched = BOWS[bundle.key];
        bundle.bow = searched === undefined ? own : reverse ? Math.max(own, searched) : searched;
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

    /* A route's width is how many reached the target. A surveyed link is one
     * width whatever it found: drawing its draw of 300 to the routes' scale
     * would put it among them, and its counts are in the panel. */
    widthOf(edge) {
      if (isSurveyed(edge)) return 4.2;
      if (!measured(edge)) return 1.6;
      return 2 + (4.5 * Math.log10(1 + reachedCount(edge.split))) / Math.log10(1 + this.maxReached);
    }

    inkClass(edge) {
      if (isSurveyed(edge)) return "rg-line--surveyed";
      if (!measured(edge)) return "rg-line--untested";
      const value = share(edge.split);
      return value === null ? "rg-line--unknown" : `rg-line--share rg-share-${shareBin(value)}`;
    }

    /* A lane's name: the route, or the mechanisms; into a read format, who
     * made the files as well, since a submitted BAM and a fastq dumped from
     * an SRA object are not the same offer. */
    laneName(edge) {
      const made = (entry) => (entry.origin ? ` (${ORIGIN_SHORT[entry.origin] ?? entry.origin})` : "");
      if (measured(edge)) return variant(edge) + made(edge.link);
      const names = [...new Set(edge.links.map((l) => this.mechanisms[l.mechanism].name + made(l)))];
      return names.slice(0, 2).join(" · ") + (names.length > 2 ? ` · +${names.length - 2}` : "");
    }

    /* Where a row is drawn: hiding submissions, analyses and platforms closes
     * up their row rather than leaving a band of nothing. */
    yOf(y) {
      return !this.state.showOther && y > ROW.other ? y - (ROW.file - ROW.other) : y;
    }

    draw() {
      // Level rows: a label in the gutter and a faint rule behind everything.
      const rows = svg("g", { class: "rg-rows", "aria-hidden": "true" });
      const shownNodes = [...this.nodes.values()].filter((n) => this.nodeShown(n.id));
      const used = new Set(shownNodes.map((n) => n.layer));
      const right = Math.max(...shownNodes.map((n) => this.place.get(n.id).x + this.place.get(n.id).w / 2)) + 40;
      for (const layer of this.layers) {
        if (!used.has(layer.id)) continue;
        const y = this.yOf(ROW[layer.id] ?? ROW.other);
        rows.append(
          svg("line", { class: "rg-row-rule", x1: 130, x2: right, y1: y, y2: y }),
          svg("text", { class: "rg-row-label", x: 20, y, dy: "0.35em", text: layer.label.toUpperCase() }),
        );
      }
      // Archive columns: a name over each, and a rule between them.
      const top = Math.min(...shownNodes.map((n) => this.place.get(n.id).y)) - 95;
      const bottom = Math.max(...shownNodes.map((n) => this.place.get(n.id).y)) + 50;
      Object.entries(COLUMNS).forEach(([column, [left, rightEdge]], i) => {
        rows.append(
          svg("text", { class: `rg-column-label rg-family-${column}`, x: (left + rightEdge) / 2, y: top, "text-anchor": "middle", text: COLUMN_LABELS[column] ?? column }),
        );
        if (i) rows.append(svg("line", { class: "rg-column-rule", x1: left, x2: left, y1: top + 14, y2: bottom }));
      });

      // Untested lanes first, then surveyed, so a measured route is never
      // drawn under either.
      const lanes = svg("g", { class: "rg-lanes" });
      const labels = svg("g", { class: "rg-lane-labels" });
      this.laneEls = new Map();
      this.labelEls = new Map();
      for (const old of this.defs.querySelectorAll("path[id^='rg-label-']")) old.remove();
      const ordered = [...this.edges.values()].sort((a, b) => DEPTH[kindOf(a)] - DEPTH[kindOf(b)]);
      ordered.forEach((edge, index) => {
        const lane = this.lanes.get(edge.id);
        const width = this.widthOf(edge);
        const kind = kindOf(edge);
        const group = svg("g", { class: `rg-lane rg-lane--${kind}`, "data-edge": edge.id });
        group.append(svg("path", { class: `rg-line ${this.inkClass(edge)}`, d: lane.d, "stroke-width": width.toFixed(2) }));
        // A surveyed line is hollow: a surface-coloured core down a solid
        // stroke, so at any width it cannot be read as a pale route.
        if (kind === "surveyed") group.append(svg("path", { class: "rg-line-core", d: lane.d, "stroke-width": (width - 2.6).toFixed(2) }));

        // Arrowhead: filled in the line's ink for a route, hollow for a
        // surveyed or untested link -- a link that answers, not a quantity
        // drawn to the routes' scale.
        const dir = scale(sub(lane.tip, lane.base), 1 / ARROW);
        const across = { x: dir.y, y: -dir.x };
        const half = measured(edge) ? 4 + width * 0.6 : kind === "surveyed" ? 5 : 4;
        const a = add(lane.base, scale(across, half));
        const b = add(lane.base, scale(across, -half));
        const arrow = measured(edge) ? this.inkClass(edge) : `rg-arrow--hollow rg-arrow--${kind}`;
        group.append(
          svg("path", { class: `rg-arrow ${arrow}`, d: `M${fmt(lane.tip)} L${fmt(a)} L${fmt(b)} Z` }),
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
            class: `rg-lane-label ${measured(edge) ? "" : `rg-lane-label--${kind}`}`,
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
            "aria-label": `${node.label}: ${node.routes_out} routes out, ${node.surveyed_out ?? 0} surveyed links out, ${node.untested_out} untested links out`,
          },
          outline.back.map((d) => svg("path", { class: "rg-node-back", d })),
          svg("path", { class: "rg-node-body", d: outline.body }),
          outline.fold ? svg("path", { class: "rg-node-fold", d: outline.fold }) : null,
          svg("text", { class: "rg-node-label", "text-anchor": "middle", dy: node.prefix ? "-0.15em" : "0.35em", text: node.label }),
          node.prefix ? svg("text", { class: "rg-node-prefix", "text-anchor": "middle", dy: "1.25em", text: node.prefix }) : null,
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
      // An untested or surveyed lane's mechanisms are named only once there is
      // room to read them: at the zoom that fits a hundred such lanes on
      // screen, their names are noise over the routes'. Hover or focus names
      // one.
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
      this.fitK = k;
      this.viewMoved = false;
      this.applyView();
    }

    /* Zoom stays between 4x and 0.15x -- or the scale that fits the whole
     * graph, where that is smaller, as on a phone: otherwise zooming out from
     * the fitted view would jump in. */
    clampZoom(k) {
      return Math.min(4, Math.max(Math.min(0.15, this.fitK ?? 0.15), k));
    }

    zoomBy(factor, cx, cy) {
      const box = this.svg.getBoundingClientRect();
      const px = cx ?? box.width / 2;
      const py = cy ?? box.height / 2;
      const k = this.clampZoom(this.view.k * factor);
      const ratio = k / this.view.k;
      this.view = { k, x: px - (px - this.view.x) * ratio, y: py - (py - this.view.y) * ratio };
      this.viewMoved = true;
      this.applyView();
    }

    bindViewport() {
      // One pointer drags to pan; two pinch to zoom. The canvas turns the
      // browser's own touch gestures off (touch-action: none) so a drag pans
      // the graph rather than the page, which means pinching is done here too.
      const pointers = new Map(); // pointerId -> where it is, in client pixels
      let drag = null; // one pointer: where it started, and the view then
      let pinch = null; // two pointers: their spread and midpoint, and the view then
      const spread = () => {
        const [a, b] = [...pointers.values()];
        return { d: Math.hypot(a.x - b.x, a.y - b.y), x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 };
      };
      const capture = (id) => {
        if (!this.svg.hasPointerCapture(id)) this.svg.setPointerCapture(id);
      };

      // A gesture does not also count as a click, which would clear the
      // selection or select what it ended on. The guard is a flag, not a
      // one-shot listener: a touch drag fires no click at all, and a listener
      // left waiting swallowed the next real tap. The click that follows a
      // pointerup is dispatched before any timer runs, so the flag is down
      // again by the next one. It stops the click *immediately* because the
      // click can land on the canvas itself, where clear() listens too: a
      // plain stopPropagation keeps clear() out only in browsers that run a
      // target's capture and bubble listeners as separate passes.
      let swallow = false;
      const swallowNextClick = () => {
        swallow = true;
        setTimeout(() => {
          swallow = false;
        }, 0);
      };
      this.svg.addEventListener(
        "click",
        (event) => {
          if (!swallow) return;
          swallow = false;
          event.stopImmediatePropagation();
        },
        { capture: true },
      );

      this.svg.addEventListener("pointerdown", (event) => {
        if (event.pointerType === "mouse" && event.button !== 0) return;
        // A first finger, or the mouse, starts afresh: a pointer whose up was
        // missed must not turn the next single touch into a pinch.
        if (event.isPrimary) pointers.clear();
        pointers.set(event.pointerId, { x: event.clientX, y: event.clientY });
        if (pointers.size === 2) {
          // A second finger turns a pan into a pinch.
          const start = spread();
          pinch = { ...start, view: { ...this.view } };
          drag = null;
          for (const id of pointers.keys()) capture(id);
          this.unhover();
        } else if (pointers.size === 1) {
          drag = { x: event.clientX, y: event.clientY, view: { ...this.view }, moved: false };
        }
      });
      this.svg.addEventListener("pointermove", (event) => {
        if (!pointers.has(event.pointerId)) return;
        pointers.set(event.pointerId, { x: event.clientX, y: event.clientY });
        if (pinch && pointers.size >= 2) {
          // Scale by how far the fingers have spread, about the point they
          // started over, and carry that point along with their midpoint.
          const now = spread();
          if (!pinch.d) return;
          const box = this.svg.getBoundingClientRect();
          const k = this.clampZoom((pinch.view.k * now.d) / pinch.d);
          const ratio = k / pinch.view.k;
          const from = { x: pinch.x - box.left, y: pinch.y - box.top };
          const to = { x: now.x - box.left, y: now.y - box.top };
          this.view = { k, x: to.x - (from.x - pinch.view.x) * ratio, y: to.y - (from.y - pinch.view.y) * ratio };
          this.viewMoved = true;
          this.applyView();
          return;
        }
        if (!drag) return;
        const dx = event.clientX - drag.x;
        const dy = event.clientY - drag.y;
        if (!drag.moved && Math.hypot(dx, dy) < 4) return;
        if (!drag.moved) {
          capture(event.pointerId);
          this.unhover();
        }
        drag.moved = true;
        this.svg.classList.add("is-panning");
        this.view = { ...drag.view, x: drag.view.x + dx, y: drag.view.y + dy };
        this.viewMoved = true;
        this.applyView();
      });
      const end = (event) => {
        if (!pointers.delete(event.pointerId)) return;
        if (this.svg.hasPointerCapture(event.pointerId)) this.svg.releasePointerCapture(event.pointerId);
        if (pinch) {
          swallowNextClick();
          if (pointers.size < 2) {
            // The finger left behind pans on from where it is, not from
            // where it first touched down, so the view does not jump.
            pinch = null;
            const [rest] = pointers.values();
            drag = rest ? { x: rest.x, y: rest.y, view: { ...this.view }, moved: false } : null;
          }
          return;
        }
        if (drag?.moved) swallowNextClick();
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
      const failed = probe.checks.filter((c) => c.passed !== true).map((c) => c.name);
      const checks = probe.checks.length ? (failed.length ? ` — failed ${failed.join(", ")}` : ` — ${probe.checks.map((c) => c.name).join(", ")} ✓`) : "";
      return `${name}: ${probe.accession} → ${number.format(probe.found_count)} (${probe.found[0]}${probe.found_count > 1 ? ", …" : ""})${checks}`;
    }

    /* One surveyed link over one corpus, in a line: how many of the draw gave
     * something usable, and the three outcomes. */
    surveyLine(split, named = true) {
      const name = named ? `${this.mechanisms[split.mechanism].name}: ` : "";
      return (
        `${name}${number.format(split.usable)}/${number.format(split.queried)} usable · ` +
        `${number.format(split.resolved)} resolved · ${number.format(split.empty)} empty · ${number.format(split.failed)} failed · ${split.corpus}`
      );
    }

    /* A measured edge's split, in words: of the source's queried accessions,
     * how many resolved, and how many reached this target. */
    splitText(edge) {
      const split = edge.split;
      const of = `${number.format(split.queried)} ${this.label(edge.source)} queried`;
      const resolved = split.resolved === null ? "resolved/empty split not recorded" : `${percent.format(share(split))} resolved`;
      return `${resolved} of ${of} · ${number.format(split.reached)} reached ${this.label(edge.target)} · ${number.format(split.distinct)} distinct`;
    }

    hover(kind, id, event) {
      if (this.svg.classList.contains("is-panning")) return;
      this.unhover();
      if (kind === "edge") {
        const edge = this.edges.get(id);
        if (measured(edge)) {
          this.tooltip.replaceChildren(
            el("div", { class: "rg-tooltip-title", text: edge.route }),
            el("div", { class: "rg-muted", text: `${this.label(edge.source)} → ${this.label(edge.target)}` }),
            el("div", { class: "rg-tooltip-values", text: outcomeText(edge.split) }),
            outcomeBar(edge.split),
            el("div", {}, this.splitText(edge)),
            edge.link.origin ? el("div", {}, `Files: ${edge.link.origin}`) : null,
            edge.of > 1 ? el("div", {}, `Rank ${edge.rank} of ${edge.of} for this direction`) : null,
          );
        } else if (isSurveyed(edge)) {
          this.tooltip.replaceChildren(
            el("div", { class: "rg-tooltip-title", text: `${this.label(edge.source)} → ${this.label(edge.target)}` }),
            el("div", { class: "rg-tooltip-values", text: `Surveyed · ${edge.links.length} mechanism${edge.links.length === 1 ? "" : "s"}` }),
            el("div", { class: "rg-muted", text: "A link survey over a seeded draw: counts, never a rank." }),
            el("ul", { class: "rg-tooltip-list" }, surveySplits(edge).map((split) => el("li", { text: this.surveyLine(split) }))),
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
          el("div", {}, `${node.surveyed_out ?? 0} surveyed links out · ${node.surveyed_in ?? 0} in`),
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

    /* Which links the path finder walks: measured routes; measured and
     * surveyed links, in the "measured + surveyed" view; or, with untested
     * links included, every drawn link. */
    pathMode() {
      if (this.state.includeUntested) return "all";
      return this.state.view === "surveyed" ? "surveyed" : "measured";
    }

    /* The edge one hop of a path is drawn along: the best visible route,
     * which is what the resolver would try first; else, where the path mode
     * allows them, the surveyed link, and then the untested one. A surveyed
     * link goes before an untested one because it has been measured, not
     * because it ranks: it carries no rank to compare. */
    hopEdge(source, target) {
      const candidates = [...this.edges.values()].filter((e) => e.source === source && e.target === target && this.visible(e));
      const routes = candidates.filter(measured).sort((a, b) => a.rank - b.rank);
      if (routes.length) return routes[0];
      const mode = this.pathMode();
      if (mode === "measured") return null;
      const linked = candidates.find(isSurveyed);
      if (linked) return linked;
      return mode === "all" ? candidates.find(isUntested) ?? null : null;
    }

    emphasis() {
      const { selected, path } = this.state;
      if (path && path.index !== null) {
        const steps = this.paths(path.from, path.to)[path.index];
        if (!steps) return null;
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

    /* A route opens its own panel; a surveyed or untested lane is its node
     * pair. */
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
     * only surveyed links reach needs at least "Measured + surveyed", one that
     * only untested links reach "All known links", and one touching a
     * submission, analysis or platform needs those shown. A pair with no
     * line at all -- only documented or absent links -- leaves the view be. */
    selectPair(source, target) {
      const needsOther = !this.nodeShown(source) || !this.nodeShown(target);
      const drawn = [...this.allEdges.values()].filter((e) => e.source === source && e.target === target);
      if (needsOther) this.setShowOther(true);
      if (drawn.length && !drawn.some((e) => this.kindShown(e))) {
        this.setView(this.state.view === "measured" && drawn.some(isSurveyed) ? "surveyed" : "all");
      }
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

    /* Measured paths come from the registry's own search, and measured plus
     * surveyed ones from the catalogue's, over those links alone; with
     * untested links included, the catalogue's shortest paths and those one
     * hop longer. A path through a hidden node is not offered. */
    paths(from, to) {
      const mode = this.pathMode();
      const source = (mode === "all" ? this.data.paths_all : mode === "surveyed" ? this.data.paths_surveyed : this.data.paths_measured) ?? {};
      return (source[from]?.[to] ?? []).filter((steps) => steps.every((id) => this.nodeShown(id)));
    }

    choosePath() {
      const from = this.fromSelect.value;
      const to = this.toSelect.value;
      if (this.state.includeUntested && this.state.view !== "all") this.setView("all");
      this.state.selected = null;
      this.state.path = from && to ? { from, to, index: this.paths(from, to).length ? 0 : null } : null;
      this.refresh();
    }

    /* -------------------------------------------------------------- panel */

    routeItem(route) {
      const value = share(route.split);
      return el(
        "button",
        { class: "rg-item", type: "button", onclick: () => this.select("edge", route.id) },
        code(variant(route)),
        route.of > 1 ? el("span", { class: "rg-muted", text: ` · rank ${route.rank} of ${route.of}` }) : null,
        el(
          "span",
          { class: "rg-item-meta" },
          outcomeBar(route.split, true),
          value === null ? "split not recorded" : `${percent.format(value)} resolved`,
          ` · ${number.format(route.split.reached)} of ${number.format(route.split.queried)} reached it`,
        ),
      );
    }

    /* What a probe checked a found file or document for, and what it said. */
    checkList(probe) {
      if (!probe?.checks?.length) return null;
      const mark = (passed) => (passed === true ? "passed" : passed === false ? "failed" : "undetermined");
      return el(
        "ul",
        { class: "rg-checks" },
        probe.checks.map((c) =>
          el("li", { class: `rg-check rg-check--${mark(c.passed)}` }, el("span", { class: "rg-check-mark", "aria-hidden": "true", text: c.passed === true ? "✓" : c.passed === false ? "✗" : "?" }), `${c.name}: ${mark(c.passed)}`),
        ),
        probe.checked ? el("li", { class: "rg-muted", text: `checked on ${probe.checked}` }) : null,
      );
    }

    /* A surveyed link's splits, one per corpus: of the seeded draw, how many
     * gave something usable, the three outcomes, how many distinct values,
     * and how each check went. Counts only -- nothing here orders one link
     * against another. */
    surveyBlock(entry) {
      if (!entry.splits?.length) return null;
      return el(
        "div",
        { class: "rg-survey" },
        entry.splits.map((split) => {
          const checks = Object.entries(split.checks ?? {});
          const half = halfWidth(split.queried);
          return el(
            "div",
            { class: "rg-survey-split" },
            el(
              "p",
              { class: "rg-survey-head" },
              el("strong", { text: `${number.format(split.usable)}/${number.format(split.queried)} usable` }),
              " over ",
              code(split.corpus),
              el("span", { class: "rg-muted", text: ` · surveyed ${split.surveyed_on}` }),
            ),
            outcomeBar(split, true),
            el(
              "p",
              { class: "rg-muted" },
              `${number.format(split.resolved)} resolved · ${number.format(split.empty)} empty · ${number.format(split.failed)} failed · ` +
                `${number.format(split.distinct)} distinct` +
                (half === null ? "" : ` · ±${half.toFixed(1)} points at ${number.format(split.queried)}`),
            ),
            checks.length
              ? el(
                  "ul",
                  { class: "rg-checks" },
                  checks.map(([name, c]) => {
                    const worst = c.failed ? "failed" : c.undetermined ? "undetermined" : "passed";
                    return el(
                      "li",
                      { class: `rg-check rg-check--${worst}` },
                      el("span", { class: "rg-check-mark", "aria-hidden": "true", text: worst === "passed" ? "✓" : worst === "failed" ? "✗" : "?" }),
                      `${name}: ${number.format(c.passed)} passed · ${number.format(c.failed)} failed · ${number.format(c.undetermined)} undetermined`,
                    );
                  }),
                )
              : null,
          );
        }),
      );
    }

    /* One mechanism of a link: the request, the field, what the docs say, and
     * the probe -- the whole case for the link existing -- and, for a surveyed
     * link, what its survey counted. */
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
        entry.origin ? el("div", {}, el("span", { class: "rg-muted", text: "Files " }), entry.origin) : null,
        m.via.length ? el("div", { class: "rg-muted", text: `Through ${m.via.map((v) => this.label(v)).join(", ")}, unseen by the caller.` }) : null,
        el(
          "div",
          {},
          m.basis === "documented"
            ? el("a", { href: m.citation_url, rel: "noopener", text: "Documented" })
            : el("span", { class: "rg-badge", text: "live-only" }),
          m.basis === "documented" ? "" : el("span", { class: "rg-muted", text: " no documentation names this field; the probe is the evidence" }),
        ),
        this.surveyBlock(entry),
        probeLine,
        this.checkList(probe),
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
      const counts = (o) =>
        o.empty === null
          ? [
              [o.queried - o.failed, "answered", "unknown"],
              [o.failed, "failed", "failed"],
            ]
          : [
              [o.resolved, "resolved", "resolved"],
              [o.empty, "empty", "empty"],
              [o.failed, "failed", "failed"],
            ];
      const countRow = (o) =>
        el(
          "div",
          { class: "rg-counts" },
          counts(o).map(([count, label, kind]) =>
            el("div", { class: "rg-count" }, el("strong", { text: number.format(count) }), el("span", {}, el("span", { class: `rg-swatch rg-swatch--${kind}` }), label)),
          ),
        );
      const split = route.split;
      const source = this.nodes.get(route.source);
      parts.push(
        el("h4", { text: "On this pair" }),
        countRow(split),
        outcomeBar(split),
        el(
          "p",
          {},
          `${number.format(split.reached)} of the ${number.format(split.queried)} ${source.label} accessions queried had a result on ${this.label(route.target)}: ` +
            `${number.format(split.distinct)} distinct.`,
        ),
        el(
          "p",
          { class: "rg-muted" },
          (source.archive && ["ncbi", "ebi", "ddbj"].includes(source.archive) && source.kind === "record"
            ? `The route's own verdicts on ${source.prefix} accessions alone. `
            : `The route's own outcomes: a ${source.label} is one node across archives. `) +
            "Read from its recorded verdicts: filtered measurement, not new evidence, and it changes no ranking.",
        ),
        route.link.origin ? el("p", {}, el("span", { class: "rg-muted", text: "Files " }), route.link.origin) : null,
      );
      if (evidence) {
        parts.push(
          el("h4", { text: "The whole route" }),
          countRow(evidence),
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
      const linked = [...this.allEdges.values()].find((e) => isSurveyed(e) && e.source === source && e.target === target);
      const untested = [...this.allEdges.values()].find((e) => isUntested(e) && e.source === source && e.target === target);
      const unconfirmed = this.unconfirmed.get(`${source}>${target}`);
      const none = this.measuredNone.get(`${source}>${target}`);
      const surveyedNone = this.surveyedNone.get(`${source}>${target}`);
      const parts = [
        el("p", { class: "rg-panel-kicker", text: source === target ? "Self link" : "Node pair" }),
        el("h3", { text: source === target ? `${this.label(source)} → another ${this.label(source)}` : `${this.label(source)} → ${this.label(target)}` }),
      ];
      if (!routes.length && !linked && !untested && !unconfirmed && !none && !surveyedNone) {
        parts.push(el("p", { text: "No archive API this catalogue knows links these in one request." }));
        return parts;
      }
      if (routes.length) parts.push(el("h4", { text: `Measured routes (${routes.length})` }), el("ul", {}, routes.map((r) => el("li", {}, this.routeItem(r)))));
      // The interval at the smallest draw on the pair: the widest it can be.
      const surveyNote = (group) => {
        const n = Math.min(...surveySplits(group).map((split) => split.queried));
        const interval = Number.isFinite(n) && n > 0 ? ` — ±${halfWidth(n).toFixed(1)} points at ${number.format(n)}` : "";
        return (
          `Each count is a link survey over a seeded draw of the source's accessions${interval} — ` +
          "so it says whether a link answers, not which answers best. Listed by name, never ranked, and never route evidence."
        );
      };
      const continued = [linked, untested].find((e) => e && e.continues);
      if (continued) {
        parts.push(
          el("h4", { text: "Read from the fetched document" }),
          el(
            "p",
            { class: "rg-muted" },
            "Not a request of its own: the second half of the one that fetched this document. " +
              "The record link that reads it is drawn with its own evidence, and this line with it.",
          ),
          el(
            "ul",
            {},
            continued.continues.map((c) =>
              el("li", {}, code(c.fetched_by), el("span", { class: "rg-muted", text: " fetches it; " }), code(c.read_by), el("span", { class: "rg-muted", text: ` reads it (${c.status}${c.route ? `, route ${c.route}` : ""})` })),
            ),
          ),
        );
      }
      if (linked) {
        parts.push(
          el("h4", { text: `Surveyed (${linked.links.length})` }),
          el("p", { class: "rg-muted", text: surveyNote(linked) }),
          el("ul", { class: "rg-mechanisms" }, linked.links.map((entry) => this.mechanismBlock(entry))),
        );
      }
      if (untested) {
        parts.push(
          el("h4", { text: `Untested (${untested.links.length})` }),
          el("p", { class: "rg-muted", text: "No survey has measured these. Each was confirmed by one live request, which proves the link exists and measures nothing." }),
          el("ul", { class: "rg-mechanisms" }, untested.links.map((entry) => this.mechanismBlock(entry))),
        );
      }
      if (none) {
        parts.push(
          el("h4", { text: `Measured, never answering (${none.routes.length})` }),
          el("p", { class: "rg-muted", text: "A route's survey queried accessions of this source and none had a result here. A measured absence, so it is not drawn." }),
          el(
            "ul",
            {},
            none.routes.map((r) =>
              el("li", {}, code(r.route), el("span", { class: "rg-muted", text: ` · 0 of ${number.format(r.split.queried)} reached it` + (r.corpus ? ` · ${r.corpus}` : "") })),
            ),
          ),
        );
      }
      if (surveyedNone) {
        parts.push(
          el("h4", { text: `Surveyed, never usable (${surveyedNone.links.length})` }),
          el("p", { class: "rg-muted", text: "A link survey answered for this source and found nothing usable here: a measured absence over a seeded draw, so it is not drawn. Where a probe confirmed the link, it exists and did not answer for this project's records." }),
          el("ul", { class: "rg-mechanisms" }, surveyedNone.links.map((entry) => this.mechanismBlock(entry))),
        );
      }
      if (unconfirmed) {
        parts.push(
          el("h4", { text: `Documented, not confirmed (${unconfirmed.links.length})` }),
          el("p", { class: "rg-muted", text: "Declared, and the probe found nothing, found a file or document that failed a check, or did not complete. Not drawn." }),
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
        if (!reach.has(edge.target)) reach.set(edge.target, { routes: [], linked: null, untested: null });
        if (measured(edge)) reach.get(edge.target).routes.push(edge);
        else if (isSurveyed(edge)) reach.get(edge.target).linked = edge;
        else reach.get(edge.target).untested = edge;
      }
      const reachItem = (target, { routes, linked, untested }) => {
        const best = routes.sort((a, b) => a.rank - b.rank)[0];
        const value = best ? share(best.split) : null;
        const status = routes.length
          ? `${routes.length} route${routes.length === 1 ? "" : "s"}` + (best ? `, best ${value === null ? "split not recorded" : `${percent.format(value)} resolved`}` : "")
          : "";
        const names = untested ? [...new Set(untested.links.map((l) => this.mechanisms[l.mechanism].name))] : [];
        // Usable over queried, per mechanism and corpus, in name order.
        const counts = linked ? surveySplits(linked).map((split) => `${this.mechanisms[split.mechanism].name} ${number.format(split.usable)}/${number.format(split.queried)}`) : [];
        return el(
          "li",
          {},
          el(
            "button",
            { class: "rg-item", type: "button", onclick: () => this.selectPair(node.id, target) },
            el("strong", { text: target === node.id ? `another ${this.label(target)}` : this.label(target) }),
            routes.length ? el("span", { class: "rg-item-meta" }, el("span", { class: "rg-chip rg-chip--measured", text: "measured" }), status) : null,
            linked ? el("span", { class: "rg-item-meta" }, el("span", { class: "rg-chip rg-chip--surveyed", text: "surveyed" }), counts.join(" · ")) : null,
            untested ? el("span", { class: "rg-item-meta" }, el("span", { class: "rg-chip rg-chip--untested", text: "untested" }), names.join(" · ")) : null,
          ),
        );
      };
      const order = (id) => this.data.nodes.findIndex((n) => n.id === id);
      const reached = [...reach.entries()].sort(([a], [b]) => order(a) - order(b));
      const documented = [...this.unconfirmed.values()].filter((u) => u.source === node.id && !reach.has(u.target));
      const none = [...this.measuredNone.values()].filter((u) => u.source === node.id && !reach.has(u.target));
      const surveyedNone = [...this.surveyedNone.values()].filter((u) => u.source === node.id && !reach.has(u.target));
      const listed = new Set([...documented, ...none, ...surveyedNone].map((u) => u.target));
      const gaps = (this.data.unreachable[node.id] ?? []).filter((t) => !listed.has(t));
      const parts = [
        el("p", { class: "rg-panel-kicker", text: `${layer} · ${node.database} · ${FAMILY_LABELS[node.family] ?? node.family}` }),
        el("h3", {}, node.label, node.prefix ? el("span", { class: "rg-muted", text: ` · ${node.prefix}` }) : null),
        node.note ? el("p", { text: node.note }) : null,
        node.accessions.length
          ? el("dl", {}, node.accessions.flatMap((a) => [el("dt", {}, code(a.pattern)), el("dd", { text: a.description })]))
          : el("p", { class: "rg-muted", text: node.kind === "record" ? "Has no accession of its own." : "Reached through links, never parsed from an identifier." }),
        el(
          "dl",
          { class: "rg-node-counts" },
          el("dt", { text: "Measured routes" }),
          el("dd", { text: `${node.routes_out} out · ${node.routes_in} in` }),
          el("dt", { text: "Surveyed links" }),
          el("dd", { text: `${node.surveyed_out ?? 0} out · ${node.surveyed_in ?? 0} in` }),
          el("dt", { text: "Untested links" }),
          el("dd", { text: `${node.untested_out} out · ${node.untested_in} in` }),
        ),
      ];
      if (node.kind === "read-format") {
        parts.push(
          el("p", { class: "rg-muted", text: "A line into a read format is drawn only where what was found is valid: it downloads at the size the archive published, the archive publishes its md5 and size, and it is complete for 10x -- both the barcode read and the cDNA read are there." }),
        );
      }
      if (node.kind !== "record") {
        parts.push(el("p", { class: "rg-muted", text: "A file or a document is where links end: nothing links onward from it." }));
      } else {
        parts.push(
          el("h4", { text: `Reaches in one hop (${reached.length})` }),
          reached.length ? el("ul", {}, reached.map(([target, entry]) => reachItem(target, entry))) : el("p", { class: "rg-muted", text: "Nothing." }),
          none.length ? el("h4", { text: "Measured, never answering" }) : null,
          none.length
            ? el("ul", {}, none.map((u) => el("li", {}, el("button", { class: "rg-item", type: "button", onclick: () => this.selectPair(node.id, u.target) }, this.label(u.target)))))
            : null,
          surveyedNone.length ? el("h4", { text: "Surveyed, never usable" }) : null,
          surveyedNone.length
            ? el("ul", {}, surveyedNone.map((u) => el("li", {}, el("button", { class: "rg-item", type: "button", onclick: () => this.selectPair(node.id, u.target) }, this.label(u.target)))))
            : null,
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
      const mode = this.pathMode();
      const kicker = { all: "Paths, untested links included", surveyed: "Paths over measured routes and surveyed links", measured: "Paths over measured routes" }[mode];
      const nothing = {
        all: "No known link connects these within four hops.",
        surveyed: "No measured route or surveyed link connects these within four hops. Try including untested links.",
        measured: "No measured route connects these within four hops. Try “Measured + surveyed”, or including untested links.",
      }[mode];
      const parts = [el("p", { class: "rg-panel-kicker", text: kicker }), el("h3", { text: `${this.label(path.from)} → ${this.label(path.to)}` })];
      if (!found.length) {
        parts.push(el("p", { text: nothing }));
        return parts;
      }
      parts.push(
        el(
          "p",
          { class: "rg-muted" },
          `${found.length} path${found.length === 1 ? "" : "s"}, shortest first. ` +
            "Each hop uses the best-ranked route the filters leave visible" +
            { all: ", else a surveyed link, else an untested one. A surveyed link carries no rank.", surveyed: ", else a surveyed link, which carries no rank.", measured: "." }[mode],
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
              hops.every(Boolean) ? hops.map((h) => (measured(h) ? variant(h) : `${kindOf(h)}: ${this.laneName(h)}`)).join("  ·  ") : "a hop has no line under the current filters",
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
        svg(
          "svg",
          { class: "rg-legend-line", viewBox: "0 0 30 8", "aria-hidden": "true" },
          svg("path", { class: `rg-line ${cls}`, d: "M1,4 H29", "stroke-width": width }),
          cls === "rg-line--surveyed" ? svg("path", { class: "rg-line-core", d: "M1,4 H29", "stroke-width": width - 2.6 }) : null,
        );
      const item = (key, label) => el("span", { class: "rg-legend-item" }, key, label);
      const order = Object.keys(FAMILY_LABELS);
      const families = [...new Set(this.data.nodes.map((n) => n.family))].sort((x, y) => (order.indexOf(x) + 1 || 99) - (order.indexOf(y) + 1 || 99));

      this.legend.replaceChildren(
        el("div", { class: "rg-legend-group" }, el("span", { class: "rg-legend-title", text: "Shape is the level" }), this.layers.map((l) => item(icon(l.id), l.label.toLowerCase()))),
        el("div", { class: "rg-legend-group" }, el("span", { class: "rg-legend-title", text: "Colour is the archive" }), families.map((f) => item(el("span", { class: `rg-swatch rg-family-${f}` }), FAMILY_LABELS[f] ?? f))),
        el(
          "div",
          { class: "rg-legend-group" },
          el("span", { class: "rg-legend-title", text: "A measured route's ink: share of its source's queried accessions resolved" }),
          SHARE_LABELS.map((label, i) => item(line(`rg-line--share rg-share-${i}`), label)),
          item(line("rg-line--unknown"), "resolved/empty split not recorded"),
          el("span", { class: "rg-legend-note", text: "Width is how many reached this target, log scale. A route is one line per node pair it covers." }),
        ),
        el(
          "div",
          { class: "rg-legend-group" },
          el("span", { class: "rg-legend-title", text: "A surveyed link" }),
          item(line("rg-line--surveyed", 4.2), "one per node pair, a link survey found it usable over a seeded draw; counts, never a rank"),
          el("span", { class: "rg-legend-note", text: "One width whatever it found: a draw of 300 is ±5.7 points, too coarse to set beside a route. Its counts are in the panel." }),
        ),
        el(
          "div",
          { class: "rg-legend-group" },
          el("span", { class: "rg-legend-title", text: "An untested link" }),
          item(line("rg-line--untested", 1.6), "one per node pair, confirmed by a probe; measures nothing"),
          el("span", { class: "rg-legend-note", text: "Into a file or document: drawn only if what was found passed every check. Names in brackets say who made the files." }),
        ),
      );
    }

    /* ------------------------------------------------------------- matrix */

    /* Every node against every node: filled by the best route's resolved
     * share where a route measures the pair, ringed where a link survey found
     * it usable and no route measures it, outlined where only untested links
     * do, struck through where a route or a link survey measured an absence,
     * dashed where a link is documented and its probe found nothing, blank
     * where no link exists. The gaps are the point. */
    renderMatrix() {
      const nodes = this.data.nodes.filter((n) => this.nodeShown(n.id));
      const cellFor = (source, target) => {
        const routes = [...this.allEdges.values()].filter((e) => measured(e) && e.source === source && e.target === target).sort((a, b) => a.rank - b.rank);
        const linked = [...this.allEdges.values()].some((e) => isSurveyed(e) && e.source === source && e.target === target);
        const untested = [...this.allEdges.values()].some((e) => isUntested(e) && e.source === source && e.target === target);
        const unconfirmed = this.unconfirmed.has(`${source}>${target}`);
        const none = this.measuredNone.has(`${source}>${target}`);
        const surveyedNone = this.surveyedNone.has(`${source}>${target}`);
        if (routes.length) {
          const value = share(routes[0].split);
          return {
            kind: value === null ? "unknown" : `share-${shareBin(value)}`,
            text:
              `${routes.length} measured route${routes.length === 1 ? "" : "s"}; best ${value === null ? "split not recorded" : `resolves ${percent.format(value)}`}` +
              (linked ? "; surveyed links too" : "") +
              (untested ? "; untested links too" : ""),
          };
        }
        if (linked) return { kind: "surveyed", text: "surveyed links, no route" + (untested ? "; untested links too" : "") };
        if (untested) return { kind: "untested", text: "untested links only" };
        if (none) return { kind: "none-measured", text: "measured, never answering" };
        if (surveyedNone) return { kind: "none-surveyed", text: "surveyed, never usable" };
        if (unconfirmed) return { kind: "unconfirmed", text: "documented, not confirmed" };
        return { kind: "none", text: "no one-hop link" };
      };

      const head = el(
        "tr",
        {},
        el("th", { class: "rg-matrix-corner", scope: "col" }, el("span", { text: "from ↓  to →" })),
        nodes.map((n) => el("th", { scope: "col", class: `rg-matrix-col rg-family-${n.family}` }, el("span", { text: n.label }))),
      );
      const rows = nodes.map((source) =>
        el(
          "tr",
          {},
          el("th", { scope: "row", class: `rg-matrix-row rg-family-${source.family}` }, source.label),
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
          key("surveyed", "surveyed, no route"),
          key("untested", "untested only"),
          key("none-measured", "measured, never answering"),
          key("none-surveyed", "surveyed, never usable"),
          key("unconfirmed", "documented, not confirmed"),
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

    /* A measured row's counts are its split: the route's verdicts on this
     * source node, and how many of them reached this target. A surveyed row
     * has one split per mechanism and corpus, listed one per line, and its
     * "reached" is what was usable; it sorts on its first, which is name
     * order, not yield order. */
    columns() {
      const ev = (r) => r.evidence ?? {};
      const first = (r) => surveySplits(r)[0] ?? {};
      const sp = (r) => (measured(r) ? r.split ?? {} : isSurveyed(r) ? { ...first(r), reached: first(r).usable } : {});
      const surveyedShare = (r) => (first(r).queried ? first(r).usable / first(r).queried : null);
      return [
        { key: "direction", title: "Direction", value: (r) => `${this.label(r.source)} → ${this.label(r.target)}` },
        { key: "status", title: "Status", value: (r) => r.status },
        { key: "route", title: "Route or mechanisms", value: (r) => (measured(r) ? r.route : this.laneName(r)) },
        { key: "rank", title: "Rank", numeric: true, value: (r) => (measured(r) ? r.rank : isSurveyed(r) ? 998 : 999) },
        { key: "corpus", title: "Corpus or probe", value: (r) => (measured(r) ? ev(r).corpus ?? "" : isSurveyed(r) ? first(r).corpus ?? "" : r.links[0].probe?.accession ?? "") },
        { key: "surveyed", title: "Date", value: (r) => (measured(r) ? ev(r).surveyed_on ?? "" : isSurveyed(r) ? first(r).surveyed_on ?? "" : r.links[0].probe?.checked_on ?? "") },
        { key: "queried", title: "Queried", numeric: true, value: (r) => sp(r).queried ?? -1 },
        { key: "resolved", title: "Resolved", numeric: true, value: (r) => sp(r).resolved ?? -1 },
        { key: "empty", title: "Empty", numeric: true, value: (r) => sp(r).empty ?? -1 },
        { key: "failed", title: "Failed", numeric: true, value: (r) => sp(r).failed ?? -1 },
        { key: "reached", title: "Reached target", numeric: true, value: (r) => sp(r).reached ?? -1 },
        { key: "results", title: "Distinct results", numeric: true, value: (r) => sp(r).distinct ?? -1 },
        { key: "share", title: "Outcomes", value: (r) => (isSurveyed(r) ? surveyedShare(r) : share(r.split)) ?? -1 },
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
        // Ties fall back to direction, routes before surveyed before
        // untested, then rank.
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

      // A surveyed row: one line per mechanism and corpus in every column
      // that has a count, so the lines read across.
      const lines = (values, cls = "rg-num") => el("td", { class: cls }, values.map((v, i) => [i ? el("br") : null, v]));
      const surveyedRow = (edge) => {
        const splits = surveySplits(edge);
        const named = edge.links.length > 1;
        const count = (key) => lines(splits.map((sp) => number.format(sp[key])));
        return [
          el("td", { class: "rg-num rg-muted rg-nowrap", title: "A surveyed link is never ranked", text: "—" }),
          lines(
            splits.map((sp) => (named ? `${this.mechanisms[sp.mechanism].name}: ${sp.corpus}` : sp.corpus)),
            "rg-corpus",
          ),
          lines(splits.map((sp) => sp.surveyed_on), "rg-nowrap"),
          count("queried"),
          count("resolved"),
          count("empty"),
          count("failed"),
          lines(splits.map((sp) => `${number.format(sp.usable)} usable`), "rg-num rg-nowrap"),
          count("distinct"),
          el(
            "td",
            {},
            splits.map((sp) =>
              el("div", { class: "rg-survey-cell" }, outcomeBar(sp, true), el("span", { class: "rg-nowrap", text: `${number.format(sp.usable)}/${number.format(sp.queried)} usable` })),
            ),
          ),
        ];
      };

      const body = rows.map((edge) => {
        const evidence = edge.evidence ?? {};
        const split = edge.split ?? {};
        const untested = isUntested(edge);
        const probes = untested ? edge.links.map((l) => l.probe).filter(Boolean) : [];
        // Rank to outcomes: a route's evidence, a surveyed link's splits, or
        // an untested link's probe and nothing counted.
        const tail = isSurveyed(edge)
          ? surveyedRow(edge)
          : [
              el("td", { class: "rg-num rg-nowrap", text: untested ? "—" : `${edge.rank} of ${edge.of}` }),
              untested
                ? el("td", { class: "rg-corpus", title: [...new Set(probes.map((p) => p.accession))].join(", "), text: `probe ${[...new Set(probes.map((p) => p.accession))].join(", ")}` })
                : el("td", { class: "rg-corpus", title: evidence.corpus ?? "", text: evidence.corpus ?? "unmeasured" }),
              el("td", { class: "rg-nowrap", text: untested ? probes[0]?.checked_on ?? "" : evidence.surveyed_on ?? "" }),
              cell(split.queried, untested),
              cell(split.resolved, untested),
              cell(split.empty, untested),
              cell(split.failed, untested),
              cell(split.reached, untested),
              cell(split.distinct, untested),
              el("td", {}, untested ? el("span", { class: "rg-muted", text: "not measured" }) : outcomeBar(edge.split, true)),
            ];
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
              measured(edge) ? code(edge.route) : edge.links.map((l, i) => [i ? " · " : "", code(this.mechanisms[l.mechanism].name)]),
            ),
          ),
          tail,
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
  let mounting = 0;

  async function mount() {
    if (current) {
      current.destroy();
      current = null;
    }
    const root = document.getElementById("route-graph");
    const source = document.getElementById("route-graph-data");
    if (!root || !source) return;
    // A newer mount -- the reader navigated on before this one's data arrived
    // -- wins; this one draws nothing.
    const mine = ++mounting;
    root.replaceChildren(el("p", { class: "rg-loading", text: "Loading the graph…" }));
    try {
      const response = await fetch(new URL(source.dataset.src, document.baseURI));
      if (!response.ok) throw new Error(`its data answered HTTP ${response.status}`);
      const data = await response.json();
      if (mine !== mounting || !root.isConnected) return;
      current = new RouteGraph(root, data);
    } catch (error) {
      if (mine !== mounting || !root.isConnected) return;
      root.replaceChildren(
        el("p", { class: "rg-error" }, `The graph could not be drawn (${error.message}). The same numbers are in `, el("a", { href: "../", text: "the route matrix" }), "."),
      );
      throw error;
    }
  }

  if (window.document$) window.document$.subscribe(mount);
  else document.addEventListener("DOMContentLoaded", mount);
})();
