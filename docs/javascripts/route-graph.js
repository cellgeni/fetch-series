/* The route graph page (routes/graph.md).
 *
 * The data is embedded at build time by hooks/route_graph.py from
 * fetch_series.export.graph.route_graph(): nodes, one edge per route with its
 * RouteEvidence and its rank, and every type path. Nothing that involves a
 * judgement -- ranking, path finding -- is recomputed here; this file only draws.
 *
 * Loaded on every page (extra_javascript), but does nothing unless the page
 * carries #route-graph, and only then fetches Cytoscape.js. Material's instant
 * navigation swaps pages without a reload, so mounting hangs off document$.
 */
(() => {
  "use strict";

  const CYTOSCAPE = {
    src: "https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.30.4/cytoscape.min.js",
    integrity:
      "sha512-xpXUCbrkyJCwC5noJXZqec9rSXdRgFfDoL7Q9/pCPe54Y04OlqwPobMcNuA5NPJ7DRR51OIr6xcOH2wUspyKDQ==",
  };

  // Layered left to right: where you usually start (series, projects,
  // ArrayExpress experiments), through the INSDC records, to runs and files.
  // Each archive family is drawn as a region, so position also says who issued
  // a type. A type not listed here falls back to an automatic layout.
  const POSITIONS = {
    geo_series: { x: 100, y: 70 },
    geo_sample: { x: 470, y: 70 },
    bioproject: { x: 100, y: 330 },
    study: { x: 330, y: 470 },
    sample: { x: 330, y: 610 },
    experiment: { x: 580, y: 260 },
    biosample: { x: 580, y: 450 },
    run: { x: 840, y: 380 },
    ae_experiment: { x: 100, y: 790 },
    file: { x: 1110, y: 380 },
  };

  // Where each region's name sits: in clear space at the top of the region.
  // Labels are their own small nodes, drawn above the edges -- a compound
  // node's own label is drawn beneath them, and the INSDC one was unreadable.
  const REGION_LABELS = {
    geo: { x: 285, y: 28 },
    insdc: { x: 330, y: 205 },
    arrayexpress: { x: 100, y: 745 },
    file: { x: 1110, y: 335 },
  };

  const FAMILIES = {
    geo: "GEO",
    insdc: "INSDC: NCBI, EBI and DDBJ",
    arrayexpress: "ArrayExpress",
    file: "Files",
  };

  // Lower bounds of the resolved-share bins, matching --rg-ramp-0 .. -4.
  const SHARE_BINS = [0, 0.25, 0.5, 0.75, 0.95];
  const SHARE_LABELS = ["under 25%", "25–50%", "50–75%", "75–95%", "95% or more"];

  const number = new Intl.NumberFormat("en-GB");
  const percent = new Intl.NumberFormat("en-GB", { style: "percent", maximumFractionDigits: 1 });

  let loading = null;

  function loadCytoscape() {
    if (window.cytoscape) return Promise.resolve(window.cytoscape);
    if (!loading) {
      loading = new Promise((resolve, reject) => {
        const script = document.createElement("script");
        script.src = CYTOSCAPE.src;
        script.integrity = CYTOSCAPE.integrity;
        script.crossOrigin = "anonymous";
        script.referrerPolicy = "no-referrer";
        script.onload = () => resolve(window.cytoscape);
        script.onerror = () => {
          loading = null;
          reject(new Error("Cytoscape.js could not be loaded"));
        };
        document.head.append(script);
      });
    }
    return loading;
  }

  /* Build DOM without innerHTML: route notes and corpus names are data. */
  function el(tag, attrs = {}, ...children) {
    const node = document.createElement(tag);
    for (const [key, value] of Object.entries(attrs)) {
      if (value === null || value === undefined || value === false) continue;
      if (key === "class") node.className = value;
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

  /* The stacked outcome bar: resolved | empty | failed, or the unrecorded
   * remainder as texture when the survey never separated resolved from empty. */
  function outcomeBar(evidence, mini = false) {
    const bar = el("div", { class: mini ? "rg-bar rg-bar--mini" : "rg-bar", "aria-hidden": "true" });
    if (!evidence || !evidence.queried) return bar;
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
    for (const [kind, count] of parts) {
      if (count > 0) {
        bar.append(el("span", { class: `rg-seg rg-seg--${kind}`, style: `flex: ${count} 0 0` }));
      }
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

  class RouteGraph {
    constructor(root, data, cytoscape) {
      this.root = root;
      this.data = data;
      this.nodes = new Map(data.nodes.map((n) => [n.id, n]));
      this.edges = new Map(data.edges.map((e) => [e.id, e]));
      this.providers = [...new Set(data.edges.map((e) => e.provider))].sort();
      this.state = {
        colourBy: "share",
        providers: new Set(this.providers),
        topOnly: false,
        selected: null, // { kind: "edge" | "node", id }
        path: null, // { from, to, index }
        sort: { key: "direction", ascending: true },
      };
      this.maxResults = Math.max(1, ...data.edges.map((e) => e.evidence?.unique_results ?? 0));

      this.buildDom();
      this.cy = cytoscape({
        container: this.canvas,
        elements: this.elements(),
        style: this.style(),
        layout: this.layout(),
        minZoom: 0.25,
        maxZoom: 3,
        boxSelectionEnabled: false,
        // Selection and emphasis are this page's state, not Cytoscape's.
        autounselectify: true,
      });
      this.bindGraphEvents();
      this.applyFilters();
      this.renderPanel();
      this.renderLegend();
      this.renderTable();

      // Cytoscape keeps its zoom when the container changes size, which on a
      // rotated phone or a resized window leaves the graph half off-canvas.
      this.resizeObserver = new ResizeObserver(() => {
        this.cy.resize();
        this.cy.fit(undefined, 40);
      });
      this.resizeObserver.observe(this.canvas);

      // Material toggles light/dark by attribute, without reloading the page.
      this.schemeObserver = new MutationObserver(() => this.restyle());
      this.schemeObserver.observe(document.body, { attributes: true, attributeFilter: ["data-md-color-scheme"] });
    }

    destroy() {
      this.unbind();
      this.resizeObserver.disconnect();
      this.schemeObserver.disconnect();
      this.cy.destroy();
      this.tooltip.remove();
    }

    /* ---------------------------------------------------------------- DOM */

    buildDom() {
      const colourBy = el(
        "fieldset",
        {},
        el("legend", { text: "Colour edges by" }),
        ...[
          ["share", "share resolved"],
          ["failures", "failures"],
        ].map(([value, label]) =>
          el(
            "label",
            {},
            el("input", {
              type: "radio",
              name: "rg-colour",
              value,
              checked: value === this.state.colourBy,
              onchange: () => {
                this.state.colourBy = value;
                this.restyle();
                this.renderLegend();
              },
            }),
            label,
          ),
        ),
      );

      const providers = el(
        "fieldset",
        {},
        el("legend", { text: "Providers" }),
        ...this.providers.map((provider) =>
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
      );

      const topOnly = el(
        "label",
        {},
        el("input", {
          type: "checkbox",
          onchange: (event) => {
            this.state.topOnly = event.target.checked;
            this.applyFilters();
          },
        }),
        "Top-ranked route per direction only",
      );

      const typeOptions = (placeholder) => [
        el("option", { value: "", text: placeholder }),
        ...[...this.nodes.values()]
          .sort((a, b) => a.label.localeCompare(b.label))
          .map((n) => el("option", { value: n.id, text: n.label })),
      ];
      this.fromSelect = el("select", { "aria-label": "Path from", onchange: () => this.choosePath() }, typeOptions("from…"));
      this.toSelect = el("select", { "aria-label": "Path to", onchange: () => this.choosePath() }, typeOptions("to…"));
      const pathFinder = el(
        "div",
        { class: "rg-path-finder" },
        el("span", { class: "rg-control-label", text: "Find a path" }),
        this.fromSelect,
        " → ",
        this.toSelect,
      );

      // Cytoscape owns its container outright, so the buttons sit beside it.
      this.canvas = el("div", { class: "rg-cy", role: "img", "aria-label": "Route graph; the table below lists the same routes" });
      const stage = el(
        "div",
        { class: "rg-canvas" },
        this.canvas,
        el(
          "div",
          { class: "rg-canvas-actions" },
          el("button", { class: "rg-button", type: "button", text: "Clear", onclick: () => this.clear() }),
          el("button", { class: "rg-button", type: "button", text: "Reset view", onclick: () => this.resetView() }),
        ),
      );
      this.panel = el("aside", { class: "rg-panel", "aria-live": "polite" });
      this.legend = el("div", { class: "rg-legend" });
      this.tableWrap = el("div", { class: "rg-table-wrap" });
      this.tooltip = el("div", { class: "rg-tooltip rg-root", hidden: true });

      this.root.replaceChildren(
        el("div", { class: "rg-controls" }, colourBy, pathFinder, topOnly, providers),
        el("div", { class: "rg-main" }, stage, this.panel),
        this.legend,
        this.tableWrap,
      );
      document.body.append(this.tooltip);
    }

    /* -------------------------------------------------------------- graph */

    elements() {
      const families = new Set(this.data.nodes.map((n) => n.family));
      const measure = document.createElement("canvas").getContext("2d");
      measure.font = `500 13px ${getComputedStyle(this.root).fontFamily}`;

      const placed = this.data.nodes.every((n) => POSITIONS[n.id]);
      const parents = [...families].map((family) => ({
        group: "nodes",
        data: {
          id: `family:${family}`,
          // Under the fallback layout there are no fixed label positions, so
          // the region carries its own name instead.
          label: placed && REGION_LABELS[family] ? "" : FAMILIES[family] ?? family,
          kind: "family",
        },
        selectable: false,
        grabbable: false,
      }));
      const labels = placed
        ? [...families]
            .filter((family) => REGION_LABELS[family])
            .map((family) => ({
              group: "nodes",
              data: {
                id: `label:${family}`,
                label: FAMILIES[family] ?? family,
                kind: "region",
                parent: `family:${family}`,
              },
              position: { ...REGION_LABELS[family] },
              selectable: false,
              grabbable: false,
            }))
        : [];
      const nodes = this.data.nodes.map((n) => ({
        group: "nodes",
        data: {
          id: n.id,
          label: n.label,
          kind: "type",
          parent: `family:${n.family}`,
          width: Math.ceil(measure.measureText(n.label).width) + 28,
        },
        position: POSITIONS[n.id] ? { ...POSITIONS[n.id] } : undefined,
      }));
      const edges = this.data.edges.map((e) => ({
        group: "edges",
        data: {
          id: e.id,
          source: e.source,
          target: e.target,
          kind: "route",
          rankLabel: e.of > 1 ? `#${e.rank}` : "",
          width: 1.5 + (5 * Math.log10(1 + (e.evidence?.unique_results ?? 0))) / Math.log10(1 + this.maxResults),
        },
      }));
      return [...parents, ...labels, ...nodes, ...edges];
    }

    layout() {
      const placed = this.data.nodes.every((n) => POSITIONS[n.id]);
      return placed ? { name: "preset", padding: 40 } : { name: "breadthfirst", directed: true, padding: 40 };
    }

    tokens() {
      const css = getComputedStyle(this.root);
      const v = (name) => css.getPropertyValue(name).trim();
      return {
        surface: v("--rg-cy-surface"),
        region: v("--rg-cy-region"),
        hairline: v("--rg-cy-hairline"),
        ink: v("--rg-cy-ink"),
        ink2: v("--rg-cy-ink-2"),
        muted: v("--rg-muted"),
        nodeBorder: v("--rg-node-border"),
        accent: v("--rg-cy-accent"),
        failed: v("--rg-failed"),
        quiet: v("--rg-quiet"),
        ramp: [0, 1, 2, 3, 4].map((i) => v(`--rg-ramp-${i}`)),
        font: css.fontFamily,
      };
    }

    edgeColour(route, t) {
      const evidence = route.evidence;
      if (this.state.colourBy === "failures") {
        return evidence && evidence.failed > 0 ? t.failed : t.quiet;
      }
      const value = share(evidence);
      return value === null ? t.muted : t.ramp[shareBin(value)];
    }

    style() {
      const t = this.tokens();
      const route = (ele) => this.edges.get(ele.id());
      return [
        {
          selector: "node[kind = 'family']",
          style: {
            shape: "round-rectangle",
            "background-color": t.region,
            "background-opacity": 1,
            "border-width": 1,
            "border-color": t.hairline,
            padding: 26,
            label: "data(label)",
            "text-valign": "top",
            "text-halign": "center",
            "text-margin-y": -6,
            "text-background-color": t.surface,
            "text-background-opacity": 1,
            "text-background-padding": 3,
            "text-background-shape": "roundrectangle",
            color: t.ink2,
            "font-family": t.font,
            "font-size": 12,
            "font-weight": 700,
            events: "no",
          },
        },
        {
          selector: "node[kind = 'region']",
          style: {
            width: 1,
            height: 1,
            "background-opacity": 0,
            "border-width": 0,
            label: "data(label)",
            "text-valign": "center",
            "text-halign": "center",
            "text-background-color": t.region,
            "text-background-opacity": 1,
            "text-background-padding": 3,
            color: t.ink2,
            "font-family": t.font,
            "font-size": 12,
            "font-weight": 700,
            "z-compound-depth": "top",
            events: "no",
          },
        },
        {
          selector: "node[kind = 'type']",
          style: {
            shape: "round-rectangle",
            width: "data(width)",
            height: 34,
            "background-color": t.surface,
            "border-width": 1.5,
            "border-color": t.nodeBorder,
            label: "data(label)",
            "text-valign": "center",
            "text-halign": "center",
            color: t.ink,
            "font-family": t.font,
            "font-size": 13,
            "font-weight": 500,
            "overlay-opacity": 0,
          },
        },
        {
          selector: "edge",
          style: {
            "curve-style": "bezier",
            "control-point-step-size": 26,
            width: "data(width)",
            "line-color": (ele) => this.edgeColour(route(ele), t),
            "target-arrow-color": (ele) => this.edgeColour(route(ele), t),
            "target-arrow-shape": "triangle",
            "arrow-scale": 0.85,
            "line-cap": "round",
            "line-style": (ele) =>
              this.state.colourBy === "share" && share(route(ele).evidence) === null ? "dashed" : "solid",
            "line-dash-pattern": [7, 5],
            label: "data(rankLabel)",
            "font-family": t.font,
            "font-size": 10,
            color: t.ink2,
            "text-background-color": t.surface,
            "text-background-opacity": 0.9,
            "text-background-padding": 2,
            "text-background-shape": "roundrectangle",
            "min-zoomed-font-size": 8,
            "overlay-opacity": 0,
          },
        },
        { selector: ".rg-hidden", style: { display: "none" } },
        { selector: ".rg-faded", style: { opacity: 0.12 } },
        {
          selector: "edge.rg-focus",
          style: {
            "underlay-color": t.accent,
            "underlay-opacity": 0.25,
            "underlay-padding": 4,
            "z-index": 10,
          },
        },
        {
          selector: "node.rg-focus",
          style: { "border-color": t.accent, "border-width": 2.5 },
        },
      ];
    }

    restyle() {
      this.cy.style().fromJson(this.style()).update();
    }

    resetView() {
      this.cy.nodes("[kind = 'type'], [kind = 'region']").forEach((node) => {
        const [prefix, family] = node.id().split(":");
        const position = prefix === "label" ? REGION_LABELS[family] : POSITIONS[node.id()];
        if (position) node.position({ ...position });
      });
      this.cy.fit(undefined, 40);
    }

    bindGraphEvents() {
      this.cy.on("tap", "edge", (event) => this.select("edge", event.target.id()));
      this.cy.on("tap", "node[kind = 'type']", (event) => this.select("node", event.target.id()));
      this.cy.on("tap", (event) => {
        if (event.target === this.cy) this.clear();
      });

      this.cy.on("mouseover", "edge, node[kind = 'type']", (event) => {
        this.canvas.style.cursor = "pointer";
        this.showTooltip(event);
      });
      this.cy.on("mousemove", "edge, node[kind = 'type']", (event) => this.moveTooltip(event.originalEvent));
      this.cy.on("mouseout", "edge, node[kind = 'type']", () => {
        this.canvas.style.cursor = "";
        this.tooltip.hidden = true;
      });
      // A fast pointer can leave the canvas without Cytoscape seeing a mouseout.
      const hide = () => {
        this.tooltip.hidden = true;
      };
      this.cy.on("viewport", hide);
      this.canvas.addEventListener("mouseleave", hide);
      window.addEventListener("scroll", hide, { passive: true });
      this.unbind = () => window.removeEventListener("scroll", hide);
    }

    /* ------------------------------------------------------------ tooltip */

    showTooltip(event) {
      const target = event.target;
      if (target.isEdge()) {
        const route = this.edges.get(target.id());
        const evidence = route.evidence;
        this.tooltip.replaceChildren(
          el("div", { class: "rg-tooltip-title", text: route.id }),
          el("div", { class: "rg-tooltip-values", text: outcomeText(evidence) }),
          outcomeBar(evidence),
          evidence
            ? el(
                "div",
                {},
                `of ${number.format(evidence.queried)} queried · ` +
                  `${number.format(evidence.unique_results ?? 0)} distinct results`,
              )
            : null,
          route.of > 1 ? el("div", {}, `Rank ${route.rank} of ${route.of} for this direction`) : null,
        );
      } else {
        const node = this.nodes.get(target.id());
        this.tooltip.replaceChildren(
          el("div", { class: "rg-tooltip-values", text: node.label }),
          el("div", {}, `${node.routes_out} routes out · ${node.routes_in} in`),
        );
      }
      this.tooltip.hidden = false;
      this.moveTooltip(event.originalEvent);
    }

    moveTooltip(pointer) {
      if (!pointer || this.tooltip.hidden) return;
      const { innerWidth, innerHeight } = window;
      const box = this.tooltip.getBoundingClientRect();
      const x = pointer.clientX + 14 + box.width > innerWidth ? pointer.clientX - box.width - 14 : pointer.clientX + 14;
      const y = pointer.clientY + 14 + box.height > innerHeight ? pointer.clientY - box.height - 14 : pointer.clientY + 14;
      this.tooltip.style.left = `${Math.max(4, x)}px`;
      this.tooltip.style.top = `${Math.max(4, y)}px`;
    }

    /* ------------------------------------------------- filters & emphasis */

    visible(route) {
      return this.state.providers.has(route.provider) && (!this.state.topOnly || route.rank === 1);
    }

    applyFilters() {
      this.cy.batch(() => {
        this.cy.edges().forEach((edge) => {
          edge.toggleClass("rg-hidden", !this.visible(this.edges.get(edge.id())));
        });
      });
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
      this.cy.batch(() => {
        this.cy.elements().removeClass("rg-faded rg-focus");
        if (!focus) return;
        this.cy.nodes("[kind = 'type']").forEach((node) => {
          node.addClass(focus.nodes.has(node.id()) ? "rg-focus" : "rg-faded");
        });
        this.cy.edges().forEach((edge) => {
          edge.addClass(focus.edges.has(edge.id()) ? "rg-focus" : "rg-faded");
        });
      });
    }

    select(kind, id) {
      this.state.selected = { kind, id };
      this.state.path = null;
      this.fromSelect.value = "";
      this.toSelect.value = "";
      this.emphasise();
      this.renderPanel();
      this.highlightRow();
    }

    clear() {
      this.state.selected = null;
      this.state.path = null;
      this.fromSelect.value = "";
      this.toSelect.value = "";
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
      if (!from || !to) {
        this.state.path = null;
      } else {
        const found = this.paths(from, to);
        this.state.path = { from, to, index: found.length ? 0 : null };
      }
      this.emphasise();
      this.renderPanel();
      this.highlightRow();
    }

    /* -------------------------------------------------------------- panel */

    routeItem(route, pressed = false) {
      const evidence = route.evidence;
      const value = share(evidence);
      return el(
        "button",
        {
          class: "rg-item",
          type: "button",
          "aria-pressed": pressed ? "true" : "false",
          onclick: () => this.select("edge", route.id),
        },
        route.of > 1 ? `#${route.rank} ` : "",
        code(route.id),
        el(
          "span",
          { class: "rg-item-meta" },
          outcomeBar(evidence, true),
          " ",
          value === null ? "split not recorded" : `${percent.format(value)} resolved`,
          evidence && evidence.failed ? ` · ${number.format(evidence.failed)} failed` : "",
        ),
      );
    }

    renderPanel() {
      const { selected, path } = this.state;
      let parts;
      if (path) parts = this.pathPanel(path);
      else if (selected?.kind === "edge") parts = this.edgePanel(this.edges.get(selected.id));
      else if (selected?.kind === "node") parts = this.nodePanel(this.nodes.get(selected.id));
      else parts = this.overviewPanel();
      // replaceChildren would print a skipped optional part as the text "null".
      this.panel.replaceChildren(...parts.filter(Boolean));
      this.panel.scrollTop = 0;
    }

    overviewPanel() {
      const routes = [...this.edges.values()];
      const directions = new Set(routes.map((r) => `${r.source}>${r.target}`));
      const split = routes.filter((r) => r.evidence && r.evidence.empty !== null).length;
      const failing = routes.filter((r) => r.evidence && r.evidence.failed > 0).length;
      return [
        el("h3", { text: "Every route, measured" }),
        el(
          "p",
          {},
          `${routes.length} routes across ${directions.size} directions between ${this.nodes.size} accession types. ` +
            `${split} record how many lookups came back empty; ${failing} failed on at least one accession.`,
        ),
        el("h4", { text: "To explore" }),
        el(
          "ul",
          {},
          el("li", {}, "Click an edge for its evidence: outcomes, corpus, date, cost and known pathologies."),
          el("li", {}, "Click a type for every route into and out of it, best first."),
          el("li", {}, "Pick two types under “Find a path” to see how to get from one to the other."),
          el("li", {}, "Hover for the numbers; scroll to zoom; drag to pan."),
        ),
      ];
    }

    edgePanel(route) {
      const evidence = route.evidence;
      const source = this.nodes.get(route.source).label;
      const target = this.nodes.get(route.target).label;
      const parts = [
        el("h3", {}, code(route.id)),
        el("p", { text: route.summary }),
        el(
          "p",
          {},
          route.of > 1
            ? `Rank ${route.rank} of ${route.of} for ${source} → ${target}, as the resolver tries them.`
            : `The only route from ${source} to ${target}.`,
        ),
      ];
      if (!evidence) {
        parts.push(el("p", { class: "rg-unrecorded", text: "No survey has measured this route yet." }));
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
          el("h4", { text: "Outcomes" }),
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
            ? el(
                "p",
                { class: "rg-unrecorded" },
                "This survey did not separate resolved from empty, so the answered accessions are shown together.",
              )
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
          ),
        );
        if (evidence.notes) parts.push(el("p", { text: evidence.notes }));
      }

      parts.push(
        el("h4", { text: "Route" }),
        el(
          "dl",
          {},
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
                p.url ? null : el("span", { class: "rg-unrecorded" }, " (not yet written up)"),
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
          const key = route[other];
          if (!byOther.has(key)) byOther.set(key, []);
          byOther.get(key).push(route);
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
      return [
        el("h3", { text: node.label }),
        el("p", {}, `${node.routes_out} routes out, ${node.routes_in} in.`),
        el("h4", { text: `From ${node.label}` }),
        out.length ? el("ul", {}, group(out, "target")) : el("p", { class: "rg-unrecorded", text: "None shown." }),
        el("h4", { text: `To ${node.label}` }),
        into.length ? el("ul", {}, group(into, "source")) : el("p", { class: "rg-unrecorded", text: "None shown." }),
      ];
    }

    pathPanel(path) {
      const from = this.nodes.get(path.from).label;
      const to = this.nodes.get(path.to).label;
      const found = this.paths(path.from, path.to);
      const parts = [el("h3", { text: `${from} → ${to}` })];
      if (!found.length) {
        parts.push(el("p", { text: "No declared route connects these, within four hops." }));
        return parts;
      }
      parts.push(
        el(
          "p",
          {},
          `${found.length} type path${found.length === 1 ? "" : "s"}, shortest first. ` +
            "Each hop uses the best-ranked route the current filters leave visible.",
        ),
      );
      found.forEach((steps, index) => {
        const hops = [];
        let complete = true;
        for (let i = 0; i < steps.length - 1; i++) {
          const route = this.hopRoute(steps[i], steps[i + 1]);
          if (!route) complete = false;
          hops.push(route);
        }
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
              complete
                ? hops.map((route) => route.id.split(":")[1]).join(" · ")
                : "a hop has no route under the current filters",
            ),
          ),
        );
      });
      return parts;
    }

    /* ------------------------------------------------------------- legend */

    renderLegend() {
      const item = (key, label) => el("span", { class: "rg-legend-item" }, key, label);
      const line = (colour, dashed = false) =>
        el("span", { class: dashed ? "rg-key rg-key--dashed" : "rg-key", style: `border-top-color: ${colour}` });
      const t = this.tokens();
      const edges =
        this.state.colourBy === "share"
          ? el(
              "div",
              { class: "rg-legend-group" },
              el("span", { class: "rg-legend-title", text: "Edge colour: share of queried accessions resolved" }),
              SHARE_LABELS.map((label, i) => item(line(t.ramp[i]), label)),
              item(line(t.muted, true), "split not recorded"),
            )
          : el(
              "div",
              { class: "rg-legend-group" },
              el("span", { class: "rg-legend-title", text: "Edge colour: failures" }),
              item(line(t.quiet), "none failed"),
              item(line(t.failed), "at least one failed"),
            );
      this.legend.replaceChildren(
        edges,
        el(
          "div",
          { class: "rg-legend-group" },
          el("span", { class: "rg-legend-title", text: "Bars" }),
          ["resolved", "empty", "failed"].map((kind) => item(el("span", { class: `rg-swatch rg-swatch--${kind}` }), kind)),
          item(el("span", { class: "rg-swatch rg-swatch--unknown" }), "resolved or empty, not separated"),
        ),
        el(
          "div",
          { class: "rg-legend-group" },
          el("span", { class: "rg-legend-title", text: "Width" }),
          "distinct results, log scale · #n is the rank within a direction",
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
        const tie =
          columns[0].value(a).localeCompare(columns[0].value(b)) || a.rank - b.rank;
        return (ascending ? order : -order) || tie;
      });

      const head = el(
        "tr",
        {},
        columns.map((c) =>
          el(
            "th",
            {
              class: c.numeric ? "rg-num" : null,
              "aria-sort": c.key === key ? (ascending ? "ascending" : "descending") : null,
            },
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

      const cell = (value, numeric) =>
        value === null || value === undefined
          ? el("td", { class: numeric ? "rg-num rg-unrecorded" : "rg-unrecorded", text: "not recorded" })
          : el("td", { class: numeric ? "rg-num" : null, text: numeric ? number.format(value) : value });

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
                  this.canvas.parentElement.scrollIntoView({ behavior: "smooth", block: "nearest" });
                },
              },
              code(route.id),
            ),
          ),
          el("td", { class: "rg-num rg-nowrap", text: `${route.rank} of ${route.of}` }),
          el("td", { class: "rg-corpus", title: evidence.corpus ?? "", text: evidence.corpus ?? "unmeasured" }),
          el("td", { class: "rg-nowrap", text: evidence.surveyed_on ?? "" }),
          cell(evidence.queried, true),
          cell(evidence.resolved, true),
          cell(evidence.empty, true),
          cell(evidence.failed, true),
          cell(evidence.unique_results, true),
          el("td", {}, outcomeBar(route.evidence, true)),
        );
      });

      this.tableWrap.replaceChildren(
        el(
          "table",
          { class: "rg-table" },
          el("thead", {}, head),
          el("tbody", {}, body),
        ),
      );
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

    const data = JSON.parse(source.textContent);
    loadCytoscape()
      .then((cytoscape) => {
        // The reader may have navigated away while the library loaded.
        if (document.getElementById("route-graph") === root) current = new RouteGraph(root, data, cytoscape);
      })
      .catch((error) => {
        root.replaceChildren(
          el(
            "p",
            { class: "rg-error" },
            `The graph could not be drawn (${error.message}). The same numbers are in `,
            el("a", { href: "../", text: "the route matrix" }),
            ".",
          ),
        );
      });
  }

  if (window.document$) window.document$.subscribe(mount);
  else document.addEventListener("DOMContentLoaded", mount);
})();
