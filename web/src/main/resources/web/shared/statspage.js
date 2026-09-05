// The engine every stats page runs on: fetch the document, draw the panels that changed, keep
// what the reader was doing in the ones that did not, and poll on the cadence the document
// states. A page supplies its panels (a name, the sections it reads, a builder).

import { ago, el, fmtDur, isoOf, stampOf } from "./page.js";

/** How old a tier may be before the page says so, when the document states no cadence. */
const STALE_AFTER_MS = 6 * 3600 * 1000;

/** How many of its own passes a tier may miss before the page calls it late. */
const STALE_PASSES = 6;

/** What each panel was last built from, by name. Cleared whenever the body is. */
const drawn = new Map();

/** The document the body was last built from, or null when the body shows none. */
let renderedAt = null;

/**
 * Put `parts` in `box`, keeping the reader's filter text (keyed by the box's name) and scroll
 * position; the scroll is restored after the filter is re-applied, since filtering changes height.
 */
function fill(box, parts) {
  const typed = new Map();
  for (const b of box.querySelectorAll("input[type=search]")) if (b.value) typed.set(b.name, b.value);
  const scrolled = [...box.querySelectorAll(".wrap.tall")].map((s) => s.scrollTop);
  box.replaceChildren(...parts.filter(Boolean));
  for (const b of box.querySelectorAll("input[type=search]")) {
    const was = typed.get(b.name);
    if (!was) continue;
    b.value = was;
    b.dispatchEvent(new Event("input"));
  }
  [...box.querySelectorAll(".wrap.tall")].forEach((s, i) => {
    if (scrolled[i]) s.scrollTop = scrolled[i];
  });
}

/**
 * The relay's own statement that this document is no longer current, and why; names the tier where
 * the relay does.
 */
function staleBanner(stale) {
  if (!stale || !stale.reason) return null;
  const p = el("p", "err");
  p.append(stale.tier ? `The ${stale.tier} on this page are not current: ${stale.reason}.` : `These numbers are not current: ${stale.reason}.`);
  if (stale.since != null) {
    const when = el("span", null, ` Noticed ${ago(stale.since)}`);
    when.title = isoOf(stale.since);
    p.appendChild(when);
    p.append(stale.generatedAt ? `; they were computed at ${stale.generatedAt}.` : ".");
  }
  return p;
}

function render(doc) {
  // Whose page this is, from the document: one file is served by the relay, the mirror and the monitor.
  if (doc.title) {
    document.title = doc.title;
    if (titleEl) titleEl.textContent = doc.title;
  }
  scopeEl.textContent = doc.scope || "";
  settlePoll(doc);
  // The containers are laid out once and kept, so an untouched panel keeps its scroll, filter
  // and ResizeObserver. Keyed on the containers: a 503 or error card must be cleared from under them.
  if (!bodyEl.querySelector("[data-panel]")) {
    bodyEl.replaceChildren();
    drawn.clear();
    // The staleness banner first, above the hero numbers.
    for (const name of ["stale", ...panels.map((p) => p.name)]) {
      const box = el("div");
      box.dataset.panel = name;
      bodyEl.appendChild(box);
    }
  }
  renderedAt = doc.generatedAt || null;
  // Unconditionally: the one thing that can change while no section has.
  fill(panelBox("stale"), [staleBanner(doc.stale)]);
  // A page-supplied hook that must run before the panels.
  beforePanels(doc);
  for (const panel of panels) {
    const stamp = panel.reads.map((member) => stampOf(doc[member])).join("|");
    // The memo must not preserve emptiness: a panel with `reads: []` would otherwise freeze on
    // the first document for the life of the page.
    if (drawn.get(panel.name) === stamp && panelBox(panel.name).hasChildNodes()) continue;
    drawn.set(panel.name, stamp);
    fill(panelBox(panel.name), panel.build(doc));
  }
  renderFoot(doc);
}

const panelBox = (name) => bodyEl.querySelector(`[data-panel="${name}"]`);

/**
 * When each tier was computed and whether it is late, one line per tier, late measured in the
 * tier's own passes. A tier the page's `tiers` list does not name still gets a line.
 */
function renderFoot(doc) {
  footEl.replaceChildren();
  const tiers = doc.tiers || {};
  const named = new Set(tiers_.map(([key]) => key));
  const rows = [...tiers_.filter(([key]) => tiers[key]), ...Object.keys(tiers).filter((k) => !named.has(k)).map((k) => [k, k])];
  for (const [key, label] of rows) {
    const tier = tiers[key] || {};
    const at = tier.generatedAt ? new Date(tier.generatedAt) : null;
    const line = el("p", "foot-tier");
    line.append(
      `${label} rolled up ${at ? ago(Math.floor(at.getTime() / 1000)) : "at an unknown time"}`,
      tier.tookMs != null ? ` in ${(tier.tookMs / 1000).toFixed(1)}s` : "",
      tier.everySeconds ? `, every ${fmtDur(tier.everySeconds)}` : "",
      ". ",
    );
    const lateAfterMs = tier.everySeconds ? tier.everySeconds * 1000 * STALE_PASSES : STALE_AFTER_MS;
    if (at && Date.now() - at.getTime() > lateAfterMs) {
      // Loud, because every number that pass computed still looks fresh.
      line.appendChild(el("span", "err", `${label} have not been recomputed since — that half of this page may have stopped.`));
    }
    footEl.appendChild(line);
  }
  if (!rows.length) footEl.append("Rolled up at an unknown time. ");
  if (doc.counted) footEl.append(doc.counted + " ");
  const a = el("a", null, docUrl);
  a.href = docUrl;
  footEl.append("Source: ");
  footEl.appendChild(a);
  footEl.append(` (schema ${doc.schema}).`);
  // A document written by a newer relay may mean things this page does not know.
  if (doc.schema > schemaFor(doc)) {
    footEl.appendChild(el("p", "err", `This page was written for schema ${schemaFor(doc)} — some panels may be missing or misread.`));
  }
}

async function load() {
  try {
    // Not `cache: "no-store"`: the default mode revalidates, so the route's 304 can fire.
    const res = await fetch(docUrl);
    if (res.status === 503) {
      // Not zeros: "nothing computed yet" and "this relay holds nothing" are different facts.
      scopeEl.textContent = "No statistics computed yet.";
      // The body no longer shows any document, so the next one must rebuild it.
      renderedAt = null;
      bodyEl.replaceChildren();
      const c = el("div", "card pending");
      c.appendChild(el("h2", null, "Waiting for the first rollup"));
      c.appendChild(el("p", "why",
        pendingNote));
      bodyEl.appendChild(c);
      return;
    }
    if (!res.ok) throw new Error(`GET ${docUrl} — ${res.status} ${res.statusText}`);
    render(await res.json());
  } catch (e) {
    const why = String(e && e.message ? e.message : e);
    // A failed poll is not a failed page: the document on screen is still the last one computed.
    if (renderedAt) {
      // Replaced, not appended: polls keep failing while a relay is down.
      const note = el("p", "err", `Could not refresh: ${why}. The numbers above are from the last successful read.`);
      note.dataset.refresh = "";
      const prior = footEl.querySelector("[data-refresh]");
      if (prior) prior.replaceWith(note);
      else footEl.appendChild(note);
      return;
    }
    scopeEl.textContent = "Could not load the statistics document.";
    renderedAt = null;
    bodyEl.replaceChildren();
    const c = el("div", "card");
    c.appendChild(el("p", "err", why));
    bodyEl.appendChild(c);
  }
}

/** The poll follows the fastest tier the document states, clamped between the floor and the ceiling. */
const POLL_FLOOR_MS = 30 * 1000;
const POLL_CEILING_MS = 5 * 60 * 1000;
let pollEveryMs = POLL_CEILING_MS;

/** The fastest cadence this document states, clamped to something a browser should poll at. */
function settlePoll(doc) {
  const stated = Object.values(doc.tiers || {})
    .map((t) => t.everySeconds)
    .filter((s) => s > 0);
  if (!stated.length) return;
  pollEveryMs = Math.min(POLL_CEILING_MS, Math.max(POLL_FLOOR_MS, Math.min(...stated) * 1000));
}

/**
 * Which schema this document was written against, from the section that names its publisher.
 * An unrecognised document falls back to the relay's, the strictest.
 */
const schemaFor = (doc) => (doc.monitor ? schema.monitor : doc.sync && !doc.corpus ? schema.sync : schema.relay);

/**
 * Mount a stats page: lay out its panels, draw the document, and keep it drawn. `panels` is
 * the table of `{name, reads, build}`; `schema` the document version the page was written
 * against; `tiers` the cadences named in the footer, in reading order; `pendingNote` what to
 * say before the first rollup; `beforePanels` a hook run before the panels.
 */
export function mountStatsPage({
  panels: panelTable,
  schema: schemaVersion,
  tiers: tierNames = [],
  // Document-relative, so one page can be served behind a path prefix.
  docUrl: url = "stats.json",
  pendingNote: pending = "",
  beforePanels: hook = () => {},
}) {
  panels = panelTable;
  schema = schemaVersion;
  tiers_ = tierNames;
  docUrl = url;
  pendingNote = pending;
  beforePanels = hook;
  bodyEl = document.getElementById("body");
  titleEl = document.querySelector("h1");
  scopeEl = document.getElementById("scope");
  footEl = document.getElementById("foot");
  // A chain rather than setInterval, so a slow response cannot stack requests and a newly
  // stated cadence takes effect without a reload.
  (async function poll() {
    await load();
    setTimeout(poll, pollEveryMs);
  })();
}

// Set once by [mountStatsPage], before anything below runs.
let panels = [];
let schema = 0;
let tiers_ = [];
let docUrl = "stats.json";
let pendingNote = "";
let beforePanels = () => {};
let bodyEl = null;
let titleEl = null;
let scopeEl = null;
let footEl = null;

