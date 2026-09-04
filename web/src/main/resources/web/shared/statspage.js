// The engine every stats page runs on: fetch the document, draw the panels
// that changed, keep what the reader was doing in the ones that did not, and
// poll on the cadence the document states. A page supplies its panels (a name,
// the sections it reads, a builder) and the engine does the rest.

import { ago, el, fmtDur, isoOf, stampOf } from "./page.js";

/**
 * How old a tier may be before the page says so, for a document that states
 * no cadence. Not a guess at the rollup interval, which is an operator
 * setting; the fallback where `tiers.<name>.everySeconds` is absent.
 */
const STALE_AFTER_MS = 6 * 3600 * 1000;

/** How many of its own passes a tier may miss before the page calls it late. */
const STALE_PASSES = 6;

/** What each panel was last built from, by name. Cleared whenever the body is. */
const drawn = new Map();

/** The document the body was last built from, or null when the body shows none. */
let renderedAt = null;

/**
 * Put `parts` in `box`, keeping what the reader was doing inside it: filter
 * text, keyed by the box's name since the page has two search boxes, and
 * scroll position by index, restored after the filter is re-applied because
 * filtering changes how tall the content is.
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
 * The relay's own statement that this document is no longer current, and why.
 * Distinct from the footer's age line, which only infers. Names the tier
 * where the relay does: with two cadences, "these numbers are not current"
 * over totals forty seconds old is a sentence a reader has to disbelieve.
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
  // Whose page this is, from the document rather than the markup: one file is
  // served by the relay, the mirror and the monitor.
  if (doc.title) {
    document.title = doc.title;
    if (titleEl) titleEl.textContent = doc.title;
  }
  scopeEl.textContent = doc.scope || "";
  settlePoll(doc);
  // The containers are laid out once and kept, so an untouched panel keeps its
  // scroll, its filter and its ResizeObserver. Keyed on the containers rather
  // than on an empty body: a 503 card or an error card must be cleared out
  // from under the panels rather than left above them.
  if (!bodyEl.querySelector("[data-panel]")) {
    bodyEl.replaceChildren();
    drawn.clear();
    // The staleness banner first, above the hero numbers: a rollup that has
    // been failing all night otherwise draws a page indistinguishable from a
    // healthy one.
    for (const name of ["stale", ...panels.map((p) => p.name)]) {
      const box = el("div");
      box.dataset.panel = name;
      bodyEl.appendChild(box);
    }
  }
  renderedAt = doc.generatedAt || null;
  // Unconditionally: one element, nothing the reader can disturb, and the one
  // thing that can change while no section has.
  fill(panelBox("stale"), [staleBanner(doc.stale)]);
  // A page-supplied hook that must run before the panels: the relay's activity
  // panel settles on a grain the document may have stopped carrying.
  beforePanels(doc);
  for (const panel of panels) {
    const stamp = panel.reads.map((member) => stampOf(doc[member])).join("|");
    // The memo preserves reader state, but must not preserve emptiness: a panel
    // memoised on an empty stamp (`reads: []`, which the verdicts panel uses)
    // would otherwise freeze on the first document for the life of the page.
    if (drawn.get(panel.name) === stamp && panelBox(panel.name).hasChildNodes()) continue;
    drawn.set(panel.name, stamp);
    fill(panelBox(panel.name), panel.build(doc));
  }
  renderFoot(doc);
}

const panelBox = (name) => bodyEl.querySelector(`[data-panel="${name}"]`);

/**
 * When each tier of the document was computed, and whether either is late,
 * one line per tier: one line for the document would cover for a charts pass
 * that died hours ago. Late is measured in the tier's own passes. A tier the
 * document carries and the page's `tiers` list does not still gets a line,
 * under its own name.
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
  // A document written by a newer relay may mean things this page does not
  // know; saying so beats charting fields we are guessing at.
  if (doc.schema > schemaFor(doc)) {
    footEl.appendChild(el("p", "err", `This page was written for schema ${schemaFor(doc)} — some panels may be missing or misread.`));
  }
}

async function load() {
  try {
    // Not `cache: "no-store"`: that neither keeps the response nor sends
    // `If-None-Match`, so the route's 304 could never fire. The default mode
    // revalidates, which is what `Cache-Control: no-cache` asks for.
    const res = await fetch(docUrl);
    if (res.status === 503) {
      // The honest empty state. Not zeros: "nothing computed yet" and "this
      // relay holds nothing" are different facts.
      scopeEl.textContent = "No statistics computed yet.";
      // The body no longer shows any document, so the next one must rebuild it
      // even if it is the rollup drawn before.
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
    // A failed poll is not a failed page: the document on screen is still the
    // last thing this relay computed, and the footer already admits its age.
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

/**
 * The poll follows the fastest tier the document states, so the counters are
 * live on screen and not only in the JSON; each poll is a conditional request
 * and costs a 304 until the rollup moves. Clamped: never below the floor, so
 * a mistuned interval cannot make every tab a per-second poller, and never
 * above the ceiling, so a charts-only relay keeps its old schedule.
 */
const POLL_FLOOR_MS = 30 * 1000;
const POLL_CEILING_MS = 5 * 60 * 1000;
let pollEveryMs = POLL_CEILING_MS;

/** The fastest cadence this document says it is on, clamped to something a browser should poll at. */
function settlePoll(doc) {
  const stated = Object.values(doc.tiers || {})
    .map((t) => t.everySeconds)
    .filter((s) => s > 0);
  if (!stated.length) return;
  pollEveryMs = Math.min(POLL_CEILING_MS, Math.max(POLL_FLOOR_MS, Math.min(...stated) * 1000));
}

/**
 * Which document this is, and so which schema it was written against: the
 * section a document carries names its publisher, and each plane versions its
 * own. An unrecognised document falls back to the relay's, the strictest, so
 * the page reports a mismatch it may not have rather than hiding one it does.
 */
const schemaFor = (doc) => (doc.monitor ? schema.monitor : doc.sync && !doc.corpus ? schema.sync : schema.relay);

/**
 * Mount a stats page: lay out its panels, draw the document, and keep it
 * drawn. `panels` is the table of `{name, reads, build}`; `schema` the
 * document version the page was written against; `tiers` the cadences to
 * name in the footer, in reading order; `pendingNote` what to say before the
 * first rollup. What the numbers cover is not an argument: the document says
 * so itself in `counted`. `beforePanels` exists for the relay's activity
 * panel alone.
 */
export function mountStatsPage({
  panels: panelTable,
  schema: schemaVersion,
  tiers: tierNames = [],
  // Document-relative, so one page can be served behind a path prefix;
  // `/stats.json` would be asked of the host root wherever the page sits.
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
  // A chain rather than setInterval: each read waits for the previous one, so
  // a slow response cannot stack requests, and the delay is re-read every
  // time, so a newly stated cadence takes effect without a reload.
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

