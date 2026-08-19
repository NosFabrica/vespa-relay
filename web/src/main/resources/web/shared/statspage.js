// THE ENGINE EVERY STATS PAGE RUNS ON: fetch the document, draw the panels that
// changed, keep what the reader was doing in the ones that did not, and poll on
// the cadence the document itself states.
//
// One copy, in :web, because none of it is about any particular document. What
// a page supplies is its PANELS — a name, the sections that panel reads, and a
// builder — and the engine does the rest. That contract is what made splitting
// the mirror's card onto its own page a move rather than a rewrite.
//
// Extracted from the relay's stats.html verbatim, comments included.

import { ago, el, fmtDur, isoOf, stampOf } from "./page.js";

/**
 * How old a tier may be before the page says so out loud, for a document that
 * does not state its own cadence.
 *
 * Not a guess at the rollup interval — that is an operator setting
 * (STATS_INTERVAL_SECONDS, STATS_COUNTERS_INTERVAL_SECONDS), and hardcoding it
 * here would mean telling anyone who changed it that fresh numbers are stale.
 * This is the point past which a document is old enough that "the rollup has
 * stopped" is the more likely explanation than "it is between runs", for any
 * sane interval.
 *
 * Used only as the fallback now: the document publishes `tiers.<name>.everySeconds`,
 * so a tier can be judged in units of its own passes — see [STALE_PASSES].
 */
const STALE_AFTER_MS = 6 * 3600 * 1000;

/**
 * How many of its own passes a tier may miss before the page calls it late.
 *
 * Six, so a single slow pass — a charts rollup that ran long, a counters pass
 * that queued behind it — is never reported as a stopped one, and a genuinely
 * dead tier is named within six minutes on the fast cadence and ninety on the
 * slow one. Both are far inside the flat six hours this replaced.
 */
const STALE_PASSES = 6;

/** What each panel was last built from, by name. Cleared whenever the body is. */
const drawn = new Map();

/** The document the body was last built from, or null when the body shows none. */
let renderedAt = null;

/**
 * Put [parts] in [box], keeping what the reader was doing inside it.
 *
 * A rebuilt panel is new DOM, so anything the reader put INTO the old DOM is
 * gone unless it is carried across. Two things are:
 *
 * Filter text, keyed by the box's NAME rather than by position — there are two
 * search boxes on this page (Kinds and Sync coverage) and a positional read
 * would carry one's query into the other. Re-applied by dispatching `input`
 * rather than by calling the filter directly: the handler is a closure over the
 * rows that were just built.
 *
 * Scroll position, by index within the panel, and AFTER the filter is re-applied
 * — filtering changes how tall the content is, so restoring a scrollTop before
 * it would land somewhere else. This is the state the old whole-body rebuild
 * could not preserve at all (measured: scrollTop 5000 → 0 on every tick), and
 * with the counters refreshing every minute it is the difference between a live
 * page and an unusable one.
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
 * The relay's own statement that this document is no longer current.
 *
 * Distinct from the footer's age line, and both are wanted. The footer measures
 * `generatedAt` against the reader's clock and INFERS that something may have
 * stopped; this is the relay reporting that its last rollup failed, or that it
 * is serving a file written by a previous process, and saying WHY. An operator
 * seeing the second one does not have to work out whether fifteen minutes is
 * long enough to worry about.
 *
 * WHICH numbers, when the relay says: a failed pass names its tier, and with two
 * cadences "these numbers are not current" over a page whose totals are forty
 * seconds old is a sentence a reader has to disbelieve. The notice is about the
 * sections that tier computes, and the page says so rather than implying the lot.
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

/*
 * A tier the document carries and a page's `tiers` list does not still gets a
 * line, under its own name — a page that silently omits a half of the document
 * it does not recognise is how a third cadence would go unnoticed for a release.
 */

function render(doc) {
  // WHOSE PAGE THIS IS, from the document rather than from the markup. One
  // file is served by the relay, the mirror and the monitor, and the reader has
  // two or three of them open at once — a tab reading "Relay stats" on the
  // monitor's port is worse than no title at all.
  if (doc.title) {
    document.title = doc.title;
    if (titleEl) titleEl.textContent = doc.title;
  }
  scopeEl.textContent = doc.scope || "";
  settlePoll(doc);
  // The containers are laid out once and then kept, which is the whole point:
  // a panel keeps its identity across polls, so an untouched one keeps its
  // scroll position, its filter, and its ResizeObserver too.
  //
  // Keyed on the containers themselves rather than on an empty body, because
  // the body is not empty in either state that has to rebuild it: a 503 puts a
  // "waiting for the first rollup" card there and a failed first load puts an
  // error card, and both must be cleared out from under the panels rather than
  // left above them.
  if (!bodyEl.querySelector("[data-panel]")) {
    bodyEl.replaceChildren();
    drawn.clear();
    // The staleness banner FIRST, above the hero numbers, because everything
    // under it is served from a document the relay has already declared out of
    // date. A rollup that has been failing all night otherwise draws a page
    // indistinguishable from a healthy one — the numbers simply stop moving,
    // which is how a stale cache comes to look like a crash.
    for (const name of ["stale", ...panels.map((p) => p.name)]) {
      const box = el("div");
      box.dataset.panel = name;
      bodyEl.appendChild(box);
    }
  }
  renderedAt = doc.generatedAt || null;
  // Unconditionally, on every read: it is one element, it holds nothing the
  // reader can disturb, and it is the one thing on this page that can change
  // while no section has.
  fill(panelBox("stale"), [staleBanner(doc.stale)]);
  // A page-supplied hook that must run BEFORE the panels: the relay's activity
  // panel settles on a grain the document may have stopped carrying, and a
  // panel drawing the old one would draw nothing.
  beforePanels(doc);
  for (const panel of panels) {
    const stamp = panel.reads.map((member) => stampOf(doc[member])).join("|");
    if (drawn.get(panel.name) === stamp) continue;
    drawn.set(panel.name, stamp);
    fill(panelBox(panel.name), panel.build(doc));
  }
  renderFoot(doc);
}

const panelBox = (name) => bodyEl.querySelector(`[data-panel="${name}"]`);

/**
 * When each half of the document was computed, and whether either is late.
 *
 * ONE LINE PER TIER, because one line for the document cannot be honest about
 * it: the counters are refreshed every minute and the charts every fifteen, so a
 * single "rolled up 40s ago" would describe the fast half and quietly cover for
 * a charts pass that died four hours ago. That is the exact failure this page
 * already refuses elsewhere — numbers that stop moving with nothing saying why.
 *
 * "Late" is measured against the tier's OWN stated cadence, which the document
 * carries as `everySeconds`. This page used to have no way of knowing it (the
 * comment on STALE_AFTER_MS says so) and warned at a flat six hours for that
 * reason; a tier that says how often it repeats can be judged in units of its
 * own passes instead.
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
    // NOT `cache: "no-store"`, which was here and quietly cost the endpoint its
    // whole caching story: no-store tells the browser neither to keep the
    // response nor to send `If-None-Match`, so the 304 the route was built
    // around could never fire and every load re-downloaded the document. The
    // route already answers `Cache-Control: no-cache` — revalidate, do not
    // reuse blind — which is exactly the behaviour wanted, so the default
    // fetch mode is the correct one.
    //
    // Do not try to verify the 304 through Playwright: its Chromium has no HTTP
    // cache, so a repeat fetch there never carries a conditional header and
    // every response is a 200 — /web/shared/kinds.js, which is max-age=60,
    // behaves identically. That measures the test browser, not this page. The
    // server side IS checkable, with curl:
    //   ET=$(curl -sD- -o/dev/null localhost:7777/stats.json | grep -i ^etag | cut -d' ' -f2)
    //   curl -o/dev/null -w '%{http_code}\n' -H "If-None-Match: $ET" localhost:7777/stats.json
    const res = await fetch(docUrl);
    if (res.status === 503) {
      // The honest empty state. Not zeros: "nothing computed yet" and "this
      // relay holds nothing" are different facts and must not share a rendering.
      scopeEl.textContent = "No statistics computed yet.";
      // The body no longer shows any document, so the next one must rebuild it
      // even if the rollup that produced it is the one we drew before.
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
    // A FAILED POLL IS NOT A FAILED PAGE. This wiped the body unconditionally,
    // which meant one blip on a five-minute timer — a relay restart, a proxy
    // 502, a dropped wifi — replaced nine cards of good numbers with a single
    // error line, and took the reader's filter text and scroll position with it.
    // The document already on screen is still the last thing this relay
    // computed, and saying so beats destroying it: the numbers were never
    // claimed to be live, and the footer is already where this page admits to
    // their age.
    if (renderedAt) {
      // Replaced, not appended: polls keep failing while a relay is down, and a
      // footer that grows a line every five minutes is its own bug.
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
 * Re-read the document on a timer, AT THE CADENCE THE DOCUMENT STATES.
 *
 * The route was written asserting this existed — "the page polls this on a
 * timer" is why it mints an ETag and answers 304 — and it did not, which made
 * the claim false and the machinery unexercised. With a conditional request
 * each poll is a header exchange until the rollup actually moves.
 *
 * It used to be a flat five minutes, because the interval was an operator
 * setting this page could not see. It can now: every tier publishes its own
 * `everySeconds`, so the page follows the FASTEST half of the document — a
 * minute by default, which is what makes the counters actually live on screen
 * rather than merely live in the JSON. Polling faster than a tier repeats costs
 * only 304s; polling slower would leave a screen sitting on numbers the relay
 * has already replaced.
 *
 * Clamped at both ends. Never below [POLL_FLOOR_MS], so a mistuned or
 * deliberately aggressive counters interval cannot turn every open tab into a
 * per-second poller. Never above the five minutes it always was, so a relay
 * running charts only — counters disabled — still refreshes on its old schedule
 * instead of every fifteen minutes.
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
 * WHICH DOCUMENT IS THIS, and therefore which schema was it written against.
 *
 * One page, three services. The section a document carries is what names its
 * publisher, and each plane versions its own document — so a page that checked
 * one number would report the mirror's schema 1 as ancient against the relay's
 * 2 and offer to explain itself in terms of neither.
 *
 * An unrecognised document falls back to the relay's, which is the oldest and
 * strictest of the three: the page then reports a mismatch it may not have,
 * rather than staying quiet about one it does.
 */
const schemaFor = (doc) => (doc.monitor ? schema.monitor : doc.sync && !doc.corpus ? schema.sync : schema.relay);

/**
 * Mount a stats page: lay out its panels, draw the document, and keep it drawn.
 *
 * Everything a page differs in is an argument, and there are only five things:
 *
 *  - [panels]      the panel table — `{name, reads, build}`; see the relay's and
 *                  the mirror's for the two shapes this has taken so far
 *  - [schema]      the document version this page was written against, so a
 *                  newer document is reported rather than mis-drawn
 *  - [tiers]       the cadences to name in the footer, in reading order
 *  - [pendingNote] what to say while nothing has been computed yet — the one
 *                  string that has to name the service's own settings
 *
 * What the numbers COVER is not an argument: the document says so itself, in
 * `counted`. It is a fact about the data, and the service that computed it is
 * the only one that can state it.
 *
 * [beforePanels] is the one hook, and it exists for exactly one caller: the
 * relay's activity panel has to settle on a grain the document still carries
 * before any panel is built.
 */
export function mountStatsPage({
  panels: panelTable,
  schema: schemaVersion,
  tiers: tierNames = [],
  docUrl: url = "/stats.json",
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
  // A chain rather than setInterval: each read waits for the previous one to
  // finish — so a slow response cannot stack requests on a struggling service —
  // and the delay is re-read every time, which is how a document that states a
  // new cadence takes effect without a reload.
  (async function poll() {
    await load();
    setTimeout(poll, pollEveryMs);
  })();
}

// Set once by [mountStatsPage], before anything below runs.
let panels = [];
let schema = 0;
let tiers_ = [];
let docUrl = "/stats.json";
let pendingNote = "";
let beforePanels = () => {};
let bodyEl = null;
let titleEl = null;
let scopeEl = null;
let footEl = null;

