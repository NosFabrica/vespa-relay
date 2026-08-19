// The stats pages' shared vocabulary — number formatting, DOM helpers, the
// hover tooltip, and a card's header.
//
// Shared because the pages are one design and one reading: the relay's corpus
// page, the mirror's status page, and whatever a later service publishes. It
// lives in :web with the routes that serve it, and every page imports it as
// `/web/shared/page.js` — a url the asset route resolves off the CLASSPATH, so
// which jar a module ships in is not something a page has to know.
//
// Extracted from the relay's stats.html verbatim, comments included: the
// reasons behind these are measurements, not preferences.

const fmt = (n) => Number(n).toLocaleString();

/**
 * The same number, short enough to label a chart with.
 *
 * "1.2M" where `fmt` says "1,234,567". Grouped digits are the right answer
 * everywhere a reader might compare or quote the number — a tile, a table cell,
 * a tooltip — and the wrong one on an AXIS, where the label is a ruler mark: a
 * y-axis reading 1,234,567 / 617,284 / 0 spends a third of the plot width
 * spelling precision nobody reads off a gridline, and the per-kind sparklines
 * put their peak in a head that already truncates.
 *
 * Exact values are never only here. Every mark carries its full count on the
 * hover tooltip, which is what this page treats as the number of record.
 *
 * The formatter is HOISTED, unlike `fmt`'s. `toLocaleString(undefined, opts)`
 * constructs a fresh `Intl.NumberFormat` per call, and options are what defeat
 * the engine's cache for the argument-less form: measured over 20,000 calls in
 * this page's own Chromium, 429ms inline against 12ms through one kept
 * instance, where `fmt`'s bare `toLocaleString()` is 12ms either way. Today's
 * axes make two dozen calls and would never notice; a label moved onto a
 * per-bar or per-row path would, and this costs one line to make impossible.
 */
const COMPACT = new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 });
const short = (n) => COMPACT.format(Number(n));

/**
 * A duration as an age — "4m ago", "6d ago".
 *
 * Every timestamp on this page is read as "is this relay keeping up", and a
 * date answers that only after the reader does the subtraction. The exact
 * instant stays available on hover, because the age is the signal and the date
 * is the evidence.
 */
const ago = (seconds) => {
  if (seconds == null) return "—";
  const d = Math.max(0, Math.floor(Date.now() / 1000) - seconds);
  if (d < 90) return `${d}s ago`;
  if (d < 5400) return `${Math.round(d / 60)}m ago`;
  if (d < 172800) return `${Math.round(d / 3600)}h ago`;
  return `${Math.round(d / 86400)}d ago`;
};
const isoOf = (seconds) => (seconds == null ? "" : new Date(seconds * 1000).toISOString().replace("T", " ").slice(0, 19) + "Z");

/**
 * "All 11" vs "the 50 most-named of 1,204".
 *
 * A capped list and a complete one are different facts, and stating a
 * truncation that did not happen invites the reader to wonder what was left
 * out. The kinds table is no longer capped at all; relay distribution still is.
 */
const shownOf = (shown, total, noun, superlative) =>
  shown >= total ? `All ${fmt(total)} ${noun}` : `The ${fmt(shown)} ${superlative} of ${fmt(total)} ${noun}`;
const el = (tag, cls, text) => {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text != null) n.textContent = text;
  return n;
};
/**
 * Size a row of tiles to its LONGEST number, and hand the row back.
 *
 * `.tile .v` sizes itself from the track it landed in and from this length, so
 * a number can never cross its own tile's border — see the CSS for why a fixed
 * size could not hold, and why the length cannot come from CSS (`ch` is defined
 * in terms of the font size the declaration is trying to compute).
 *
 * Written once on the ROW rather than per tile, and not only because it is one
 * line instead of four: sizing each tile to its own text left "1,882,401" a
 * third smaller than the "27" beside it, which reads as the small number being
 * the important one. One size per row, set by the value with the least room,
 * keeps a row of headline numbers reading as one row. Custom properties
 * inherit, and the grid's tracks are all `1fr`, so every tile resolves the same
 * `cqw` against the same length and they land on the same size.
 */
const fitTiles = (row) => {
  const len = [...row.querySelectorAll(".v")].reduce((m, v) => Math.max(m, v.textContent.length), 1);
  row.style.setProperty("--len", String(len));
  return row;
};
const SVG = "http://www.w3.org/2000/svg";
const svgEl = (tag, attrs) => {
  const n = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs || {})) n.setAttribute(k, v);
  return n;
};

/** A frame timestamp as a plain day, which is the only precision this card reads at. */
const dayOf = (t) => new Date(t * 1000).toISOString().slice(0, 10);


/** `4m` / `2h 10m` — a duration in seconds, for the phase clock. */
function fmtDur(sec) {
  if (sec < 90) return `${sec}s`;
  if (sec < 5400) return `${Math.round(sec / 60)}m`;
  const h = Math.floor(sec / 3600);
  return `${h}h ${Math.round((sec - h * 3600) / 60)}m`;
}

/**
 * The hover tooltip, shared by every mark on every page.
 *
 * Resolved lazily rather than at import: a module is evaluated before the
 * page's own script runs, and on a page that mounts its body dynamically the
 * element may not exist yet. Absent is tolerated — a page with no `#tip` simply
 * has no tooltips, which is a smaller failure than a module that throws on
 * import and takes the whole page with it.
 */
let tipEl = null;
const tip = () => (tipEl ||= document.getElementById("tip"));

export function showTip(e, text) {
  const el_ = tip();
  if (!el_) return;
  el_.textContent = text;
  el_.style.opacity = "1";
  moveTip(e);
}
export function moveTip(e) {
  const el_ = tip();
  if (!el_) return;
  el_.style.left = Math.min(e.clientX + 12, innerWidth - el_.offsetWidth - 8) + "px";
  el_.style.top = (e.clientY - el_.offsetHeight - 10) + "px";
}
export function hideTip() {
  const el_ = tip();
  if (el_) el_.style.opacity = "0";
}

/** A section's header, carrying its status when that status is worth reading. */
export function cardHead(parent, title, sub, section) {
  const h = el("h2", null, title);
  const status = section && section.status;
  // `failed` is worse than `partial` and used to get the QUIETER badge — the
  // ternary read "partial ? loud : quiet", which is backwards for every status
  // that is not partial.
  if (status && status !== "ok") h.appendChild(el("span", status === "ok" ? "badge" : "badge bad", status));
  parent.appendChild(h);
  // [sub] is a STATE, not a description: "no kind histogram in this document",
  // or how much of a truncated list is on screen. The panels no longer explain
  // themselves — the document's own `note` is where a caveat lives now.
  if (sub) parent.appendChild(el("p", "card-sub", sub));
  // The engine's own message, and the query that produced it — enough to
  // correct a rejected pipeline without attaching a debugger to the relay.
  if (section && section.errors) {
    for (const [k, v] of Object.entries(section.errors)) {
      const p = el("p", "err");
      p.append(`${k}: `);
      p.appendChild(el("code", null, v));
      parent.appendChild(p);
    }
  }
}

/**
 * A section's identity for redraw purposes: when the relay computed it, and
 * whether it worked.
 *
 * The rollup stamps every section with its own `generatedAt`, so this is the
 * relay's answer to "is this the same section I already drew" rather than this
 * page's guess at one. `status` is in the key because a section can be
 * recomputed into the same numbers and a different verdict — a chart that came
 * back `partial` needs its badge and its error lines drawn even when the bars
 * are identical. A section this document does not carry stamps as `-`, so a
 * panel whose section disappears is redrawn into its empty state exactly once.
 */
export const stampOf = (section) => (section ? `${section.generatedAt || "?"}/${section.status || "?"}` : "-");

export { fmt, short, ago, isoOf, shownOf, el, fitTiles, SVG, svgEl, dayOf, fmtDur };
