// The stats pages' shared vocabulary: number formatting, DOM helpers, the
// hover tooltip, and a card's header. One module because the relay's corpus
// page and the mirror's status page are one design. Every page imports it as
// `/web/shared/page.js`, a url the asset route resolves off the classpath, so
// which jar a module ships in is not something a page has to know.

const fmt = (n) => Number(n).toLocaleString();

/**
 * The same number short enough for an axis label: "1.2M" where `fmt` says
 * "1,234,567". Exact values are never only here; every mark carries its full
 * count on the hover tooltip. The formatter is hoisted because
 * `toLocaleString(undefined, opts)` builds a fresh `Intl.NumberFormat` per call.
 */
const COMPACT = new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 });
const short = (n) => COMPACT.format(Number(n));

/**
 * A timestamp as an age, "4m ago", since every timestamp here is read as "is
 * this relay keeping up". The exact instant stays available on hover.
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
 * "All 11" against "The 50 most-named of 1,204". A capped list and a complete
 * one are different facts, and stating a truncation that did not happen
 * invites the reader to wonder what was left out.
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
 * Size a row of tiles to its longest number, and hand the row back. `.tile .v`
 * sizes itself from its track and `--len`; the length cannot come from CSS
 * because `ch` is defined by the font size being computed. Set once on the row
 * so a row of headline numbers reads as one row.
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

/** A frame timestamp as a plain day, the only precision the card reads at. */
const dayOf = (t) => new Date(t * 1000).toISOString().slice(0, 10);


/** `4m` / `2h 10m`, a duration in seconds, for the phase clock. */
function fmtDur(sec) {
  if (sec < 90) return `${sec}s`;
  if (sec < 5400) return `${Math.round(sec / 60)}m`;
  const h = Math.floor(sec / 3600);
  return `${h}h ${Math.round((sec - h * 3600) / 60)}m`;
}

/**
 * The hover tooltip, shared by every mark on every page. Resolved lazily
 * because a module is evaluated before the page's own script runs; a page with
 * no `#tip` simply has no tooltips.
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

/**
 * A section's header, with its status badge when the status is not `ok`.
 * [sub] is a state ("no kind histogram in this document"), not a description;
 * a caveat lives in the document's own `note`.
 */
export function cardHead(parent, title, sub, section) {
  const h = el("h2", null, title);
  const status = section && section.status;
  if (status && status !== "ok") h.appendChild(el("span", status === "ok" ? "badge" : "badge bad", status));
  parent.appendChild(h);
  if (sub) parent.appendChild(el("p", "card-sub", sub));
  // The engine's own message and the query that produced it, enough to
  // correct a rejected pipeline without a debugger on the relay.
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
 * A section's identity for redraw purposes: the relay's own `generatedAt`,
 * plus `status` because a section can be recomputed into the same numbers and
 * a different verdict. A section this document lacks stamps as `-`, so a
 * panel whose section disappears is redrawn into its empty state once.
 */
export const stampOf = (section) => (section ? `${section.generatedAt || "?"}/${section.status || "?"}` : "-");

export { fmt, short, ago, isoOf, shownOf, el, fitTiles, SVG, svgEl, dayOf, fmtDur };
