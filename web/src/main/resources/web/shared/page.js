// The stats pages' shared vocabulary: number formatting, DOM helpers, the hover tooltip,
// and a card's header. Imported as `/web/shared/page.js`, which the asset route resolves
// off the classpath, so which jar a module ships in is not something a page has to know.

const fmt = (n) => Number(n).toLocaleString();

/** "1.2M" for an axis label; the exact value is always on the hover tooltip too. */
const COMPACT = new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 });
const short = (n) => COMPACT.format(Number(n));

/** A timestamp as an age, "4m ago"; the exact instant stays available on hover. */
const ago = (seconds) => {
  if (seconds == null) return "—";
  const d = Math.max(0, Math.floor(Date.now() / 1000) - seconds);
  if (d < 90) return `${d}s ago`;
  if (d < 5400) return `${Math.round(d / 60)}m ago`;
  if (d < 172800) return `${Math.round(d / 3600)}h ago`;
  return `${Math.round(d / 86400)}d ago`;
};
const isoOf = (seconds) => (seconds == null ? "" : new Date(seconds * 1000).toISOString().replace("T", " ").slice(0, 19) + "Z");

/** "All 11" against "The 50 most-named of 1,204"; a truncation that did not happen is never stated. */
const shownOf = (shown, total, noun, superlative) =>
  shown >= total ? `All ${fmt(total)} ${noun}` : `The ${fmt(shown)} ${superlative} of ${fmt(total)} ${noun}`;
const el = (tag, cls, text) => {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text != null) n.textContent = text;
  return n;
};
/**
 * Size a row of tiles to its longest number and hand the row back. One `--len` on the row,
 * not per tile, so a row of headline numbers reads as one row.
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

/** A frame timestamp as a plain day. */
const dayOf = (t) => new Date(t * 1000).toISOString().slice(0, 10);


/** Seconds as `4m` or `2h 10m`. */
function fmtDur(sec) {
  if (sec < 90) return `${sec}s`;
  if (sec < 5400) return `${Math.round(sec / 60)}m`;
  const h = Math.floor(sec / 3600);
  return `${h}h ${Math.round((sec - h * 3600) / 60)}m`;
}

/** The hover tooltip, resolved lazily because a module runs before the page's own script. */
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
 * A section's header, with its status badge when the status is not `ok`. [sub] is a state,
 * not a description.
 */
export function cardHead(parent, title, sub, section) {
  const h = el("h2", null, title);
  const status = section && section.status;
  if (status && status !== "ok") h.appendChild(el("span", status === "ok" ? "badge" : "badge bad", status));
  parent.appendChild(h);
  if (sub) parent.appendChild(el("p", "card-sub", sub));
  // The engine's own message and the query that produced it.
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
 * A section's identity for redraws: `generatedAt` plus `status`, since the same numbers
 * can carry a different verdict. A missing section stamps as `-`, so it is redrawn empty once.
 */
export const stampOf = (section) => (section ? `${section.generatedAt || "?"}/${section.status || "?"}` : "-");

export { fmt, short, ago, isoOf, shownOf, el, fitTiles, SVG, svgEl, dayOf, fmtDur };
