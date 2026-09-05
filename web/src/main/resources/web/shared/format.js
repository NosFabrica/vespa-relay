// Small pure helpers: escaping, clipping, dates, and the tag extractors that decide what a
// generic event "is" on screen. The extractors mirror the fields the search indexes.

export const esc = (v) => String(v ?? "").replace(/[&<>"']/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;" }[c]));
export const clip = (s, n) => { s = String(s || "").trim(); return s.length > n ? s.slice(0, n - 1) + "…" : s; };

// A date these cannot read prints nothing rather than "Invalid Date".
const secsOf = (ev) => (Number.isFinite(Number(ev && ev.created_at)) ? Number(ev.created_at) : null);
const dateOf = (ev) => new Date(secsOf(ev) * 1000);
export const fullDate = (ev) =>
  secsOf(ev) == null ? "" : dateOf(ev).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });

/** Recent events read better relative; older ones as a plain date. */
export function when(ev) {
  if (secsOf(ev) == null) return "";
  const secs = Math.max(0, Date.now() / 1000 - ev.created_at);
  const mins = secs / 60, hours = mins / 60, days = hours / 24;
  if (mins < 1) return "just now";
  if (mins < 60) return `${Math.floor(mins)}m ago`;
  if (hours < 24) return `${Math.floor(hours)}h ago`;
  if (days < 30) return `${Math.floor(days)}d ago`;
  return dateOf(ev).toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
}

/** The first value of the first of [names] present. Total over any shape of event, tags array or not. */
export const firstTag = (ev, ...names) => {
  for (const name of names) {
    for (const t of (ev && ev.tags) || []) if (Array.isArray(t) && t[0] === name && t[1]) return t[1];
  }
  return null;
};
/** A `d` that is a UUID, a hex blob or a bare unix timestamp is never a title. */
const OPAQUE_D = /^(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{16,}|\d{10,})$/i;
export const titleOf = (ev) => {
  const named = firstTag(ev, "title", "name", "subject");
  if (named) return named;
  const d = firstTag(ev, "d");
  return d && !OPAQUE_D.test(d) ? d : null;
};
export const summaryOf = (ev) => firstTag(ev, "summary", "description", "alt");
export const imageOf = (ev) => firstTag(ev, "image", "thumb", "picture", "icon");

/**
 * A markdown body reduced to the prose a preview can show. A reducer to text, never a
 * renderer to html: it emits no markup, and the call site still escapes the result.
 */
const MD_HEADING = /^\s{0,3}#{1,6}\s+/;
const MD_RULE = /^\s*(?:[-*_=]\s*){3,}$/;          // thematic break, setext underline
const MD_TABLE_RULE = /^\s*\|?[\s:|-]*\|[\s:|-]*$/;

export function mdExcerpt(md, title = "") {
  const body = String(md || "")
    .replace(/```[\s\S]*?(?:```|$)|~~~[\s\S]*?(?:~~~|$)/g, " ")  // fenced code
    .replace(/!\[[^\]]*\]\([^)]*\)/g, " ")                       // images
    .replace(/\[([^\]]*)\]\([^)]*\)/g, "$1")                     // inline links
    .replace(/\[([^\]]*)\]\[[^\]]*\]/g, "$1")                    // reference links
    .replace(/<((?:https?|wss?|mailto):[^>\s]+)>/g, "$1")        // autolinks, before the tag strip
    .replace(/<\/?[a-zA-Z][^>]*>/g, " ");                        // html tags

  // The first prose run, without its headings; a body that is nothing but headings falls back to them.
  const heads = [], prose = [];
  let started = false, ended = false;
  for (const raw of body.split("\n")) {
    if (MD_RULE.test(raw) || MD_TABLE_RULE.test(raw)) continue;
    const line = raw
      .replace(/^\s{0,3}>+\s?/, "")                              // quote
      .replace(MD_HEADING, "")                                   // heading
      .replace(/^\s{0,3}(?:[-*+]|\d+[.)])\s+/, "");              // list marker
    if (MD_HEADING.test(raw)) { heads.push(line); ended = started; continue; }
    if (ended) continue;
    if (line.trim()) started = true;
    prose.push(line);
  }

  const s = (started ? prose : heads).join(" ")
    .replace(/(\*\*|__|~~)(.+?)\1/g, "$2")                       // strong, strike
    .replace(/(^|[^\w*])[*_](?!\s)([^*_]+?)[*_](?![\w*])/g, "$1$2")  // emphasis
    .replace(/`+([^`]+)`+/g, "$1")                               // inline code
    .replace(/\s+/g, " ")                                        // paragraphs read as one run
    .trim();
  const t = String(title || "").trim();
  return t && s.slice(0, t.length).toLowerCase() === t.toLowerCase() ? s.slice(t.length).trim() : s;
}
