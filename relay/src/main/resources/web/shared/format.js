// Small pure helpers: escaping, clipping, dates, and the tag extractors that
// decide what a generic event "is" on screen. The extractors mirror exactly
// the fields the search indexes, so what matched is what shows.

export const esc = (v) => String(v ?? "").replace(/[&<>"']/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;" }[c]));
export const clip = (s, n) => { s = String(s || "").trim(); return s.length > n ? s.slice(0, n - 1) + "…" : s; };

// A date these can't read prints nothing rather than "Invalid Date", which is
// a JavaScript diagnostic wearing the byline's clothes.
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

/**
 * Tag access has to be TOTAL, because the events these read are not all this
 * relay's. entity.js dials the relay hints in an identifier when the index
 * does not hold it, and renders what comes back BEFORE handing it here for
 * verification — deliberately, so the reader sees the thing and the relay's
 * verdict on it. This line used to iterate `ev.tags` bare while base.js's
 * tagsOf guarded, so an event with no tags array threw in 70 of the 118
 * renderers, and the entity render is outside showEntity's try/catch: the
 * page stopped at its skeleton with the loading line still on it.
 */
export const firstTag = (ev, ...names) => {
  for (const name of names) {
    for (const t of (ev && ev.tags) || []) if (Array.isArray(t) && t[0] === name && t[1]) return t[1];
  }
  return null;
};
/**
 * A `d` is an IDENTIFIER, and reading it as a title is a bet that its author
 * wrote something a person can read there. Often they did — a community's
 * name, a wiki slug, a bookmarked url — and often a client generated it: a
 * kind 22 short video from Amethyst carries
 * `d f56d739a-09c9-4f0b-ba82-f8c21e1a6b8e`, and that UUID was what every one
 * of those cards led with, standing in for a caption the event was carrying
 * in `content` all along.
 *
 * These three shapes are never prose: a UUID, a hex blob (an event id, a
 * pubkey, a file hash), a bare unix timestamp. When the `d` is one of them the
 * ladder falls past it to whatever the card would have said next, which is
 * always more informative than a hash — including nothing at all.
 */
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
 * A NIP-23 markdown body reduced to the prose a PREVIEW can show.
 *
 * An article that carries a `summary` needs none of this. Most do not, and the
 * card then falls back to the body — which is markup. The preview for "On
 * Relays, Bandwidth, and Who Pays" opened with `## Somebody is paying for
 * this` and spent its remaining lines on `1. **Paid relays.**`: every mark
 * visible, nothing rendered, and a summary slot showing syntax instead of the
 * sentence underneath it.
 *
 * This is a reducer to TEXT, not a renderer to HTML — the distinction article.js
 * turns on. It DROPS what a preview cannot show (fenced code, images, rules,
 * table rules) and UNWRAPS what it can (headings, quotes, list markers,
 * emphasis, the text of a link). Nothing here emits markup, the call site still
 * escapes what comes back, and so this adds no surface to the audit that a
 * markdown renderer would.
 *
 * [title] drops a leading heading that only repeats it: opening the body with
 * the article's own title is what most long-form clients write, and a preview
 * that says the title twice has spent its first line saying nothing.
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
    .replace(/<((?:https?|wss?|mailto):[^>\s]+)>/g, "$1")        // autolinks, before
    .replace(/<\/?[a-zA-Z][^>]*>/g, " ");                        // ...the tags they look like

  // A heading is a LABEL for the prose under it, not prose. Every line here
  // ends up in ONE run, so a kept heading runs straight into the sentence it
  // introduces — "Somebody is paying for this A relay that accepts every
  // event from everybody…", which is what the fallback read like. The excerpt
  // is therefore the FIRST PROSE RUN: leading headings skipped, and the run
  // ending where the next section's heading starts it over. A body that is
  // nothing BUT headings falls back to them, since some words beat none.
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
