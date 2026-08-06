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
