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
export const titleOf = (ev) => firstTag(ev, "title", "name", "subject") || firstTag(ev, "d");
export const summaryOf = (ev) => firstTag(ev, "summary", "description", "alt");
export const imageOf = (ev) => firstTag(ev, "image", "thumb", "picture", "icon");
