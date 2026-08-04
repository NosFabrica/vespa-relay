// Small pure helpers: escaping, clipping, dates, and the tag extractors that
// decide what a generic event "is" on screen. The extractors mirror exactly
// the fields the search indexes, so what matched is what shows.

export const esc = (v) => String(v ?? "").replace(/[&<>"']/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;" }[c]));
export const clip = (s, n) => { s = String(s || "").trim(); return s.length > n ? s.slice(0, n - 1) + "…" : s; };

const dateOf = (ev) => new Date(ev.created_at * 1000);
export const fullDate = (ev) => dateOf(ev).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });

/** Recent events read better relative; older ones as a plain date. */
export function when(ev) {
  const secs = Math.max(0, Date.now() / 1000 - ev.created_at);
  const mins = secs / 60, hours = mins / 60, days = hours / 24;
  if (mins < 1) return "just now";
  if (mins < 60) return `${Math.floor(mins)}m ago`;
  if (hours < 24) return `${Math.floor(hours)}h ago`;
  if (days < 30) return `${Math.floor(days)}d ago`;
  return dateOf(ev).toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
}

export const firstTag = (ev, ...names) => {
  for (const name of names) for (const t of ev.tags) if (t[0] === name && t[1]) return t[1];
  return null;
};
export const titleOf = (ev) => firstTag(ev, "title", "name", "subject") || firstTag(ev, "d");
export const summaryOf = (ev) => firstTag(ev, "summary", "description", "alt");
export const imageOf = (ev) => firstTag(ev, "image", "thumb", "picture", "icon");
