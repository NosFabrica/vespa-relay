// What this relay mirrors: the kind bound that makes a count taken against it
// mean anything. The mirror is a filtered subset of the network, so a count of
// ours over an upstream's count is only like-for-like when both carry the same
// kinds. The router publishes the union of its `down` kinds as
// `sync.mirrors.kinds` on GET /stats.json (`SyncManifest`, `MirrorReport`);
// this module reads that set and puts it on both sides of the comparison. It
// is fetched, never copied here, because the list lives in router.conf.

/** Where the relay publishes what it holds. Document-relative, so a page behind a path prefix asks its own service. */
const STATS_URL = "stats.json";

/**
 * The kind bound out of a `/stats.json` document: `{kinds: [...]}`,
 * `{kinds: null}` for an unbounded mirror, or null when the document does not
 * say. On null the caller must not count at all; an unscoped count is wrong.
 */
export function mirrorScope(stats) {
  // Sections are wrapped in the `StatsRollup.section` envelope, so the member
  // is under `sync.data`; the bare shape is accepted for fixtures.
  const section = stats && stats.sync;
  const mirrors = section && ((section.data && section.data.mirrors) || section.mirrors);
  if (!mirrors || typeof mirrors !== "object") return null;
  // `allKinds` wins over `kinds`: an unbounded stream publishes the flag and no list.
  if (mirrors.allKinds === true) return { kinds: null };
  const kinds = Array.isArray(mirrors.kinds)
    ? [...new Set(mirrors.kinds.filter((k) => Number.isInteger(k) && k >= 0))]
    : [];
  // An empty list is not an unbounded mirror: the writer drops `kinds` when it
  // could not read a stream's bound.
  return kinds.length ? { kinds } : null;
}

/** [filter] with the mirror's kind bound on it. A null scope throws: passing the filter through unscoped is the bug. */
export function scopedTo(filter, scope) {
  if (!scope) throw new Error("no mirror scope: a count against this relay cannot be scoped, so it must not be taken");
  return scope.kinds ? { ...filter, kinds: scope.kinds } : filter;
}

/**
 * Read the scope off this relay: one same-origin GET, null for every way it
 * can fail. [fetcher] is injectable for tests; the default wraps the global
 * so `fetch` keeps its receiver.
 */
export async function readMirrorScope(fetcher = (url, init) => globalThis.fetch(url, init)) {
  try {
    const res = await fetcher(STATS_URL, { headers: { accept: "application/json" } });
    if (!res || !res.ok) return null;
    return mirrorScope(await res.json());
  } catch (e) {
    return null;
  }
}
