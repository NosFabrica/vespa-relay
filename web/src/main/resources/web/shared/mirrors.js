// The kind bound this relay mirrors, read from `sync.mirrors.kinds` on GET /stats.json. A
// count of ours over an upstream's is only like-for-like when both sides carry the same
// kinds, so the set goes on both sides of the comparison.

/** Document-relative, so a page behind a path prefix asks its own service. */
const STATS_URL = "stats.json";

/**
 * The kind bound out of a `/stats.json` document: `{kinds: [...]}`, `{kinds: null}` for an
 * unbounded mirror, or null when the document does not say, in which case nothing may be counted.
 */
export function mirrorScope(stats) {
  // Sections sit under the `StatsRollup.section` envelope; the bare shape is accepted for fixtures.
  const section = stats && stats.sync;
  const mirrors = section && ((section.data && section.data.mirrors) || section.mirrors);
  if (!mirrors || typeof mirrors !== "object") return null;
  // An unbounded stream publishes `allKinds` and no list.
  if (mirrors.allKinds === true) return { kinds: null };
  const kinds = Array.isArray(mirrors.kinds)
    ? [...new Set(mirrors.kinds.filter((k) => Number.isInteger(k) && k >= 0))]
    : [];
  // An empty list is not an unbounded mirror: the writer drops `kinds` when it could not read a bound.
  return kinds.length ? { kinds } : null;
}

/** [filter] with the mirror's kind bound on it. A null scope throws rather than counting unscoped. */
export function scopedTo(filter, scope) {
  if (!scope) throw new Error("no mirror scope: a count against this relay cannot be scoped, so it must not be taken");
  return scope.kinds ? { ...filter, kinds: scope.kinds } : filter;
}

/** The scope off this relay, null for every way the read can fail. [fetcher] is injectable for tests. */
export async function readMirrorScope(fetcher = (url, init) => globalThis.fetch(url, init)) {
  try {
    const res = await fetcher(STATS_URL, { headers: { accept: "application/json" } });
    if (!res || !res.ok) return null;
    return mirrorScope(await res.json());
  } catch (e) {
    return null;
  }
}
