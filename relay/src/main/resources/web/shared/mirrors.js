// What this relay MIRRORS — the kind bound that makes a count taken against it
// mean anything.
//
// The mirror is a FILTERED subset of the network. `router.conf` names the kinds
// each stream pulls down, and nothing outside the sync process knows which — so
// "how much of my stuff is here yet", asked as our count for an author over
// that author's own relay's count for them, is a filtered numerator over an
// unfiltered denominator: a quotient that cannot reach 100% however complete
// the mirror is. Measured, 31,118 here of 89,485 on vitor.nostr1.com, drawn
// under the search box as "35% mirroring", on a mirror missing nothing it had
// ever been asked to hold. The whole gap was kinds 3, 4, 5, 6, 7 and 1059 — no
// stream asks for any of them, and two are encrypted DMs and gift wraps this
// relay must never hold at all.
//
// The router writes what it is running to its manifest at boot, and the relay
// publishes the union of the `down` kinds as `sync.mirrors.kinds` on
// GET /stats.json (`SyncManifest` and `MirrorReport`, Kotlin side). This module
// is the client's half of that: read the set, and put it on BOTH sides of the
// comparison.
//
// It is FETCHED rather than copied here, and that is the point of the whole
// arrangement. The kind list lives in router.conf and nowhere else, it is
// actively edited, and a JS copy would drift silently — in the direction of the
// same wrong number.

/** Where the relay publishes what it holds. Same origin, public, ETag'd. */
const STATS_URL = "/stats.json";

/**
 * The kind bound for a count against this relay, out of a `/stats.json`
 * document — or null when the document does not say.
 *
 * Two answers, and they are not the same claim:
 *
 *   {kinds: [0, 1, …]}  the union over every stream that pulls events DOWN.
 *                       Both counts carry it.
 *   {kinds: null}       `allKinds` — some mirroring stream names no kinds at
 *                       all, so this relay asks its upstreams for everything
 *                       they serve and there is no bound to apply. An unscoped
 *                       comparison is already like-for-like.
 *
 * NULL is the third answer and the one with teeth: no `sync` section, no
 * `mirrors` member, a document we could not read, a relay that mirrors nothing,
 * or a rollup that has not run yet (the route serves 503 until it has). The
 * caller must then ask NOTHING, rather than fall back to the unscoped count —
 * that fallback is the 35%.
 *
 * `allKinds` beats `kinds` if a document somehow carries both, matching the
 * writer's own rule: an unbounded stream publishes the flag and NO list,
 * because a union taken over only the streams that name kinds is smaller than
 * the truth, and scoping a COUNT to it would under-count the very denominator
 * this exists to fix.
 *
 * `writtenAt` is deliberately not read. It is stamped once, at the router's
 * boot, so a mirror that has been running healthily for a month carries a
 * month-old timestamp and a mirror switched off an hour ago carries whatever it
 * last booted with — the two are indistinguishable, and treating age as staleness
 * would suppress the honest number on exactly the relays that earned it.
 */
export function mirrorScope(stats) {
  const mirrors = stats && stats.sync && stats.sync.mirrors;
  if (!mirrors || typeof mirrors !== "object") return null;
  if (mirrors.allKinds === true) return { kinds: null };
  const kinds = Array.isArray(mirrors.kinds)
    ? [...new Set(mirrors.kinds.filter((k) => Number.isInteger(k) && k >= 0))]
    : [];
  // An empty list is not an unbounded mirror. The writer drops `kinds` when it
  // could not read a stream's bound, and reading that absence as "everything"
  // would put kinds we do not hold into a set somebody counts against.
  return kinds.length ? { kinds } : null;
}

/**
 * [filter] with the mirror's kind bound on it.
 *
 * Both sides of the comparison are built here, which is the whole point: the
 * bug was two filters that looked identical and were not, because one of them
 * was narrowed by the ROUTER rather than by the filter.
 *
 * A null scope throws instead of passing the filter through. Returning it
 * unscoped is the 35% — and at the call site it would look exactly like working
 * code, which is how it survived this long.
 */
export function scopedTo(filter, scope) {
  if (!scope) throw new Error("no mirror scope: a count against this relay cannot be scoped, so it must not be taken");
  return scope.kinds ? { ...filter, kinds: scope.kinds } : filter;
}

/**
 * Read the scope off this relay.
 *
 * One same-origin GET, and null for every way it can fail to answer — a 503
 * before the first rollup, a relay serving no statistics at all, a body that is
 * not JSON. Null is a supported answer everywhere it is used; see [mirrorScope]
 * for what the caller owes it.
 *
 * [fetcher] is injectable so the failure paths are testable without a network:
 * they are the paths that decide whether a number appears at all.
 */
export async function readMirrorScope(fetcher = globalThis.fetch) {
  try {
    const res = await fetcher(STATS_URL, { headers: { accept: "application/json" } });
    if (!res || !res.ok) return null;
    return mirrorScope(await res.json());
  } catch (e) {
    return null;
  }
}
