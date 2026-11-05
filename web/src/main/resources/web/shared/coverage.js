// How far the mirror has got with one reader's own posts, asked of the sync process itself: it
// keeps, per relay it reads that reader from, whether the walk has reached its floor. The sync's
// address comes from the relay, which writes it into the page as <meta name="sync-url">; no meta
// means no sync to ask, and the posts stage is skipped rather than guessed.

/** The sync's base url off the page, or null when this relay names none. */
export function syncUrl(doc = globalThis.document) {
  const raw = doc?.querySelector?.('meta[name="sync-url"]')?.getAttribute("content")?.trim();
  if (!raw || !/^https?:\/\//i.test(raw)) return null;
  return raw.replace(/\/+$/, "");
}

/** One relay of the answer, or null when the row carries no relay or no status. */
function row(r) {
  if (!r || typeof r !== "object") return null;
  if (typeof r.relay !== "string" || !r.relay || typeof r.syncStatus !== "string") return null;
  return {
    relay: r.relay,
    stream: typeof r.stream === "string" ? r.stream : null,
    syncStatus: r.syncStatus,
    coveredTo: Number.isFinite(r.coveredTo) ? r.coveredTo : null,
    verifiedAgoSec: Number.isFinite(r.verifiedAgoSec) ? r.verifiedAgoSec : null,
    tailed: r.tailed === true,
  };
}

/**
 * The sync's `/coverage/<pubkey>` document as the verdict reads it: `{followed, relays, unread}`, or
 * null for anything that is not one, which skips the stage like no answer at all. `unread` is the
 * reader's own relays that nothing here reads.
 */
export function coverageOf(doc) {
  if (!doc || typeof doc !== "object" || typeof doc.followed !== "boolean" || !Array.isArray(doc.relays)) return null;
  const relays = doc.relays.map(row).filter(Boolean);
  // `followed` with no readable row is a document this page cannot read, not a reader nobody follows.
  if (doc.followed && !relays.length) return null;
  const unread = Array.isArray(doc.unread) ? doc.unread.filter((u) => typeof u === "string" && u) : [];
  return { followed: doc.followed, relays, unread };
}

/**
 * Per relay, whether every pair the sync holds against it is `complete`: a relay read by two
 * streams is done only when both are. `done` and `total` count relays, not pairs.
 */
export function progressOf(coverage) {
  const byRelay = new Map();
  for (const r of coverage?.relays || []) {
    const was = byRelay.get(r.relay);
    byRelay.set(r.relay, {
      relay: r.relay,
      done: (was ? was.done : true) && r.syncStatus === "complete",
      // The least finished status speaks for the relay.
      status: was && rank(was.status) < rank(r.syncStatus) ? was.status : r.syncStatus,
    });
  }
  const relays = [...byRelay.values()].sort((a, b) => rank(a.status) - rank(b.status) || a.relay.localeCompare(b.relay));
  return { total: relays.length, done: relays.filter((r) => r.done).length, relays };
}

/** Least finished first, as the sync's status page orders them. */
const ORDER = ["refused", "notStarted", "paging", "complete"];
const rank = (status) => {
  const at = ORDER.indexOf(status);
  return at < 0 ? 0 : at;
};

/** How many of the reader's relays are named; the sync reads no more than this many. */
const MAX_RELAYS = 20;

/**
 * The coverage for [pubkey] off the sync, null for every way the read can fail. [relays] are the
 * reader's write relays: a stream that reads everyone on a relay reads them there, and only the
 * reader's own list says where. [fetcher] is injectable for tests.
 */
export async function readCoverage(pubkey, { relays = [], base = syncUrl(), fetcher = (url, init) => globalThis.fetch(url, init) } = {}) {
  if (!base || !/^[0-9a-f]{64}$/.test(pubkey || "")) return null;
  const query = relays.slice(0, MAX_RELAYS).map((r) => `relay=${encodeURIComponent(r)}`).join("&");
  try {
    const res = await fetcher(`${base}/coverage/${pubkey}${query ? `?${query}` : ""}`, { headers: { accept: "application/json" } });
    if (!res || !res.ok) return null;
    return coverageOf(await res.json());
  } catch (e) {
    return null;
  }
}
