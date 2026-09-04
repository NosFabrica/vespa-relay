// The last ranked ask, kept so the next view can reuse it instead of asking
// the relay the same question again.
//
// This exists because of a measurement. The relay's cost for a NIP-50 search
// is the MATCH SET, not the page: against staging (2026-09-03, `bitcoin`,
// kind 1) `limit: 1` answered in 4.4s and `limit: 200` in 4.0s. So two asks
// that differ only in their limit are two full searches for one answer — and
// the page made exactly that pair on every search: the type-ahead's eight rows
// and then, on Enter, the results view's first page. Same words, same tab,
// same lens, same sort; a second 4s for nothing.
//
// One entry, keyed by everything the filters say EXCEPT the limit, and reused
// only when the limit matches too — a shorter answer is not a prefix of a
// longer one that can be trusted for paging, and a longer one would hand the
// pager a `got` it never asked for. The views therefore agree on one width
// (paging.js's askLimit) and the second of them gets the first one's answer.
//
// A cache of PROMISES, not of answers: Enter usually lands while the popup's
// ask is still in flight, and the point is to wait on that ask rather than to
// start a twin beside it. A rejected promise is dropped so a failure is
// retried rather than replayed.
//
// FRESHNESS is bounded because a search is a question about a moving corpus:
// a popup answered ten minutes ago is not what Enter means now. Sixty seconds
// is a keystroke-to-Enter gap with room to spare, and far under the pager's own
// notion of stale (it re-asks at a wider limit on every page turn anyway).

/** Ms a kept answer may be reused for. */
export const ASK_FRESH_MS = 60_000;

/**
 * Everything about [filters] that decides the answer, minus the limit — the
 * key under which an answer is kept. Field ORDER inside a filter does not
 * matter to a relay and must not matter here, so keys are sorted.
 */
export function askKey(filters) {
  const list = Array.isArray(filters) ? filters : [filters];
  return JSON.stringify(list.map((f) => {
    const out = {};
    for (const k of Object.keys(f).sort()) if (k !== "limit") out[k] = f[k];
    return out;
  }));
}

/** The one limit a list of filters carries — the largest, if they disagree. */
export function askLimitOf(filters) {
  const list = Array.isArray(filters) ? filters : [filters];
  return Math.max(0, ...list.map((f) => f.limit ?? 0));
}

export class AskCache {
  constructor({ freshMs = ASK_FRESH_MS, now = () => Date.now() } = {}) {
    this.freshMs = freshMs;
    this.now = now;
    this.last = null; // { key, limit, at, promise }
  }

  /**
   * The answer to [filters]: the kept one when it is the same question at the
   * same width and still fresh, else [ask]'s, which is kept in its place.
   */
  take(filters, ask) {
    const key = askKey(filters);
    const limit = askLimitOf(filters);
    const at = this.now();
    const kept = this.last;
    if (kept && kept.key === key && kept.limit === limit && at - kept.at <= this.freshMs) return kept.promise;
    // Asked NOW, not on a microtask: the caller's next line may be the twin
    // take() this exists to fold, and a promise that has not asked yet is still
    // the one it should get.
    let promise;
    try { promise = Promise.resolve(ask()); } catch (e) { promise = Promise.reject(e); }
    const entry = { key, limit, at, promise };
    this.last = entry;
    // A failed ask is not an answer to keep, and neither is a SHORT one: a
    // read the page stopped listening to — its timeout, or the abort a
    // submit sends a type-ahead it has overtaken — resolves with whatever
    // arrived and `complete: false` (shared/relay.js), and handing that to the
    // next identical take() would draw a page of results the relay never
    // finished sending. The next take() asks again instead.
    const drop = () => { if (this.last === entry) this.last = null; };
    promise.then((answer) => { if (answer?.complete === false) drop(); }, drop);
    return promise;
  }

  /** Forget the kept answer — the corpus the page reads through changed. */
  clear() { this.last = null; }
}
