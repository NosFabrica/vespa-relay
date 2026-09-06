// The last ranked ask, kept so the next view reuses the answer instead of asking the relay
// the same question again. One entry, keyed by everything the filters say except the limit,
// reused only when the limit matches too. Promises are cached, since Enter usually lands
// while the popup's ask is still in flight.

/** Ms a kept answer may be reused for: a keystroke-to-Enter gap. */
export const ASK_FRESH_MS = 60_000;

/** Everything about [filters] that decides the answer, minus the limit, with keys sorted. */
export function askKey(filters) {
  const list = Array.isArray(filters) ? filters : [filters];
  return JSON.stringify(list.map((f) => {
    const out = {};
    for (const k of Object.keys(f).sort()) if (k !== "limit") out[k] = f[k];
    return out;
  }));
}

/** The one limit a list of filters carries: the largest, if they disagree. */
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

  /** The kept answer when it is the same question at the same width and still fresh, else [ask]'s. */
  take(filters, ask) {
    const key = askKey(filters);
    const limit = askLimitOf(filters);
    const at = this.now();
    const kept = this.last;
    if (kept && kept.key === key && kept.limit === limit && at - kept.at <= this.freshMs) return kept.promise;
    // Asked synchronously: the caller's next line may be the twin take() this exists to fold.
    let promise;
    try { promise = Promise.resolve(ask()); } catch (e) { promise = Promise.reject(e); }
    const entry = { key, limit, at, promise };
    this.last = entry;
    // A failed ask is dropped, and so is an abandoned one (`complete: false`), so the next
    // take() asks again instead of drawing an unfinished page.
    const drop = () => { if (this.last === entry) this.last = null; };
    promise.then((answer) => { if (answer?.complete === false) drop(); }, drop);
    return promise;
  }

  /** Forget the kept answer. */
  clear() { this.last = null; }
}
