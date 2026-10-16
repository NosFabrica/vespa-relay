// The relay's headline numbers, counted live over its own websocket: a REQ for the newest event and
// NIP-45 COUNTs the engine answers in milliseconds. /stats.json is a rollup hours old by design, so
// what a reader takes to be "now" is asked here, and everything below it on the page is dated.

export const MINUTE = 60;
export const HOUR = 3600;

/** Kinds that hold one event per author, so their count is a count of people. */
export const PEOPLE_KINDS = [0, 3, 10002];

/** A failed ask is retried sooner than its cadence, but not in a loop. */
const RETRY_AFTER = 30;

/** How often the scheduler looks for due asks; the cadences below are what bound the load. */
const TICK_MS = 2000;

/** Wait this long after the socket fails before dialling again. */
const RECONNECT_AFTER_MS = 10000;

/** Same-origin and document-relative: the socket this page was served beside, behind any prefix. */
export function socketUrlOf(href) {
  const u = new URL(".", href);
  u.protocol = u.protocol === "https:" ? "wss:" : "ws:";
  u.search = "";
  u.hash = "";
  return u.toString();
}

/**
 * Every ask the page wants at [now], each with how long its answer stays good. Windows are bounded
 * at now: `created_at` is author-signed, and a note dated in 2100 would otherwise count in every
 * window that reaches the present.
 */
export function wantedAsks(now) {
  const asks = [];
  const count = (key, filter, ttl) => asks.push({ key, verb: "count", filter, ttl });
  asks.push({ key: "newest", verb: "req", filter: { limit: 1, until: now }, ttl: 15, read: (evs) => (evs[0] ? evs[0].created_at : null) });
  count("last.60", { since: now - MINUTE, until: now }, 15);
  count("last.3600", { since: now - HOUR, until: now }, 15);
  // A whole kind is a longer count, and a population moves slowly.
  for (const k of PEOPLE_KINDS) count(`people.${k}`, { kinds: [k] }, 1800);
  return asks;
}

/** A count answer that is not a number: the relay refused or went quiet. */
const unanswered = (v) => v && typeof v === "object" && "unanswered" in v;

/**
 * The asks, their answers, and the loop that keeps them fresh. [relay] is a `Relay` from
 * shared/relay.js (or anything with `count(filter)` and `req(filter)`), lensless so every filter
 * carries `include:spam`. An answer is kept while its refresh fails, with the failure beside it.
 */
export class LiveCounts {
  constructor(relay, { now = () => Math.floor(Date.now() / 1000), concurrency = 4, onChange = () => {} } = {}) {
    this.relay = relay;
    this.now = now;
    this.concurrency = concurrency;
    this.onChange = onChange;
    /** key -> { value, at, error, errorAt } */
    this.answers = new Map();
    this.inFlight = new Set();
    this.paused = false;
    this.connError = null;
    this.connRetryAt = 0;
    this.timer = null;
  }

  get(key) {
    return this.answers.get(key);
  }

  /** The value of [key], or null when it has never been answered. */
  value(key) {
    const a = this.answers.get(key);
    return a && a.value !== undefined ? a.value : null;
  }

  /** Every ask due at [now]: never answered, past its cadence, or failed long enough ago. */
  due(now = this.now()) {
    return wantedAsks(now).filter((a) => {
      if (this.inFlight.has(a.key)) return false;
      const got = this.answers.get(a.key);
      if (!got) return true;
      if (got.errorAt != null && (got.at == null || got.errorAt >= got.at)) return now - got.errorAt >= Math.min(a.ttl, RETRY_AFTER);
      return now - got.at >= a.ttl;
    });
  }

  /** Start as many due asks as the concurrency allows. Safe to call at any time. */
  tick() {
    if (this.paused) return;
    if (this.connError && Date.now() < this.connRetryAt) return;
    const room = this.concurrency - this.inFlight.size;
    if (room <= 0) return;
    const now = this.now();
    for (const ask of this.due(now).slice(0, room)) this.run(ask, now);
  }

  async run(ask, now) {
    this.inFlight.add(ask.key);
    let value, error = null;
    try {
      if (ask.verb === "count") {
        const n = await this.relay.count(ask.filter);
        if (unanswered(n)) error = n.unanswered === "timeout" ? "timed out" : "refused";
        else value = n;
      } else {
        const evs = await this.relay.req(ask.filter);
        if (evs.complete === false) error = "timed out";
        else value = ask.read(evs);
      }
      this.connError = null;
    } catch (e) {
      error = String((e && e.message) || e);
      // A CLOSED for one subscription is that ask's answer; a dead socket is every ask's.
      if (!this.relay.ws || this.relay.ws.readyState !== 1) {
        this.connError = error;
        this.connRetryAt = Date.now() + RECONNECT_AFTER_MS;
      }
    } finally {
      this.inFlight.delete(ask.key);
    }
    const prior = this.answers.get(ask.key) || {};
    if (error == null) this.answers.set(ask.key, { value, at: now });
    else this.answers.set(ask.key, { ...prior, error, errorAt: now });
    this.onChange(ask.key);
    this.tick();
  }

  /** Run the loop while the page is visible; a hidden tab asks nothing. */
  start(doc = globalThis.document) {
    this.timer = setInterval(() => this.tick(), TICK_MS);
    if (doc) {
      doc.addEventListener("visibilitychange", () => {
        this.paused = doc.hidden;
        if (!this.paused) this.tick();
        this.onChange(null);
      });
      this.paused = !!doc.hidden;
    }
    this.tick();
  }

  stop() {
    clearInterval(this.timer);
    this.timer = null;
  }
}
