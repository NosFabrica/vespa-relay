// The relay's statistics, counted live over its own websocket: NIP-45 COUNTs over windows the
// engine answers in milliseconds, plus a REQ or two. Nothing here walks the store unasked; the
// one count that does (`store`) runs only when the reader presses for it.

export const MINUTE = 60;
export const HOUR = 3600;
export const DAY = 86400;

/** How many hourly and daily bars the charts draw. */
export const HOURS = 24;
export const DAYS = 30;

/** Kinds that are one event per author, so their count is a count of people. */
export const PEOPLE_KINDS = [0, 3, 10002, 10040];

/** How many of the newest events are read to find kinds the router's manifest does not name. */
export const SAMPLE_SIZE = 100;

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

/** The start of the UTC hour / day holding [t]. */
export const hourOf = (t) => t - (t % HOUR);
export const dayOf = (t) => t - (t % DAY);

/**
 * A window bounded at [now]: `created_at` is author-signed, and a note dated in 2100 would
 * otherwise count in every window that reaches the present. NIP-01 bounds are inclusive.
 */
const window = (since, until, now, extra = {}) => ({ ...extra, since, until: Math.min(until, now) });

/**
 * Every ask the page wants at [now], in the order they should be answered, each with how long
 * its answer stays good. A closed bucket fills slowly from mirrored history, so it is re-asked
 * rarely; the open one, often. [kinds] are the kinds the table counts.
 */
export function wantedAsks(now, kinds = []) {
  const asks = [];
  const count = (key, filter, ttl) => asks.push({ key, verb: "count", filter, ttl });

  asks.push({ key: "newest", verb: "req", filter: { limit: 1, until: now }, ttl: 15, read: (evs) => (evs[0] ? evs[0].created_at : null) });
  count("last.60", window(now - MINUTE, now, now), 15);
  count("last.3600", window(now - HOUR, now, now), 15);
  count("last.86400", window(now - DAY, now, now), 60);

  const thisHour = hourOf(now);
  for (let i = 0; i < HOURS; i++) {
    const start = thisHour - i * HOUR;
    count(`hour.${start}`, window(start, start + HOUR - 1, now), i === 0 ? 30 : i === 1 ? 120 : 600);
  }
  asks.push({
    key: "sample",
    verb: "req",
    filter: { limit: SAMPLE_SIZE, since: now - HOUR, until: now },
    ttl: 600,
    read: (evs) => [...new Set(evs.map((e) => e.kind))].sort((a, b) => a - b),
  });
  const today = dayOf(now);
  for (let i = 0; i < DAYS; i++) {
    const start = today - i * DAY;
    count(`day.${start}`, window(start, start + DAY - 1, now), i === 0 ? 120 : i === 1 ? 600 : 3600);
  }
  for (const k of kinds) count(`kind.${k}.86400`, window(now - DAY, now, now, { kinds: [k] }), 600);
  for (const k of PEOPLE_KINDS) count(`people.${k}`, { kinds: [k] }, 1800);
  return asks;
}

/** The asks that only exist once another answer says they are worth asking. */
function followUps(now, kinds, answers) {
  const asks = [];
  for (const k of kinds) {
    // An hour inside a silent day is zero without asking.
    if (!(answers.get(`kind.${k}.86400`)?.value > 0)) continue;
    asks.push({ key: `kind.${k}.3600`, verb: "count", filter: window(now - HOUR, now, now, { kinds: [k] }), ttl: 600 });
  }
  return asks;
}

/** The ask that walks the whole store; only ever made on request. */
const storeAsk = () => ({ key: "store", verb: "count", filter: {}, ttl: Infinity });

/** A count answer that is not a number: the relay refused or went quiet. */
const unanswered = (v) => v && typeof v === "object" && "unanswered" in v;

/**
 * The asks, their answers, and the loop that keeps them fresh. [relay] is a `Relay` from
 * shared/relay.js (or anything with `count(filter)` and `req(filter)`), lensless so every
 * filter carries `include:spam`. Answers are kept while a refresh fails, with the failure beside them.
 */
export class LiveCounts {
  constructor(relay, { now = () => Math.floor(Date.now() / 1000), concurrency = 4, onChange = () => {}, fallbackKinds = [] } = {}) {
    this.relay = relay;
    /** What the table counts when the router names no kinds. */
    this.fallbackKinds = fallbackKinds;
    this.now = now;
    this.concurrency = concurrency;
    this.onChange = onChange;
    /** key -> { value, at, error, errorAt } */
    this.answers = new Map();
    this.inFlight = new Set();
    this.mirrored = null;
    this.storeWanted = false;
    this.paused = false;
    this.connError = null;
    this.connRetryAt = 0;
    this.timer = null;
  }

  /** The kinds the router mirrors, or null when it names none (no router, or an unbounded stream). */
  setMirrored(kinds) {
    this.mirrored = Array.isArray(kinds) && kinds.length ? [...kinds] : null;
  }

  /** The kinds the table counts: the router's, plus any seen among the newest events. */
  kinds() {
    const seen = this.answers.get("sample")?.value || [];
    return [...new Set([...(this.mirrored || this.fallbackKinds), ...seen])].sort((a, b) => a - b);
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
    const kinds = this.kinds();
    const asks = [...wantedAsks(now, kinds), ...followUps(now, kinds, this.answers)];
    if (this.storeWanted) asks.push(storeAsk());
    const wanted = new Set(asks.map((a) => a.key));
    // A bucket that rolled off the chart is forgotten, so the map does not grow for the life of the tab.
    for (const key of this.answers.keys()) if (!wanted.has(key) && key !== "store") this.answers.delete(key);
    return asks.filter((a) => {
      if (this.inFlight.has(a.key)) return false;
      const got = this.answers.get(a.key);
      if (!got) return true;
      if (got.errorAt != null && (got.at == null || got.errorAt >= got.at)) return now - got.errorAt >= Math.min(a.ttl, RETRY_AFTER);
      return now - got.at >= a.ttl;
    });
  }

  /** Ask the whole store once, answered or not: a failed walk is not retried on a timer. */
  countStore() {
    if (this.storeWanted || this.inFlight.has("store")) return;
    this.storeWanted = true;
    this.tick();
    this.onChange("store");
  }

  /** True from the press until the whole-store count answers or fails. */
  get countingStore() {
    return this.storeWanted || this.inFlight.has("store");
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
    if (ask.key === "store") this.storeWanted = false;
    const startedMs = Date.now();
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
    if (error == null) this.answers.set(ask.key, { value, at: now, tookMs: Date.now() - startedMs, filter: ask.filter });
    else this.answers.set(ask.key, { ...prior, error, errorAt: now });
    this.onChange(ask.key);
    this.tick();
  }

  /** Run the loop while the page is visible; a hidden tab asks nothing. */
  start(doc = globalThis.document) {
    this.timer = setInterval(() => this.tick(), TICK_MS);
    if (doc) {
      const onVisibility = () => {
        this.paused = doc.hidden;
        if (!this.paused) this.tick();
        this.onChange(null);
      };
      doc.addEventListener("visibilitychange", onVisibility);
      this.paused = !!doc.hidden;
    }
    this.tick();
  }

  stop() {
    clearInterval(this.timer);
    this.timer = null;
  }
}
