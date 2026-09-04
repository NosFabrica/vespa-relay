// A minimal Nostr relay client (REQ/EOSE/CLOSE + NIP-42 AUTH) on the browser's
// WebSocket, with no third-party code.

import { withoutLensAll } from "./lens.js";

const REQ_TIMEOUT_MS = 10000;

/**
 * How long a COUNT may stay silent before we give up on it: an idle window
 * reset by any message on the connection, not a deadline. A relay answers
 * serially, so a cheap COUNT queued behind an expensive one would expire on a
 * deadline while the socket is busy with somebody else's answer.
 */
const COUNT_IDLE_MS = 20000;

/** The relay answered something that was not a count. Its answer, not our budget. */
export const REFUSED = { unanswered: "refused" };

/** It went quiet with the ask outstanding. Our budget, not its answer. */
export const TIMED_OUT = { unanswered: "timeout" };

export class Relay {
  /**
   * `lensless` marks a connection that will never authenticate; every filter
   * it sends is stamped `include:spam` (shared/lens.js) because the relay
   * refuses an unauthenticated read that names no lens. The authenticated
   * socket leaves it off so a read before its NIP-42 fails loudly with
   * `auth-required:` and retries, rather than answering out of the unranked corpus.
   */
  constructor(url, { lensless = false } = {}) {
    this.url = url;
    this.lensless = lensless;
    this.ws = null;
    this.subs = new Map();       // subId -> { onEvent, finish }
    this.counts = new Map();     // subId -> resolver for its COUNT
    this.countIdle = null;       // one idle watchdog for every outstanding count
    this.okWaiters = new Map();  // event id -> resolver for its OK
    this.nextId = 1;
    this.challenge = null;       // the connection's NIP-42 challenge
    this.challengeWaiters = [];
    this.authed = false;         // did this connection complete NIP-42?
    this.opening = null;
    this.onclose = null;
    this.onAuthRequired = null;  // async: authenticate this connection, however the page does that
  }

  connect() {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) return Promise.resolve();
    if (this.opening) return this.opening;
    this.opening = new Promise((resolve, reject) => {
      let settled = false;
      const ws = new WebSocket(this.url);
      this.ws = ws;
      // A new socket carries neither the previous challenge nor its auth.
      // Cleared here rather than by the old socket's close event, which is late.
      this.challenge = null;
      this.authed = false;
      const settle = (fn) => { if (settled) return; settled = true; clearTimeout(timer); this.opening = null; fn(); };
      const fail = (e) => settle(() => reject(e));
      const timer = setTimeout(() => fail(new Error("relay connect timeout")), 8000);
      ws.onopen = () => settle(resolve);
      ws.onerror = () => fail(new Error("relay connection failed"));
      ws.onclose = () => {
        fail(new Error("relay connection closed"));
        // A close event lands after close() returns, and sign-out builds the
        // replacement immediately; a dead socket must not tear down the live one.
        if (this.ws !== ws) return;
        this.ws = null;
        this.challenge = null;
        this.authed = false;
        for (const s of this.subs.values()) s.finish(new Error("connection closed"));
        this.subs.clear();
        // A count outstanding on a socket that is gone is refused, not timed
        // out: nobody is waiting any more.
        this.failCounts(REFUSED);
        // A challenge that will now never arrive: wake the waiters rather than
        // let them spend their budget on a socket that is gone.
        this.wakeChallengeWaiters();
        this.onclose && this.onclose();
      };
      // Same guard: a replaced socket must not write the live one's challenge.
      ws.onmessage = (m) => { if (this.ws !== ws) return; try { this.handle(JSON.parse(m.data)); } catch (e) {} };
    });
    return this.opening;
  }

  handle(msg) {
    // Traffic of any kind, including for somebody else's ask, is proof the
    // relay is still working through its queue. See COUNT_IDLE_MS.
    this.bumpCountIdle();
    switch (msg[0]) {
      case "EVENT": { const s = this.subs.get(msg[1]); if (s) s.onEvent(msg[2]); break; }
      case "EOSE": { const s = this.subs.get(msg[1]); if (s) s.finish(null); break; }
      case "CLOSED": {
        const s = this.subs.get(msg[1]);
        if (s) { s.finish(new Error(msg[2] || "subscription closed")); break; }
        // A relay without NIP-45 commonly answers a COUNT by closing its
        // subscription. That is an answer, not our impatience.
        const c = this.counts.get(msg[1]);
        if (c) c(REFUSED);
        break;
      }
      case "COUNT": {
        const c = this.counts.get(msg[1]);
        // A COUNT frame carrying no number is the relay declining in a different dialect.
        if (c) c(typeof msg[2]?.count === "number" ? msg[2].count : REFUSED);
        break;
      }
      // A NOTICE carries no subscription id, and answering an unsupported COUNT
      // with one is what relays without NIP-45 do. Counts are the only ask this
      // client has outstanding on a probe connection, so it is attributed to
      // all of them. Wrong in the safe direction: a false "no COUNT" costs a
      // denominator, a false count would be a number no relay stated.
      case "NOTICE": this.failCounts(REFUSED); break;
      case "AUTH": this.challenge = msg[1]; this.wakeChallengeWaiters(); break;
      case "OK": { const w = this.okWaiters.get(msg[1]); if (w) { this.okWaiters.delete(msg[1]); w(msg); } break; }
    }
  }

  /** Arm the idle watchdog while anything is outstanding, and only then. */
  bumpCountIdle() {
    if (this.countIdle) { clearTimeout(this.countIdle); this.countIdle = null; }
    if (!this.counts.size) return;
    this.countIdle = setTimeout(() => this.failCounts(TIMED_OUT), COUNT_IDLE_MS);
  }

  /**
   * Hand every outstanding count the same non-answer. Iterates a snapshot and
   * does not clear the map first: each `finish` deletes its own entry and
   * returns early when it is already gone.
   */
  failCounts(reason) {
    if (!this.counts.size) return;
    for (const finish of [...this.counts.values()]) finish(reason);
  }

  /** Hand the current challenge (or its absence) to everyone waiting on one. */
  wakeChallengeWaiters() {
    if (!this.challengeWaiters.length) return;
    const waiters = this.challengeWaiters;
    this.challengeWaiters = [];
    for (const w of waiters) w(this.challenge);
  }

  /** The connection's NIP-42 challenge, or null if none arrives within [timeoutMs]. Awaited, not polled. */
  waitForChallenge(timeoutMs) {
    if (this.challenge) return Promise.resolve(this.challenge);
    return new Promise((resolve) => {
      const waiter = (c) => { clearTimeout(timer); resolve(c); };
      const timer = setTimeout(() => {
        const i = this.challengeWaiters.indexOf(waiter);
        if (i >= 0) this.challengeWaiters.splice(i, 1);
        resolve(this.challenge);
      }, timeoutMs);
      this.challengeWaiters.push(waiter);
    });
  }

  /**
   * One REQ, collected until EOSE (or timeout: resolve with what arrived).
   * `filter` is one filter or an array of them; NIP-01 ORs the filters within
   * a subscription, the only way to ask a union question in one round trip.
   */
  async req(filter, timeoutMs = REQ_TIMEOUT_MS, { signal } = {}) {
    try {
      return await this.reqOnce(filter, timeoutMs, signal);
    } catch (e) {
      // A relay answers an unauthenticated REQ with CLOSED "auth-required:" and
      // the AUTH that follows does not revive it; the client must ask again.
      // One retry only. The anonymous reference connection installs no hook.
      if (this.onAuthRequired && !this.authed &&
          String((e && e.message) || "").startsWith("auth-required")) {
        await this.onAuthRequired();
        return await this.reqOnce(filter, timeoutMs, signal);
      }
      throw e;
    }
  }

  /**
   * The single send-and-collect attempt behind [req]. The resolved array
   * carries `complete`: true on EOSE, false when the timeout or [signal] ended
   * it first. A caller must not cache an absence off an incomplete read.
   */
  async reqOnce(filter, timeoutMs, signal) {
    await this.connect();
    const id = "sot" + this.nextId++;
    const events = [];
    if (signal?.aborted) { events.complete = false; return events; }
    return await new Promise((resolve, reject) => {
      const finish = (err, complete = true) => {
        if (!this.subs.delete(id)) return;
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        try { this.ws && this.ws.send(JSON.stringify(["CLOSE", id])); } catch (e) {}
        if (err) { reject(err); return; }
        events.complete = complete;
        resolve(events);
      };
      const onAbort = () => finish(null, false);
      signal?.addEventListener("abort", onAbort, { once: true });
      const timer = setTimeout(() => finish(null, false), timeoutMs);
      this.subs.set(id, { onEvent: (ev) => events.push(ev), finish });
      const asked = this.lensless ? withoutLensAll(filter) : filter;
      this.ws.send(JSON.stringify(["REQ", id, ...(Array.isArray(asked) ? asked : [asked])]));
    });
  }

  /**
   * NIP-45 COUNT: the number, or [REFUSED] / [TIMED_OUT], never a throw. The
   * two non-answers are values because the caller renders each differently.
   */
  async count(filter) {
    await this.connect();
    const id = "cnt" + this.nextId++;
    return await new Promise((resolve) => {
      const finish = (v) => {
        if (!this.counts.delete(id)) return;
        // Closed however we leave: relays cap concurrent subscriptions.
        try { this.ws && this.ws.send(JSON.stringify(["CLOSE", id])); } catch (e) {}
        this.bumpCountIdle();
        resolve(v);
      };
      this.counts.set(id, finish);
      this.bumpCountIdle();
      try { this.ws.send(JSON.stringify(["COUNT", id, this.lensless ? withoutLensAll(filter) : filter])); }
      catch (e) { finish(REFUSED); }
    });
  }

  /** NIP-01 EVENT submission: send, and wait for the relay's OK verdict. */
  async publish(ev, timeoutMs = 8000) {
    await this.connect();
    return await new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.okWaiters.delete(ev.id); reject(new Error("publish timed out")); }, timeoutMs);
      this.okWaiters.set(ev.id, (msg) => {
        clearTimeout(timer);
        if (msg[2]) resolve(); else reject(new Error(msg[3] || "rejected"));
      });
      this.ws.send(JSON.stringify(["EVENT", ev]));
    });
  }

  /** NIP-42: send the signed kind-22242 and wait for its OK. */
  async auth(signed, timeoutMs = 8000) {
    await this.connect();
    return await new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.okWaiters.delete(signed.id); reject(new Error("auth timed out")); }, timeoutMs);
      this.okWaiters.set(signed.id, (msg) => {
        clearTimeout(timer);
        if (msg[2]) { this.authed = true; resolve(); } else reject(new Error(msg[3] || "auth rejected"));
      });
      this.ws.send(JSON.stringify(["AUTH", signed]));
    });
  }
}
