// A minimal Nostr relay client (REQ/EOSE/CLOSE + NIP-42 AUTH). No third-party
// code — the browser's WebSocket and ~80 lines are the whole client, and every
// line is inspectable right here.

const REQ_TIMEOUT_MS = 10000;

/**
 * How long a COUNT may stay SILENT before we give up on it — an idle window,
 * not a deadline, reset by any message on the connection.
 *
 * The distinction is the one quartz made to its accessory timeouts in
 * 1622bd7109 and observer_stats.html re-learned by measurement: a relay
 * answers serially, so a cheap COUNT queued behind an expensive one expires on
 * a deadline while the socket is busy delivering somebody else's answer.
 * Measured there — 146 services, 4 in flight — a 15s deadline scored 55
 * answered and 91 "timed out" with a median answer of 75ms, and ZERO of the 91
 * were ever refused.
 *
 * Shorter than that page's 60s because the waiter is different: it is an
 * operator watching a run there, and here it is a panel under somebody's
 * search box. Giving up early costs a denominator and says so, which is the
 * safe direction — the alternative is a bar drawn on a guess.
 */
const COUNT_IDLE_MS = 20000;

/** The relay answered something that was not a count. Its answer, not our budget. */
export const REFUSED = { unanswered: "refused" };

/** It went quiet with the ask outstanding. Our budget, not its answer. */
export const TIMED_OUT = { unanswered: "timeout" };

export class Relay {
  constructor(url) {
    this.url = url;
    this.ws = null;
    this.subs = new Map();       // subId -> { onEvent, finish }
    this.counts = new Map();     // subId -> resolver for its COUNT
    this.countIdle = null;       // ONE idle watchdog for every outstanding count
    this.okWaiters = new Map();  // event id -> resolver for its OK
    this.nextId = 1;
    this.challenge = null;       // the connection's NIP-42 challenge
    this.challengeWaiters = [];  // resolvers for whoever is waiting on it
    this.authed = false;         // did THIS connection complete NIP-42?
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
      // A new socket is a new connection: it carries neither the previous
      // challenge nor its NIP-42 auth. Cleared HERE, when the replacement is
      // made, rather than relying on the old socket's close event to do it —
      // that event is late, and the guard below now (correctly) ignores it.
      this.challenge = null;
      this.authed = false;
      // Cleared on both exits. It used to be armed and forgotten: one live
      // timer per socket opened, so every reconnect left another behind, and
      // a page that had reconnected a few times was holding timers whose only
      // job was to check a flag that was already set.
      const settle = (fn) => { if (settled) return; settled = true; clearTimeout(timer); this.opening = null; fn(); };
      const fail = (e) => settle(() => reject(e));
      const timer = setTimeout(() => fail(new Error("relay connect timeout")), 8000);
      ws.onopen = () => settle(resolve);
      ws.onerror = () => fail(new Error("relay connection failed"));
      ws.onclose = () => {
        fail(new Error("relay connection closed"));
        // Only if this is still OUR socket.
        //
        // A close event lands a task or more after close() was called, and the
        // page does not wait for it: signing out is `ws.close(); connect()`,
        // which builds the replacement immediately. The dead socket's event
        // then arrived and nulled `this.ws` — the LIVE one — and cleared the
        // auth flag and the challenge belonging to it. From there the client
        // held no socket while one was open: the next req() opened a third,
        // the second stayed open forever with its handlers still writing this
        // object's challenge, and every sign-out leaked one more.
        if (this.ws !== ws) return;
        this.ws = null;
        this.challenge = null;
        this.authed = false;
        for (const s of this.subs.values()) s.finish(new Error("connection closed"));
        this.subs.clear();
        // A count outstanding on a socket that is gone is REFUSED, not timed
        // out: we are not waiting any more, and the caller must not read it as
        // "the relay is thinking".
        this.failCounts(REFUSED);
        // A challenge that will now never arrive: wake the waiters with the
        // answer instead of leaving them to age out. The caller's next step is
        // to reconnect and ask again, and it should not spend its whole budget
        // waiting on a socket that is already gone.
        this.wakeChallengeWaiters();
        this.onclose && this.onclose();
      };
      // Same guard, for the same reason: a replaced socket must not keep
      // writing the challenge the live one is authenticating against.
      ws.onmessage = (m) => { if (this.ws !== ws) return; try { this.handle(JSON.parse(m.data)); } catch (e) {} };
    });
    return this.opening;
  }

  handle(msg) {
    // Traffic of ANY kind is proof the relay is still working through its
    // queue, including a message for somebody else's ask. Bumped before the
    // switch for that reason — see COUNT_IDLE_MS.
    this.bumpCountIdle();
    switch (msg[0]) {
      case "EVENT": { const s = this.subs.get(msg[1]); if (s) s.onEvent(msg[2]); break; }
      case "EOSE": { const s = this.subs.get(msg[1]); if (s) s.finish(null); break; }
      case "CLOSED": {
        const s = this.subs.get(msg[1]);
        if (s) { s.finish(new Error(msg[2] || "subscription closed")); break; }
        // A relay that does not serve NIP-45 commonly answers the COUNT by
        // closing its subscription. That is an ANSWER — it declined — and it
        // must not be reported as our impatience.
        const c = this.counts.get(msg[1]);
        if (c) c(REFUSED);
        break;
      }
      case "COUNT": {
        const c = this.counts.get(msg[1]);
        // `{"count": n}` or nothing usable. A COUNT frame carrying no number
        // is the relay declining in a different dialect.
        if (c) c(typeof msg[2]?.count === "number" ? msg[2].count : REFUSED);
        break;
      }
      // NIP-01's NOTICE carries no subscription id, so it cannot be addressed
      // to the ask it is about — and answering an unsupported COUNT with one is
      // exactly what the relays that do not implement NIP-45 do (measured:
      // nip85.brainstorm.world, on all 45 of the services it serves). The only
      // thing we can do is attribute it to what is outstanding, and counts are
      // the only ask this client ever has outstanding on a probe connection.
      // Wrong in the safe direction: a false "no COUNT" costs a denominator,
      // where a false count would be a number no relay ever stated.
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
   * Hand every outstanding count the same non-answer.
   *
   * A SNAPSHOT of the resolvers, and the map is emphatically NOT cleared first:
   * each one is the `finish` from count(), which begins `if
   * (!this.counts.delete(id)) return;` to make itself idempotent. Clearing here
   * made every one of those early-return without resolving, so a relay that
   * answered a COUNT with a NOTICE — the exact case this path exists for — left
   * the caller awaiting a promise nothing would ever settle. Each finish
   * removes its own entry, so the map is empty when the loop ends.
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

  /**
   * The connection's NIP-42 challenge, or null if none arrives within
   * [timeoutMs].
   *
   * AWAITED, not polled. This was a `while (!relay.challenge) await sleep(100)`
   * in the page, which is on the critical path of every load: sign-in gates the
   * first REQ (search() awaits ensureLogin()), so the whole page waited on a
   * 100ms tick for a message that arrives within a millisecond or two of the
   * handshake. Averaged 50ms of nothing per load, and it was the FIRST thing
   * every visitor paid.
   */
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
   *
   * `filter` is one filter or an ARRAY of them. NIP-01 ORs the filters within
   * a subscription, which is the only way to ask a union question — "tagged
   * `t: nostr`, OR a comment whose `I` says the same" is two filters and one
   * REQ. Sending them as separate REQs would mean two EOSEs, two timeouts and
   * two `limit`s to reconcile before anything could render.
   */
  async req(filter, timeoutMs = REQ_TIMEOUT_MS) {
    try {
      return await this.reqOnce(filter, timeoutMs);
    } catch (e) {
      // NIP-42's second half, the part that is easy to forget: a relay
      // answers an unauthenticated REQ with CLOSED "auth-required:", and the
      // AUTH that follows does NOT revive it — a relay never re-runs a
      // subscription it already answered under the old auth state. The
      // client must authenticate and ASK AGAIN. `onAuthRequired` is how the
      // page lends this class its signer without this class knowing NIP-07
      // exists. One retry only: if the resend is refused too, the refusal is
      // the answer. The anonymous reference connection installs no hook on
      // purpose — being unauthenticated is its whole point — so there a
      // CLOSED still surfaces as the error it is.
      if (this.onAuthRequired && !this.authed &&
          String((e && e.message) || "").startsWith("auth-required")) {
        await this.onAuthRequired();
        return await this.reqOnce(filter, timeoutMs);
      }
      throw e;
    }
  }

  /**
   * The single send-and-collect attempt behind [req]. The resolved array
   * carries `complete`: true when the relay said EOSE, false when the
   * timeout fired first. The two used to be indistinguishable, and a caller
   * caching "this pubkey has no profile" off a timed-out read was recording
   * a fact the relay never stated.
   */
  async reqOnce(filter, timeoutMs) {
    await this.connect();
    const id = "sot" + this.nextId++;
    const events = [];
    return await new Promise((resolve, reject) => {
      const finish = (err, complete = true) => {
        if (!this.subs.delete(id)) return;
        clearTimeout(timer);
        try { this.ws && this.ws.send(JSON.stringify(["CLOSE", id])); } catch (e) {}
        if (err) { reject(err); return; }
        events.complete = complete;
        resolve(events);
      };
      const timer = setTimeout(() => finish(null, false), timeoutMs);
      this.subs.set(id, { onEvent: (ev) => events.push(ev), finish });
      this.ws.send(JSON.stringify(["REQ", id, ...(Array.isArray(filter) ? filter : [filter])]));
    });
  }

  /**
   * NIP-45 COUNT: the number, or [REFUSED] / [TIMED_OUT] — never a throw.
   *
   * The two non-answers are values rather than exceptions because the caller
   * has to TELL THEM APART and render each: "the relay declined" and "we
   * stopped waiting" lead to different conclusions, and collapsing them into
   * one catch is how a status page ends up saying zero when it means silence.
   */
  async count(filter) {
    await this.connect();
    const id = "cnt" + this.nextId++;
    return await new Promise((resolve) => {
      const finish = (v) => {
        if (!this.counts.delete(id)) return;
        // Closed however we leave: relays cap concurrent subscriptions, and an
        // abandoned one is a later ask rejected for a reason nothing on the
        // page would explain.
        try { this.ws && this.ws.send(JSON.stringify(["CLOSE", id])); } catch (e) {}
        this.bumpCountIdle();
        resolve(v);
      };
      this.counts.set(id, finish);
      this.bumpCountIdle();
      try { this.ws.send(JSON.stringify(["COUNT", id, filter])); }
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
