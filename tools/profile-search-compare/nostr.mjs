/*
 * Minimal Nostr relay client over the WebSocket the Node runtime ships with
 * (Node 22+). No dependencies: the harness stays a `node file.mjs` away from
 * running anywhere the relays are reachable.
 *
 * It speaks the sliver of the protocol the comparison needs: one REQ, collect
 * EVENTs until EOSE (or a timeout), and one EVENT publish waiting for its OK.
 */

/**
 * Open a socket, send a single REQ with `filter`, and resolve with every EVENT
 * received up to EOSE. Resolves on EOSE, CLOSED, or `timeoutMs`, whichever comes
 * first — relays that never send EOSE still terminate. Order is preserved as the
 * relay sent it, which is what lets us read a relay's own ranking off the wire.
 */
export function reqOnce(url, filter, { timeoutMs = 15000, subId = "q" } = {}) {
  return new Promise((resolve) => {
    const events = [];
    let settled = false;
    let ws;
    const done = (meta = {}) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try {
        ws && ws.close();
      } catch {}
      resolve({ url, events, ...meta });
    };
    const timer = setTimeout(() => done({ reason: "timeout" }), timeoutMs);
    try {
      ws = new WebSocket(url);
    } catch (e) {
      return done({ reason: "error", error: String(e) });
    }
    ws.onopen = () => ws.send(JSON.stringify(["REQ", subId, filter]));
    ws.onmessage = (m) => {
      let d;
      try {
        d = JSON.parse(m.data.toString());
      } catch {
        return;
      }
      if (d[0] === "EVENT" && d[1] === subId) events.push(d[2]);
      else if (d[0] === "EOSE" && d[1] === subId) done({ reason: "eose" });
      else if (d[0] === "CLOSED" && d[1] === subId) done({ reason: "closed", notice: d[2] });
      else if (d[0] === "NOTICE") done({ reason: "notice", notice: d[1] });
    };
    ws.onerror = (e) => done({ reason: "error", error: e?.message || e?.type || "error" });
    ws.onclose = () => done({ reason: "close" });
  });
}

/**
 * A persistent connection for driving many REQs / publishes down one socket —
 * far cheaper than a fresh handshake per query when syncing thousands of events.
 */
export class RelayConn {
  constructor(url, { timeoutMs = 20000 } = {}) {
    this.url = url;
    this.timeoutMs = timeoutMs;
    this.ws = null;
    this.nextId = 1;
    this.reqs = new Map(); // subId -> { events, resolve, timer }
    this.oks = new Map(); // eventId -> { resolve, timer }
  }

  connect() {
    return new Promise((resolve, reject) => {
      try {
        this.ws = new WebSocket(this.url);
      } catch (e) {
        return reject(e);
      }
      const t = setTimeout(() => reject(new Error("connect timeout: " + this.url)), this.timeoutMs);
      this.ws.onopen = () => {
        clearTimeout(t);
        resolve(this);
      };
      this.ws.onerror = (e) => {
        clearTimeout(t);
        reject(new Error("connect error: " + this.url + " " + (e?.message || e?.type || "")));
      };
      this.ws.onmessage = (m) => this._onMessage(m);
      this.ws.onclose = () => this._onClose();
    });
  }

  _onMessage(m) {
    let d;
    try {
      d = JSON.parse(m.data.toString());
    } catch {
      return;
    }
    const [type] = d;
    if (type === "EVENT") {
      const r = this.reqs.get(d[1]);
      if (r) r.events.push(d[2]);
    } else if (type === "EOSE") {
      this._finishReq(d[1], "eose");
    } else if (type === "CLOSED") {
      this._finishReq(d[1], "closed", d[2]);
    } else if (type === "OK") {
      const ok = this.oks.get(d[1]);
      if (ok) {
        clearTimeout(ok.timer);
        this.oks.delete(d[1]);
        ok.resolve({ accepted: d[2] === true, message: d[3] || "" });
      }
    }
  }

  _onClose() {
    for (const [, r] of this.reqs) {
      clearTimeout(r.timer);
      r.resolve({ events: r.events, reason: "socket-closed" });
    }
    this.reqs.clear();
    for (const [, ok] of this.oks) {
      clearTimeout(ok.timer);
      ok.resolve({ accepted: false, message: "socket-closed" });
    }
    this.oks.clear();
  }

  _finishReq(subId, reason, notice) {
    const r = this.reqs.get(subId);
    if (!r) return;
    clearTimeout(r.timer);
    this.reqs.delete(subId);
    try {
      this.ws.send(JSON.stringify(["CLOSE", subId]));
    } catch {}
    r.resolve({ events: r.events, reason, notice });
  }

  req(filter, { timeoutMs = this.timeoutMs } = {}) {
    const subId = "r" + this.nextId++;
    return new Promise((resolve) => {
      const timer = setTimeout(() => this._finishReq(subId, "timeout"), timeoutMs);
      this.reqs.set(subId, { events: [], resolve, timer });
      try {
        this.ws.send(JSON.stringify(["REQ", subId, filter]));
      } catch (e) {
        this._finishReq(subId, "send-error");
      }
    });
  }

  publish(event, { timeoutMs = this.timeoutMs } = {}) {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.oks.delete(event.id);
        resolve({ accepted: false, message: "ok-timeout" });
      }, timeoutMs);
      this.oks.set(event.id, { resolve, timer });
      try {
        this.ws.send(JSON.stringify(["EVENT", event]));
      } catch (e) {
        clearTimeout(timer);
        this.oks.delete(event.id);
        resolve({ accepted: false, message: "send-error" });
      }
    });
  }

  close() {
    try {
      this.ws && this.ws.close();
    } catch {}
  }
}

/** Best-effort display name pulled from a kind:0 profile's JSON content. */
export function profileName(event) {
  try {
    const c = JSON.parse(event.content);
    return c.name || c.display_name || c.displayName || c.nip05 || "";
  } catch {
    return "";
  }
}
