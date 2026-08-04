// A minimal Nostr relay client (REQ/EOSE/CLOSE + NIP-42 AUTH). No third-party
// code — the browser's WebSocket and ~80 lines are the whole client, and every
// line is inspectable right here.

const REQ_TIMEOUT_MS = 10000;

export class Relay {
  constructor(url) {
    this.url = url;
    this.ws = null;
    this.subs = new Map();       // subId -> { onEvent, finish }
    this.okWaiters = new Map();  // event id -> resolver for its OK
    this.nextId = 1;
    this.challenge = null;       // the connection's NIP-42 challenge
    this.authed = false;         // did THIS connection complete NIP-42?
    this.opening = null;
    this.onclose = null;
  }

  connect() {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) return Promise.resolve();
    if (this.opening) return this.opening;
    this.opening = new Promise((resolve, reject) => {
      let settled = false;
      const ws = new WebSocket(this.url);
      this.ws = ws;
      const fail = (e) => { if (!settled) { settled = true; this.opening = null; reject(e); } };
      setTimeout(() => fail(new Error("relay connect timeout")), 8000);
      ws.onopen = () => { if (!settled) { settled = true; this.opening = null; resolve(); } };
      ws.onerror = () => fail(new Error("relay connection failed"));
      ws.onclose = () => {
        fail(new Error("relay connection closed"));
        this.ws = null;
        this.challenge = null;
        this.authed = false;
        for (const s of this.subs.values()) s.finish(new Error("connection closed"));
        this.subs.clear();
        this.onclose && this.onclose();
      };
      ws.onmessage = (m) => { try { this.handle(JSON.parse(m.data)); } catch (e) {} };
    });
    return this.opening;
  }

  handle(msg) {
    switch (msg[0]) {
      case "EVENT": { const s = this.subs.get(msg[1]); if (s) s.onEvent(msg[2]); break; }
      case "EOSE": { const s = this.subs.get(msg[1]); if (s) s.finish(null); break; }
      case "CLOSED": { const s = this.subs.get(msg[1]); if (s) s.finish(new Error(msg[2] || "subscription closed")); break; }
      case "AUTH": this.challenge = msg[1]; break;
      case "OK": { const w = this.okWaiters.get(msg[1]); if (w) { this.okWaiters.delete(msg[1]); w(msg); } break; }
    }
  }

  /** One REQ, collected until EOSE (or timeout: resolve with what arrived). */
  async req(filter, timeoutMs = REQ_TIMEOUT_MS) {
    await this.connect();
    const id = "sot" + this.nextId++;
    const events = [];
    return await new Promise((resolve, reject) => {
      const finish = (err) => {
        if (!this.subs.delete(id)) return;
        clearTimeout(timer);
        try { this.ws && this.ws.send(JSON.stringify(["CLOSE", id])); } catch (e) {}
        err ? reject(err) : resolve(events);
      };
      const timer = setTimeout(() => finish(null), timeoutMs);
      this.subs.set(id, { onEvent: (ev) => events.push(ev), finish });
      this.ws.send(JSON.stringify(["REQ", id, filter]));
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
