// The page's two sockets: the authenticated connection that ranks every search, and the
// anonymous reference connection for facts about subjects (names, faces, scores, observer
// lists) that the trust-gated socket would silently narrow. The reference socket is
// `lensless`, so every filter it sends carries NIP-50 `include:spam` (shared/lens.js).

import { Relay } from "./relay.js";

// Same-origin: the page is served by the relay it talks to. Point RELAY_URL at a remote
// server's ws:// url to develop against it.
export const RELAY_URL = (location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/";

export const relay = new Relay(RELAY_URL);

let refRelay = null;
let refOpening = null;

export async function refConn() {
  if (refRelay && refRelay.ws && refRelay.ws.readyState === 1) return refRelay;
  // Callers race here on page load; each one that missed the in-flight open would leak a socket.
  if (refOpening) return refOpening;
  refOpening = (async () => {
    const r = new Relay(RELAY_URL, { lensless: true });
    try {
      await r.connect();
      refRelay = r;
      return r;
    } finally {
      refOpening = null;
    }
  })();
  return refOpening;
}

// Closing the sockets on pagehide keeps the page eligible for the back-forward cache; the
// pageshow re-auth lives with the login flow in app.js.
window.addEventListener("pagehide", () => {
  for (const r of [relay, refRelay]) {
    if (r && r.ws) { try { r.ws.close(); } catch (e) {} }
  }
});
