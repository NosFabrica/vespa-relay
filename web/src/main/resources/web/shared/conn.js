// The page's two sockets: the authenticated connection, whose NIP-42 state
// ranks every search, and the anonymous reference connection for facts about
// subjects (names, faces, scores, observer lists) that the trust-gated socket
// would silently narrow. The reference socket is `lensless`: it never
// authenticates, so every filter it sends carries NIP-50 `include:spam` or the
// relay refuses the read with `auth-required:` (see shared/lens.js).

import { Relay } from "./relay.js";

// Same-origin by default: the page is served by the relay it talks to.
// To develop against a remote server, set RELAY_URL here to its ws:// url.
export const RELAY_URL = (location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/";

export const relay = new Relay(RELAY_URL);

let refRelay = null;
let refOpening = null;

export async function refConn() {
  if (refRelay && refRelay.ws && refRelay.ws.readyState === 1) return refRelay;
  // Dedupe the opening, not just the open socket: callers race here on page
  // load by design, and each one that missed would leak a socket.
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

// Closing the sockets on pagehide is what makes the page eligible for the
// back-forward cache; the browser closes them anyway when it freezes the page.
// The restore half (the pageshow re-auth) lives with the login flow in app.js.
window.addEventListener("pagehide", () => {
  for (const r of [relay, refRelay]) {
    if (r && r.ws) { try { r.ws.close(); } catch (e) {} }
  }
});
