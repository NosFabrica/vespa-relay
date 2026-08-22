// The page's two sockets, owned in one place: the authenticated connection
// (whose NIP-42 state ranks every search) and the anonymous reference
// connection (names, faces, scores, observer lists — facts about subjects,
// not about the reader, which the trust-gated socket would silently narrow).
//
// The reference socket is `lensless`, which is now a thing the relay makes it
// SAY: it never authenticates, so every filter it sends carries NIP-50
// `include:spam` or the relay refuses the read with `auth-required:` — see
// shared/lens.js. That is the same bargain this socket was always making,
// finally written on the wire instead of assumed by both ends.

import { Relay } from "./relay.js";

// Same-origin by default: the page is served by the relay it talks to.
// To develop against a remote server, set RELAY_URL here to its ws:// url.
export const RELAY_URL = (location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/";

export const relay = new Relay(RELAY_URL);

let refRelay = null;
let refOpening = null;

export async function refConn() {
  if (refRelay && refRelay.ws && refRelay.ws.readyState === 1) return refRelay;
  // Dedupe the OPENING, not just the open socket. Reusing only at readyState
  // 1 meant every caller arriving while the socket was still connecting built
  // its own Relay and overwrote this one — and the overwritten sockets were
  // never closed, so they stayed open. Three independent entry points reach
  // here (enrichProfiles, rankServiceOf, the score-chip fill) and they race
  // on page load by design; the comment on the sign-in path already says so.
  // Relay.connect() guards itself exactly this way.
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

// ---- the back/forward cache -------------------------------------------
//
// Navigating away does not unload this page: the browser FREEZES it into the
// back-forward cache so going back is instant. An open WebSocket cannot be
// frozen, so the browser closes ours and logs
//
//     WebSocket connection to 'ws://localhost:7777/' failed:
//     Page entered Back-Forward Cache.
//
// — once per socket, which is where those repeated console lines came from.
// They are the browser reporting what it did, not an error in the connection.
//
// Closing them ourselves on the way out is also what makes the page eligible
// for the cache in the first place — a live socket can keep it out entirely,
// trading the instant back-navigation for nothing. The RESTORE half — the
// pageshow re-auth — lives with the login flow in app.js, because identity
// is its state, not this module's.
window.addEventListener("pagehide", () => {
  for (const r of [relay, refRelay]) {
    if (r && r.ws) { try { r.ws.close(); } catch (e) {} }
  }
});
