// Who the reader is: NIP-07 sign-in, proved to the relay with a NIP-42 AUTH on the ranking
// socket. Signing the challenge switches the connection's ranking observer to you; it enrols
// nothing. The state is this module's; the page reads it through [me], [claimed] and
// [loginSettled] and hears about changes through [onSignIn].

import { RELAY_URL, relay } from "./shared/conn.js";
import { HEX64 } from "./shared/format.js";
import { profiles, displayName, enrichProfiles } from "./shared/profiles.js";

// ---- the pure half ----------------------------------------------------------

/** A cookie so every page the relay serves reads the choice. */
export const SIGNIN_COOKIE = "sot_signedin";

/** Whether [cookie] (a `document.cookie`) wants a sign-in; unset defaults to signed in. */
export function signInWanted(cookie) {
  const m = String(cookie || "").match(new RegExp(`(?:^|;\\s*)${SIGNIN_COOKIE}=([01])`));
  return m ? m[1] === "1" : true;
}

/** The cookie that remembers [yes] for a year. */
export const signInCookie = (yes) => `${SIGNIN_COOKIE}=${yes ? 1 : 0}; path=/; max-age=31536000; SameSite=Lax`;

/** The NIP-42 event that answers [challenge] for [relayUrl], unsigned. */
export const authEvent = (challenge, relayUrl, nowSec) => ({
  kind: 22242,
  created_at: nowSec,
  content: "",
  tags: [["relay", relayUrl], ["challenge", challenge]],
});

/** A remembered face read back, or null for anything that is not one. */
export function parseFace(json) {
  try {
    const f = JSON.parse(json || "null");
    return f && HEX64.test(f.pubkey || "") ? f : null;
  } catch (e) { return null; }
}

/**
 * What the avatar draws for [who]: the relay's answer once it has one (`known`), the remembered
 * face until then, and nothing for anybody else.
 */
export function faceOf(who, { known, profile, remembered }) {
  const kept = who && !known && remembered && remembered.pubkey === who ? remembered : null;
  return {
    picture: (profile && profile.picture) || (kept && kept.picture) || "",
    name: displayName(profile) || (kept ? kept.name : ""),
  };
}

// ---- the state ----------------------------------------------------------------

let proven = null;     // the pubkey the relay accepted a NIP-42 AUTH for
let pending = null;    // the pubkey the extension named, before that proof
let tried = false;     // the first attempt has settled (labels key on this)
let flight = null;     // the first attempt itself, awaited by every search

// The last signed-in account's picture and name, so the field can draw them before the socket,
// the challenge and the kind 0 read. A cache of what was on screen; it authorises nothing.
const FACE_KEY = "sot_face";
let face = readFace();

const hooks = {
  /** Who is signed in changed: redraw the avatar and the line under the box. */
  changed: () => {},
  /** Only the face the extension named changed: redraw the avatar alone, never the line under it. */
  face: () => {},
  /** The first attempt ended with nobody proved; [error] is why, or null for a choice. */
  unproved: (_error) => {},
  /** The socket's lens is settled: [pubkey] signed in, or null. */
  settled: (_pubkey) => {},
};

/** Install the page's reactions; each is optional. */
export function onSignIn(h) { Object.assign(hooks, h); }

/** The pubkey the relay accepted, or null. */
export const me = () => proven;
/** The pubkey the extension named and the relay has not accepted yet, or null. */
export const claimed = () => pending;
/** Whether the first attempt has settled, signed in or not. */
export const loginSettled = () => tried;
/** The face remembered from the last load, or null. */
export const rememberedFace = () => face;
/** Whether the reader has not chosen to stay signed out. */
export const wantsSignIn = () => signInWanted(document.cookie);

function readFace() {
  try { return parseFace(localStorage.getItem(FACE_KEY)); } catch (e) { return null; }
}
function writeFace() {
  try {
    if (face) localStorage.setItem(FACE_KEY, JSON.stringify(face));
    else localStorage.removeItem(FACE_KEY);
  } catch (e) {}
}
/** Keep what the relay said about [pk]'s face, but only once it answered (`has`); a dropped read is not "no picture". */
function rememberFace(pk) {
  if (!pk || !profiles.has(pk)) return;
  const p = profiles.get(pk);
  face = p ? { pubkey: pk, picture: p.picture || "", name: displayName(p) } : null;
  writeFace();
}

/** The face from the last load, drawn on the first paint as an assumption [prefetchFace] corrects. */
export function assumeRememberedFace() {
  if (wantsSignIn() && face) pending = face.pubkey;
}

// Sign the current challenge and send it. A challenge belongs to one connection: if the socket
// was replaced while the human signed, re-read it and sign again.
async function signAndAuth(attempt = 0, waitMs = 3000) {
  const challenge = await relay.waitForChallenge(waitMs);
  if (!challenge) throw new Error("The relay sent no NIP-42 challenge");
  const signed = await window.nostr.signEvent(authEvent(challenge, RELAY_URL, Math.floor(Date.now() / 1000)));
  if (relay.challenge !== challenge) {
    // Two tries only: looping would spam the signer.
    if (attempt >= 1) throw new Error("The connection kept resetting while signing — try again");
    await relay.connect();
    return signAndAuth(attempt + 1);
  }
  await relay.auth(signed);
  return signed.pubkey;
}

/**
 * Who the extension says you are, and your face, both before the proof. `getPublicKey()` is
 * local, so this runs beside the handshake. The pubkey draws and prefetches; it never authorises.
 */
async function prefetchFace() {
  let pk = null;
  try { pk = await window.nostr.getPublicKey(); } catch (e) { pk = null; }
  if (!HEX64.test(pk || "")) {
    // The extension would not say, so the boot's assumed face comes down.
    pending = null;
    hooks.face();
    return null;
  }
  pending = pk;
  hooks.face();            // the remembered face, if this is the same account
  // enrichProfiles skips a pubkey it has seen, including a cached `null`, so delete first.
  profiles.delete(pk);
  try { await enrichProfiles([pk]); } catch (e) {}
  hooks.face();
  return pk;
}

async function login() {
  if (!(window.nostr && window.nostr.signEvent)) throw new Error("No Nostr extension found (window.nostr / NIP-07)");
  // The socket and the face start together; only the AUTH needs both.
  const connecting = relay.connect();
  const named = prefetchFace();
  await connecting;
  // A click gets a longer wait than a background attempt: the challenge arrives after the handshake.
  proven = await signAndAuth(0, 10000);
  pending = null;
  hooks.changed();
  // Awaited so callers can rely on the face being fetched when login() returns.
  if ((await named) !== proven) {
    // The extension never answered getPublicKey, or signed as another account.
    profiles.delete(proven);
    try { await enrichProfiles([proven]); } catch (e) {}
  }
  rememberFace(proven);
  hooks.changed();
  // The read above races the reference socket's opening. Retry on `has` only: a cached `null` is an answer.
  for (let i = 0; i < 3 && proven && !profiles.has(proven); i++) {
    await new Promise((r) => setTimeout(r, 600 * (i + 1)));
    if (!proven) break;
    profiles.delete(proven);
    try { await enrichProfiles([proven]); } catch (e) {}
    rememberFace(proven);
    hooks.changed();
  }
}

/**
 * Signed in automatically, once, best effort; a failure downgrades to the unranked corpus and
 * says so. One in-flight promise that every read awaits, so no REQ goes out on this socket
 * before its auth question is settled.
 */
export async function ensureLogin() {
  // Started, not awaited: only the AUTH needs the socket, and it connects itself.
  relay.connect().catch(() => {});
  if (!flight) {
    flight = (async () => {
      // A remembered "signed out" is a decision; do not prompt the extension.
      if (!wantsSignIn() || !(window.nostr && window.nostr.signEvent)) {
        pending = null;
        hooks.unproved(null);
        return;
      }
      try { await login(); } catch (e) {
        proven = null;
        pending = null;   // the extension named somebody it could not prove
        hooks.unproved(e);
      }
    })().finally(() => { tried = true; });
  }
  await flight;
  if (!proven) { hooks.settled(null); return; }
  // Keyed on the connection's auth state: a reconnect leaves `me` set but the socket unauthenticated.
  if (relay.authed) { hooks.settled(proven); return; }
  proven = await signAndAuth();
  hooks.changed();
  // Both paths to an authenticated socket meet here, and the check is about the connection's lens.
  hooks.settled(proven);
}

/** The reader pressed sign in: remembered, and the extension asked now. Throws what went wrong. */
export async function signIn() {
  document.cookie = signInCookie(true);
  tried = true;
  await login();
}

/**
 * The reader pressed sign out: remembered, and the face forgotten, not merely unrendered, or it
 * flashes back on the next load. The socket's AUTH is the caller's to drop by reconnecting.
 */
export function signOut() {
  document.cookie = signInCookie(false);
  proven = null;
  pending = null;
  face = null;
  writeFace();
}

/** A press that failed: nobody is signed in, whatever the extension said. */
export function signInFailed() {
  proven = null;
  pending = null;
}

/**
 * The restore half of the back/forward cache (pagehide is in shared/conn.js): `me` is still set,
 * the socket is closed and nothing is authenticated. NIP-42 belongs to the connection, so the
 * proof and the settled flight are both stale.
 */
export function forgetConnection() {
  tried = false;
  flight = null;
}

// The resend half of NIP-42: on CLOSED "auth-required:" the client authenticates and resends.
// The anonymous connection gets no such hook; for it auth-required is a real answer.
relay.onAuthRequired = () => ensureLogin();
