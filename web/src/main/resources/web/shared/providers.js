// The publishers a reader's kind 10040 delegates, by kind. A NIP-85 assertion
// or a Tapestry Trusted List is only worth drawing when this reader's Map named
// its signer for that kind; once the page fetches declarations itself, off an
// anonymous socket that gates nothing, that rule is re-imposed here as an
// `authors` filter. Two tag shapes must both be read:
//
//     ["30382:rank", <64-hex service>, <relay>]   NIP-85's ServiceProviderTag
//     ["30392",      <64-hex service>, <relay>]   a bare kind, the Tapestry ADR's
//
// No DOM; the parse is pure so providers.test.mjs can hold it.

import { refConn } from "./conn.js";

/** A key a Map may delegate to: 64 lowercase hex, the only thing an `authors` filter takes. */
const HEX64 = /^[0-9a-f]{64}$/;

/** NIP-85's Map, the event this module reads. */
export const MAP_KIND = 10040;

/**
 * The delegations a Map states, keyed by the entry as written (`30382:rank`,
 * `30382:followers`, `30392`) and order-preserving. Dimensions are kept apart:
 * a followers service cannot rank, so callers wanting the whole kind use
 * [publishersOf]. A tag whose second element is not a key is dropped.
 */
export function delegationsOf(map) {
  const out = new Map();
  for (const t of (map && map.tags) || []) {
    if (!Array.isArray(t) || typeof t[0] !== "string" || !HEX64.test(t[1] || "")) continue;
    if (!Number.isInteger(Number.parseInt(t[0].split(":")[0], 10))) continue;
    let keys = out.get(t[0]);
    if (!keys) out.set(t[0], (keys = []));
    if (!keys.includes(t[1])) keys.push(t[1]);
  }
  return out;
}

/** Every publisher [delegations] names for [kind], across all its dimensions. All of them, never the first. */
export function publishersOf(delegations, kind) {
  const out = [];
  for (const [entry, keys] of delegations || []) {
    if (Number.parseInt(entry.split(":")[0], 10) !== kind) continue;
    for (const k of keys) if (!out.includes(k)) out.push(k);
  }
  return out;
}

// observer pubkey -> the delegations their Map states. Never written off an
// incomplete read: see [providersFor].
const cache = new Map();

// observer pubkey -> the read that will settle it. Dedupes the read, not just
// the answer: the provenance lookup and the score chips ask in the same render.
const inFlight = new Map();

/**
 * The delegations [observer] states, from their Map: one read, cached only
 * when the relay answered completely, down the anonymous reference socket.
 */
export async function providersFor(observer) {
  if (!observer || !HEX64.test(observer)) return new Map();
  if (cache.has(observer)) return cache.get(observer);
  const running = inFlight.get(observer);
  if (running) return running;
  const read = (async () => {
    let answered = false, map = null;
    try {
      const conn = await refConn();
      // 10040 is replaceable, so `limit: 1` cannot hand back a superseded Map.
      const evs = await conn.req({ kinds: [MAP_KIND], authors: [observer], limit: 1 });
      answered = evs.complete === true;
      map = evs[0] || null;
    } catch (e) { answered = false; }
    const found = delegationsOf(map);
    if (answered) cache.set(observer, found);
    return found;
  })();
  // Cleared whatever happened, so an uncached incomplete read cannot stand in
  // for the cache entry it was kept out of.
  inFlight.set(observer, read);
  read.then(() => inFlight.delete(observer), () => inFlight.delete(observer));
  return read;
}

/**
 * The delegations already in hand for [observer], or null when none are.
 * Synchronous, so a caller deciding mid-render can tell "delegates nobody"
 * from "not known yet".
 */
export function knownProviders(observer) {
  return cache.get(observer) || null;
}

/**
 * File a Map somebody else already read, so this module never asks for it.
 * [complete] is the caller's `evs.complete`: a null map off a finished read
 * means "delegates nobody", off a timed-out one it is a gap and must not seed.
 */
export function seedProviders(observer, map, complete) {
  if (!observer || !HEX64.test(observer) || complete !== true) return;
  cache.set(observer, delegationsOf(map));
}

/** Forget every Map read. For tests, and for a sign-out that must not leave one reader's delegations behind. */
export function forgetProviders() {
  cache.clear();
  inFlight.clear();
}
