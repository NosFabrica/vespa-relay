// The publishers a reader's kind 10040 delegates, by kind. A NIP-85 assertion or a Tapestry
// Trusted List is only drawn when this reader's Map named its signer for that kind, and the
// same rule is re-imposed as an `authors` filter on the anonymous socket. Two tag shapes are
// read: NIP-85's `["30382:rank", <service>, <relay>]` and the Tapestry ADR's bare `["30392", ...]`.

import { refConn } from "./conn.js";

/** A key a Map may delegate to: 64 lowercase hex, the only thing an `authors` filter takes. */
const HEX64 = /^[0-9a-f]{64}$/;

/** NIP-85's Map, the event this module reads. */
export const MAP_KIND = 10040;

/**
 * The delegations a Map states, keyed by the entry as written (`30382:rank`, `30392`) and
 * order-preserving. Dimensions are kept apart; callers wanting the whole kind use [publishersOf].
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

/** Every publisher [delegations] names for [kind], across all its dimensions. */
export function publishersOf(delegations, kind) {
  const out = [];
  for (const [entry, keys] of delegations || []) {
    if (Number.parseInt(entry.split(":")[0], 10) !== kind) continue;
    for (const k of keys) if (!out.includes(k)) out.push(k);
  }
  return out;
}

// observer pubkey -> the delegations their Map states, written only off a complete read.
const cache = new Map();

// observer pubkey -> the read that will settle it, so concurrent askers share one read.
const inFlight = new Map();

/**
 * The delegations [observer] states, from their Map, read once on the anonymous socket and
 * cached only when the relay answered completely.
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
  // Cleared whatever happened, so an incomplete read cannot stand in for a cache entry.
  inFlight.set(observer, read);
  read.then(() => inFlight.delete(observer), () => inFlight.delete(observer));
  return read;
}

/** The delegations already in hand for [observer], or null when not known yet. Synchronous. */
export function knownProviders(observer) {
  return cache.get(observer) || null;
}

/**
 * File a Map somebody else already read. [complete] is the caller's `evs.complete`; a null
 * map off a timed-out read is a gap and must not seed.
 */
export function seedProviders(observer, map, complete) {
  if (!observer || !HEX64.test(observer) || complete !== true) return;
  cache.set(observer, delegationsOf(map));
}

/** Forget every Map read; for tests and for sign-out. */
export function forgetProviders() {
  cache.clear();
  inFlight.clear();
}
