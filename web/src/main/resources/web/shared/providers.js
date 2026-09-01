// WHOSE WORD THIS READER TOOK — the publishers a kind 10040 delegates, by kind.
//
// A NIP-85 assertion or a Tapestry Trusted List means nothing on its own:
// anyone may sign one about anyone. What makes it worth drawing is that THIS
// reader's Map named its signer for that kind. Until now the page never had to
// know who those signers were, because the relay applied the rule for us — a
// declaration only reached a search result if the reader's 10040 delegated its
// signer, so "it arrived" WAS the verdict (web/provenance.js said exactly
// that). Once the page fetches declarations itself, off an anonymous socket
// that gates nothing, the rule has to be re-imposed here as an `authors`
// filter. This module is the one place that answers "which keys".
//
// TWO SHAPES, AND THE SECOND IS THE ONE THAT BITES:
//
//     ["30382:rank", <64-hex service>, <relay>]   NIP-85's ServiceProviderTag
//     ["30392",      <64-hex service>, <relay>]   a bare kind, the Tapestry ADR's
//
// A bare kind carries no `:`, so quartz's `ServiceProviderTag.parse` has never
// matched one and a reader whose Map delegates lists that way resolves to the
// empty set — silently, with no error anywhere, and every list pill quietly
// gone. That is not a hypothetical: relay's ObserverTrustListIT exists for a
// production Map with exactly that shape, and the one measured here on
// 2026-09-01 (search-staging, the reader in the export) carries BOTH —
// `30382:rank` and `30382:followers` naming one service, and a bare `30392`
// naming another. Any reader of a Map must take both or it takes half.
//
// EVERY PUBLISHER FOR A KIND, NEVER THE FIRST. TrustNotice.kt records what
// reading only the first cost: "a reader whose SECOND provider is fully
// mirrored was told their scores were missing, on every login, forever." A
// NIP-01 `authors` list is an OR, so all of them is the same one round trip.
//
// No DOM, and the parse is pure so providers.test.mjs can hold it; only
// [providersFor] touches a socket.

import { refConn } from "./conn.js";

/** A key a Map may delegate to: 64 lowercase hex, the only thing an `authors` filter takes. */
const HEX64 = /^[0-9a-f]{64}$/;

/** NIP-85's Map — the event this whole module reads. */
export const MAP_KIND = 10040;

/**
 * The delegations a Map states, keyed by the ENTRY AS WRITTEN — `30382:rank`,
 * `30382:followers`, `30392`.
 *
 * Lossless on purpose, and the dimension is why. A Map naming one service for
 * `30382:rank` and another for `30382:followers` has said two different
 * things: a followers service orders a set and cannot rank one (TrustNotice.kt
 * refuses to treat it as a rank service for exactly that reason), so a reader
 * that collapsed the two here would fill a trust chip off a service that never
 * claimed to measure trust. Callers that legitimately want the whole kind —
 * "may this signer's 30382s be drawn at all" — ask [publishersOf], which does
 * the union in the one place it is correct.
 *
 * Deduped per entry and ORDER-PRESERVING: a relay is free to answer a
 * duplicated `authors` entry twice, and provenance.js counts records.
 *
 * A tag whose second element is not a key is DROPPED rather than passed on: an
 * `authors` entry the relay cannot match narrows nothing, but it is also the
 * shape a malformed Map takes, and carrying it forward turns "my Map is
 * broken" into "this relay has no lists".
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

/**
 * Every publisher [delegations] names for [kind], across all its dimensions.
 *
 * The union is right for the question the provenance row asks — the reader
 * named this signer for this kind, so its declarations of that kind are ones
 * they asked for — and wrong for the question a score chip asks, which is
 * about one dimension. Both callers exist, so the collapse is a function
 * rather than the storage shape.
 *
 * ALL OF THEM, never the first. TrustNotice.kt records what reading only the
 * first cost: "a reader whose SECOND provider is fully mirrored was told their
 * scores were missing, on every login, forever."
 */
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

// observer pubkey -> the read that will settle it, while one is in flight.
//
// DEDUPE THE READ, NOT JUST THE ANSWER — the same distinction refConn() draws
// about the socket, for the same reason, and it bites here on every search
// rather than only on page load. Both callers fire off one render:
// `hydrate` starts the provenance lookup and the render that follows it paints
// the score chips, so the second arrives while the first is still on the wire,
// finds an empty cache, and asks for the same replaceable event again. Two
// REQs for one 10040, measured, on every search.
const inFlight = new Map();

/**
 * The delegations [observer] states, from their Map — cached, one read.
 *
 * Every caller that needs a Map goes through here, which is the point: the
 * page reads a 10040 for the score chips and again for the provenance row, and
 * two independent reads of one replaceable event is a round trip nobody asked
 * for and two caches to get the completeness rule wrong in.
 *
 * CACHED ONLY WHEN THE RELAY ANSWERED, which is this page's oldest hard-won
 * rule and the one it has now got wrong in three separate caches (profiles.js,
 * the score chips, rankServiceOf). An empty answer off a dropped read records
 * "this reader delegates nobody", nothing ever re-asks, and the whole pill row
 * is gone for the session with nothing on screen to say so.
 *
 * ANONYMOUS, down the reference socket. A Map is a fact about its author, and
 * asking on the trust-gated socket would answer it only for readers the reader
 * already trusts — circular in exactly the way the score chip documents.
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
      // 10040 is replaceable, so the store holds one per author and `limit: 1`
      // cannot hand back a superseded Map.
      const evs = await conn.req({ kinds: [MAP_KIND], authors: [observer], limit: 1 });
      answered = evs.complete === true;
      map = evs[0] || null;
    } catch (e) { answered = false; }
    const found = delegationsOf(map);
    if (answered) cache.set(observer, found);
    return found;
  })();
  // Cleared whatever happened, and that is what keeps the completeness rule
  // intact: an incomplete read is not cached, so the entry must go too or it
  // would stand in for the cache it was deliberately kept out of — a dropped
  // read made permanent by the back door.
  inFlight.set(observer, read);
  read.then(() => inFlight.delete(observer), () => inFlight.delete(observer));
  return read;
}

/**
 * The delegations already in hand for [observer], or null when none are.
 *
 * SYNCHRONOUS, and the null is the point: a caller that must decide RIGHT NOW
 * whether a signer is one this reader delegated cannot await, and must be able
 * to tell "delegates nobody" from "not known yet". provenance.js's first seed
 * is that caller — it runs before the render, and after the readiness panel has
 * usually already filed the Map (see [seedProviders]).
 */
export function knownProviders(observer) {
  return cache.get(observer) || null;
}

/**
 * File a Map somebody else already read, so this module never asks for it.
 *
 * THE PRELOAD, and it costs nothing because the read exists either way: the
 * readiness panel asks the reader's own kind 0, 10002 and 10040 in one
 * anonymous REQ the moment a sign-in lands, well before the first search. Left
 * unshared, that event was fetched a second time here a few hundred
 * milliseconds later for the same reader — the same replaceable event, off the
 * same socket, parsed for the same tags.
 *
 * [complete] is the caller's `evs.complete`, and it is required rather than
 * assumed: a `null` map off a FINISHED read is the relay stating this reader
 * delegates nobody, and off a timed-out one it is a gap. Seeding the second as
 * the first is the poisoning bug wearing a different hat, so a caller that
 * cannot vouch for its read must not seed.
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
