// Author profiles: the kind-0 cache and its batched enrichment REQ. One cache
// for the whole page, exported as the live Map, so every view reads the same
// names and faces.

import { refConn } from "./conn.js";
import { shortNpub } from "./nip19.js";

export const profiles = new Map(); // pubkey -> {name, display_name, picture, nip05, about, website, lud16}

/**
 * The one name to show when there is room for one: `display_name`, else
 * `name`, and blank means blank so a whitespace value falls through. Where
 * both are shown, this is the primary.
 */
export const displayName = (p) => (p && (p.display_name || "").trim()) || (p && (p.name || "").trim()) || "";

export function parseProfile(ev) {
  let c = {};
  try { c = JSON.parse(ev.content) || {}; } catch (e) {}
  return {
    name: c.name || c.username || "",
    display_name: c.display_name || c.displayName || "",
    picture: c.picture || "",
    nip05: c.nip05 || "",
    about: c.about || "",
    website: c.website || "",
    lud16: c.lud16 || "",
    created_at: ev.created_at,
  };
}

export function seedProfiles(events) {
  for (const ev of events) {
    if (ev.kind !== 0) continue;
    const known = profiles.get(ev.pubkey);
    if (!known || known.created_at <= ev.created_at) profiles.set(ev.pubkey, parseProfile(ev));
  }
}

/**
 * Load the profiles for [pubkeys] that are not cached yet; returns how many
 * new ones it learned, so a caller that rendered before the names arrived
 * knows whether repainting would change anything.
 */
export async function enrichProfiles(pubkeys) {
  const missing = [...new Set(pubkeys)].filter(p => p && !profiles.has(p));
  if (!missing.length) return 0;
  let asked = false;
  try {
    // Anonymous, like every reference lookup: the authenticated socket gates
    // kind 0 to authors the reader has scored, which would leave exactly the
    // unrated people nameless.
    const conn = await refConn();
    const found = await conn.req({ kinds: [0], authors: missing, limit: missing.length }, 5000);
    seedProfiles(found);
    // Only an EOSE is an answer; req() resolves with whatever arrived at its
    // timeout.
    asked = found.complete === true;
  } catch (e) { asked = false; }
  // "No profile" is cached only when the relay answered; a null recorded off
  // a failed read is read before every later render.
  const learned = missing.filter((p) => profiles.get(p)).length;
  if (asked) for (const p of missing) if (!profiles.has(p)) profiles.set(p, null);
  return learned;
}

export function authorOf(ev) {
  const p = profiles.get(ev.pubkey);
  const name = displayName(p) || shortNpub(ev.pubkey);
  return { name, picture: (p && p.picture) || "" };
}
