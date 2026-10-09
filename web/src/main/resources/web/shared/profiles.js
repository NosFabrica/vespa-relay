// Author profiles: the kind-0 cache and its batched enrichment REQ. One live Map for the
// whole page, so every view reads the same names and faces.

import { refConn } from "./conn.js";
import { shortNpub } from "./nip19.js";

export const profiles = new Map(); // pubkey -> {name, display_name, picture, nip05, about, website, lud16}

// Kind-0 content is anyone's JSON: a field that is not a string reads as absent.
const str = (v) => (typeof v === "string" ? v : "");

/** The one name to show: `display_name`, else `name`; a whitespace value falls through. */
export const displayName = (p) => (p && (str(p.display_name).trim() || str(p.name).trim())) || "";

export function parseProfile(ev) {
  let c = {};
  try { c = JSON.parse(ev.content); } catch (e) {}
  if (!c || typeof c !== "object") c = {};
  return {
    name: str(c.name) || str(c.username),
    display_name: str(c.display_name) || str(c.displayName),
    picture: str(c.picture),
    nip05: str(c.nip05),
    about: str(c.about),
    website: str(c.website),
    lud16: str(c.lud16),
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
 * Load the uncached profiles among [pubkeys]; returns how many it learned, so a caller
 * knows whether to repaint.
 */
export async function enrichProfiles(pubkeys) {
  const missing = [...new Set(pubkeys)].filter(p => p && !profiles.has(p));
  if (!missing.length) return 0;
  let asked = false;
  try {
    // Anonymous: the authenticated socket gates kind 0 to authors the reader has scored.
    const conn = await refConn();
    const found = await conn.req({ kinds: [0], authors: missing, limit: missing.length }, 5000);
    seedProfiles(found);
    // Only an EOSE is an answer; req() resolves with whatever arrived at its timeout.
    asked = found.complete === true;
  } catch (e) { asked = false; }
  // "No profile" is cached only when the relay answered.
  const learned = missing.filter((p) => profiles.get(p)).length;
  if (asked) for (const p of missing) if (!profiles.has(p)) profiles.set(p, null);
  return learned;
}

export function authorOf(ev) {
  const p = profiles.get(ev.pubkey);
  const name = displayName(p) || shortNpub(ev.pubkey);
  return { name, picture: (p && p.picture) || "" };
}
