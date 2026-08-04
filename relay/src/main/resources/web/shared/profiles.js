// Author profiles: the kind-0 cache and its batched enrichment REQ. One cache
// for the whole page, exported as the live Map — every view reads the same
// names and faces.

import { refConn } from "./conn.js";
import { shortNpub } from "./nip19.js";

export const profiles = new Map(); // pubkey -> {name, display_name, picture, nip05, about, website, lud16}

/**
 * The one name to show when there is room for one.
 *
 * `display_name` first, `name` as the fallback — and blank means blank, so a
 * profile carrying `display_name: "   "` falls through instead of rendering a
 * gap. Three call sites had this the other way round and disagreed with the
 * rest of the page about what somebody is called.
 *
 * Where BOTH are shown — the profile card — this gives the primary and the
 * other is rendered beside it.
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

export async function enrichProfiles(pubkeys) {
  const missing = [...new Set(pubkeys)].filter(p => p && !profiles.has(p));
  if (!missing.length) return;
  let asked = false;
  try {
    // Anonymous, like every other reference lookup. A displayed author's name
    // and face are not a personalised question, and the authenticated socket
    // gates kind 0 to authors the reader has scored — so with "include spam"
    // on, results would render nameless and faceless for exactly the people
    // you have not rated.
    const conn = await refConn();
    const found = await conn.req({ kinds: [0], authors: missing, limit: missing.length }, 5000);
    seedProfiles(found);
    asked = true;
  } catch (e) { asked = false; }
  // Cache "no profile" ONLY when the relay actually answered. This used to
  // record null on failure too, so one dropped lookup meant that pubkey had
  // no face for the rest of the session — which is what made signing in
  // appear to need a page refresh: the very first attempt poisoned the entry
  // for your own key, and every later render read the poison.
  if (asked) for (const p of missing) if (!profiles.has(p)) profiles.set(p, null);
}

export function authorOf(ev) {
  const p = profiles.get(ev.pubkey);
  const name = displayName(p) || shortNpub(ev.pubkey);
  return { name, picture: (p && p.picture) || "" };
}
