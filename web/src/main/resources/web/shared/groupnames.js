// What a NIP-29 group is called: the kind-39000 cache and the batched REQ that fills it, for
// the `group:<id>` pill and a chat card's room pill. A group is the pair (id, host relay) while
// the pill carries the bare id, so an id is named only where the answers agree: the reader's
// own kind 10009 wins outright, else every 39000 must say the same name.

import { refConn } from "./conn.js";
import { metaGroup } from "./groups.js";

// id -> Map(host key -> { name, mine, secret }). The inner key is a relay url out of a 10009
// or a signing key out of a 39000, and the two spellings are never joined (groups.js).
const learned = new Map();

// Ids a relay has answered for. Only an EOSE adds one; a dropped read leaves the id askable.
const asked = new Set();

/** Which host a candidate names, in whichever of the two spellings it has. */
const hostKey = (cand) => cand.relayUrl || cand.host || "";

/** The one name a set of rows agrees on, or "" where they do not agree. */
const agreed = (rows) => {
  const names = new Set(rows.map((r) => r.name));
  return names.size === 1 ? [...names][0] : "";
};

/**
 * Record what these candidates (groups.js's shape) say they are called; a nameless row is
 * skipped. Returns how many ids draw differently now, not how many rows were recorded.
 */
export function seedGroupNames(cands) {
  const was = new Map();
  for (const c of cands || []) {
    if (!c || !c.id) continue;
    const name = String(c.name || "").trim();
    if (!name) continue;
    if (!was.has(c.id)) was.set(c.id, groupName(c.id));
    const by = learned.get(c.id) || new Map();
    by.set(hostKey(c), { name, mine: !!c.mine, secret: !!c.secret });
    learned.set(c.id, by);
  }
  let changed = 0;
  for (const [id, before] of was) if (groupName(id) !== before) changed++;
  return changed;
}

/** The same, from the kind 39000s among raw events. */
export const seedGroupEvents = (events) =>
  seedGroupNames((events || []).map(metaGroup).filter(Boolean));

/** Drop every name that came out of a reader's encrypted group list; public names stay. */
export function forgetPrivateGroupNames() {
  for (const [id, by] of learned) {
    for (const [host, row] of by) if (row.secret) by.delete(host);
    if (!by.size) learned.delete(id);
  }
}

/** What to draw for this id, or "" when nothing can be said with one name. */
export function groupName(id) {
  const by = learned.get(id);
  if (!by) return "";
  const rows = [...by.values()];
  return agreed(rows.filter((r) => r.mine)) || agreed(rows.filter((r) => !r.mine));
}

/** Has this id been answered, by a lookup or by a row that named it? */
export const knowsGroup = (id) => learned.has(id) || asked.has(id);

const HOSTS_PER_ID = 8;

/**
 * Learn the names of the ids not known yet, on the anonymous socket; returns how many now
 * have a name to draw, so a caller knows whether to repaint.
 */
export async function enrichGroupNames(ids) {
  const missing = [...new Set(ids)].filter((id) => id && !knowsGroup(id));
  if (!missing.length) return 0;
  try {
    const conn = await refConn();
    const found = await conn.req(
      { kinds: [39000], "#d": missing, limit: missing.length * HOSTS_PER_ID },
      5000,
    );
    seedGroupNames(found.map(metaGroup).filter(Boolean));
    // Only an EOSE says the relay has never heard of the group.
    if (found.complete === true) for (const id of missing) asked.add(id);
  } catch (e) { /* a dropped read states nothing, and leaves the id askable */ }
  return missing.filter((id) => groupName(id)).length;
}
