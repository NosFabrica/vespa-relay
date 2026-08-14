// What a NIP-29 group is CALLED — the kind-39000 cache, and the batched REQ
// that fills it. For the one place on the page that holds a group id with no
// row beside it to read a name off: the `group:<id>` pill in the search box.
//
// profiles.js's shape, and deliberately so — one cache the whole page shares,
// a lookup that reports how many names it LEARNED so a caller only repaints
// when repainting would change something, and the same anonymous connection
// for the same reason (below). What it cannot borrow is profiles.js's key, and
// that is the whole of the design here.
//
// A GROUP IS THE PAIR (id, host relay) — shared/groups.js is the long version
// — while an `h` filter, the URL, the export and this pill all carry the bare
// id. So "what is this id called" is a question that can have more than one
// answer, and this module answers it only where the answers AGREE:
//
//   - The reader's OWN kind 10009 wins outright where it names the id. It is
//     what they call the group, it is the name the picker offered them, and a
//     group they listed is one they can be assumed to have meant.
//   - Failing that, the corpus's 39000s have to agree. Two relays that each
//     signed a group with this id under DIFFERENT names is exactly the case
//     groups.js refuses to merge: the search returns both, so naming the pill
//     after either would tell the reader the results are about one group when
//     they are about two. The id stands, which is what it did before this
//     module existed.
//
// A name here is never a second source of truth. The field's VALUE stays
// `group:<id>` — the token, the url and the export are untouched — the pill's
// hover carries the raw token and the id, and the name goes away the moment
// the id is edited by hand. That is the person chip's bargain exactly: 63
// characters of bech32 drawn as a face and a name over a value that is still
// `from:npub1…`. A group has more claim on it than anybody, since a group id is
// the one filter value in this box that NOBODY can read.

import { refConn } from "./conn.js";
import { metaGroup } from "./groups.js";

// id -> Map(host key -> { name, mine }). Two levels because one id can be two
// groups. The inner key is the other half of the pair, spelled the way its
// source spells it — a relay url out of a 10009 `group` tag, a signing key out
// of a 39000 — and those two spellings are never joined here, for the reason
// groups.js gives at length: nothing in this store maps a relay's key to its
// url, so folding them would print one host's name over another host's group.
const learned = new Map();

// The ids a relay has ANSWERED for, whether or not it had anything to say.
// profiles.js's rule and its scar: a "nothing known" recorded off a read that
// never finished is an absence the relay never stated, and this cache is read
// before every draw. So a dropped or timed-out lookup leaves the id askable,
// and only an EOSE stops the next render asking again.
const asked = new Set();

/** Which host a candidate names, in whichever of the two spellings it has. */
const hostKey = (cand) => cand.relayUrl || cand.host || "";

/** The one name a set of rows agrees on, or "" where they do not agree. */
const agreed = (rows) => {
  const names = new Set(rows.map((r) => r.name));
  return names.size === 1 ? [...names][0] : "";
};

/**
 * Record what these candidates say they are called.
 *
 * Takes groups.js's candidate shape — `{ id, name, host | relayUrl, mine }` —
 * so both sources feed one cache: the picker's rows (your list, and the
 * corpus's answers to a name search) and any 39000 that arrived in a search.
 * A row with no name teaches nothing and is skipped rather than recorded as a
 * blank, which would otherwise be a "name" for the id to disagree with.
 *
 * Returns how many ids DRAW differently now, which is not the same as how many
 * rows were recorded: a second host confirming a name changes nothing, and a
 * second host contradicting one takes a name away. Either way it is what a
 * caller that has already painted a pill needs, and for the same reason
 * enrichProfiles reports its own count — a seed that changed nothing must not
 * cost a repaint.
 */
export function seedGroupNames(cands) {
  const was = new Map();
  for (const c of cands || []) {
    if (!c || !c.id) continue;
    const name = String(c.name || "").trim();
    if (!name) continue;
    if (!was.has(c.id)) was.set(c.id, groupName(c.id));
    const by = learned.get(c.id) || new Map();
    by.set(hostKey(c), { name, mine: !!c.mine });
    learned.set(c.id, by);
  }
  let changed = 0;
  for (const [id, before] of was) if (groupName(id) !== before) changed++;
  return changed;
}

/**
 * The same, from raw events — the kind 39000s among them.
 *
 * seedProfiles' shape, and it earns its place the same way: a `group:` search
 * ALREADY asks for the group's own metadata beside its posts (query.js's
 * buildFilters sends `#d` alongside the `#h`), so the answer to "what is this
 * called" is usually sitting in the results the reader just got.
 */
export const seedGroupEvents = (events) =>
  seedGroupNames((events || []).map(metaGroup).filter(Boolean));

/** What to draw for this id, or "" when nothing can be said with one name. */
export function groupName(id) {
  const by = learned.get(id);
  if (!by) return "";
  const rows = [...by.values()];
  return agreed(rows.filter((r) => r.mine)) || agreed(rows.filter((r) => !r.mine));
}

/** Has this id been answered — by a lookup, or by a row that named it? */
export const knowsGroup = (id) => learned.has(id) || asked.has(id);

// Room for several hosts per id, since one id signed by two relays is the case
// this module exists to notice. It is a bound rather than a guarantee: an id
// carried by more hosts than fit could come back agreeing when the corpus does
// not, and the pill would name it. A group id that popular is not a thing this
// store has seen, and the alternative — an unbounded read on every render — is
// a real cost against a guessed one.
const HOSTS_PER_ID = 8;

/**
 * Learn the names of the group ids that are not known yet.
 *
 * Returns how many of them ended up with a name to draw, so a caller that
 * rendered before the answer arrived knows whether repainting would change
 * anything — a lookup that learned nothing must not cost a second render.
 *
 * ANONYMOUS, and this is the one place its reasoning parts company with the
 * group PICKER's, which asks on the authenticated socket (app.js says why, and
 * groups.test.mjs pins it). The picker asks two questions about the reader —
 * which groups are yours, which should be offered first — and the trust-gated
 * socket is what makes those answers theirs. This asks what a group the reader
 * has ALREADY named is called, which is a fact about a subject, exactly like
 * the kind 0 behind a face: the store applies the observer as a filter, so
 * asking it there would leave a reader with no scores mirrored here staring at
 * the hex id they were staring at before.
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
    // EOSE, not merely "resolved": req() resolves with whatever arrived when
    // its timeout fires, and a slow read is not the relay saying it has never
    // heard of this group.
    if (found.complete === true) for (const id of missing) asked.add(id);
  } catch (e) { /* a dropped read states nothing, and leaves the id askable */ }
  return missing.filter((id) => groupName(id)).length;
}
