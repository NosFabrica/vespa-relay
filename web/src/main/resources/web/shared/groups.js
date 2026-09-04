// Which NIP-29 group somebody means by `group:chachi`, with no DOM and no
// relay in it. A group id is opaque and minted by its host relay, so the box
// offers rows from two sources that are kept apart: the store's kind 39000s,
// ranked by the relay and kept in that order, and the reader's own kind 10009,
// whose `group` tags are the only place an id, its host url and a name arrive
// together. A group's identity is the pair (id, host relay); a 10009 names the
// host as a url and a 39000 as a signing key, and nothing here joins the two,
// so rows sharing an id stay separate and [rank] marks them `ambiguous`.

/** A relay url as a row shows it: no scheme, no trailing slash, still exact. */
export const relayLabel = (url) => String(url || "").replace(/^wss?:\/\//i, "").replace(/\/+$/, "");

/** A tag's value at [i], trimmed to nothing when it is absent or blank. */
const at = (tag, i) => (tag.length > i ? String(tag[i] || "").trim() : "");

// The (id, host) pair as one key. `\u0000` because both halves are free-form
// and any printable separator could appear in one of them; written as the
// escape, never the byte (searchfield.js's chipFace has the rule).
const idKey = (id, host) => `${id}\u0000${host}`;

/**
 * The `group` tags of one kind-10009, as candidates. Public tags only: the
 * encrypted half is [sealed] and [privateGroups]. An entry missing its id or
 * host url is dropped, as quartz's `GroupTag.parse` requires both.
 */
export function ownGroups(ev) {
  const out = [];
  const seen = new Set();
  for (const tag of (ev && ev.tags) || []) {
    if (!Array.isArray(tag) || tag[0] !== "group") continue;
    const id = at(tag, 1);
    const relayUrl = at(tag, 2);
    if (!id || !relayUrl) continue;
    const key = idKey(id, relayUrl);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({ id, relayUrl, host: null, name: at(tag, 3), about: "", picture: "", mine: true });
  }
  return out;
}

/**
 * Is this ciphertext NIP-04 rather than NIP-44? quartz's `EncryptedInfo.isNIP04`
 * rule for rule: NIP-04 appends `?iv=<24 base64 chars>`, so the marker sits 28
 * characters from the end. The `-null` strip covers a client bug that shipped.
 */
export function isNip04(encoded) {
  const s = String(encoded || "").replace(/-null$/, "");
  return s.length >= 28 && s.slice(-28, -24) === "?iv=";
}

/**
 * The locked half of a kind-10009 as `{ content, scheme }`, or null. A
 * non-empty payload is not proof there is anything inside: an empty private
 * list encrypts the empty string, and only decrypting can tell.
 */
export function sealed(ev) {
  const content = String((ev && ev.content) || "").trim();
  if (!content) return null;
  return { content, scheme: isNip04(content) ? "nip04" : "nip44" };
}

/**
 * The groups inside a decrypted 10009 payload: [ownGroups] over the tag array
 * it holds, marked `secret`. The empty string, non-JSON (a scheme mix-up) and
 * JSON that is not a tag array are all no groups rather than an error.
 */
export function privateGroups(plaintext) {
  const t = String(plaintext || "").trim();
  if (!t) return [];
  let tags;
  try { tags = JSON.parse(t); } catch (e) { return []; }
  if (!Array.isArray(tags)) return [];
  return ownGroups({ tags }).map((g) => ({ ...g, secret: true }));
}

/**
 * The group an event was posted to, NIP-29's `h` tag, or "". The bare id only:
 * the host relay is nowhere in the message, and naming it is groupnames.js's job.
 */
export const postedTo = (ev) => {
  const tag = ((ev && ev.tags) || []).find((t) => Array.isArray(t) && t[0] === "h");
  return tag ? at(tag, 1) : "";
};

/** One kind-39000 as a candidate: its `d` is the id, its author is the host. */
export function metaGroup(ev) {
  if (!ev || ev.kind !== 39000) return null;
  const tags = ev.tags || [];
  const first = (name) => {
    const t = tags.find((x) => Array.isArray(x) && x[0] === name);
    return t ? at(t, 1) : "";
  };
  const id = first("d");
  if (!id) return null;
  return {
    id,
    relayUrl: null,
    host: ev.pubkey || null,
    name: first("name"),
    about: first("about"),
    picture: first("picture"),
    mine: false,
  };
}

/**
 * Does one of the reader's own groups answer what has been typed? Asked only
 * of the local list, which arrives whole and unsearched. The relay's rows are
 * never put through this: they are here because an index matched them, and a
 * substring test would silently discard the hits it cannot make.
 */
const hit = (cand, needle) =>
  !needle ||
  cand.id.toLowerCase().includes(needle) ||
  (cand.name || "").toLowerCase().includes(needle);

/**
 * The rows to offer for a half-typed `group:`, best first: an exact id
 * (case-sensitive, as the store matches an `h` tag), then the reader's own
 * groups that [hit], then everything the relay found in the relay's own order.
 * With nothing typed the result is the reader's whole list. `ambiguous` marks
 * every row sharing its id with another, since either writes the same `#h`.
 */
export function rank(partial, { own = [], meta = [] } = {}) {
  const typed = String(partial ?? "");
  const needle = typed.toLowerCase();
  const rows = [];
  const seen = new Set();
  const key = (c) => idKey(c.id, c.mine ? c.relayUrl : c.host);
  const take = (c) => {
    const k = key(c);
    if (seen.has(k)) return;
    seen.add(k);
    rows.push(c);
  };
  const all = [...own, ...meta];
  // Own before meta within the exact band too: two rows can carry the typed
  // id, and yours is the one you meant.
  for (const c of all) if (typed && c.id === typed) take(c);
  for (const c of own) if (hit(c, needle)) take(c);
  for (const c of meta) take(c);

  const times = new Map();
  for (const c of rows) times.set(c.id, (times.get(c.id) || 0) + 1);
  return rows.map((c) => ({ ...c, ambiguous: times.get(c.id) > 1 }));
}

/**
 * What a row says about where it is, as `{ text, exact }`. `exact` only for a
 * url out of a `group` tag; a name for the host's signing key is a claim the
 * key made about itself, and [hostName] is passed in because this module
 * holds no caches.
 */
export function where(cand, hostName = "") {
  if (cand.relayUrl) return { text: relayLabel(cand.relayUrl), exact: true };
  const named = String(hostName || "").trim();
  if (named) return { text: named, exact: false };
  return { text: cand.host ? `relay ${cand.host.slice(0, 8)}…` : "unknown relay", exact: false };
}
