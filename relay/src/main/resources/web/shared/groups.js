// Which NIP-29 group somebody means by "chachi" — the decision, with no DOM
// and no relay in it.
//
// A group is the one subject in this search box whose NAME is not its filter.
// A hashtag is the word it means; a group is an opaque id its host relay
// minted (`chachi`, `TgvyRQ`, a hex blob), and nobody remembers those. So the
// box has to offer options, and offering them means answering two questions
// this module keeps carefully apart:
//
//   WHICH GROUPS EXIST — the store's kind 39000s, whose `name` and `about` are
//   full-text indexed by the relay, so a half-typed name is an ordinary NIP-50
//   search. What comes back is ranked by the relay and that order is KEPT here:
//   re-sorting it on substring position would be this module second-guessing
//   the thing that actually read the corpus.
//
//   WHICH GROUPS ARE YOURS — the reader's own NIP-51 kind 10009, whose `group`
//   tags are `["group", <id>, <relay url>, <name?>]`. That is the only place in
//   the protocol where an id, its host relay and a name arrive together, which
//   makes it both the best-known answer and the ONLY one that can print a url.
//   Half of it can be LOCKED: a 10009 is a private-tag event, so items may sit
//   NIP-44-encrypted in `.content` (self-encrypted, to the author's own key)
//   instead of in the tags. [sealed] spots that, [privateGroups] reads it back
//   once somebody with a signer has decrypted it, and the asking lives in the
//   page because it costs a permission prompt and this module holds no state.
//
// THE TWO ARE NEVER MERGED, and that is the whole design rather than a gap in
// it. A group's identity is the pair (id, host relay) — quartz's `GroupId`
// says so, and its `GroupTag.equals` keys on exactly that pair while treating
// the name as cosmetic. A 10009 tag names the host as a URL; a 39000 names it
// as the PUBKEY that signed it, because in NIP-29 the relay signs its own
// groups' metadata. Nothing in this store joins a relay pubkey to a relay url
// — there is no per-relay provenance here at all, and NIP-66's records carry
// no such tag — so a 10009 row and a 39000 row that share an id are two
// statements about possibly-different groups, and folding them together would
// print one host's url under another host's name. They stay two rows, each
// saying where it came from, and [rank] marks both `ambiguous`.
//
// That ambiguity is real and is not this module's to fix: an `h` tag carries
// the bare id, so two relays with a `general` are one `#h` filter no matter
// which row the reader picked. What the picker fixes is the human's half of
// the problem — remembering the id — and what `ambiguous` does is refuse to
// pretend it fixed the other half.

/** A relay url as a row shows it: no scheme, no trailing slash, still exact. */
export const relayLabel = (url) => String(url || "").replace(/^wss?:\/\//i, "").replace(/\/+$/, "");

/** A tag's value at [i], trimmed to nothing when it is absent or blank. */
const at = (tag, i) => (tag.length > i ? String(tag[i] || "").trim() : "");

// The identity of a candidate, as one string: the (id, host) pair a group IS.
// Joined with `\u0000` — written as the ESCAPE, never as the byte, which is
// the rule searchfield.js's chipFace pays for at length. The character itself
// is the right one for the same reason it is right there: a relay url and a
// group id are both free-form, and any printable separator is a character one
// of them may legitimately contain, which would collide two distinct groups
// onto one key.
const idKey = (id, host) => `${id}\u0000${host}`;

/**
 * The `group` tags of one kind-10009, as candidates.
 *
 * PUBLIC tags only. A 10009 is a NIP-51 private-tag event: its `.content` can
 * hold NIP-44-encrypted items alongside them, and reading those needs a
 * `nip44.decrypt` this page does not have (it is optional in NIP-07 and used
 * nowhere in this codebase). A partial list is a fine thing to offer and a
 * terrible thing to present as complete, so the caller is told how many rows
 * it got and says "your public groups" — it never claims these are all of them.
 *
 * An entry with no id or no relay url is dropped rather than half-drawn:
 * quartz's own parser demands both (`GroupTag.parse` requires elements 1 and
 * 2), and a group tag missing its host is a row that cannot say where it is.
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
 * Is this ciphertext NIP-04 rather than NIP-44?
 *
 * quartz's `EncryptedInfo.isNIP04`, ported rule for rule, because the two
 * schemes are told apart by SHAPE and nothing else — a 10009 records which
 * scheme it used nowhere. NIP-04 appends `?iv=<24 base64 chars>`, so the
 * marker sits exactly 28 characters from the end; a NIP-44 payload is one
 * base64 blob with no such tail. The `-null` strip is quartz's too, and it is
 * there for a client bug that shipped: without it those events read as NIP-44
 * and fail to decrypt with an error about the wrong scheme entirely.
 *
 * Getting this backwards is not a silent failure — the extension refuses —
 * but it IS a wasted permission prompt, which is the one cost this whole
 * feature is trying not to pay twice.
 */
export function isNip04(encoded) {
  const s = String(encoded || "").replace(/-null$/, "");
  return s.length >= 28 && s.slice(-28, -24) === "?iv=";
}

/**
 * The locked half of a kind-10009, or null when there is nothing locked.
 *
 *   { content, scheme: "nip04" | "nip44" }
 *
 * NON-EMPTY CONTENT IS NOT PROOF THERE IS ANYTHING IN THERE, and that is worth
 * stating because it is the one thing this cannot decide before asking. An
 * EMPTY private list encrypts the empty STRING rather than `[]` (quartz's
 * `PrivateTagsInContent.encryptNip44`), so a reader who once had a private
 * group and removed it carries a perfectly valid ciphertext whose plaintext is
 * nothing at all. The only way to know is to decrypt, and decrypting is what
 * costs a permission prompt — so the honest reading of "has something
 * encrypted" is "has an encrypted payload", and [privateGroups] handles the
 * empty case by returning no rows rather than by pretending it never asked.
 */
export function sealed(ev) {
  const content = String((ev && ev.content) || "").trim();
  if (!content) return null;
  return { content, scheme: isNip04(content) ? "nip04" : "nip44" };
}

/**
 * The groups inside a DECRYPTED 10009 payload.
 *
 * The plaintext is a JSON array of tag arrays — the same shape as the event's
 * public `tags` — so this is [ownGroups] over it, and every rule that function
 * states (both halves of the pair required, deduped on the pair, name
 * cosmetic) applies unchanged. `secret` marks where the row came from, because
 * a reader who unlocked their list should be able to see which of these the
 * network could already read.
 *
 * Three shapes come back as no groups rather than as an error, and they are
 * ordinary rather than corrupt: the empty string an empty private list
 * encrypts to, a payload that is not JSON at all (an nip04/nip44 mix-up
 * decrypting to gibberish), and JSON that is not an array of tags.
 */
export function privateGroups(plaintext) {
  const t = String(plaintext || "").trim();
  if (!t) return [];
  let tags;
  try { tags = JSON.parse(t); } catch (e) { return []; }
  if (!Array.isArray(tags)) return [];
  return ownGroups({ tags }).map((g) => ({ ...g, secret: true }));
}

/** One kind-39000 as a candidate: its `d` is the id, its AUTHOR is the host. */
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
 * Does one of the reader's OWN groups answer what has been typed so far?
 *
 * Only ever asked of the local list, and that restriction is the whole point.
 * A 10009 arrives whole and unsearched — nothing has decided which of its
 * entries the reader meant — so something has to match here, and a substring
 * over the id and the cached name is the honest amount of matching to do on
 * four entries with no index behind them.
 *
 * The relay's rows are NOT put through this. They are here BECAUSE the relay
 * matched them, against a real index: `name` in the primary search tier,
 * `about` in the secondary, both reachable through the prefix/fuzzy `near`
 * column. Re-testing them with `includes` discards every hit that index can
 * make and this cannot — an `about`-only match, a prefix that is not a
 * substring, a typo the near tier forgave — and it discards them silently, as
 * "No group matches". Measured on the real module: `about` mentioning bitcoin
 * → 0 rows, "alices" against "Alice's Club" → 0 rows.
 */
const hit = (cand, needle) =>
  !needle ||
  cand.id.toLowerCase().includes(needle) ||
  (cand.name || "").toLowerCase().includes(needle);

/**
 * The rows to offer for a half-typed `group:`, best first.
 *
 * Three bands, and the order between them is the argument:
 *
 *   1. An EXACT id, however it got here. Somebody who typed or pasted a whole
 *      id has named one group, and the picker's job at that point is to get out
 *      of the way — which it does by putting that row under Enter. Compared
 *      case-SENSITIVELY, because that is how the store matches an `h` tag and
 *      how [parseQuery] asks for one; `General` and `general` are two groups.
 *   2. YOUR groups. The reader's own list is the best answer available: it is
 *      short, they chose it, its names are the ones they know, and it is the
 *      only band that can print a relay url.
 *   3. Everything else the relay found, IN THE RELAY'S OWN ORDER and ENTIRELY.
 *      That ranking already reflects the corpus and the reader's lens;
 *      re-sorting it here on where a substring landed would replace a
 *      measurement with a guess, and re-FILTERING it — which this did until it
 *      was measured — throws away every hit the index can make and `includes`
 *      cannot. See [hit], which is deliberately asked only of band 2.
 *
 * With nothing typed yet the first two bands are empty of matches by
 * definition, so what comes back is the reader's whole list — which is the
 * behaviour worth having: `group:` alone opens on your own groups.
 *
 * `ambiguous` is set on every row sharing its id with another row. It is the
 * one thing the reader cannot see for themselves and must know: picking either
 * row writes the same `#h` filter, so the results are the union whatever they
 * chose.
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
  // Band 1 keeps own-before-meta within it for the same reason band 2 exists
  // at all: two rows can carry the id that was typed, and yours is the one you
  // meant.
  for (const c of all) if (typed && c.id === typed) take(c);
  for (const c of own) if (hit(c, needle)) take(c);
  for (const c of meta) take(c);

  const times = new Map();
  for (const c of rows) times.set(c.id, (times.get(c.id) || 0) + 1);
  return rows.map((c) => ({ ...c, ambiguous: times.get(c.id) > 1 }));
}

/**
 * What a row says about WHERE it is, and how sure that is.
 *
 *   { text, exact }
 *
 * `exact` marks the only answer this page can stand behind: a url that came
 * out of a `group` tag, where the protocol wrote the host down. Everything
 * else is the host relay's signing key, and the caller may well be able to
 * draw a name for it (a relay that publishes its own kind 0 — this one does)
 * — but a name for a key is a claim the key made about itself, not the same
 * kind of fact, and the two must not read alike.
 *
 * `hostName` is that name when the caller has one, and is asked for rather
 * than looked up here because this module holds no caches.
 */
export function where(cand, hostName = "") {
  if (cand.relayUrl) return { text: relayLabel(cand.relayUrl), exact: true };
  const named = String(hostName || "").trim();
  if (named) return { text: named, exact: false };
  return { text: cand.host ? `relay ${cand.host.slice(0, 8)}…` : "unknown relay", exact: false };
}
