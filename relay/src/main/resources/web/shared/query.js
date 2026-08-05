// The search box's own small language, parsed in one place.
//
// Two prefixes narrow a search to people instead of words:
//
//     from:npub1…   the author       -> a NIP-01 `authors` filter
//     to:npub1…     who they named   -> a NIP-01 `#p` filter
//
// Both are NIP-01 filter fields, not NIP-50 extensions — the store never sees
// the prefixes at all. That is deliberate: `authors` and `#p` are indexed
// filters every relay implements, so narrowing by person costs nothing and
// composes with the trust ranking rather than competing with it.
//
// Pure functions over a string, with no DOM and no relay: the field renderer
// (searchfield.js) and the query builder (app.js) must agree exactly about
// where a token starts and ends, and they agree by both asking here. It is
// also the only part of the feature that can be tested without a browser, and
// tools/webtest/query.test.mjs does.

import { pubkeyParam } from "./nip19.js";

// An npub is a FIXED 63 characters — `npub1` plus 52 data words plus a 6-word
// checksum — over bech32's alphabet, which excludes `1`, `b`, `i` and `o`.
// Matching the exact shape rather than `npub1\w+` means the token boundary is
// known before any decoding, so "npub1…abc." ends at the `c` and the full stop
// stays punctuation.
const NPUB = "npub1[qpzry9x8gf2tvdw0s3jn54khce6mua7l]{58}";

// The lead group anchors the token to a word start, so a `to:` inside a url
// is not a filter. `i` because npubs get pasted out of clients that upcase
// them; pubkeyParam lowercases before decoding.
const TOKEN = new RegExp(`(^|\\s)((?:from|to):)?(${NPUB})(?![a-z0-9])`, "gi");
const WHOLE = new RegExp(`^${NPUB}$`, "i");

// The same two prefixes while they are still being TYPED: everything after the
// colon up to the caret, which is what the author picker searches for.
const PARTIAL = /(^|\s)(from|to):(\S*)$/i;

/**
 * Is this exactly one finished key — something [tokenize] will draw as a
 * person and [parseQuery] will lift into a filter?
 *
 * NPUB ONLY, deliberately, even though pubkeyParam also takes bare hex for the
 * `as=` url parameter. The two questions have to give the same answer or the
 * feature comes apart in the middle: mentionAt calls a token finished, the
 * picker gets out of the way — and if the tokenizer then declined to chip it,
 * `from:<64 hex>` would sit there looking like a filter while being searched
 * for as literal text. Hex pasted after `from:` stays UNfinished on purpose,
 * so the picker resolves it and writes the npub back.
 */
export const isKey = (v) => WHOLE.test(String(v ?? "").trim()) && !!pubkeyParam(v);

/**
 * The typed string as a list of segments: plain text, and the keys inside it.
 *
 *   { type: "text", text }
 *   { type: "key", raw, field: "from" | "to" | null, pubkey }
 *
 * `raw` is the token exactly as typed, so a renderer can put it back verbatim
 * and a caret measured in characters stays measured in characters.
 *
 * An npub whose CHECKSUM fails stays text. A corrupted identifier must not
 * become a chip naming a plausible stranger — the same rule nip19.js's decoder
 * states for entity pages, applied one layer up.
 */
export function tokenize(text) {
  const s = String(text ?? "");
  const out = [];
  let at = 0;
  TOKEN.lastIndex = 0;
  for (let m; (m = TOKEN.exec(s)); ) {
    const start = m.index + m[1].length;
    const pubkey = pubkeyParam(m[3]);
    if (!pubkey) continue;
    if (start > at) out.push({ type: "text", text: s.slice(at, start) });
    out.push({
      type: "key",
      raw: s.slice(start, TOKEN.lastIndex),
      field: m[2] ? m[2].slice(0, -1).toLowerCase() : null,
      pubkey,
    });
    at = TOKEN.lastIndex;
  }
  if (at < s.length) out.push({ type: "text", text: s.slice(at) });
  return out;
}

/**
 * What the relay is actually asked, from what the person typed:
 *
 *   { terms, authors, mentions }
 *
 * `terms` is what is left for NIP-50 — the from:/to: tokens are lifted out
 * entirely, because leaving them in would search the full-text index for the
 * literal string "from:npub1…", which matches nothing and would make a
 * narrowed search look like an empty one.
 *
 * A BARE npub keeps its existing meaning and stays a term. It renders as a
 * face in the field like any other key, but rendering is not semantics: this
 * page has never guessed whether a pasted key means "by them" or "about
 * them", and a chip is not a reason to start.
 */
export function parseQuery(text) {
  const authors = [];
  const mentions = [];
  let terms = "";
  for (const seg of tokenize(text)) {
    if (seg.type === "text") { terms += seg.text; continue; }
    const into = seg.field === "from" ? authors : seg.field === "to" ? mentions : null;
    if (!into) { terms += seg.raw; continue; }
    if (!into.includes(seg.pubkey)) into.push(seg.pubkey);
  }
  return { terms: terms.replace(/\s+/g, " ").trim(), authors, mentions };
}

/**
 * The `from:`/`to:` token the caret is currently inside, or null.
 *
 *   { field, partial, start, end, complete }
 *
 * `start`/`end` are character offsets into `text`, so a pick can splice the
 * finished token straight back in. `complete` means the partial already
 * decodes to a pubkey — the token is finished, so there is nothing left to
 * pick and the picker must get out of the way.
 *
 * A caret in the MIDDLE of a word is not a mention: `after` must be empty or
 * whitespace, or editing the middle of an unrelated sentence would pop a
 * people list over it.
 */
export function mentionAt(text, caret) {
  const s = String(text ?? "");
  const end = Math.max(0, Math.min(Number(caret) || 0, s.length));
  const rest = s.slice(end);
  if (rest && !/^\s/.test(rest)) return null;
  const m = PARTIAL.exec(s.slice(0, end));
  if (!m) return null;
  return {
    field: m[2].toLowerCase(),
    partial: m[3],
    start: m.index + m[1].length,
    end,
    complete: isKey(m[3]),
  };
}
