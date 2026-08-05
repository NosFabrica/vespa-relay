// The search box's own small language, parsed in one place.
//
// Two prefixes narrow a search to people instead of words:
//
//     from:npub1…   the author       -> a NIP-01 `authors` filter
//     to:npub1…     who they named   -> a NIP-01 `#p` filter
//
// …and one mark narrows it to a subject:
//
//     #hashtag      the topic        -> a NIP-01 `#t` filter, plus the NIP-22
//                                       comments written ON that topic
//
// All of them are NIP-01 filter fields, not NIP-50 extensions — the store never
// sees the prefixes at all. That is deliberate: `authors`, `#p` and `#t` are
// indexed filters every relay implements, so narrowing this way costs nothing
// and composes with the trust ranking rather than competing with it.
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

// A hashtag: a `#` that starts a word, then the characters a `t` tag actually
// holds. Three decisions, each one paid for:
//
//   - Letters, MARKS and digits by unicode property rather than `\w`, because
//     most of Nostr's hashtags are not ASCII: `\w` cut `#café` to `caf` and
//     filtered for half a word.
//   - The HYPHEN is part of the tag. Without it `#covid-19` lifted `covid` and
//     left `-19` behind as a term — and a leading `-` is NIP-50's exclusion
//     operator (README's search table), so the query asked for the topic and
//     then excluded results containing "19". A trailing hyphen is trimmed
//     below: `#nostr-` is the tag, not the punctuation after it.
//   - The lead anchor is any character that cannot be part of a word, not just
//     whitespace, so `(#nostr)` and `"#nostr` are tags while `C#` stays a
//     language and `…/a#frag` stays a url fragment.
//   - Emoji count as tag characters. Clients let people tag `#🔥`, and `\p{L}`
//     does not reach it; ZWJ and the variation selector are in the class too,
//     or a family emoji would tag its first person and drop the rest.
const WORD = "\\p{L}\\p{M}\\p{N}_";
const EMOJI = "\\p{Extended_Pictographic}\\u200D\\uFE0F";
const HASHTAG = new RegExp(`(^|[^${WORD}])#([${WORD}${EMOJI}-]+)`, "gu");

// What a lifted hashtag can strand: the punctuation that was attached to it.
// `#bitcoin.` leaves `.`, `#nostr, cats` leaves `, cats` — and an orphan is not
// a search term. Left in, it also flipped the filter's SHAPE: a non-empty
// `terms` is what decides whether `search` is sent at all, so a trailing full
// stop turned a plain tag read into a text query for ".".
// `#`, `"` and `-` are NOT orphans: a `#` the tag rule declined to take is
// still the reader's text, and `-word` / `"phrase"` are NIP-50's own operators.
const ORPHAN = new RegExp(`(^|\\s)[^${WORD}${EMOJI}#"-]+(?=\\s|$)`, "gu");

/** The leftover words, with the punctuation a lifted token stranded removed. */
const tidyTerms = (s) => s.replace(ORPHAN, "$1").replace(/\s+/g, " ").trim();

/**
 * Lift every hashtag out of `text` into `into`, and give back what is left.
 *
 * `#` dropped and lowercased, which is the value a `t` tag carries — NIP-24
 * says a `t` tag SHOULD be lowercase. SHOULD, not MUST, is why [tagValues]
 * exists: the ask has to cover the tags that ignored it.
 */
function liftHashtags(text, into) {
  return String(text).replace(HASHTAG, (_m, lead, tag) => {
    const t = tag.replace(/-+$/, "").toLowerCase();
    if (t && !into.includes(t)) into.push(t);
    return lead;
  });
}

/**
 * Every spelling of `tag` worth asking a tag filter for, best first.
 *
 * The store matches tag values CASED — its schema says so in as many words:
 * "a stored `t:MixedCase` matched a `#t:["mixedcase"]` filter. Cased matching
 * restores byte equality." So the lowercase NIP-24 spelling is the right ask
 * and an incomplete one: a note tagged `t: Bitcoin` is invisible to it, and
 * plenty are. A tag filter's value list is an OR and the store compiles a
 * multi-value list to one dictionary-backed `in` (EventYql.tagClause), so the
 * extra spellings cost a string each, not a query each.
 *
 * Four at most, deduped: as typed, lowercase, Capitalized, UPPERCASE.
 */
export function tagValues(tag) {
  const t = String(tag ?? "");
  if (!t) return [];
  const lower = t.toLowerCase();
  return [...new Set([t, lower, lower.charAt(0).toUpperCase() + lower.slice(1), t.toUpperCase()])];
}

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
 *   { terms, authors, mentions, hashtags }
 *
 * `terms` is what is left for NIP-50 — the from:/to: tokens are lifted out
 * entirely, because leaving them in would search the full-text index for the
 * literal string "from:npub1…", which matches nothing and would make a
 * narrowed search look like an empty one.
 *
 * `#hashtag` leaves for the same reason and a sharper one: the full-text index
 * is built from an event's CONTENT, and a note tagged `t: nostr` need not say
 * the word anywhere in it. Searching the text for "#nostr" therefore answers a
 * different question from the one a `#` asks, and answers it worse — it misses
 * every note that tagged the topic properly, and matches the ones that merely
 * spelled it out.
 *
 * A BARE npub keeps its existing meaning and stays a term. It renders as a
 * face in the field like any other key, but rendering is not semantics: this
 * page has never guessed whether a pasted key means "by them" or "about
 * them", and a chip is not a reason to start.
 */
export function parseQuery(text) {
  const authors = [];
  const mentions = [];
  const hashtags = [];
  let terms = "";
  for (const seg of tokenize(text)) {
    // Only the TEXT between the person tokens is scanned for hashtags — a `#`
    // cannot occur inside an npub (bech32 has no such character), and running
    // the scan over `raw` would be looking for it there anyway.
    if (seg.type === "text") { terms += liftHashtags(seg.text, hashtags); continue; }
    const into = seg.field === "from" ? authors : seg.field === "to" ? mentions : null;
    if (!into) { terms += seg.raw; continue; }
    if (!into.includes(seg.pubkey)) into.push(seg.pubkey);
  }
  return { terms: tidyTerms(terms), authors, mentions, hashtags };
}

// A NIP-22 comment (kind 1111) says what it is about in `I` — the thread's
// ROOT scope — and in `i`, its immediate parent's. A top-level comment on a
// topic carries both; a reply further down the thread carries `I` for the topic
// and `e` for the comment above it, so it has no `i` naming the topic at all.
// Hence one filter per tag: both in one filter would AND them and drop exactly
// those replies, and separate filters is the only way NIP-01 spells "or".
const COMMENT_KIND = 1111;
const COMMENT_SCOPE_TAGS = ["#I", "#i"];

/**
 * The NIP-73 external ids a hashtag is written as, for a comment's `i`/`I`.
 *
 * NIP-73 spells the id `#topic`, lowercase — the `#` is part of the value, and
 * the `k`/`K` beside it is the bare `"#"`. No case variants here, unlike
 * [tagValues]: NIP-73 fixes the case where NIP-24 only suggests it. The
 * unprefixed form goes in the list because a tag filter is an OR and one extra
 * string is cheaper than missing every comment written by something that reused
 * the `t` value here.
 */
const hashtagIds = (tags) => tags.flatMap((t) => [`#${t}`, t]);

/** The `#t`/`#l` value list for a set of hashtags: every spelling, deduped. */
const tagAsks = (tags) => [...new Set(tags.flatMap(tagValues))];

/**
 * How many results a SECONDARY filter of a union may return.
 *
 * The store applies `limit` per filter, so a four-filter hashtag search at the
 * page's limit could return four times the rows a word search does — four times
 * the cards, the profile lookups and the score batches, for a screen the reader
 * scrolls maybe twice. The `t` filter keeps the full limit because it answers
 * the question people mean; the other three ride along at a quarter, enough to
 * be represented near the top and not enough to dominate the page.
 */
const sideLimit = (limit) => Math.max(4, Math.round(limit / 4));

/**
 * The typed string as the REQ the page sends — NIP-01 filters, ORed inside one
 * subscription. `kinds` is the tab's kinds (null for all), `limit` the page's,
 * and `searchString(terms)` the caller's NIP-50 string builder: the sort, spam
 * and observer extensions are page state, so the one impure input is a function.
 *
 * `from:npub…` and `to:npub…` become the NIP-01 filter fields they are —
 * `authors` and `#p` — which are indexed on every relay and compose with the
 * ranking rather than competing with it, whereas leaving them in `search` would
 * have the full-text index hunting for the literal string "from:npub1…".
 *
 * `#hashtag` becomes THREE questions, because Nostr writes a topic down three
 * ways and none of them is a superset of the others:
 *
 *   - the event is about the topic          -> `t`             (NIP-01, NIP-24)
 *   - it is a comment ON the topic          -> `i`/`I`         (NIP-22, NIP-73)
 *   - it is LABELLED with the topic         -> `l`             (NIP-32)
 *
 * A note tagging `t` need carry no label; a self-labelled note need carry no
 * `t`; a comment on a topic carries neither. (Four filters for three questions:
 * `i` and `I` in one filter would AND, so the comment question needs one each.)
 *
 * The person filters ride on EVERY filter — they narrow the same search, and
 * leaving them off the comment half would make `from:alice #nostr` return
 * everybody's comments alongside alice's notes. So do the tab's kinds, except on
 * the comment filters, which name their own kind and are therefore GATED on it
 * instead: a tab that excludes 1111 sends the other filters alone rather than
 * two that can only come back empty. The label filter needs no such gate — a
 * label rides on an event of any kind, so under Notes it finds self-labelled
 * notes, and under Everything it also finds the kind 1985 that labelled one.
 *
 * `search` is sent whenever it would CARRY something — words, or a sort/spam/
 * observer extension. It used to be dropped whenever the words were empty, on
 * the belief that "a filter carrying only the extensions is a text query for
 * nothing"; the store says otherwise in as many words ("a query that is nothing
 * but extensions becomes unconstrained, not match-nothing", and `sort:` with no
 * terms "is a match-all in that order"). That belief cost a hashtag search its
 * ranking: `#nostr` with "Most trusted" picked, or the spam floor lifted, or a
 * lens chosen, sent none of it — three visible controls doing nothing, on the
 * most ordinary query this page has. An EMPTY string is still omitted, because
 * then there is genuinely nothing to say and a plain NIP-01 read is the honest
 * shape.
 */
export function buildFilters(text, { kinds = null, limit, searchString = (t) => t } = {}) {
  const q = parseQuery(text);
  const base = {};
  const search = searchString(q.terms);
  if (search.trim()) base.search = search;
  if (kinds) base.kinds = kinds;
  if (q.authors.length) base.authors = q.authors;
  if (q.mentions.length) base["#p"] = q.mentions;
  if (!q.hashtags.length) return [{ ...base, limit }];

  const side = sideLimit(limit);
  const filters = [
    { ...base, "#t": tagAsks(q.hashtags), limit },
    { ...base, "#l": tagAsks(q.hashtags), limit: side },
  ];
  if (!kinds || kinds.includes(COMMENT_KIND)) {
    const ids = hashtagIds(q.hashtags);
    for (const tag of COMMENT_SCOPE_TAGS) filters.push({ ...base, kinds: [COMMENT_KIND], [tag]: ids, limit: side });
  }
  return filters;
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
