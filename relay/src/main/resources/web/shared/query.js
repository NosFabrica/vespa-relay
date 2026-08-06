// The search box's own small language, parsed in one place.
//
// Two prefixes narrow a search to people instead of words:
//
//     from:npub1…   the author       -> a NIP-01 `authors` filter
//     to:npub1…     who they named   -> a NIP-01 `#p` filter
//
// …two narrow it to a stretch of time:
//
//     since:2026-08-06   written that day or after   -> a NIP-01 `since`
//     until:2026-08-06   written that day or before  -> a NIP-01 `until`
//
// …and one mark narrows it to a subject:
//
//     #hashtag      the topic        -> a NIP-01 `#t` filter, plus the NIP-22
//                                       comments written ON that topic
//
// All of them are NIP-01 filter fields, not NIP-50 extensions — the store never
// sees the prefixes at all. That is deliberate: `authors`, `#p`, `#t`, `since`
// and `until` are indexed filters every relay implements, so narrowing this way
// costs nothing and composes with the trust ranking rather than competing with
// it.
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

// A calendar day, as the ISO spelling the date picker writes and the one a
// person types by hand. Only this shape: `06/08/2026` is the sixth of August
// to half the world and the eighth of June to the other half, and a search box
// is the last place to guess which reader is in front of it.
const YMD = "\\d{4}-\\d{2}-\\d{2}";

// Every token in one scan, because they interleave and their order in the
// string is what the field measures its caret against — two passes would have
// to be merged back together in position order anyway.
//
// The lead group anchors a token to a word start, so a `to:` inside a url is
// not a filter. `i` because npubs get pasted out of clients that upcase them
// (pubkeyParam lowercases before decoding), and because `Since:` at the start
// of a sentence-cased field is still the prefix.
//
// A date ends on anything that is not a word character or a hyphen, so
// `until:2026-08-06.` ends at the `6` while `2026-08-06-07` is not a date at
// all — a half-typed range must not silently filter for its first half.
const TOKEN = new RegExp(
  `(?<lead>^|\\s)(?:` +
    `(?<who>(?:from|to):)?(?<key>${NPUB})(?![a-z0-9])` +
    `|(?<when>(?:since|until):)(?<day>${YMD})(?![\\w-])` +
    `)`,
  "gi",
);
const WHOLE = new RegExp(`^${NPUB}$`, "i");

// The prefixes while they are still being TYPED: everything after the colon up
// to the caret, which is what the author picker searches for and what tells
// the calendar which month to open on.
const PARTIAL = /(^|\s)(from|to):(\S*)$/i;
const PARTIAL_DAY = /(^|\s)(since|until):(\S*)$/i;

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

const pad = (n, w = 2) => String(n).padStart(w, "0");

/** A Date as the `YYYY-MM-DD` this language writes, in the reader's timezone. */
export const ymd = (d) => `${pad(d.getFullYear(), 4)}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;

/**
 * `YYYY-MM-DD` as the unix second that bound means, or null if it is not a day.
 *
 * A DAY, not an instant, and the two ends of one are not symmetric:
 *
 *   - `since` is 00:00:00 and `until` is 23:59:59, both of the named day.
 *     NIP-01's `until` is INCLUSIVE, so an `until` that stopped at midnight
 *     would exclude the whole of the day it names — `since:X until:X` would be
 *     one second wide instead of one day, which is not what anybody means by
 *     "between the 6th and the 6th".
 *   - The reader's timezone, not UTC. The date came off a calendar they read;
 *     "the 6th" is their 6th. A UTC reading shifts the window by up to a day,
 *     most visibly for whoever types today's date in the evening east of
 *     Greenwich and gets back nothing they wrote today.
 *
 * The round-trip check is what rejects `2026-02-31` and `2026-13-01`: the Date
 * constructor ROLLS those over — to 3 March, to January 2027 — rather than
 * failing, so without it a date nobody typed would become a filter. It also
 * catches the two-digit-year rule, under which `0026-01-01` is 1926.
 */
export function dayBound(day, field) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(day ?? ""));
  if (!m) return null;
  const [y, mo, d] = m.slice(1).map(Number);
  const at = new Date(y, mo - 1, d);
  if (at.getFullYear() !== y || at.getMonth() !== mo - 1 || at.getDate() !== d) return null;
  if (field !== "until") return Math.floor(at.getTime() / 1000);
  // The last second of the day, as the second BEFORE the next midnight rather
  // than this one plus 86,399. Twice a year a local day is 23 or 25 hours long,
  // and the arithmetic version lands an hour inside the neighbouring day on
  // both of them — the one day of the year a "since today until today" search
  // would quietly reach into tomorrow.
  return Math.floor(new Date(y, mo - 1, d + 1).getTime() / 1000) - 1;
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

/** The hashtags inside one stretch of plain text, as segments in place. */
function tagSegments(chunk, out) {
  let at = 0;
  HASHTAG.lastIndex = 0;
  for (let m; (m = HASHTAG.exec(chunk)); ) {
    const start = m.index + m[1].length;
    const raw = chunk.slice(start, HASHTAG.lastIndex);
    const tag = m[2].replace(/-+$/, "").toLowerCase();
    // A tag that is nothing but hyphens normalizes to empty and is not a tag.
    if (!tag) continue;
    if (start > at) out.push({ type: "text", text: chunk.slice(at, start) });
    // `raw` is what the field draws over and measures; the trailing hyphen the
    // NORMALIZED tag drops is part of the token all the same, or the pill would
    // cover fewer characters than it stands for and the caret would shift.
    out.push({ type: "tag", raw, tag });
    at = HASHTAG.lastIndex;
  }
  if (at < chunk.length) out.push({ type: "text", text: chunk.slice(at) });
}

/**
 * The typed string as a list of segments: plain text, and the tokens in it.
 *
 *   { type: "text", text }
 *   { type: "key",  raw, field: "from" | "to" | null, pubkey }
 *   { type: "date", raw, field: "since" | "until", at }
 *   { type: "tag",  raw, tag }
 *
 * `raw` is the token exactly as typed, so a renderer can put it back verbatim
 * and a caret measured in characters stays measured in characters. For a tag,
 * `tag` is the normalized value the filters ask for — `#Nostr` draws as typed
 * and is asked for as `nostr`, which is exactly the split a `from:npub1…` chip
 * already makes between what it shows and what it means. A date makes the same
 * split the other way round: `at` is the unix second, and the pill draws the
 * day in the reader's own spelling.
 *
 * An npub whose CHECKSUM fails stays text, and so does a date that is not a
 * day — `since:2026-02-31`. A corrupted value must not become a chip claiming
 * to filter for something nobody asked for: the rule nip19.js's decoder states
 * for entity pages, applied one layer up and to both kinds of token.
 *
 * Hashtags are only ever found in the TEXT between the other tokens: a `#`
 * cannot occur inside an npub — bech32 has no such character — nor inside a
 * date, so scanning them for one would be looking where it cannot be.
 */
export function tokenize(text) {
  const s = String(text ?? "");
  const out = [];
  let at = 0;
  TOKEN.lastIndex = 0;
  for (let m; (m = TOKEN.exec(s)); ) {
    const g = m.groups;
    const start = m.index + g.lead.length;
    const raw = s.slice(start, TOKEN.lastIndex);
    let seg;
    if (g.key) {
      const pubkey = pubkeyParam(g.key);
      if (!pubkey) continue;
      seg = { type: "key", raw, field: g.who ? g.who.slice(0, -1).toLowerCase() : null, pubkey };
    } else {
      const field = g.when.slice(0, -1).toLowerCase();
      const bound = dayBound(g.day, field);
      if (bound == null) continue;
      seg = { type: "date", raw, field, at: bound };
    }
    if (start > at) tagSegments(s.slice(at, start), out);
    out.push(seg);
    at = TOKEN.lastIndex;
  }
  if (at < s.length) tagSegments(s.slice(at), out);
  return out;
}

/** The token types that pill only once the caret has left them. */
const SETTLES = new Set(["tag", "date"]);

/**
 * The segments to DRAW: [tokenize]'s, minus the tag or date the caret is inside.
 *
 * A hashtag becomes a token one character in — `#n` is already a tag — so
 * drawing every tag would re-render the field on EVERY keystroke of one,
 * which is exactly what structureChanged() exists to avoid: it fights the
 * browser over the caret, the undo stack and IME composition, and an npub
 * only ever crosses that line once, at its 63rd character.
 *
 * A date is worse than a tag, not better: `since:2026-08-06` is a whole token
 * at the `6`, and one more digit takes it back to text again — so a field that
 * pilled on sight would flicker a pill in and out inside one typed year.
 *
 * So both stay text under the caret and pill the moment it leaves. That is the
 * same rule the people picker follows — a token being built is not yet a token
 * — and it means the pill's appearance is the field saying "this one is
 * finished, and it is a filter now".
 *
 * `typingAt` null draws everything: a paste, a URL restore or a blur is not
 * somebody midway through typing a word.
 */
export function drawable(text, typingAt) {
  const segs = tokenize(text);
  if (typingAt == null) return segs;
  let at = 0;
  return segs.map((seg) => {
    const start = at;
    at += seg.type === "text" ? seg.text.length : seg.raw.length;
    if (!SETTLES.has(seg.type) || typingAt <= start || typingAt > at) return seg;
    return { type: "text", text: seg.raw };
  });
}

/**
 * What the relay is actually asked, from what the person typed:
 *
 *   { terms, authors, mentions, hashtags, since, until }
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
 * `since` and `until` are unix seconds or null, and leave the words for the
 * plainest reason of the lot: a date is not a thing to full-text search for.
 * Two of the same prefix keep the NARROWER bound — the later `since`, the
 * earlier `until`. Unlike the author and tag lists there is no OR to fall back
 * on, one filter carries one of each; and every token in this box narrows the
 * search, so the narrower bound is the reading that keeps that promise. It is
 * also the same answer whichever order the two were typed in.
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
  let since = null;
  let until = null;
  let terms = "";
  for (const seg of tokenize(text)) {
    if (seg.type === "text") { terms += seg.text; continue; }
    // `#Nostr` asks for `nostr`: the segment already carries both, so the field
    // and the filters cannot disagree about which is which.
    if (seg.type === "tag") { if (!hashtags.includes(seg.tag)) hashtags.push(seg.tag); continue; }
    if (seg.type === "date") {
      if (seg.field === "since") since = since == null ? seg.at : Math.max(since, seg.at);
      else until = until == null ? seg.at : Math.min(until, seg.at);
      continue;
    }
    const into = seg.field === "from" ? authors : seg.field === "to" ? mentions : null;
    if (!into) { terms += seg.raw; continue; }
    if (!into.includes(seg.pubkey)) into.push(seg.pubkey);
  }
  return { terms: tidyTerms(terms), authors, mentions, hashtags, since, until };
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
 *
 * "Near the top" became literal in store 8a45e4d1a2: filters of one REQ that
 * share a rank profile — which these four always do, they carry the same search
 * string — come back as ONE ranking of the union rather than run after run. So
 * a side filter's quarter is no longer just a cap on tail volume; it bounds how
 * many of its hits can COMPETE for the top of the page. A labelled note that
 * outranks everything tagged still lands first.
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
 * `since:`/`until:` become the NIP-01 fields of the same names, one window over
 * `created_at` however many filters the rest of the query turns into.
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
  // On `base`, so the window rides every filter of a hashtag union for the same
  // reason the person filters do: `since:2026-01-01 #nostr` narrowed only the
  // `t` half would return this week's comments beside last year's notes.
  if (q.since != null) base.since = q.since;
  if (q.until != null) base.until = q.until;
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
 * A prefixed token the caret is currently inside, or null.
 *
 *   { field, partial, start, end, complete }
 *
 * `start`/`end` are character offsets into `text`, so a pick can splice the
 * finished token straight back in. `complete` means the partial already reads
 * as a value — the token is finished, so there is nothing left to pick and the
 * picker must get out of the way.
 *
 * A caret in the MIDDLE of a word is not a token being built: what follows must
 * be empty or whitespace, or editing the middle of an unrelated sentence would
 * pop a picker over it.
 */
function partialAt(text, caret, re, finished) {
  const s = String(text ?? "");
  const end = Math.max(0, Math.min(Number(caret) || 0, s.length));
  const rest = s.slice(end);
  if (rest && !/^\s/.test(rest)) return null;
  const m = re.exec(s.slice(0, end));
  if (!m) return null;
  const field = m[2].toLowerCase();
  return { field, partial: m[3], start: m.index + m[1].length, end, complete: finished(m[3], field) };
}

/** The `from:`/`to:` token the caret is inside — what the people picker asks. */
export const mentionAt = (text, caret) => partialAt(text, caret, PARTIAL, isKey);

/**
 * The `since:`/`until:` token the caret is inside — what the calendar opens on.
 *
 * The same shape as [mentionAt] and for the same reason: the two pickers share
 * one box and one set of arrow keys, so whichever is up has to be able to say
 * exactly which characters it will replace when something is picked.
 *
 * A partial that is already a day is COMPLETE — the calendar closes, the same
 * way the people list does over a finished npub. `since:2026-02-3` keeps it
 * open, and so does `since:2026-02-31`, because a day that does not exist is
 * not a day and the reader has more typing to do.
 */
export const dateAt = (text, caret) => partialAt(text, caret, PARTIAL_DAY, (v, f) => dayBound(v, f) != null);
