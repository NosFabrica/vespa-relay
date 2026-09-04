// The search box's own small language, parsed in one place. `from:`/`to:`
// narrow to people, `since:`/`until:` to a stretch of days, `#hashtag` to a
// topic, `group:<id>` to a NIP-29 group and `site:`/`isbn:`/`geo:`/`isan:`/
// `doi:`/`podcast:*` to the other NIP-73 subjects. All of them become NIP-01
// filter fields, never NIP-50 extensions, so they compose with the ranking.
// Pure functions over a string: the field renderer (searchfield.js) and the
// query builder (app.js) agree about token boundaries by both asking here, and
// web/src/test/js/query.test.mjs asserts them.

import { pubkeyParam } from "./nip19.js";

// An npub is a fixed 63 characters over bech32's alphabet, so the token
// boundary is known before decoding: "npub1…abc." ends at the `c`.
const NPUB = "npub1[qpzry9x8gf2tvdw0s3jn54khce6mua7l]{58}";

// Only the ISO spelling: `06/08/2026` is a different day to half the world.
const YMD = "\\d{4}-\\d{2}-\\d{2}";

// The NIP-73 scope prefixes. `podcast:guid` cannot half-match
// `podcast:item:guid:…`; after `podcast:` the next characters decide alone.
const SCOPES = "podcast:item:guid|podcast:publisher|podcast:guid|site|isbn|geo|isan|doi";

// A scope's value: everything to the next whitespace, minus trailing sentence
// punctuation. Only `. , ; ! ?`: DOIs contain parentheses and `;`, urls nearly
// anything, and cutting more would break ids that exist to help ones that do not.
const SCOPE_VALUE = "\\S*[^\\s.,;!?]";

// A NIP-29 group id is whatever its host relay minted, so it is delimited like
// a scope's value and not validated beyond being non-empty.
const GROUP_ID = SCOPE_VALUE;

/**
 * Can this id be written as a `group:` token that reads back as itself? The
 * tokenizer's own source, anchored, so the answer cannot drift. Whitespace ends
 * a token and trailing `.,;!?` is punctuation, so an id carrying either would
 * silently link to somebody else's room; the link is refused instead.
 */
const GROUP_ONLY = new RegExp(`^${GROUP_ID}$`);
export const groupTokenizes = (id) => GROUP_ONLY.test(String(id ?? ""));

// Every token in one scan, because they interleave and the field measures its
// caret against their order. The lead group anchors a token to a word start so
// a `to:` inside a url is not a filter; `i` because pasted npubs are often
// upcased. A date ends on anything but a word character or a hyphen, so a
// half-typed range `2026-08-06-07` is not a date at all.
const TOKEN = new RegExp(
  `(?<lead>^|\\s)(?:` +
    `(?<who>(?:from|to):)?(?<key>${NPUB})(?![a-z0-9])` +
    `|(?<when>(?:since|until):)(?<day>${YMD})(?![\\w-])` +
    `|(?<ext>(?:${SCOPES}):)(?<sid>${SCOPE_VALUE})` +
    `|(?<grp>group:)(?<gid>${GROUP_ID})` +
    `)`,
  "gi",
);
const WHOLE = new RegExp(`^${NPUB}$`, "i");

// The prefixes while they are still being typed: everything after the colon up
// to the caret, which is what the pickers search for.
const PARTIAL = /(^|\s)(from|to):(\S*)$/i;
const PARTIAL_DAY = /(^|\s)(since|until):(\S*)$/i;
const PARTIAL_GROUP = /(^|\s)(group):(\S*)$/i;

// A hashtag: a `#` that starts a word, then the characters a `t` tag holds.
// Letters, marks and digits by unicode property, not `\w`; the hyphen is part
// of the tag (a trailing one is trimmed below); the lead anchor is anything
// that cannot be part of a word, so `(#nostr)` is a tag while `C#` and
// `…/a#frag` are not; emoji count, with ZWJ and the variation selector so a
// family emoji is not cut to its first person.
const WORD = "\\p{L}\\p{M}\\p{N}_";
const EMOJI = "\\p{Extended_Pictographic}\\u200D\\uFE0F";
const HASHTAG = new RegExp(`(^|[^${WORD}])#([${WORD}${EMOJI}-]+)`, "gu");

// The punctuation a lifted hashtag strands (`#bitcoin.` leaves `.`), which is
// not a search term and, left in, would make `terms` non-empty. `#`, `"` and
// `-` are not orphans: `-word` and `"phrase"` are NIP-50's own operators.
const ORPHAN = new RegExp(`(^|\\s)[^${WORD}${EMOJI}#"-]+(?=\\s|$)`, "gu");

/** The leftover words, with the punctuation a lifted token stranded removed. */
const tidyTerms = (s) => s.replace(ORPHAN, "$1").replace(/\s+/g, " ").trim();

const pad = (n, w = 2) => String(n).padStart(w, "0");

/** A Date as the `YYYY-MM-DD` this language writes, in the reader's timezone. */
export const ymd = (d) => `${pad(d.getFullYear(), 4)}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;

/**
 * `YYYY-MM-DD` as the unix second that bound means, or null if it is not a day.
 * `since` is 00:00:00 and `until` 23:59:59 of the named day, in the reader's
 * timezone, because NIP-01's `until` is inclusive and "the 6th" is their 6th.
 * The round-trip check rejects `2026-02-31`, which the Date constructor would
 * roll over to March rather than refuse.
 */
export function dayBound(day, field) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(day ?? ""));
  if (!m) return null;
  const [y, mo, d] = m.slice(1).map(Number);
  const at = new Date(y, mo - 1, d);
  if (at.getFullYear() !== y || at.getMonth() !== mo - 1 || at.getDate() !== d) return null;
  if (field !== "until") return Math.floor(at.getTime() / 1000);
  // The second before the next midnight, not midnight plus 86,399: twice a year
  // a local day is 23 or 25 hours long.
  return Math.floor(new Date(y, mo - 1, d + 1).getTime() / 1000) - 1;
}

/**
 * Every spelling of `tag` worth asking a tag filter for, best first: as typed,
 * lowercase, Capitalized, UPPERCASE, deduped. The store matches tag values
 * cased, and a tag filter's value list is an OR compiled to one dictionary probe.
 */
export function tagValues(tag) {
  const t = String(tag ?? "");
  if (!t) return [];
  const lower = t.toLowerCase();
  return [...new Set([t, lower, lower.charAt(0).toUpperCase() + lower.slice(1), t.toUpperCase()])];
}

/**
 * Is this exactly one finished key, one the tokenizer will draw as a person
 * and `parseQuery` lift into a filter? Npub only, though `pubkeyParam` also
 * takes hex: the two questions must agree, and hex pasted after `from:` stays
 * unfinished so the picker resolves it and writes the npub back.
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
    // `raw` is what the field draws over and measures, trailing hyphen
    // included, or the pill would cover fewer characters than it stands for.
    out.push({ type: "tag", raw, tag });
    at = HASHTAG.lastIndex;
  }
  if (at < chunk.length) out.push({ type: "text", text: chunk.slice(at) });
}

/**
 * The typed string as a list of segments: plain text, and the tokens in it.
 *
 *   { type: "text",  text }
 *   { type: "key",   raw, field: "from" | "to" | null, pubkey }
 *   { type: "date",  raw, field: "since" | "until", at }
 *   { type: "tag",   raw, tag }
 *   { type: "scope", raw, field: "site" | "isbn" | …, value }
 *   { type: "group", raw, id }
 *
 * `raw` is the token exactly as typed, so a renderer can put it back verbatim
 * and a caret measured in characters stays so; the normalised value beside it
 * is what the filters ask for. A corrupt value (a failed checksum, a day that
 * does not exist) stays text rather than becoming a chip that filters for
 * nothing. Hashtags are scanned only in the text between other tokens, which
 * is everywhere one can be.
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
    } else if (g.ext) {
      const field = g.ext.slice(0, -1).toLowerCase();
      // A scope with no askable id (`site:#top`) is not a token: the pill would
      // claim a filter while buildFilters sent none.
      if (!scopeIds(field, g.sid).length) continue;
      seg = { type: "scope", raw, field, value: g.sid };
    } else if (g.grp) {
      seg = { type: "group", raw, id: g.gid };
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
const SETTLES = new Set(["tag", "date", "scope", "group"]);

/**
 * The segments to draw: `tokenize`'s, minus the settling token the caret is
 * inside. A tag is a token one character in and a date flickers in and out of
 * being one inside a typed year, so both stay text under the caret and pill
 * the moment it leaves. `typingAt` null draws everything (a paste or a restore).
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
 * What the relay is asked, from what the person typed:
 *
 *   { terms, authors, mentions, hashtags, scopes, groups, since, until }
 *
 * `terms` is what is left for NIP-50; every token is lifted out, since the
 * full-text index would hunt for the literal "from:npub1…" and a note tagged
 * `t: nostr` need not say the word. Two of one date prefix keep the narrower
 * bound, whichever order they were typed in. A bare npub stays a term: this
 * page does not guess whether a pasted key means "by them" or "about them".
 */
export function parseQuery(text) {
  const authors = [];
  const mentions = [];
  const hashtags = [];
  const scopes = [];
  const groups = [];
  let since = null;
  let until = null;
  let terms = "";
  for (const seg of tokenize(text)) {
    if (seg.type === "text") { terms += seg.text; continue; }
    // The segment carries both spellings, so the field and the filters agree.
    if (seg.type === "tag") { if (!hashtags.includes(seg.tag)) hashtags.push(seg.tag); continue; }
    // Verbatim, deduped as typed; the spellings worth asking are scopeIds's question.
    if (seg.type === "scope") {
      if (!scopes.some((s) => s.field === seg.field && s.value === seg.value)) scopes.push({ field: seg.field, value: seg.value });
      continue;
    }
    // Verbatim and case-exact: a group id is opaque, `General` and `general`
    // are two groups, and asking for both would pour a stranger's channel in.
    if (seg.type === "group") { if (!groups.includes(seg.id)) groups.push(seg.id); continue; }
    if (seg.type === "date") {
      if (seg.field === "since") since = since == null ? seg.at : Math.max(since, seg.at);
      else until = until == null ? seg.at : Math.min(until, seg.at);
      continue;
    }
    const into = seg.field === "from" ? authors : seg.field === "to" ? mentions : null;
    if (!into) { terms += seg.raw; continue; }
    if (!into.includes(seg.pubkey)) into.push(seg.pubkey);
  }
  return { terms: tidyTerms(terms), authors, mentions, hashtags, scopes, groups, since, until };
}

// A NIP-22 comment names its thread's root scope in `I` and its parent's in
// `i`; a reply deeper in the thread has no `i` naming the topic. One filter per
// tag, because both in one filter would AND them and drop those replies.
const COMMENT_KIND = 1111;
const COMMENT_SCOPE_TAGS = ["#I", "#i"];

// A NIP-29 group's own metadata, signed by the host relay's key, which is the
// only thing in the store that tells two groups sharing an id apart.
const GROUP_META_KIND = 39000;
// Single letter, so the store indexes it in `tag_index` exactly as it does `t`.
const GROUP_TAG = "#h";

/**
 * The NIP-73 external ids a hashtag is written as, for a comment's `i`/`I`:
 * `#topic` per the spec, lowercase, plus the unprefixed form for clients that
 * reused the `t` value.
 */
const hashtagIds = (tags) => tags.flatMap((t) => [`#${t}`, t]);

/** The `#t`/`#l` value list for a set of hashtags: every spelling, deduped. */
const tagAsks = (tags) => [...new Set(tags.flatMap(tagValues))];

/**
 * The NIP-73 web ids a `site:` value may be written as, canonical first. The
 * fragment is dropped, a value with no scheme is asked as both `https://` and
 * `http://`, and every candidate is asked with and without its trailing slash,
 * since the store matches tags by byte equality.
 */
function siteIds(value) {
  const bare = value.replace(/#.*$/, "");
  if (!bare) return [];
  const typed = /^[a-z][a-z0-9+.-]*:\/\//i.test(bare) ? [bare] : [`https://${bare}`, `http://${bare}`];
  // Scheme and host lowercased, canonical first; the path keeps its case
  // because it is case-sensitive.
  const cased = typed.flatMap((u) => {
    const m = /^([a-z][a-z0-9+.-]*:\/\/)([^/]*)(.*)$/i.exec(u);
    return m ? [m[1].toLowerCase() + m[2].toLowerCase() + m[3], u] : [u];
  });
  return [...new Set(cased.flatMap((u) => [u, u.endsWith("/") ? u.slice(0, -1) : `${u}/`]))];
}

/**
 * Every spelling of one scope worth a tag filter's while, canonical first and
 * as typed beside it: `isbn:` drops hyphens, `geo:` and `doi:` are lowercase,
 * `isan:` is uppercase and also asked as its 5-segment root, `podcast:*` guids
 * are asked lowercased too, and `podcast:publisher:` takes a `guid:` segment
 * the feed's prefix does not.
 */
export function scopeIds(field, value) {
  const v = String(value ?? "");
  if (!v) return [];
  if (field === "site") return siteIds(v);
  if (field === "isbn") return [...new Set([`isbn:${v.replace(/-/g, "")}`, `isbn:${v}`])];
  if (field === "geo") return [...new Set([`geo:${v.toLowerCase()}`, `geo:${v}`])];
  if (field === "doi") return [...new Set([`doi:${v.toLowerCase()}`, `doi:${v}`])];
  if (field === "isan") {
    const parts = v.split("-");
    const root = parts.length === 8 ? parts.slice(0, 5).join("-") : v;
    return [...new Set([`isan:${root.toUpperCase()}`, `isan:${root}`, `isan:${v.toUpperCase()}`, `isan:${v}`])];
  }
  if (field === "podcast:publisher" && !/^guid:/i.test(v)) {
    return [...new Set([`podcast:publisher:guid:${v}`, `podcast:publisher:guid:${v.toLowerCase()}`, `podcast:publisher:${v}`, `podcast:publisher:${v.toLowerCase()}`])];
  }
  return [...new Set([`${field}:${v}`, `${field}:${v.toLowerCase()}`])];
}

/** The `#I`/`#i` value list for a set of scopes: every spelling, deduped. */
const scopeAsks = (scopes) => [...new Set(scopes.flatMap((s) => scopeIds(s.field, s.value)))];

/**
 * How many results a secondary filter of a union may return. The store
 * applies `limit` per filter, and filters sharing a rank profile come back as
 * one ranking of the union, so this bounds how many side hits compete for the top.
 */
const sideLimit = (limit) => Math.max(4, Math.round(limit / 4));

/**
 * The `sort:` the store will apply to a search string, which need not be the
 * one the menu shows: a typed `sort:` survives `parseQuery`. The store's rules:
 * last one wins, a quoted span is a phrase, a leading minus is an exclusion,
 * and the key is case-sensitive and must start a token. Returns the value
 * alone, or `""` for no sort, the menu's own vocabulary.
 */
export function effectiveSort(searchString) {
  let out = "";
  // An unclosed quote runs to the end of the string, as the lexer reads it,
  // which is why this walks the string rather than splitting on whitespace.
  const s = String(searchString ?? "");
  let i = 0;
  while (i < s.length) {
    if (s[i] === '"') {
      const close = s.indexOf('"', i + 1);
      if (close === -1) break;
      i = close + 1;
      continue;
    }
    const next = s.indexOf('"', i);
    const chunk = s.slice(i, next === -1 ? s.length : next);
    for (const m of chunk.matchAll(/(?:^|\s)sort:(\S+)/g)) out = m[1];
    i = next === -1 ? s.length : next;
  }
  return out;
}

/**
 * The typed string as the REQ the page sends: NIP-01 filters ORed in one
 * subscription. `kinds` is the tab's (null for all), `limit` the page's, and
 * `searchString(terms)` the caller's NIP-50 builder.
 *
 * A hashtag asks three questions (`t`, `i`/`I` for comments on it, `l` for
 * labels); a scope asks the comment question alone; a group asks `h` plus its
 * own kind-39000 metadata by `d`. The person and date fields ride on every
 * filter. The comment filters name kind 1111 and are gated on the tab; the
 * scope and metadata filters name their kind and are not, or an off-tab
 * token would answer as if it had never been typed. `search` is sent whenever
 * it carries anything, extensions included: an extensions-only query is
 * unconstrained at the store, not match-nothing.
 */
export function buildFilters(text, { kinds = null, limit, searchString = (t) => t } = {}) {
  const q = parseQuery(text);
  const base = {};
  const search = searchString(q.terms);
  if (search.trim()) base.search = search;
  if (kinds) base.kinds = kinds;
  if (q.authors.length) base.authors = q.authors;
  if (q.mentions.length) base["#p"] = q.mentions;
  // On `base`, so the window rides every filter of a union: `since:2026-01-01
  // #nostr` narrowed only the `t` half returns last year's notes.
  if (q.since != null) base.since = q.since;
  if (q.until != null) base.until = q.until;
  const scoped = scopeAsks(q.scopes);
  if (!q.hashtags.length && !scoped.length && !q.groups.length) return [{ ...base, limit }];

  const side = sideLimit(limit);
  const filters = [];
  if (q.groups.length) {
    filters.push({ ...base, [GROUP_TAG]: q.groups, limit });
    filters.push({ ...base, kinds: [GROUP_META_KIND], "#d": q.groups, limit: side });
  }
  if (q.hashtags.length) {
    filters.push({ ...base, "#t": tagAsks(q.hashtags), limit });
    filters.push({ ...base, "#l": tagAsks(q.hashtags), limit: side });
    if (!kinds || kinds.includes(COMMENT_KIND)) {
      const ids = hashtagIds(q.hashtags);
      for (const tag of COMMENT_SCOPE_TAGS) filters.push({ ...base, kinds: [COMMENT_KIND], [tag]: ids, limit: side });
    }
  }
  if (scoped.length) {
    filters.push({ ...base, kinds: [COMMENT_KIND], "#I": scoped, limit });
    filters.push({ ...base, kinds: [COMMENT_KIND], "#i": scoped, limit: side });
  }
  return filters;
}

/**
 * A prefixed token the caret is inside, or null: `{ field, partial, start,
 * end, complete }`, with offsets into `text` so a pick can splice the finished
 * token in. A caret in the middle of a word is not a token being built.
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
 * The `since:`/`until:` token the caret is inside, which the calendar opens on.
 * A partial that is already a day is complete; `since:2026-02-31` is not a day
 * and keeps the calendar open.
 */
export const dateAt = (text, caret) => partialAt(text, caret, PARTIAL_DAY, (v, f) => dayBound(v, f) != null);

/**
 * The `group:` token the caret is inside, which the group picker asks. Never
 * `complete`: `group:gen` is a plausible id and the first three characters of
 * `general`, so only a space ends it. With rows offered, Enter picks the
 * highlighted group; groups.js ranks an exact match first, so that is safe.
 */
export const groupAt = (text, caret) => partialAt(text, caret, PARTIAL_GROUP, () => false);
