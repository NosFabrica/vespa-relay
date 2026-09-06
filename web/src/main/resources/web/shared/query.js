// The search box's own small language, parsed in one place: `from:`/`to:`, `since:`/`until:`,
// `#hashtag`, `group:<id>` and the NIP-73 scopes (`site:`, `isbn:`, `geo:`, `isan:`, `doi:`,
// `podcast:*`). All become NIP-01 filter fields, never NIP-50 extensions, so they compose with
// the ranking. The field renderer and the query builder both take token boundaries from here.

import { pubkeyParam } from "./nip19.js";

// An npub is a fixed 63 characters, so the token boundary is known before decoding.
const NPUB = "npub1[qpzry9x8gf2tvdw0s3jn54khce6mua7l]{58}";

// Only the ISO spelling: `06/08/2026` is a different day to half the world.
const YMD = "\\d{4}-\\d{2}-\\d{2}";

// The NIP-73 scope prefixes, longest `podcast:` form first so a shorter one cannot half-match.
const SCOPES = "podcast:item:guid|podcast:publisher|podcast:guid|site|isbn|geo|isan|doi";

// A scope's value: everything to the next whitespace, minus trailing sentence punctuation.
// Only `. , ; ! ?`, since DOIs and urls contain nearly anything else.
const SCOPE_VALUE = "\\S*[^\\s.,;!?]";

// A group id is whatever its host relay minted, so it is delimited like a scope's value.
const GROUP_ID = SCOPE_VALUE;

/**
 * Can this id be written as a `group:` token that reads back as itself? Whitespace or trailing
 * punctuation in an id would silently link to somebody else's room, so the link is refused.
 */
const GROUP_ONLY = new RegExp(`^${GROUP_ID}$`);
export const groupTokenizes = (id) => GROUP_ONLY.test(String(id ?? ""));

// Every token in one scan, because they interleave and the field measures its caret against
// their order. The lead group anchors a token to a word start so a `to:` inside a url is not a
// filter; a date ends on anything but a word character or a hyphen.
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

// The prefixes while they are still being typed: everything after the colon up to the caret.
const PARTIAL = /(^|\s)(from|to):(\S*)$/i;
const PARTIAL_DAY = /(^|\s)(since|until):(\S*)$/i;
const PARTIAL_GROUP = /(^|\s)(group):(\S*)$/i;

// A hashtag: a `#` that starts a word, then letters, marks and digits by unicode property, the
// hyphen, and emoji with ZWJ and the variation selector so a family emoji is not cut short.
const WORD = "\\p{L}\\p{M}\\p{N}_";
const EMOJI = "\\p{Extended_Pictographic}\\u200D\\uFE0F";
const HASHTAG = new RegExp(`(^|[^${WORD}])#([${WORD}${EMOJI}-]+)`, "gu");

// The punctuation a lifted hashtag strands (`#bitcoin.` leaves `.`). `#`, `"` and `-` are not
// orphans: they are NIP-50's own operators.
const ORPHAN = new RegExp(`(^|\\s)[^${WORD}${EMOJI}#"-]+(?=\\s|$)`, "gu");

/** The leftover words, with the punctuation a lifted token stranded removed. */
const tidyTerms = (s) => s.replace(ORPHAN, "$1").replace(/\s+/g, " ").trim();

const pad = (n, w = 2) => String(n).padStart(w, "0");

/** A Date as the `YYYY-MM-DD` this language writes, in the reader's timezone. */
export const ymd = (d) => `${pad(d.getFullYear(), 4)}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;

/**
 * `YYYY-MM-DD` as the unix second that bound means, or null if it is not a day: `since` is
 * 00:00:00 and `until` 23:59:59 of that day in the reader's timezone, since NIP-01's `until` is
 * inclusive. The round-trip check rejects `2026-02-31`, which the Date constructor would roll over.
 */
export function dayBound(day, field) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(day ?? ""));
  if (!m) return null;
  const [y, mo, d] = m.slice(1).map(Number);
  const at = new Date(y, mo - 1, d);
  if (at.getFullYear() !== y || at.getMonth() !== mo - 1 || at.getDate() !== d) return null;
  if (field !== "until") return Math.floor(at.getTime() / 1000);
  // The second before the next midnight, not midnight plus 86,399: a local day can be 23 or 25 hours.
  return Math.floor(new Date(y, mo - 1, d + 1).getTime() / 1000) - 1;
}

/**
 * Every spelling of `tag` worth asking a tag filter for, best first: as typed, lowercase,
 * Capitalized, UPPERCASE, deduped. The store matches tag values cased.
 */
export function tagValues(tag) {
  const t = String(tag ?? "");
  if (!t) return [];
  const lower = t.toLowerCase();
  return [...new Set([t, lower, lower.charAt(0).toUpperCase() + lower.slice(1), t.toUpperCase()])];
}

/**
 * Is this exactly one finished key? Npub only: hex pasted after `from:` stays unfinished so the
 * picker resolves it and writes the npub back.
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
    // `raw` keeps the trailing hyphen, or the pill would cover fewer characters than it stands for.
    out.push({ type: "tag", raw, tag });
    at = HASHTAG.lastIndex;
  }
  if (at < chunk.length) out.push({ type: "text", text: chunk.slice(at) });
}

/**
 * The typed string as segments: `{ type: "text", text }` and the tokens in it (key, date, tag,
 * scope, group), each carrying `raw` exactly as typed, so a renderer can put it back verbatim,
 * beside its normalised value. A corrupt value (a failed checksum, a day that does not exist) stays text.
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
 * The segments to draw: `tokenize`'s, minus the settling token the caret is inside, which stays
 * text until the caret leaves. `typingAt` null draws everything.
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
 * What the relay is asked, from what the person typed: `{ terms, authors, mentions, hashtags,
 * scopes, groups, since, until }`. `terms` is what is left for NIP-50 once every token is lifted
 * out; two of one date prefix keep the narrower bound; a bare npub stays a term.
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
    if (seg.type === "tag") { if (!hashtags.includes(seg.tag)) hashtags.push(seg.tag); continue; }
    // Verbatim, deduped as typed; the spellings worth asking are scopeIds's question.
    if (seg.type === "scope") {
      if (!scopes.some((s) => s.field === seg.field && s.value === seg.value)) scopes.push({ field: seg.field, value: seg.value });
      continue;
    }
    // Verbatim and case-exact: a group id is opaque, and `General` and `general` are two groups.
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

// A NIP-22 comment names its thread's root scope in `I` and its parent's in `i`. One filter
// per tag: both in one filter would AND them and drop the deeper replies.
const COMMENT_KIND = 1111;
const COMMENT_SCOPE_TAGS = ["#I", "#i"];

// A NIP-29 group's own metadata, signed by the host relay's key.
const GROUP_META_KIND = 39000;
// Single letter, so the store indexes it in `tag_index` exactly as it does `t`.
const GROUP_TAG = "#h";

/**
 * The NIP-73 external ids a hashtag is written as, for a comment's `i`/`I`: `#topic` per the
 * spec, plus the unprefixed form some clients reused.
 */
const hashtagIds = (tags) => tags.flatMap((t) => [`#${t}`, t]);

/** The `#t`/`#l` value list for a set of hashtags: every spelling, deduped. */
const tagAsks = (tags) => [...new Set(tags.flatMap(tagValues))];

/**
 * The NIP-73 web ids a `site:` value may be written as, canonical first: fragment dropped, both
 * schemes when none was typed, and each with and without its trailing slash.
 */
function siteIds(value) {
  const bare = value.replace(/#.*$/, "");
  if (!bare) return [];
  const typed = /^[a-z][a-z0-9+.-]*:\/\//i.test(bare) ? [bare] : [`https://${bare}`, `http://${bare}`];
  // Scheme and host lowercased, canonical first; the path keeps its case.
  const cased = typed.flatMap((u) => {
    const m = /^([a-z][a-z0-9+.-]*:\/\/)([^/]*)(.*)$/i.exec(u);
    return m ? [m[1].toLowerCase() + m[2].toLowerCase() + m[3], u] : [u];
  });
  return [...new Set(cased.flatMap((u) => [u, u.endsWith("/") ? u.slice(0, -1) : `${u}/`]))];
}

/**
 * Every spelling of one scope worth a tag filter's while, canonical first and as typed beside
 * it: `isbn:` drops hyphens, `geo:` and `doi:` are lowercase, `isan:` is uppercase and also
 * asked as its 5-segment root, and `podcast:publisher:` takes a `guid:` segment.
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

/** How many results a secondary filter of a union may return; the store applies `limit` per filter. */
const sideLimit = (limit) => Math.max(4, Math.round(limit / 4));

/**
 * The `sort:` the store will apply to a search string, or `""`: last one wins, a quoted span is
 * a phrase, and the key is case-sensitive and must start a token.
 */
export function effectiveSort(searchString) {
  let out = "";
  // An unclosed quote runs to the end of the string, as the lexer reads it.
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
 * The typed string as the REQ the page sends: NIP-01 filters ORed in one subscription. `kinds`
 * is the tab's (null for all), `limit` the page's, `searchString(terms)` the caller's NIP-50
 * builder. A hashtag asks `t`, `i`/`I` and `l`; a scope asks the comment question alone; a group
 * asks `h` plus its kind-39000 metadata. Only the hashtag comment filters are gated on the tab.
 */
export function buildFilters(text, { kinds = null, limit, searchString = (t) => t } = {}) {
  const q = parseQuery(text);
  const base = {};
  const search = searchString(q.terms);
  if (search.trim()) base.search = search;
  if (kinds) base.kinds = kinds;
  if (q.authors.length) base.authors = q.authors;
  if (q.mentions.length) base["#p"] = q.mentions;
  // On `base`, so the window rides every filter of a union.
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
 * A prefixed token the caret is inside, or null: `{ field, partial, start, end, complete }`,
 * with offsets into `text` so a pick can splice the finished token in.
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
 * The `since:`/`until:` token the caret is inside, which the calendar opens on; complete once it is
 * a real day.
 */
export const dateAt = (text, caret) => partialAt(text, caret, PARTIAL_DAY, (v, f) => dayBound(v, f) != null);

/**
 * The `group:` token the caret is inside, which the group picker asks. Never `complete`:
 * `group:gen` is a plausible id and a prefix of `general`, so only a space ends it.
 */
export const groupAt = (text, caret) => partialAt(text, caret, PARTIAL_GROUP, () => false);
