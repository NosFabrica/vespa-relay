// The search box's own language — `from:`/`to:`, `since:`/`until:` and
// `#hashtag`: what the field draws, and what the relay is asked.
//
// This is the whole feature's contract in one place. The field renderer and
// the query builder are in different modules and must agree EXACTLY about
// where a token starts and ends — the renderer splices text at character
// offsets to keep the caret still, and the builder lifts the same spans out
// into filters. They agree by both calling shared/query.js, and that is what
// is asserted here rather than any particular arrangement of the DOM.
import assert from "assert";

const { tokenize, parseQuery, mentionAt, dateAt, isKey, tagValues, buildFilters, drawable, dayBound, ymd } =
  await import(new URL("../../relay/src/main/resources/web/shared/query.js", import.meta.url));

// Real npubs, minted by the page's own encoder from these hex keys.
const HEX_A = "a".repeat(64);
const HEX_B = "b".repeat(64);
const A = "npub1424242424242424242424242424242424242424242424242424qamrcaj";
const B = "npub1hwamhwamhwamhwamhwamhwamhwamhwamhwamhwamhwamhwamhwasxw04hu";

// ---- tokenize: what the field draws ---------------------------------------

const kinds = (t) => tokenize(t).map((s) => (s.type === "text" ? s.text : `${s.field || "-"}:${s.pubkey.slice(0, 2)}`));

assert.deepStrictEqual(kinds(`from:${A}`), ["from:aa"], "a from: token is one key segment");
assert.deepStrictEqual(kinds(`to:${A}`), ["to:aa"], "…and so is a to:");
assert.deepStrictEqual(kinds(A), ["-:aa"], "a bare npub still renders as a person");
assert.deepStrictEqual(
  kinds(`cats from:${A} to:${B} dogs`),
  ["cats ", "from:aa", " ", "to:bb", " dogs"],
  "text and keys interleave, and the text between two adjacent tokens survives",
);

// `raw` must be the token EXACTLY as typed — the renderer measures the caret
// in characters of it, so a normalized or re-encoded token would shift every
// offset after it by however much it changed.
const raw = tokenize(`hi from:${A}!`).find((s) => s.type === "key");
assert.strictEqual(raw.raw, `from:${A}`, "raw is the token verbatim");
assert.strictEqual(tokenize(`hi from:${A}!`).at(-1).text, "!", "punctuation after an npub is punctuation");

// A corrupted identifier is text. Anything else would put a face and a name on
// a key nobody holds — the rule nip19.js's decoder states for entity pages.
const BROKEN = A.slice(0, -1) + "q";
assert.deepStrictEqual(tokenize(`from:${BROKEN}`).map((s) => s.type), ["text"], "a bad checksum stays text");
assert.deepStrictEqual(tokenize("from:alice").map((s) => s.type), ["text"], "a half-typed name is not a key");

// The lead anchor: a token has to start a word, or a url with `to:` in it
// would become a filter.
assert.deepStrictEqual(tokenize(`x${A}`).map((s) => s.type), ["text"], "an npub glued to a word is not a token");

// ---- parseQuery: what the relay is asked ----------------------------------

let q = parseQuery(`cats from:${A} to:${B} dogs`);
assert.strictEqual(q.terms, "cats dogs", "the tokens leave the NIP-50 search entirely");
assert.deepStrictEqual(q.authors, [HEX_A], "from: is an authors filter");
assert.deepStrictEqual(q.mentions, [HEX_B], "to: is a #p filter");

q = parseQuery(`from:${A} from:${B} from:${A}`);
assert.deepStrictEqual(q.authors, [HEX_A, HEX_B], "repeats collapse; two authors are two authors");
assert.strictEqual(q.terms, "", "a person-only query has no words left");

q = parseQuery(`  from:${A}   cats  `);
assert.strictEqual(q.terms, "cats", "the hole a lifted token leaves does not become whitespace in the query");

q = parseQuery(A);
assert.deepStrictEqual([q.authors, q.mentions], [[], []], "a BARE npub is not a filter — rendering is not semantics");
assert.strictEqual(q.terms, A, "…it stays the search term it has always been");

q = parseQuery("from:alice cats");
assert.strictEqual(q.terms, "from:alice cats", "an unresolvable from: is left alone, so the search still says why");

// ---- parseQuery: hashtags -------------------------------------------------
//
// A hashtag is a TAG question, not a text one: a note tagged `t: nostr` need
// not contain the word, so leaving `#nostr` in the NIP-50 search would answer
// a different question — and miss every note that tagged the topic properly.

q = parseQuery("#nostr");
assert.deepStrictEqual(q.hashtags, ["nostr"], "a hashtag leaves the search string and becomes a tag filter");
assert.strictEqual(q.terms, "", "…so a hashtag-only query has no words left");

q = parseQuery("cats #Nostr dogs");
assert.strictEqual(q.terms, "cats dogs", "the hole a lifted hashtag leaves does not become whitespace");
assert.deepStrictEqual(q.hashtags, ["nostr"], "lowercased — that is the value a `t` tag carries (NIP-24)");

q = parseQuery("#nostr #NOSTR #bitcoin");
assert.deepStrictEqual(q.hashtags, ["nostr", "bitcoin"], "repeats collapse after normalizing, and order is what was typed");

// `\w` would have cut this to `caf` and filtered for half a word.
assert.deepStrictEqual(parseQuery("#café").hashtags, ["café"], "a hashtag is unicode letters, not ASCII");
assert.deepStrictEqual(parseQuery("#日本").hashtags, ["日本"], "…in any script");

// The same word-start anchor the person tokens use: a `#` mid-word is
// punctuation, and treating it otherwise silently rewrites the query.
assert.deepStrictEqual(parseQuery("C# rocks").hashtags, [], "a # inside a word is not a hashtag");
assert.strictEqual(parseQuery("C# rocks").terms, "C# rocks", "…and the term keeps it");
assert.deepStrictEqual(parseQuery("https://x.example/a#frag").hashtags, [], "a url fragment is not a hashtag");
assert.deepStrictEqual(parseQuery("# spaced").hashtags, [], "a lone # is not a hashtag");

// Hashtags and people compose — both are filters, and both leave the words.
q = parseQuery(`cats from:${A} #nostr`);
assert.deepStrictEqual([q.terms, q.authors, q.hashtags], ["cats", [HEX_A], ["nostr"]], "person and topic narrow the same search");

// A HYPHEN belongs to the tag. It was excluded, and the leftover became a
// NIP-50 exclusion: `#covid-19` asked for the topic and then filtered OUT
// everything containing "19" — the query silently inverted on itself.
assert.deepStrictEqual(parseQuery("#covid-19").hashtags, ["covid-19"], "a hyphen is part of the tag");
assert.strictEqual(parseQuery("#covid-19").terms, "", "…so nothing is left behind to become `-19`");
assert.deepStrictEqual(parseQuery("#nostr- x").hashtags, ["nostr"], "a trailing hyphen is punctuation, not tag");

// Punctuation the lift STRANDS is not a search term. Left in, `#bitcoin.` also
// changed the filter's shape: a non-empty `terms` is what decides whether
// `search` is sent, so a full stop turned a tag read into a text query for ".".
assert.strictEqual(parseQuery("#bitcoin.").terms, "", "a stranded full stop is dropped");
assert.strictEqual(parseQuery("#nostr, cats").terms, "cats", "…and so is a stranded comma");
assert.strictEqual(parseQuery("cats -dogs").terms, "cats -dogs", "but NIP-50's own operators survive");
assert.strictEqual(parseQuery(`a "b c" #x`).terms, `a "b c"`, "…quotes included");
assert.strictEqual(parseQuery("C# rocks").terms, "C# rocks", "…and so does a # the tag rule declined");

// The anchor is any non-word character, not only whitespace: a tag inside
// brackets or quotes is still a tag, and `C#` is still a language.
assert.deepStrictEqual(parseQuery("(#nostr)").hashtags, ["nostr"], "a bracketed hashtag is a hashtag");
assert.strictEqual(parseQuery("(#nostr)").terms, "", "…and the brackets go with it");

assert.deepStrictEqual(parseQuery("#🔥 fire").hashtags, ["🔥"], "an emoji hashtag is a hashtag");
assert.strictEqual(parseQuery("🔥 alone").terms, "🔥 alone", "…while a bare emoji stays a term");

// ---- since:/until: — a day, in the reader's own timezone -------------------
//
// The one token whose value is not what it says: `since:2026-08-06` is a unix
// second, and WHICH second depends on where the reader is and on which of the
// two prefixes carried it. Everything below is asserted against local Date
// arithmetic rather than a fixed epoch, because a test that hardcoded one would
// pass only in the timezone it was written in.

const secs = (y, m, d) => Math.floor(new Date(y, m - 1, d).getTime() / 1000);

assert.strictEqual(dayBound("2026-08-06", "since"), secs(2026, 8, 6), "since: is midnight of its day, local");
assert.strictEqual(dayBound("2026-08-06", "until"), secs(2026, 8, 7) - 1, "until: is the LAST SECOND of its day");
// NIP-01's until is inclusive, so an until that stopped at midnight would
// exclude the day it names: `since:X until:X` would be one second wide.
assert(dayBound("2026-08-06", "until") > dayBound("2026-08-06", "since"), "a day to itself is a day, not an instant");

// The two bounds have to MEET, or a search split at a date boundary loses (or
// double-counts) whatever was written in the gap. Asserted across a whole year
// because that is what catches the two days it could fail on: wherever this
// runs, a local day is 23 or 25 hours long twice a year, and computing the end
// of one as its start plus 86,400 lands an hour inside its neighbour.
for (let d = new Date(2026, 0, 1); d.getFullYear() === 2026; d = new Date(d.getFullYear(), d.getMonth(), d.getDate() + 1)) {
  const next = new Date(d.getFullYear(), d.getMonth(), d.getDate() + 1);
  assert.strictEqual(dayBound(ymd(d), "until") + 1, dayBound(ymd(next), "since"), `${ymd(d)} ends where ${ymd(next)} begins`);
}

assert.strictEqual(ymd(new Date(2026, 7, 6)), "2026-08-06", "a Date writes as the token this language uses");
assert.strictEqual(ymd(new Date(2026, 0, 2)), "2026-01-02", "…zero-padded, both halves");

// A date that is not a day is not a token. The Date constructor ROLLS these
// over rather than failing — 31 February is 3 March, month 13 is next January —
// so without the round-trip check the box would filter for a day nobody typed.
assert.strictEqual(dayBound("2026-02-31", "since"), null, "31 February is not a day");
assert.strictEqual(dayBound("2026-13-01", "since"), null, "…nor is the thirteenth month");
assert.strictEqual(dayBound("0026-01-01", "since"), null, "…nor is a year the two-digit rule would move to 1926");
assert.strictEqual(dayBound("2026-2-3", "since"), null, "the padded spelling only — this is the one the picker writes");
assert.strictEqual(dayBound("", "since"), null, "and nothing is not a day");
assert.strictEqual(dayBound("2026-02-28", "since"), secs(2026, 2, 28), "a real day survives all of that");

let d = tokenize("since:2026-08-06").find((s) => s.type === "date");
assert.deepStrictEqual([d.type, d.field, d.raw], ["date", "since", "since:2026-08-06"], "a date is its own segment");
assert.strictEqual(d.at, secs(2026, 8, 6), "…carrying the second, not the spelling");
assert.strictEqual(tokenize("UNTIL:2026-08-06")[0].field, "until", "the prefix is case-insensitive, like from:/to:");

// A bad date stays TEXT, the same rule a failed npub checksum follows: a chip
// must never claim a filter the reader did not ask for.
assert.deepStrictEqual(tokenize("since:2026-02-31").map((s) => s.type), ["text"], "an impossible day is not a token");
assert.deepStrictEqual(tokenize("since:2026-08").map((s) => s.type), ["text"], "a half-typed date is not a token");
assert.deepStrictEqual(tokenize("since:2026-08-061").map((s) => s.type), ["text"], "…and neither is one digit too many");
assert.deepStrictEqual(tokenize("since:2026-08-06-07").map((s) => s.type), ["text"], "a half-typed RANGE filters for neither half");
assert.deepStrictEqual(tokenize("x since:2026-08-06").at(-1).type, "date", "a date token starts a word");
assert.deepStrictEqual(tokenize("xsince:2026-08-06").map((s) => s.type), ["text"], "…and one glued to a word does not");
assert.strictEqual(tokenize("until:2026-08-06.").at(-1).text, ".", "punctuation after a date is punctuation");

// The three token families interleave, in position order, with their text
// between them intact — the caret invariant below depends on it.
assert.deepStrictEqual(
  tokenize(`cats since:2026-08-06 from:${A} #nostr`).map((s) => s.type),
  ["text", "date", "text", "key", "text", "tag"],
  "dates, people and tags come back in the order they were typed",
);

q = parseQuery("cats since:2026-08-06 dogs");
assert.strictEqual(q.terms, "cats dogs", "a date leaves the NIP-50 search — nobody full-text searches for a day");
assert.strictEqual(q.since, secs(2026, 8, 6), "…and becomes the NIP-01 since");
assert.strictEqual(q.until, null, "one bound is a half-open window, not two");

q = parseQuery("until:2026-08-06");
assert.strictEqual(q.until, secs(2026, 8, 7) - 1, "until: is the other end, and it is inclusive");

// Two of the same prefix cannot be ORed the way two authors are: one filter
// carries one `since`. Every token in this box NARROWS, so the narrower bound
// wins — and that answer is the same whichever order they were typed in.
assert.strictEqual(parseQuery("since:2026-01-01 since:2026-06-01").since, secs(2026, 6, 1), "the later since wins");
assert.strictEqual(parseQuery("since:2026-06-01 since:2026-01-01").since, secs(2026, 6, 1), "…in either order");
assert.strictEqual(parseQuery("until:2026-06-01 until:2026-01-01").until, secs(2026, 1, 2) - 1, "the earlier until wins");

// An IMPOSSIBLE window is a legal query and a distinguishable one. Nothing here
// stops it — the reader may have meant it, and a URL can carry it in — but the
// empty results page reads the two bounds back off the parse so it can say "the
// window is empty" instead of "try a different term", which is advice that
// cannot work.
q = parseQuery("since:2026-08-06 until:2026-01-01");
assert(q.since > q.until, "a crossed window survives the parse, so the page can tell that apart from no matches");

q = parseQuery(`from:${A} #nostr since:2026-01-01 until:2026-12-31 cats`);
assert.deepStrictEqual(
  [q.terms, q.authors, q.hashtags, q.since, q.until],
  ["cats", [HEX_A], ["nostr"], secs(2026, 1, 1), secs(2027, 1, 1) - 1],
  "a window composes with everything else the box can say",
);

// ---- drawable: which tokens the FIELD may pill ----------------------------
//
// A hashtag is a token one character in — `#n` already is one — so a field that
// pilled every tag would re-render itself on every keystroke of one, fighting
// the browser over the caret, the undo stack and IME composition. An npub only
// crosses that line once, at its 63rd character. So the tag under the caret
// stays text and pills when the caret leaves, which is the same rule the people
// picker follows: a token being built is not yet a token.

const drawn = (t, at) => drawable(t, at).map((s) => (s.type === "text" ? s.text : `[${s.raw}]`)).join("");

assert.strictEqual(drawn("cats #nostr", 11), "cats #nostr", "the tag under the caret is still being typed");
assert.strictEqual(drawn("cats #nostr", 8), "cats #nostr", "…anywhere inside it");
assert.strictEqual(drawn("cats #nostr", 6), "cats #nostr", "…including just after the #");
assert.strictEqual(drawn("cats #nostr", 5), "cats [#nostr]", "the caret before the # is not inside it");
assert.strictEqual(drawn("cats #nostr", 0), "cats [#nostr]", "…nor is one further back");
assert.strictEqual(drawn("cats #nostr", null), "cats [#nostr]", "a paste, a restore or a blur draws them all");
assert.strictEqual(drawn("#a #b", 2), "#a [#b]", "only the tag under the caret is held back");
assert.strictEqual(drawn("#a #b", 5), "[#a] #b", "…so moving to the second one pills the first");
assert.strictEqual(drawn("#a #b", 3), "[#a] [#b]", "a caret between them is inside neither");

// A key is NEVER held back: an npub is unreadable, so hiding the chip while the
// caret sits in it would show 63 characters of bech32 exactly when the field is
// narrowest. It also cannot be mid-typed into existence one character at a time.
assert.strictEqual(drawable(`from:${A}`, 5).filter((s) => s.type === "key").length, 1, "a person chip draws under the caret too");

// A date settles like a tag, and needs it more than a tag does: it is a whole
// token at `since:2026-08-06` and text again at `since:2026-08-061`, so a field
// that pilled on sight would flicker one in and out inside a single typed year.
const DATED = "since:2026-08-06";
assert.strictEqual(drawn(DATED, DATED.length), DATED, "the date under the caret is still being typed");
assert.strictEqual(drawn(DATED, 3), DATED, "…anywhere inside it, prefix included");
assert.strictEqual(drawn(DATED, 0), `[${DATED}]`, "the caret before it is not inside it");
assert.strictEqual(drawn(DATED, null), `[${DATED}]`, "and a paste, a restore or a blur pills it");
assert.strictEqual(drawn(`${DATED} cats`, 20), `[${DATED}] cats`, "…as does typing on past it");

// THE caret invariant, and the reason any of this is safe: whatever is drawn,
// the raw text of the segments IS the value, character for character. Every
// offset this feature passes around — the caret, a splice, a drop point — is an
// index into that string, so a segmentation that lost or added a character
// would move the caret by exactly that much.
for (const typed of [
  "cats #nostr dogs", `#a from:${A} #b!`, "#covid-19, x", "(#nostr)", "#🔥 fire", `hi from:${A}`,
  "since:2026-08-06 until:2026-09-01", `#a since:2026-08-06 to:${B} x`, "since:2026-02-31 nope",
]) {
  for (const at of [null, 0, 1, 3, 7, typed.length]) {
    const back = drawable(typed, at).map((s) => (s.type === "text" ? s.text : s.raw)).join("");
    assert.strictEqual(back, typed, `the segments put ${JSON.stringify(typed)} back verbatim at caret ${at}`);
  }
}

// ---- tagValues: the ask has to cover what was written ---------------------
//
// The store matches tag values CASED (its event.sd says so: "a stored
// `t:MixedCase` matched a `#t:["mixedcase"]` filter. Cased matching restores
// byte equality"), and NIP-24 only SAYS tags should be lowercase. So the
// lowercase ask alone cannot see `t: Bitcoin`, and plenty of clients write it.
assert.deepStrictEqual(tagValues("nostr"), ["nostr", "Nostr", "NOSTR"], "every spelling worth asking, lowercase first");
assert.deepStrictEqual(tagValues("NOSTR"), ["NOSTR", "nostr", "Nostr"], "as given comes first, whatever it was");
assert.deepStrictEqual(tagValues("x"), ["x", "X"], "no duplicates for a one-letter tag");
assert.deepStrictEqual(tagValues(""), [], "nothing to ask for an empty tag");

// ---- buildFilters: the REQ itself -----------------------------------------
//
// The page's own state is the argument, not a global: kinds from the tab, limit
// from the view, and the NIP-50 extension string from the sort/spam/lens
// controls. Everything else about the REQ is decided here and asserted here.

const sortRank = (t) => (t ? t + " sort:rank" : " sort:rank");
const plain = (t) => t;
const build = (text, opts) => buildFilters(text, { limit: 40, searchString: plain, ...opts });

let f = build("cats");
assert.deepStrictEqual(f, [{ search: "cats", limit: 40 }], "a word search is one filter, as it always was");

f = build("#nostr");
assert.strictEqual(f.length, 4, "a hashtag asks three questions, in four filters");
assert.deepStrictEqual(f[0], { "#t": ["nostr", "Nostr", "NOSTR"], limit: 40 }, "the tag, at the full limit");
assert.deepStrictEqual(f[1], { "#l": ["nostr", "Nostr", "NOSTR"], limit: 10 }, "the NIP-32 label, at a side limit");
assert.deepStrictEqual(f[2], { kinds: [1111], "#I": ["#nostr", "nostr"], limit: 10 }, "the comment thread's root scope");
assert.deepStrictEqual(f[3], { kinds: [1111], "#i": ["#nostr", "nostr"], limit: 10 }, "…and its parent scope, separately");

// `i` and `I` in ONE filter would AND: a reply below the top of a thread has
// only `I`, and would vanish. This is the whole reason there are four.
assert(f.every((x) => !("#i" in x && "#I" in x)), "the two scopes never share a filter");

// A tab is a kinds filter, and 1111 is not in every tab.
f = build("#nostr", { kinds: [0] });
assert.strictEqual(f.length, 2, "a tab without 1111 sends no comment filters");
assert.deepStrictEqual(f.map((x) => x.kinds), [[0], [0]], "…and the tab's kinds ride on the rest");
f = build("#nostr", { kinds: [1, 11, 1111] });
assert.strictEqual(f.length, 4, "a tab WITH 1111 gets them");
assert.deepStrictEqual(f[2].kinds, [1111], "the comment filters name their own kind");

// The person filters narrow the same search, so they ride on every filter —
// `from:alice #nostr` must not return everybody's comments.
f = build(`from:${A} #nostr`);
assert(f.every((x) => x.authors && x.authors[0] === HEX_A), "every filter of the union carries the author");

// The window is a NIP-01 field like the rest, and rides the union for the same
// reason: `since:… #nostr` narrowed only on the `t` half would hand back this
// week's comments beside last year's notes, in one list, ranked as if they
// answered the same question.
f = build("cats since:2026-08-06 until:2026-08-31");
assert.deepStrictEqual(
  f,
  [{ search: "cats", since: secs(2026, 8, 6), until: secs(2026, 9, 1) - 1, limit: 40 }],
  "a bounded word search is still one filter, with the window on it",
);
f = build("#nostr since:2026-08-06");
assert(f.every((x) => x.since === secs(2026, 8, 6)), "every filter of the union carries the window");
assert(f.every((x) => !("until" in x)), "…and an unset bound is absent, not null");
f = build("cats");
assert(!("since" in f[0]) && !("until" in f[0]), "an unbounded search says nothing about time");

// The volume rule: `limit` is per FILTER, so a union multiplies it. The tag
// filter keeps the full limit; the rest ride at a quarter.
assert.deepStrictEqual(build("#nostr", { limit: 8 }).map((x) => x.limit), [8, 4, 4, 4], "a small limit floors at 4");
assert.deepStrictEqual(build("#nostr", { limit: 40 }).map((x) => x.limit), [40, 10, 10, 10], "…and a page limit divides");

// `search` rides when it CARRIES something. It used to be dropped whenever the
// words were empty, which silently disabled the sort menu, the spam toggle and
// the lens for every hashtag-only query — the store's own mapping says a query
// of nothing but extensions is unconstrained, not match-nothing.
f = buildFilters("#nostr", { limit: 40, searchString: sortRank });
assert(f.every((x) => x.search === " sort:rank"), "the extensions reach every filter of the union");
f = buildFilters("#nostr", { limit: 40, searchString: plain });
assert(f.every((x) => !("search" in x)), "…and an EMPTY string is still omitted, not sent");
f = buildFilters(`from:${A}`, { limit: 40, searchString: sortRank });
assert.deepStrictEqual(f, [{ search: " sort:rank", authors: [HEX_A], limit: 40 }], "the same holds for a person-only query");

// ---- mentionAt: the token being TYPED --------------------------------------

let m = mentionAt("from:ali", 8);
assert.deepStrictEqual([m.field, m.partial, m.start, m.end], ["from", "ali", 0, 8]);
assert.strictEqual(m.complete, false);

m = mentionAt("cats to:bo", 10);
assert.deepStrictEqual([m.field, m.partial, m.start], ["to", "bo", 5], "start is the token, not the space before it");

assert.strictEqual(mentionAt("from:", 5).partial, "", "an empty partial is still a mention — the picker opens on the colon");
assert.strictEqual(mentionAt("cats", 4), null, "plain words are not a mention");
assert.strictEqual(mentionAt("from:ali cats", 13), null, "a finished token is behind the caret, not under it");
assert.strictEqual(mentionAt("from:ali cats", 8).partial, "ali", "…but the caret can come back to it");
assert.strictEqual(mentionAt("from:alice", 7), null, "a caret mid-word must not pop a people list over the sentence");

// A complete key ends the picker: there is nothing left to pick, and the field
// has already drawn it as a face.
m = mentionAt(`from:${A}`, `from:${A}`.length);
assert.strictEqual(m.complete, true, "a decoded npub finishes the token");

// The two must agree about the same string: whatever mentionAt reports as
// COMPLETE is exactly what tokenize turns into a chip and parseQuery turns
// into a filter. A disagreement here is a caret that jumps or a chip that
// searches for its own text.
for (const typed of [`from:${A}`, `to:${B}`, `cats from:${A}`]) {
  const m = mentionAt(typed, typed.length);
  const keys = tokenize(typed).filter((s) => s.type === "key");
  assert.strictEqual(m.complete, keys.length === 1, `complete agrees with tokenize for ${typed}`);
}

// The disagreement that WAS there: pubkeyParam also takes bare hex (the `as=`
// url parameter is typed by hand), so hex after `from:` looked finished to the
// picker while the tokenizer refused to chip it — a filter-shaped thing being
// searched for as literal text. Hex is deliberately UNfinished, so the picker
// resolves it and writes the npub back.
assert.strictEqual(isKey(A), true, "an npub is a finished key");
assert.strictEqual(isKey(HEX_A), false, "bare hex is not — the field speaks npub");
assert.strictEqual(mentionAt(`from:${HEX_A}`, 5 + 64).complete, false, "…so the picker stays up for it");
assert.deepStrictEqual(tokenize(`from:${HEX_A}`).map((s) => s.type), ["text"], "…and nothing is chipped behind its back");
assert.strictEqual(isKey(` ${A} `), true, "surrounding space is not part of the key");
assert.strictEqual(isKey(A + "q"), false, "one character too many is not an npub");

// ---- dateAt: the token the CALENDAR opens on -------------------------------
//
// The same contract mentionAt has, for the same reason: the two pickers share
// one box and one set of arrow keys, so whichever is up has to say exactly
// which characters a pick will replace.

let dt = dateAt("since:", 6);
assert.deepStrictEqual([dt.field, dt.partial, dt.start, dt.end], ["since", "", 0, 6], "the calendar opens on the colon");
assert.strictEqual(dt.complete, false, "…with nothing picked yet");

dt = dateAt("cats until:2026-08", 18);
assert.deepStrictEqual([dt.field, dt.partial, dt.start], ["until", "2026-08", 5], "start is the token, not the space before it");

assert.strictEqual(dateAt("cats", 4), null, "plain words open no calendar");
assert.strictEqual(dateAt("from:ali", 8), null, "…and neither does the other picker's token");
assert.strictEqual(mentionAt("since:2026", 10), null, "…which is mutual: one caret, one token");
assert.strictEqual(dateAt("since:2026-08-06 cats", 21), null, "a finished token is behind the caret, not under it");
assert.strictEqual(dateAt("since:2026-08 x", 9), null, "a caret mid-word must not pop a calendar over the sentence");

// Complete closes the calendar, exactly as a decoded npub closes the people
// list: there is nothing left to pick. A day that does not exist is NOT
// complete — the reader has more typing to do, and tokenize agrees by refusing
// to chip it, which is the disagreement that would otherwise strand a
// filter-shaped thing being searched for as literal text.
assert.strictEqual(dateAt("since:2026-08-06", 16).complete, true, "a real day finishes the token");
assert.strictEqual(dateAt("since:2026-08-0", 15).complete, false, "…and a partial one does not");
assert.strictEqual(dateAt("since:2026-02-31", 16).complete, false, "…nor does an impossible one");
for (const typed of ["since:2026-08-06", "until:2026-02-28", "since:2026-02-31", "until:2026-08-0"]) {
  const at = dateAt(typed, typed.length);
  const dates = tokenize(typed).filter((s) => s.type === "date");
  assert.strictEqual(at.complete, dates.length === 1, `complete agrees with tokenize for ${typed}`);
}

console.log("query: from:/to:, since:/until: and #hashtags tokenize, build their REQ, and complete consistently");
