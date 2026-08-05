// The search box's own language — `from:`/`to:` and `#hashtag`: what the field
// draws, and what the relay is asked.
//
// This is the whole feature's contract in one place. The field renderer and
// the query builder are in different modules and must agree EXACTLY about
// where a token starts and ends — the renderer splices text at character
// offsets to keep the caret still, and the builder lifts the same spans out
// into filters. They agree by both calling shared/query.js, and that is what
// is asserted here rather than any particular arrangement of the DOM.
import assert from "assert";

const { tokenize, parseQuery, mentionAt, isKey } =
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

console.log("query: from:/to: and #hashtags tokenize, filter and complete consistently");
