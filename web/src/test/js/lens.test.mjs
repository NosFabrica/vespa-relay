import assert from 'assert';
const { SPAM_TOKEN, declaresLens, withoutLens, withoutLensAll } =
  await import(new URL("../../main/resources/web/shared/lens.js", import.meta.url));

// Each claim here mirrors one the relay's LensRequiredPolicy makes on the
// other side of the socket.

const HEX = "4".repeat(64);

assert.ok(!declaresLens({ kinds: [1] }), "a plain NIP-01 filter names no lens");
assert.ok(!declaresLens({ kinds: [1], search: "alice" }), "words are not a lens");
assert.ok(declaresLens({ search: SPAM_TOKEN }), "the waiver alone is a declaration");
assert.ok(declaresLens({ search: "alice include:spam" }), "…and beside words");
assert.ok(declaresLens({ search: `alice observer:${HEX}` }), "an observer is the other one");
assert.ok(declaresLens({ search: `observer:${HEX.toUpperCase()}` }), "hex compares either case");

// Near-misses the store also refuses to read as a lens.
assert.ok(!declaresLens({ search: "include:spammy" }), "a longer word starting with the token is a word");
assert.ok(!declaresLens({ search: "xinclude:spam" }), "…and so is one ending with it");
assert.ok(!declaresLens({ search: "observer:npub1qqqq" }), "bech32 here ranks nothing, so it declares nothing");
assert.ok(!declaresLens({ search: "observer:" + "4".repeat(63) }), "63 hex is not a pubkey");
assert.ok(!declaresLens({ search: null }), "a null search is no search");
assert.ok(!declaresLens(null), "and no filter is no lens");

assert.deepStrictEqual(
  withoutLens({ kinds: [0], authors: [HEX] }),
  { kinds: [0], authors: [HEX], search: SPAM_TOKEN },
  "a plain filter gains the waiver and keeps every field it had",
);
assert.deepStrictEqual(
  withoutLens({ search: "alice" }).search,
  "alice " + SPAM_TOKEN,
  "words are kept and the waiver is appended, never substituted",
);
const once = withoutLens({ kinds: [1] });
assert.strictEqual(withoutLens(once), once, "already declared: the very same object back");
// Appending the waiver to a lensed read would lift the trust floor the lens applies.
const lensed = { search: `alice observer:${HEX}` };
assert.strictEqual(withoutLens(lensed), lensed, "a filter naming an observer is never given the waiver");

const union = withoutLensAll([{ "#t": ["nostr"] }, { "#l": ["nostr"] }]);
assert.ok(union.every((f) => declaresLens(f)), "EVERY filter of a union declares: the relay refuses the REQ if one does not");
assert.strictEqual(withoutLensAll({ kinds: [1] }).search, SPAM_TOKEN, "…and one filter is still one filter");

console.log("lens: declaration + waiver over filters and unions, all assertions passed");
