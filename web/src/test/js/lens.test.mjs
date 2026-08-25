import assert from 'assert';
const { SPAM_TOKEN, declaresLens, withoutLens, withoutLensAll } =
  await import(new URL("../../main/resources/web/shared/lens.js", import.meta.url));

// The relay refuses an unauthenticated read that names no lens, so what these
// two functions decide is whether an ask comes back at all. Every claim below
// is one the relay's LensRequiredPolicy makes on the other side of the socket.

const HEX = "4".repeat(64);

// ---- what counts as a declaration -----------------------------------------
assert.ok(!declaresLens({ kinds: [1] }), "a plain NIP-01 filter names no lens");
assert.ok(!declaresLens({ kinds: [1], search: "alice" }), "words are not a lens");
assert.ok(declaresLens({ search: SPAM_TOKEN }), "the waiver alone is a declaration");
assert.ok(declaresLens({ search: "alice include:spam" }), "…and beside words");
assert.ok(declaresLens({ search: `alice observer:${HEX}` }), "an observer is the other one");
assert.ok(declaresLens({ search: `observer:${HEX.toUpperCase()}` }), "hex compares either case");

// The near-misses, each of which the store would ALSO refuse to read as a lens
// — the point of matching the same shapes it does rather than a loose one.
assert.ok(!declaresLens({ search: "include:spammy" }), "a longer word starting with the token is a word");
assert.ok(!declaresLens({ search: "xinclude:spam" }), "…and so is one ending with it");
assert.ok(!declaresLens({ search: "observer:npub1qqqq" }), "bech32 here ranks nothing, so it declares nothing");
assert.ok(!declaresLens({ search: "observer:" + "4".repeat(63) }), "63 hex is not a pubkey");
assert.ok(!declaresLens({ search: null }), "a null search is no search");
assert.ok(!declaresLens(null), "and no filter is no lens");

// ---- waiving one ----------------------------------------------------------
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
// Idempotent, because a lensless connection stamps on the way out and a caller
// may hand it a filter it built with the token already in place.
const once = withoutLens({ kinds: [1] });
assert.strictEqual(withoutLens(once), once, "already declared: the very same object back");
// The one case where stamping would be WRONG rather than redundant: appending
// the waiver to a lensed read lifts the trust floor the lens is there to apply.
const lensed = { search: `alice observer:${HEX}` };
assert.strictEqual(withoutLens(lensed), lensed, "a filter naming an observer is never given the waiver");

// ---- the array form NIP-01 ORs inside one subscription ---------------------
const union = withoutLensAll([{ "#t": ["nostr"] }, { "#l": ["nostr"] }]);
assert.ok(union.every((f) => declaresLens(f)), "EVERY filter of a union declares: the relay refuses the REQ if one does not");
assert.strictEqual(withoutLensAll({ kinds: [1] }).search, SPAM_TOKEN, "…and one filter is still one filter");

console.log("lens: declaration + waiver over filters and unions, all assertions passed");
