// The latest feed's two rules: what counts as content, and what a time-ordered
// list has to drop before it can honestly be called "latest".
//
// The ASK is not tested here, because there is nothing of the feed's own in it
// — it is the page's ordinary buildFilters() with no words and no extensions,
// which query.test.mjs already covers. What IS the feed is the shaping, and
// each rule below is a thing that would otherwise be on screen: a reply with
// no thread around it, a note dated 2050 nailed to the top of the page
// forever, or the same note twice.
import assert from "assert";

// feed.js asks shared/parents.js what a reply is, and that module's own import
// of the relay client reads `location` at load. The same two stubs parents'
// suite installs, for the same reason: this is a browser module tree, and
// nothing here opens a socket.
globalThis.location = { protocol: "http:", host: "localhost:7787" };
globalThis.window = { addEventListener: () => {} };

const { FEED_KINDS, HOME_CARDS, PAGE_CARDS, askFor, pickFeed } =
  await import(new URL("../../relay/src/main/resources/web/feed.js", import.meta.url));
const { buildFilters } =
  await import(new URL("../../relay/src/main/resources/web/shared/query.js", import.meta.url));

const NOW = 1_800_000_000;
const id = (n) => String(n).padStart(64, "0");
const ev = (n, over = {}) => ({ id: id(n), pubkey: "a".repeat(64), kind: 1, created_at: NOW - n, tags: [], ...over });

// ---- an empty search IS the feed's ask ------------------------------------
// The one claim worth pinning about the ask: with no words and no
// searchString, the page's own builder produces a filter carrying NO `search`
// field. That is what makes it a plain NIP-01 read, which is the only kind of
// read the store answers newest-first — a filter that had picked up a `sort:`
// or an `observer:` on the way here would come back in rank order under a
// heading that says "latest".
const filters = buildFilters("", { kinds: FEED_KINDS, limit: askFor(PAGE_CARDS) });
assert.strictEqual(filters.length, 1, "no hashtags, no union — one filter");
assert.deepStrictEqual(Object.keys(filters[0]).sort(), ["kinds", "limit"], "kinds and limit and nothing else");
assert.deepStrictEqual(filters[0].kinds, FEED_KINDS);

// ---- the kinds ------------------------------------------------------------
for (const k of [1, 20, 21, 22]) assert.ok(FEED_KINDS.includes(k), `kind ${k} is content`);
// The index is mostly these, and a reader browsing "latest" wants none of them.
for (const k of [0, 3, 7, 10002, 30166, 30382]) {
  assert.ok(!FEED_KINDS.includes(k), `kind ${k} is not something anybody browses`);
}

// ---- the over-ask ---------------------------------------------------------
// Replies are dropped AFTER the relay answers, because NIP-01 cannot ask for
// their absence. A preview that asked for exactly three cards would routinely
// draw one, so the ask is widened — and the floor is what does the work at the
// preview's size, not the multiplier.
assert.ok(askFor(HOME_CARDS) >= 24, "three cards are asked for as a sample, not as three");
assert.ok(askFor(PAGE_CARDS) > PAGE_CARDS, "…and a hundred are asked for as more than a hundred");

// ---- the shaping ----------------------------------------------------------
const root = ev(1);
const reply = ev(2, { tags: [["e", id(1), "", "root"]] });
const future = ev(3, { created_at: NOW + 86_400 });
const skewed = ev(4, { created_at: NOW + 60 });

let out = pickFeed([root, reply, future, skewed], 10, NOW).map((e) => e.id);
assert.ok(!out.includes(reply.id), "a reply is a fragment without its thread — dropped");
assert.ok(!out.includes(future.id), "a note dated tomorrow would own the top of the feed forever");
assert.ok(out.includes(skewed.id), "…but a minute of clock skew is a wrong clock, not a claim");
assert.deepStrictEqual(out, [skewed.id, root.id], "newest first");

// Replies are dropped by NIP-10's rule, whatever kind carries them: a voice
// reply (1222 with an `e`) is as much a fragment as a kind 1 one, and both
// answer to the same shared/parents.js the "in reply to" line asks.
const voiceReply = ev(5, { kind: 1222, tags: [["e", id(1), "", "reply"]] });
assert.deepStrictEqual(pickFeed([voiceReply], 10, NOW), [], "any feed kind can be a reply");

// A `mention` marker is a quote, not a parent — the same distinction parents.js
// draws — so a note quoting another note is still a post of its own.
const quote = ev(6, { tags: [["e", id(1), "", "mention"]] });
assert.deepStrictEqual(pickFeed([quote], 10, NOW).map((e) => e.id), [quote.id], "a quote is not a reply");

// One event, one card — the same rule the search applies to its own union.
assert.deepStrictEqual(pickFeed([root, { ...root }], 10, NOW).length, 1, "duplicates collapse");

// The cap is applied after the drops, so it is a promise about CARDS rather
// than about how many events the relay happened to send.
assert.strictEqual(pickFeed([reply, ev(7), ev(8), ev(9)], 2, NOW).length, 2, "at most `want` cards");

// Garbage in a list this page did not author must not throw: entity.js renders
// events fetched from third-party relay hints, and format.js documents the
// same rule for the same reason.
assert.deepStrictEqual(pickFeed([null, {}, { id: id(9) }], 5, NOW), [], "no id, no date, no card — and no throw");
assert.deepStrictEqual(pickFeed(undefined, 5, NOW), [], "nothing at all is an empty feed");

console.log("feed: the ask carries no search string, and the shaping holds");
