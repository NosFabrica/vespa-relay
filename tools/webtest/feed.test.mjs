// The latest feed's three rules: what counts as content, which of those kinds
// a given chip asks for, and what a time-ordered list has to drop before it
// can honestly be called "latest".
//
// The ASK is barely tested here, because there is almost nothing of the feed's
// own in it — it is the page's ordinary buildFilters() with no words and no
// extensions, which query.test.mjs already covers. The exception is `kinds`,
// which the chips now set: that one field is the feed's, so the chip's route
// into it is held below. The rest of what IS the feed is the shaping, and each
// rule there is a thing that would otherwise be on screen: a reply with no
// thread around it, a note dated 2050 nailed to the top of the page forever,
// or the same note twice.
import assert from "assert";
import { readFileSync } from "node:fs";

// feed.js asks shared/parents.js what a reply is, and that module's own import
// of the relay client reads `location` at load. The same two stubs parents'
// suite installs, for the same reason: this is a browser module tree, and
// nothing here opens a socket.
globalThis.location = { protocol: "http:", host: "localhost:7787" };
globalThis.window = { addEventListener: () => {} };

const { FEED_KINDS, feedKinds, PREVIEW_CARDS, PAGE_CARDS, askFor, pickFeed } =
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

// ---- the chip picks the kinds ----------------------------------------------
//
// The chips are the one part of the bar that reaches this view, because
// `kinds` is a field of the plain read rather than an extension on a search
// string. Three claims, and the first two are the whole reason the rule is a
// function instead of `tab.kinds || FEED_KINDS` written at the call site.
assert.deepStrictEqual(feedKinds(null), FEED_KINDS, `"Everything" is the content default, not every kind in the index`);
assert.deepStrictEqual(feedKinds([]), FEED_KINDS, "an empty kinds list is not a filter for nothing");
assert.deepStrictEqual(feedKinds([0]), [0], "a chip REPLACES the default — the newest kind 0s are the latest people");

// Every chip has to move the feed, which is the whole point of this change:
// the chips sat over the landing page's preview doing nothing. Parsed out of
// app.js the same way cards.test.mjs parses the same table — the tabs are page
// state, not a module, and a second copy of them here would be the thing that
// goes stale.
const appJs = readFileSync(new URL("../../relay/src/main/resources/web/app.js", import.meta.url), "utf8");
const tabs = [...appJs.matchAll(/slug:\s*"([a-z]+)",\s*kinds:\s*(null|\[([^\]]*)\])/g)]
  .map((m) => [m[1], m[3] ? m[3].split(",").map((s) => Number(s.trim())).filter(Number.isFinite) : null]);
assert.ok(tabs.length >= 2, "the tab table parsed");
const everything = tabs.filter(([, kinds]) => kinds === null);
assert.strictEqual(everything.length, 1, "exactly one chip means 'no chip'");
for (const [slug, kinds] of tabs) {
  const asked = feedKinds(kinds);
  assert.ok(asked.length, `chip "${slug}" asks for no kinds at all — it would answer "nothing here yet" always`);
  if (kinds) assert.deepStrictEqual(asked, kinds, `chip "${slug}" must ask for its own kinds`);
  // …and whatever the chip asks for, the ask stays a plain NIP-01 read.
  const f = buildFilters("", { kinds: asked, limit: askFor(PREVIEW_CARDS) });
  assert.strictEqual(f.length, 1, `chip "${slug}": one filter`);
  assert.deepStrictEqual(Object.keys(f[0]).sort(), ["kinds", "limit"], `chip "${slug}": no search field`);
}

// And the measurement that decided REPLACE over intersect: narrowing the
// content default by the chip would leave several chips asking for nothing at
// all. Pinned rather than described, because a later edit to either table is
// exactly when somebody would reach for the intersection again.
const inert = tabs.filter(([, kinds]) => kinds && !kinds.some((k) => FEED_KINDS.includes(k)));
assert.ok(
  inert.length >= 3,
  "intersecting the chip with FEED_KINDS is only obviously wrong while several chips share no kind with it: " +
  `found ${inert.length} (${inert.map(([s]) => s).join(", ")})`,
);

// ---- a narrowed feed is a place ---------------------------------------------
//
// `?feed=1` used to be the whole of this view's state, so applyUrl() cleared
// the tab along with the sort and the lens. Now the chip is state too, and a
// view whose state cannot be linked to dies on reload — the same lie as a
// control that does nothing, one step later. Written by feedUrl() and read
// back in applyUrl()'s feed branch: a half either side does not know about is
// a link that loses the chip, or a chip nothing can share.
assert.ok(/const feedUrl = .*tab=\$\{t\.slug\}/.test(appJs), "feedUrl() must write the chip into the url");
const feedBranch = appJs.slice(appJs.indexOf(`if (p.get("feed") === "1") {`));
assert.ok(feedBranch.startsWith(`if (p.get("feed") === "1") {`), "applyUrl() has no feed branch");
assert.ok(
  feedBranch.slice(0, feedBranch.indexOf("runFeed();")).includes(`p.get("tab")`),
  "applyUrl()'s feed branch must read ?tab= — otherwise a shared narrowed feed opens as Everything",
);

// The chips have to be ON SCREEN on the feed page for any of that to be
// reachable: the whole bar used to be hidden there, which took the one working
// control off the page along with the three that could not work on it.
//
// So the rule names the PANEL — the three controls that ride on a search
// string this view does not send — and not the row they sit in, which also
// carries the chips and the Syntax sheet's button. Both of those act here: a
// chip narrows the feed, and every prefix the sheet explains can be typed into
// the box above it.
const html = readFileSync(new URL("../../relay/src/main/resources/index.html", import.meta.url), "utf8");
assert.ok(/body\.feed #adv \{ display: none/.test(html), "the feed hides the Filters panel");
assert.ok(!/body\.feed \.bar(-right)? \{ display: none/.test(html), "…and not the row it sits in, which carries the chips and the syntax button");

// ---- the over-ask ---------------------------------------------------------
// Replies are dropped AFTER the relay answers, because NIP-01 cannot ask for
// their absence. A preview that asked for exactly three cards would routinely
// draw one, so the ask is widened — and the floor is what does the work at the
// preview's size, not the multiplier.
assert.ok(askFor(PREVIEW_CARDS) >= 24, "three cards are asked for as a sample, not as three");
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

console.log(`feed: the ask carries no search string, ${tabs.length} chips each pick its kinds, and the shaping holds`);
