// The latest feed's three rules: what counts as content, which kinds a chip asks for, and what a
// time-ordered list drops before it can be called "latest".
import assert from "assert";
import { readFileSync } from "node:fs";

// feed.js imports shared/parents.js, whose relay client reads `location` at load.
globalThis.location = { protocol: "http:", host: "localhost:7787" };
globalThis.window = { addEventListener: () => {} };

const { FEED_KINDS, feedKinds, PREVIEW_CARDS, PAGE_CARDS, askFor, pickFeed } =
  await import(new URL("../../main/resources/web/feed.js", import.meta.url));
const { buildFilters } =
  await import(new URL("../../main/resources/web/shared/query.js", import.meta.url));

const NOW = 1_800_000_000;
const id = (n) => String(n).padStart(64, "0");
const ev = (n, over = {}) => ({ id: id(n), pubkey: "a".repeat(64), kind: 1, created_at: NOW - n, tags: [], ...over });

// No `search` field is what makes this a plain NIP-01 read, the only read the store answers
// newest-first. feedSearchString returns "" only when there is a `me`.
const filters = buildFilters("", { kinds: FEED_KINDS, limit: askFor(PAGE_CARDS) });
assert.strictEqual(filters.length, 1, "no hashtags, no union — one filter");
assert.deepStrictEqual(Object.keys(filters[0]).sort(), ["kinds", "limit"], "kinds and limit and nothing else");
assert.deepStrictEqual(filters[0].kinds, FEED_KINDS);

// Signed out, the ask must declare the waiver or the relay refuses it.
const anon = buildFilters("", { kinds: FEED_KINDS, limit: askFor(PAGE_CARDS), searchString: () => "include:spam" });
assert.deepStrictEqual(Object.keys(anon[0]).sort(), ["kinds", "limit", "search"], "the waiver and nothing else joins it");
assert.strictEqual(anon[0].search, "include:spam", "no sort: and no observer: rode along with it");

for (const k of [1, 20, 21, 22]) assert.ok(FEED_KINDS.includes(k), `kind ${k} is content`);
for (const k of [0, 3, 7, 10002, 30166, 30382]) {
  assert.ok(!FEED_KINDS.includes(k), `kind ${k} is not something anybody browses`);
}

assert.deepStrictEqual(feedKinds(null), FEED_KINDS, `"Everything" is the content default, not every kind in the index`);
assert.deepStrictEqual(feedKinds([]), FEED_KINDS, "an empty kinds list is not a filter for nothing");
assert.deepStrictEqual(feedKinds([0]), [0], "a chip REPLACES the default — the newest kind 0s are the latest people");

// The tab table is page state in app.js, parsed the way cards.test.mjs parses it.
const appJs = readFileSync(new URL("../../main/resources/web/app.js", import.meta.url), "utf8");
const tabs = [...appJs.matchAll(/slug:\s*"([a-z]+)",\s*kinds:\s*(null|\[([^\]]*)\])/g)]
  .map((m) => [m[1], m[3] ? m[3].split(",").map((s) => Number(s.trim())).filter(Number.isFinite) : null]);
assert.ok(tabs.length >= 2, "the tab table parsed");
const everything = tabs.filter(([, kinds]) => kinds === null);
assert.strictEqual(everything.length, 1, "exactly one chip means 'no chip'");
for (const [slug, kinds] of tabs) {
  const asked = feedKinds(kinds);
  assert.ok(asked.length, `chip "${slug}" asks for no kinds at all — it would answer "nothing here yet" always`);
  if (kinds) assert.deepStrictEqual(asked, kinds, `chip "${slug}" must ask for its own kinds`);
  const f = buildFilters("", { kinds: asked, limit: askFor(PREVIEW_CARDS) });
  assert.strictEqual(f.length, 1, `chip "${slug}": one filter`);
  assert.deepStrictEqual(Object.keys(f[0]).sort(), ["kinds", "limit"], `chip "${slug}": no search field`);
}

// The chip replaces the default rather than intersecting it.
const inert = tabs.filter(([, kinds]) => kinds && !kinds.some((k) => FEED_KINDS.includes(k)));
assert.ok(
  inert.length >= 3,
  "intersecting the chip with FEED_KINDS is only obviously wrong while several chips share no kind with it: " +
  `found ${inert.length} (${inert.map(([s]) => s).join(", ")})`,
);

// The chip is view state: written by feedUrl(), read back in applyUrl()'s feed branch.
assert.ok(/const feedUrl = .*tab=\$\{t\.slug\}/.test(appJs), "feedUrl() must write the chip into the url");
const feedBranch = appJs.slice(appJs.indexOf(`if (p.get("feed") === "1") {`));
assert.ok(feedBranch.startsWith(`if (p.get("feed") === "1") {`), "applyUrl() has no feed branch");
assert.ok(
  feedBranch.slice(0, feedBranch.indexOf("runFeed();")).includes(`p.get("tab")`),
  "applyUrl()'s feed branch must read ?tab= — otherwise a shared narrowed feed opens as Everything",
);

// The rule hides the panel, not the row, which also carries the chips and the syntax button.
const html = readFileSync(new URL("../../main/resources/index.html", import.meta.url), "utf8");
assert.ok(/body\.feed #adv \{ display: none/.test(html), "the feed hides the Filters panel");
assert.ok(!/body\.feed \.bar(-right)? \{ display: none/.test(html), "…and not the row it sits in, which carries the chips and the syntax button");

// Replies are dropped after the relay answers (NIP-01 cannot ask for their absence), so the ask is widened.
assert.ok(askFor(PREVIEW_CARDS) >= 24, "three cards are asked for as a sample, not as three");
assert.ok(askFor(PAGE_CARDS) > PAGE_CARDS, "…and a hundred are asked for as more than a hundred");

const root = ev(1);
const reply = ev(2, { tags: [["e", id(1), "", "root"]] });
const future = ev(3, { created_at: NOW + 86_400 });
const skewed = ev(4, { created_at: NOW + 60 });

let out = pickFeed([root, reply, future, skewed], 10, NOW).map((e) => e.id);
assert.ok(!out.includes(reply.id), "a reply is a fragment without its thread — dropped");
assert.ok(!out.includes(future.id), "a note dated tomorrow would own the top of the feed forever");
assert.ok(out.includes(skewed.id), "…but a minute of clock skew is a wrong clock, not a claim");
assert.deepStrictEqual(out, [skewed.id, root.id], "newest first");

const voiceReply = ev(5, { kind: 1222, tags: [["e", id(1), "", "reply"]] });
assert.deepStrictEqual(pickFeed([voiceReply], 10, NOW), [], "any feed kind can be a reply");

const quote = ev(6, { tags: [["e", id(1), "", "mention"]] });
assert.deepStrictEqual(pickFeed([quote], 10, NOW).map((e) => e.id), [quote.id], "a quote is not a reply");

assert.deepStrictEqual(pickFeed([root, { ...root }], 10, NOW).length, 1, "duplicates collapse");

// The cap applies after the drops: a promise about cards, not about events received.
assert.strictEqual(pickFeed([reply, ev(7), ev(8), ev(9)], 2, NOW).length, 2, "at most `want` cards");

// entity.js renders events fetched from third-party relay hints, so garbage must not throw.
assert.deepStrictEqual(pickFeed([null, {}, { id: id(9) }], 5, NOW), [], "no id, no date, no card — and no throw");
assert.deepStrictEqual(pickFeed(undefined, 5, NOW), [], "nothing at all is an empty feed");

console.log(`feed: the ask carries no search string, ${tabs.length} chips each pick its kinds, and the shaping holds`);
