// The pager: which prefix each page asks for, where the cut falls, how a
// widened answer folds into the one on screen, and the two ways a list can
// end. The rules that need a DOM are held against app.js's source, the way
// filters.test.mjs holds its bijection.
import assert from "assert";
import { readFileSync } from "node:fs";

const {
  PAGE_SIZE, PRELOAD_PAGES, MAX_ASK, MAX_PAGES,
  firstAsk, askLimit, pageOf, pageCount, covered, canGrow, lastPage, mergePages, drained,
} = await import(new URL("../../main/resources/web/paging.js", import.meta.url));

const app = readFileSync(new URL("../../main/resources/web/app.js", import.meta.url), "utf8");
const html = readFileSync(new URL("../../main/resources/index.html", import.meta.url), "utf8");

const id = (n) => String(n).padStart(64, "0");
const ev = (n) => ({ id: id(n), pubkey: "a".repeat(64), kind: 1, created_at: 1_800_000_000 - n, tags: [] });
const many = (n, from = 0) => Array.from({ length: n }, (_, i) => ev(from + i));

// The page and its preload are one ask: a ranked search costs its match set,
// not its limit, so two asks for one answer are two full searches.
assert.strictEqual(firstAsk(0), askLimit(0), "the first ask of a search already covers the preload");
assert.strictEqual(askLimit(0), PAGE_SIZE * (1 + PRELOAD_PAGES), "…which is the page and three more");
assert.strictEqual(PRELOAD_PAGES, 3, "three pages ahead is the promise the pager makes");
// shared/asks.js reuses an answer only at an identical width.
assert.ok(/search\(text, askLimit\(0\), false, abort\.signal\)/.test(app), "the popup asks at the results view's first-ask width");

// A restored `?q=cats&page=4`: the page being restored is the one in front of the reader.
assert.strictEqual(firstAsk(3), PAGE_SIZE * 7, "a deep-linked page is reached by the FIRST ask, with its three ahead");
assert.strictEqual(askLimit(3), PAGE_SIZE * 7, "…and nothing is left to fetch behind it");

assert.strictEqual(firstAsk(999), MAX_ASK, "no ask goes past the ceiling");
assert.strictEqual(askLimit(999), MAX_ASK, "…including the preload's");
assert.strictEqual(MAX_PAGES, MAX_ASK / PAGE_SIZE, "the ceiling in pages is the same ceiling");
assert.ok(MAX_ASK <= 5000, "a REQ's limit is capped at 5,000 by the relay (EnvSettings.maxLimit)");
assert.ok(canGrow(PAGE_SIZE) && !canGrow(MAX_ASK), "canGrow is about the ASK, not about the corpus");

const buf = many(95);
assert.strictEqual(pageOf(buf, 0).length, PAGE_SIZE, "a full page is a full page");
assert.strictEqual(pageOf(buf, 0)[0].id, id(0), "…starting at the top of the buffer");
assert.strictEqual(pageOf(buf, 1)[0].id, id(PAGE_SIZE), "page two starts where page one ended");
assert.strictEqual(pageOf(buf, 2).length, 95 - 2 * PAGE_SIZE, "the last page is whatever is left");
assert.deepStrictEqual(pageOf(buf, 3), [], "a page past the buffer is empty, not a throw");
assert.deepStrictEqual(pageOf(undefined, 0), [], "…and neither is no buffer at all");
assert.strictEqual(pageCount(buf), 3, "95 results are three pages");
assert.strictEqual(pageCount([]), 0, "and nothing is NO pages — not one empty one");

// Coverage is measured in pages, not events: NIP-01's `limit` is per filter,
// so a hashtag search's four filters can answer an ask of forty with 160.
assert.ok(covered(many(PAGE_SIZE * 4), 0), "four pages held is three pages ahead of page one");
assert.ok(!covered(many(PAGE_SIZE * 4), 1), "…and only two ahead of page two");
assert.ok(covered(many(PAGE_SIZE * 3 + 1), 0), "a part page counts as a page — it is one the reader can turn to");
assert.ok(!covered(many(PAGE_SIZE * 3), 0), "…and three whole ones are one short, however full they are");

const held = many(PAGE_SIZE * 3);
assert.strictEqual(
  lastPage(held, { exhausted: false, asked: PAGE_SIZE * 3 }), 3,
  "a page past the buffer is offered while there is anything left to ask: that is how a reader outruns the preload",
);
assert.strictEqual(
  lastPage(held, { exhausted: true, asked: PAGE_SIZE * 3 }), 2,
  "…but never once the relay has proved there is nothing there",
);
assert.strictEqual(
  lastPage(held, { exhausted: false, asked: MAX_ASK }), 2,
  "…nor once the ask has stopped growing, which is a different reason and the note says so",
);
assert.strictEqual(lastPage([], { exhausted: false, asked: 0 }), 0, "an empty answer is page one and nothing else");

// Pages already held keep their order: a trust-ranked event published while
// the reader reads can land on page one, and taking the new order wholesale
// renumbers the page under them.
const held40 = many(40);
const widened = [ev(500), ...many(40), ...many(20, 40)];   // one new arrival ranked first
const grown = mergePages(held40, widened);
assert.strictEqual(grown.length, 61, "everything new is kept, and nothing is kept twice");
assert.deepStrictEqual(
  grown.slice(0, 40).map((e) => e.id), held40.map((e) => e.id),
  "the page already on screen does not move",
);
assert.strictEqual(grown[40].id, id(500), "a late arrival lands at the end of the buffer, not at the top of the page");
assert.deepStrictEqual(mergePages(held40, held40).map((e) => e.id), held40.map((e) => e.id), "a repeat answer adds nothing");
assert.deepStrictEqual(mergePages([], [null, {}, ev(1)]).map((e) => e.id), [id(1)], "junk with no id is not an event");
assert.deepStrictEqual(mergePages(undefined, undefined), [], "and neither half has to be there at all");

// Both ends need EOSE: a timed-out read came back short because we stopped listening.
assert.ok(drained({ complete: true, got: 12, asked: 40, added: 12 }), "short of the prefix it asked for: that is the end");
assert.ok(drained({ complete: true, got: 200, asked: 40, added: 0 }), "a longer ask that brought nothing new is also the end");
assert.ok(!drained({ complete: false, got: 12, asked: 40, added: 12 }), "a TIMED-OUT read proves nothing about the corpus");
assert.ok(!drained({ complete: true, got: 40, asked: 40, added: 40 }), "a full answer is a reason to ask for more");

const renderResults = app.slice(app.indexOf("function renderResults()"));
assert.ok(
  /pageOf\(s\.hits, s\.page\)/.test(renderResults.slice(0, renderResults.indexOf("\n}"))),
  "renderResults() must draw ONE page of the buffer",
);

// The page is written by currentUrl() and read back by applyUrl(), like the three filters.
assert.ok(/p\.set\("page"/.test(app), "currentUrl() never writes ?page= — a page four does not survive a share");
assert.ok(/p\.get\("page"\)/.test(app), "…and applyUrl() never reads it back, so it does not survive a reload");
assert.ok(
  /runFull\(text, pageParam\(p\)\)/.test(app),
  "the restore must hand the page to runFull(), or a deep link opens at page one",
);

// Exhaustion is decided on the relay's count, not on what survived the page's
// own de-duplication: a relay that does not dedupe across filters would end the pager early.
const drainedCalls = [...app.matchAll(/drained\(\{[^}]*\}/gs)];
assert.strictEqual(drainedCalls.length, 2, "app.js decides exhaustion in two places: the first answer and each widening");
for (const c of drainedCalls) {
  assert.ok(/got: found\.got/.test(c[0]), `drained() must be given the relay's own count: ${c[0].replace(/\s+/g, " ").slice(0, 90)}`);
}

// requestId is what every in-flight lookup of the first ask checks itself
// against; bumping it drops the names and pills out of the page being read.
const preload = app.slice(app.indexOf("async function preload()"));
assert.ok(
  !/requestId\+\+/.test(preload.slice(0, preload.indexOf("\n}\n"))),
  "preload() must not invalidate the answer it is extending",
);

// The pager is redrawn with the cards, so a listener bound to a button dies with the page it turned.
assert.ok(/closest\("\.pg"\)/.test(app), "the pager's clicks must be delegated off the list");
assert.ok(/\.pg \{/.test(html), "…and index.html must style what it draws");

console.log(
  `paging: ${PAGE_SIZE} to a page, ${PRELOAD_PAGES} fetched ahead, ceiling ${MAX_ASK} (${MAX_PAGES} pages) — the cut, the fold and the two ends hold`,
);
