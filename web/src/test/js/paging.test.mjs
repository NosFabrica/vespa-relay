// The pager: which prefix each page asks the relay for, where the cut falls,
// how a widened answer folds into the one already on screen, and the two
// different ways a list of results can end.
//
// All of it is arithmetic, which is exactly the kind of code that ships with
// an off-by-one nobody sees until page two draws thirty-nine cards. The rules
// that need a DOM are held the way filters.test.mjs holds its bijection —
// against app.js's source — because the ones that matter there are not what
// the arithmetic says, they are whether app.js asked it at all.
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

// ---- the two asks ----------------------------------------------------------
//
// The ask in front of the reader must stay ONE PAGE. This is the whole claim
// of the feature: three pages are fetched ahead, and the reader waits for none
// of them — so a first ask that had quietly become four pages long would have
// moved the cost of the preload back in front of the very answer it exists to
// keep fast.
assert.strictEqual(firstAsk(0), PAGE_SIZE, "the first ask of a search is one page and no more");
assert.strictEqual(askLimit(0), PAGE_SIZE * (1 + PRELOAD_PAGES), "…and the preload behind it covers three more");
assert.strictEqual(PRELOAD_PAGES, 3, "three pages ahead is the promise the pager makes");

// A restored `?q=cats&page=4` is the exception, and it is not an exception to
// the rule above: the page being restored IS the one in front of the reader,
// so the first ask has to reach it or the restore draws a skeleton over a page
// that arrives a round trip later.
assert.strictEqual(firstAsk(3), PAGE_SIZE * 4, "a deep-linked page is reached by the FIRST ask");
assert.strictEqual(askLimit(3), PAGE_SIZE * 7, "…and three more are still fetched behind it");

// The ceiling binds both, or the deep link is a way around it.
assert.strictEqual(firstAsk(999), MAX_ASK, "no ask goes past the ceiling");
assert.strictEqual(askLimit(999), MAX_ASK, "…including the preload's");
assert.strictEqual(MAX_PAGES, MAX_ASK / PAGE_SIZE, "the ceiling in pages is the same ceiling");
assert.ok(MAX_ASK <= 5000, "a REQ's limit is capped at 5,000 by the relay (EnvSettings.maxLimit)");
assert.ok(canGrow(PAGE_SIZE) && !canGrow(MAX_ASK), "canGrow is about the ASK, not about the corpus");

// ---- the cut ---------------------------------------------------------------
const buf = many(95);
assert.strictEqual(pageOf(buf, 0).length, PAGE_SIZE, "a full page is a full page");
assert.strictEqual(pageOf(buf, 0)[0].id, id(0), "…starting at the top of the buffer");
assert.strictEqual(pageOf(buf, 1)[0].id, id(PAGE_SIZE), "page two starts where page one ended");
assert.strictEqual(pageOf(buf, 2).length, 95 - 2 * PAGE_SIZE, "the last page is whatever is left");
assert.deepStrictEqual(pageOf(buf, 3), [], "a page past the buffer is empty, not a throw");
assert.deepStrictEqual(pageOf(undefined, 0), [], "…and neither is no buffer at all");
assert.strictEqual(pageCount(buf), 3, "95 results are three pages");
assert.strictEqual(pageCount([]), 0, "and nothing is NO pages — not one empty one");

// ---- is the buffer far enough ahead? --------------------------------------
//
// Asked in PAGES, not in events. A hashtag search is four filters in one REQ
// and NIP-01's `limit` is per filter, so an ask of forty can come back with a
// hundred and sixty: measured against the ask it would look starved and send a
// round trip for pages it was already holding.
assert.ok(covered(many(PAGE_SIZE * 4), 0), "four pages held is three pages ahead of page one");
assert.ok(!covered(many(PAGE_SIZE * 4), 1), "…and only two ahead of page two");
assert.ok(covered(many(PAGE_SIZE * 3 + 1), 0), "a part page counts as a page — it is one the reader can turn to");
assert.ok(!covered(many(PAGE_SIZE * 3), 0), "…and three whole ones are one short, however full they are");

// ---- how far the pager may offer ------------------------------------------
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

// ---- folding a widened answer in ------------------------------------------
//
// The naive fold is to take the longer answer whole — same query, same order,
// plus a tail. It is not: events are published while a reader reads, and a
// trust-ranked one can land on page one. Taking the new order wholesale
// renumbers the page under the reader — the card they were about to click
// moves, and the page they Back into has different cards on it than when they
// left. So the pages already held keep their order, and anything new goes on
// the end.
const held40 = many(40);
const widened = [ev(500), ...many(40), ...many(20, 40)];   // one new arrival ranked FIRST
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

// ---- when is there nothing more? ------------------------------------------
//
// Both tests need EOSE. A read that timed out came back short because we
// stopped listening, and reading that as the end of the corpus puts a full
// stop after page three of a search over a slow connection.
assert.ok(drained({ complete: true, got: 12, asked: 40, added: 12 }), "short of the prefix it asked for: that is the end");
assert.ok(drained({ complete: true, got: 200, asked: 40, added: 0 }), "a longer ask that brought nothing new is also the end");
assert.ok(!drained({ complete: false, got: 12, asked: 40, added: 12 }), "a TIMED-OUT read proves nothing about the corpus");
assert.ok(!drained({ complete: true, got: 40, asked: 40, added: 40 }), "a full answer is a reason to ask for more");

// ---- and the page that has to ask for all of it ---------------------------
//
// Three claims about app.js, each one a way the arithmetic above can be
// perfectly right and the page still wrong.

// The results view must CUT. It used to render `s.hits` whole, and a buffer
// four pages deep rendered that way is not a pager, it is a longer list with
// numbers under it.
const renderResults = app.slice(app.indexOf("function renderResults()"));
assert.ok(
  /pageOf\(s\.hits, s\.page\)/.test(renderResults.slice(0, renderResults.indexOf("\n}"))),
  "renderResults() must draw ONE page of the buffer",
);

// A page is a place. Written by currentUrl() and read back by applyUrl(),
// exactly as the three filters are — a half either side does not know about is
// a link that loses the page, or a page nothing can share.
assert.ok(/p\.set\("page"/.test(app), "currentUrl() never writes ?page= — a page four does not survive a share");
assert.ok(/p\.get\("page"\)/.test(app), "…and applyUrl() never reads it back, so it does not survive a reload");
assert.ok(
  /runFull\(text, pageParam\(p\)\)/.test(app),
  "the restore must hand the page to runFull(), or a deep link opens at page one",
);

// The preload is the same answer asked for at greater length, so it must NOT
// bump requestId: that is the counter every in-flight lookup of the FIRST ask
// checks itself against, and bumping it drops the names and pills out of the
// page the reader is reading.
const preload = app.slice(app.indexOf("async function preload()"));
assert.ok(
  !/requestId\+\+/.test(preload.slice(0, preload.indexOf("\n}\n"))),
  "preload() must not invalidate the answer it is extending",
);

// The pager is drawn with the cards, so its clicks can only be delegated —
// a listener bound to a button dies with the page that button turned.
assert.ok(/closest\("\.pg"\)/.test(app), "the pager's clicks must be delegated off the list");
assert.ok(/\.pg \{/.test(html), "…and index.html must style what it draws");

console.log(
  `paging: ${PAGE_SIZE} to a page, ${PRELOAD_PAGES} fetched ahead, ceiling ${MAX_ASK} (${MAX_PAGES} pages) — the cut, the fold and the two ends hold`,
);
