// A dropped answer must leave its view's state consistent: run() superseded from outside
// clears `loading`, and a widening begun before a reset writes nothing. The functions are
// lifted out of app.js's source and run against stubs, since app.js needs a page to load.
import assert from "assert";
import { readFileSync } from "node:fs";

const paging = await import(new URL("../../main/resources/web/paging.js", import.meta.url));
const app = readFileSync(new URL("../../main/resources/web/app.js", import.meta.url), "utf8");

/** The source of a top-level function starting with [head], closing brace included. */
function lift(head) {
  const start = app.indexOf(head);
  assert.notStrictEqual(start, -1, `app.js has no ${head}`);
  return app.slice(start, app.indexOf("\n}", start) + 2);
}

const src = ["async function run(", "function superseded(", "async function preload()", "function resetPages()"].map(lift).join("\n");
const s = { requestId: 0, hits: [], loading: false, error: null, pages: 0 };
const stubs = {
  ...paging, s, performance: { now: () => 0 }, paintLate: () => {}, settlePage: () => false,
  renderResults: () => {}, repaintPager: () => {}, document: { querySelector: () => null },
};
const { run, preload, resetPages } =
  new Function(...Object.keys(stubs), `${src}\nreturn { run, preload, resetPages };`)(...Object.values(stubs));

function deferred() {
  let resolve;
  const promise = new Promise((r) => { resolve = r; });
  return { promise, resolve };
}
const answer = (n) => ({ events: Array.from({ length: n }, (_, i) => ({ id: String(i).padStart(64, "0") })), complete: true });
const tick = () => new Promise((r) => setTimeout(r, 0));

// A view change bumps the popup's requestId without starting a run of its own.
const pop = { requestId: 0, hits: [], loading: false, error: null };
let d = deferred();
let p = run(pop, () => d.promise, true, () => {});
assert.strictEqual(pop.loading, true);
pop.requestId++;
d.resolve(answer(3));
assert.strictEqual(await p, false, "a superseded run is not live");
assert.strictEqual(pop.loading, false, "…and does not leave the popup loading forever");
assert.deepStrictEqual(pop.hits, [], "…nor paint its answer");

// Superseded by a newer run: that run owns `loading` until it lands.
const first = deferred(), second = deferred();
const p1 = run(pop, () => first.promise, true, () => {});
const p2 = run(pop, () => second.promise, true, () => {});
first.resolve(answer(1));
assert.strictEqual(await p1, false);
assert.strictEqual(pop.loading, true, "the newer run is still in flight");
second.resolve(answer(2));
assert.strictEqual(await p2, true);
assert.strictEqual(pop.loading, false);
assert.strictEqual(pop.hits.length, 2, "the newer answer is the one drawn");

// The same for a run that fails after it was superseded.
d = deferred();
p = run(pop, () => d.promise.then(() => { throw new Error("closed"); }), true, () => {});
pop.requestId++;
d.resolve();
assert.strictEqual(await p, false);
assert.strictEqual(pop.loading, false);
assert.strictEqual(pop.error, null, "a superseded failure is not this view's error");

// A widening in flight across a reset: the old one lands short and must not mark the new view's
// list exhausted, shrink `asked`, or free the flag the new widening holds.
function viewOnPage1(ask) {
  resetPages();
  s.more = ask; s.page = 1; s.asked = paging.askLimit(0); s.hits = answer(paging.askLimit(0)).events; s.error = null;
}
const old = deferred(), cur = deferred();
viewOnPage1(() => old.promise);
const oldRun = preload();
assert.strictEqual(s.preloading, true);
viewOnPage1(() => cur.promise);
const curRun = preload();
assert.strictEqual(s.preloading, true, "the new view widens");
old.resolve(answer(0));
await oldRun; await tick();
assert.strictEqual(s.exhausted, false, "a stale short answer must not end the new list");
assert.strictEqual(s.asked, paging.askLimit(0), "…nor move its pager");
assert.strictEqual(s.preloading, true, "…nor free the flag the new widening holds");
cur.resolve(answer(paging.askLimit(1)));
await curRun; await tick();
assert.strictEqual(s.asked, paging.askLimit(1), "the current widening lands");
assert.strictEqual(s.preloading, false);

console.log("supersede: a dropped run clears its loading, and a widening from an older view writes nothing");
