// The ask cache: Enter after a type-ahead is one ranked search, not two. The
// two app.js rules the reuse depends on (one type-ahead in flight, the latest
// text runs next) are held against app.js's source.
import assert from "assert";
import { readFileSync } from "node:fs";

const { AskCache, askKey, askLimitOf, ASK_FRESH_MS } = await import(new URL("../../main/resources/web/shared/asks.js", import.meta.url));
const app = readFileSync(new URL("../../main/resources/web/app.js", import.meta.url), "utf8");

const f = (limit, extra = {}) => ({ kinds: [1, 11, 1111], search: "bitcoin include:spam", limit, ...extra });

assert.strictEqual(askKey(f(8)), askKey(f(160)), "the limit is not part of the question");
assert.notStrictEqual(askKey(f(8)), askKey(f(8, { search: "bitcoin sort:rank include:spam" })), "the sort is");
assert.notStrictEqual(askKey(f(8)), askKey(f(8, { kinds: [0] })), "so is the tab");
assert.strictEqual(askKey({ search: "x", kinds: [1], limit: 1 }), askKey({ kinds: [1], search: "x", limit: 1 }), "field order does not");
assert.strictEqual(askKey([f(8), f(8, { "#t": ["bitcoin"] })]), askKey([f(160), f(160, { "#t": ["bitcoin"] })]), "a hashtag search's several filters key together");
assert.strictEqual(askLimitOf([f(40), f(160)]), 160, "the width of a list of filters is its widest");

{
  let clock = 1000;
  const cache = new AskCache({ now: () => clock });
  let asked = 0;
  const ask = () => { asked++; return new Promise((r) => setTimeout(() => r(["answer", asked]), 5)); };

  const popup = cache.take(f(160), ask);
  const enter = cache.take(f(160), ask);
  assert.strictEqual(popup, enter, "the same question at the same width, still in flight, is the same promise");
  assert.strictEqual(asked, 1, "…and the relay was asked once");
  assert.deepStrictEqual(await enter, ["answer", 1]);

  cache.take(f(40), ask);
  assert.strictEqual(asked, 2, "a different width is a different ask — a short answer is no prefix a pager can trust");
  cache.take(f(160, { search: "nostr include:spam" }), ask);
  assert.strictEqual(asked, 3, "a different question is a different ask");

  clock += ASK_FRESH_MS + 1;
  cache.take(f(160, { search: "nostr include:spam" }), ask);
  assert.strictEqual(asked, 4, "an answer older than the freshness bound is asked again");

  cache.clear();
  cache.take(f(160, { search: "nostr include:spam" }), ask);
  assert.strictEqual(asked, 5, "clear() forgets the kept answer");
}

{
  const cache = new AskCache();
  let n = 0;
  const failing = () => { n++; return Promise.reject(new Error("relay connection closed")); };
  await cache.take(f(160), failing).catch(() => {});
  await cache.take(f(160), failing).catch(() => {});
  assert.strictEqual(n, 2, "a rejected ask is retried, not replayed");
}

// `complete = false` is what a timed-out or aborted read comes back as.
{
  const cache = new AskCache();
  let n = 0;
  const short = () => { n++; const a = [{ id: "1" }]; a.complete = false; return Promise.resolve(a); };
  await cache.take(f(160), short);
  await cache.take(f(160), short);
  assert.strictEqual(n, 2, "an incomplete answer is not reused");
  const whole = () => { n++; const a = [{ id: "1" }]; a.complete = true; return Promise.resolve(a); };
  await cache.take(f(160), whole);
  await cache.take(f(160), whole);
  assert.strictEqual(n, 3, "…and a complete one is");
}

assert.ok(/const asks = new AskCache\(\)/.test(app), "app.js keeps one ask cache");
assert.ok(/asks\.take\(filters, \(\) => relay\.req\(filters, undefined, \{ signal \}\)\)/.test(app), "…and every ranked search goes through it");
assert.ok(/if \(pop\.loading\) \{ popupQueued = text; return; \}/.test(app), "a type-ahead while one is in flight only replaces the waiting text");
assert.ok(/if \(live && next != null && next !== text && \$q\.value\.trim\(\) === next\) armPopup\(next\)/.test(app), "the waiting text runs when the ask lands, if the box still says it — through the debounce again");
assert.ok(/if \(pop\.abort && pop\.inFlightFor !== text\) pop\.abort\.abort\(\)/.test(app), "Enter closes a type-ahead for any other text, so the submit is not queued behind it");
assert.ok(/search\(text, askLimit\(0\), false, abort\.signal\)/.test(app), "…and the type-ahead's ask carries the signal that closes it");
assert.ok(/popupQueued = null; \/\/ a text that queued/.test(app), "closing the popup drops the waiting text");
assert.ok(/const DEBOUNCE_MS = 250/.test(app), "the debounce is a pause, not a keystroke");

console.log("asks: one ranked search per typed question — the key, the width, the freshness, and app.js's two rules hold");
