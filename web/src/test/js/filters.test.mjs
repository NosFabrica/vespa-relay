// The advanced filters panel, and the two things that must never fall out of
// step with it.
//
// A filter behind a disclosure is invisible twice over: the panel is shut, and
// `?sort=…&spam=1&as=…` means a link can arrive with somebody else's filters
// already on. Two things make that honest — the count on the button, and the
// URL carrying the filter so the link is the search. Both are hand-written per
// filter, and both fail SILENTLY when a fourth control is added to the panel:
// the badge simply undercounts, the URL simply drops it, and the page looks
// exactly like a page with no filter on.
//
// So the bijection is asserted here, the same way avatar.test.mjs checks the
// renderer's sizes against index.html's --av table. Adding a control to the
// panel fails this test until it is either a filter with a badge fact and a URL
// param, or named below as something that is not a filter at all.

import assert from "assert";
import { readFileSync } from "node:fs";

const html = readFileSync(new URL("../../main/resources/index.html", import.meta.url), "utf8");
const app = readFileSync(new URL("../../main/resources/web/app.js", import.meta.url), "utf8");

/** The source of a top-level `function name() { … }`, closing brace included. */
function bodyOf(src, name) {
  const start = src.indexOf(`function ${name}(`);
  assert.notStrictEqual(start, -1, `app.js has no function ${name}()`);
  const end = src.indexOf("\n}", start);
  assert.notStrictEqual(end, -1, `function ${name}() never closes`);
  return src.slice(start, end);
}

// ---- what the panel actually holds, from the markup ------------------------
const panelAt = html.indexOf('<div class="adv-panel">');
assert.notStrictEqual(panelAt, -1, "index.html has no .adv-panel");
const panel = html.slice(panelAt, html.indexOf("</details>", panelAt));
const controls = [...panel.matchAll(/<(?:select|input)\b[^>]*\bid="([^"]+)"/g)].map((m) => m[1]).sort();

// ---- and what each one is ---------------------------------------------------
//
// `state` is what the badge reads to decide the control is off its default —
// which is NOT always the control itself: the observer field is a search box
// over the picker list, and the filter it sets is the lens, `viewingAs`.
const FILTERS = {
  sort:      { state: "$sort.value",    param: "sort" },
  obsfilter: { state: "viewingAs",      param: "as" },
  spam:      { state: "$spam.checked",  param: "spam" },
};

assert.deepStrictEqual(
  controls, Object.keys(FILTERS).sort(),
  "a control in the filters panel is not accounted for here (or one named here is gone): " +
  "every one of them needs a badge fact and a URL param, or the filter it applies is invisible",
);

// ---- the badge counts every one of them ------------------------------------
const badge = bodyOf(app, "renderAdvCount");
for (const [id, f] of Object.entries(FILTERS)) {
  assert.ok(badge.includes(f.state), `renderAdvCount() never reads ${f.state} — #${id} would not be counted`);
}
assert.strictEqual(
  [...badge.matchAll(/\bon\.push\(/g)].length, Object.keys(FILTERS).length,
  "renderAdvCount() counts a different number of things than the panel holds",
);

// ---- …and the URL carries every one of them, both ways ---------------------
//
// Written by currentUrl() and read back by applyUrl(): a filter only one half
// knows about is a link that loses it, or a link that cannot be re-shared.
const write = bodyOf(app, "currentUrl");
const read = bodyOf(app, "applyUrl");
for (const [id, f] of Object.entries(FILTERS)) {
  assert.ok(write.includes(`p.set("${f.param}"`), `currentUrl() never writes ?${f.param}= — #${id} does not survive a share`);
  assert.ok(read.includes(`p.get("${f.param}")`), `applyUrl() never reads ?${f.param}= — #${id} does not survive a reload`);
}

// ---- a sort value the page REASONS about is one the menu still offers ------
//
// The options are otherwise pure data — app.js appends whichever is selected to
// the search string and the store owns the grammar — with one exception, and it
// fails the same silent way the two above do. exportText() asks a different
// "question for the reader" under `sort:recent`, because that order is not a
// ranking: the trust question would send a reader hunting for a misranking in a
// list that was asked for in time order. That branch is a comparison against a
// bare token, so renaming or dropping the option breaks nothing visibly — it
// just quietly restores the wrong question.
//
// What this can hold is only the SPELLING; whether the branch reads the right
// thing at all is behaviour, and lives in query.test.mjs against
// effectiveSort() (the export used to key on the menu, so a shared
// `/?q=cats sort:recent` link got the trust question).
//
// The empty case is the trap this block was written with and failed: scraping
// source for comparisons yields nothing the moment the comparison is respelled,
// and a loop over nothing passes. So finding nothing is a FAILURE here, not a
// silent success.
const sortOptions = [...panel.matchAll(/<select id="sort"[\s\S]*?<\/select>/g)]
  .flatMap((sel) => [...sel[0].matchAll(/<option value="([^"]*)"/g)].map((m) => m[1]));
assert.ok(sortOptions.includes(""), "#sort must offer the empty default (relevance)");
assert.ok(sortOptions.length > 1, "the sort menu lost its options (or the markup changed shape)");

const exportBody = bodyOf(app, "exportText");
const branchedOn = [...new Set([...exportBody.matchAll(/\bsort ===? "([^"]+)"/g)].map((m) => m[1]))];
assert.ok(
  branchedOn.length > 0,
  "exportText() no longer compares `sort` against any literal — either the chronological question is gone, " +
  "or the comparison was respelled and this guard is now watching nothing",
);
for (const v of branchedOn) {
  assert.ok(sortOptions.includes(v), `exportText() branches on the sort value "${v}", which #sort does not offer`);
}

// ---- the badge is markup the page can actually hide ------------------------
//
// It is drawn with the `hidden` attribute, and `display: inline-flex` beats
// `hidden`'s UA `display: none` on specificity — without an explicit rule the
// button would carry a permanent "0".
assert.ok(/<span id="advcount"[^>]*\bhidden\b/.test(html), "the badge must start hidden — nothing is on at load");
assert.ok(/\.adv-count\[hidden\]\s*{\s*display:\s*none/.test(html), "index.html must hide .adv-count[hidden] explicitly");

// ---- the panel's controls are named for a screen reader --------------------
//
// The <label> wrappers ARE the accessible names now that the bar's own text is
// gone; the observer field has no visible label of its own, so it carries one.
for (const id of ["sort", "spam"]) {
  const wrapped = new RegExp(`<label[^>]*class="adv-row"[^>]*>[\\s\\S]{0,400}?id="${id}"`);
  assert.ok(wrapped.test(panel), `#${id} must sit inside its <label>, which is the only name it has`);
}
assert.ok(/id="obsfilter"[^>]*aria-label="/.test(panel), "#obsfilter has no visible label, so it needs an aria-label");

console.log(`filters: ${controls.length} panel controls, each counted on the badge and carried in the URL`);
