// The advanced filters panel against the badge count and the URL params: a control added to the
// panel fails here until it has a badge fact and a URL param, or is named below as not a filter.

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

const panelAt = html.indexOf('<div class="adv-panel">');
assert.notStrictEqual(panelAt, -1, "index.html has no .adv-panel");
const panel = html.slice(panelAt, html.indexOf("</details>", panelAt));
const controls = [...panel.matchAll(/<(?:select|input)\b[^>]*\bid="([^"]+)"/g)].map((m) => m[1]).sort();

// `state` is what the badge reads, which is not always the control itself.
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

const badge = bodyOf(app, "renderAdvCount");
for (const [id, f] of Object.entries(FILTERS)) {
  assert.ok(badge.includes(f.state), `renderAdvCount() never reads ${f.state} — #${id} would not be counted`);
}
assert.strictEqual(
  [...badge.matchAll(/\bon\.push\(/g)].length, Object.keys(FILTERS).length,
  "renderAdvCount() counts a different number of things than the panel holds",
);

const write = bodyOf(app, "currentUrl");
const read = bodyOf(app, "applyUrl");
for (const [id, f] of Object.entries(FILTERS)) {
  assert.ok(write.includes(`p.set("${f.param}"`), `currentUrl() never writes ?${f.param}= — #${id} does not survive a share`);
  assert.ok(read.includes(`p.get("${f.param}")`), `applyUrl() never reads ?${f.param}= — #${id} does not survive a reload`);
}

// exportText() compares `sort` against a bare token, so only the spelling is held here.
const sortOptions = [...panel.matchAll(/<select id="sort"[\s\S]*?<\/select>/g)]
  .flatMap((sel) => [...sel[0].matchAll(/<option value="([^"]*)"/g)].map((m) => m[1]));
assert.ok(sortOptions.includes(""), "#sort must offer the empty default (relevance)");
assert.ok(sortOptions.length > 1, "the sort menu lost its options (or the markup changed shape)");

const exportBody = bodyOf(app, "exportText");
const branchedOn = [...new Set([...exportBody.matchAll(/\bsort ===? "([^"]+)"/g)].map((m) => m[1]))];
// Finding nothing is a failure: a respelled comparison would otherwise pass a loop over nothing.
assert.ok(
  branchedOn.length > 0,
  "exportText() no longer compares `sort` against any literal — either the chronological question is gone, " +
  "or the comparison was respelled and this guard is now watching nothing",
);
for (const v of branchedOn) {
  assert.ok(sortOptions.includes(v), `exportText() branches on the sort value "${v}", which #sort does not offer`);
}

// `display: inline-flex` beats the UA's `[hidden] { display: none }` on specificity.
assert.ok(/<span id="advcount"[^>]*\bhidden\b/.test(html), "the badge must start hidden — nothing is on at load");
assert.ok(/\.adv-count\[hidden\]\s*{\s*display:\s*none/.test(html), "index.html must hide .adv-count[hidden] explicitly");

// The <label> wrappers are the controls' only accessible names.
for (const id of ["sort", "spam"]) {
  const wrapped = new RegExp(`<label[^>]*class="adv-row"[^>]*>[\\s\\S]{0,400}?id="${id}"`);
  assert.ok(wrapped.test(panel), `#${id} must sit inside its <label>, which is the only name it has`);
}
assert.ok(/id="obsfilter"[^>]*aria-label="/.test(panel), "#obsfilter has no visible label, so it needs an aria-label");

console.log(`filters: ${controls.length} panel controls, each counted on the badge and carried in the URL`);
