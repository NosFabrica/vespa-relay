// The syntax sheet in index.html against the tokenizer in query.js, both ways, and the sheet's
// split: the page's prefixes become filter fields, NIP-50's tokens reach the store as text.

import assert from "assert";
import { readFileSync } from "node:fs";
import { parseQuery, tokenize } from "../../main/resources/web/shared/query.js";

const html = readFileSync(new URL("../../main/resources/index.html", import.meta.url), "utf8");
const app = readFileSync(new URL("../../main/resources/web/app.js", import.meta.url), "utf8");
const query = readFileSync(new URL("../../main/resources/web/shared/query.js", import.meta.url), "utf8");

// A real npub, minted by the page's own encoder, stands in for the sheet's `npub1…`.
const NPUB = "npub1424242424242424242424242424242424242424242424242424qamrcaj";

const text = (frag) =>
  frag
    .replace(/<[^>]+>/g, "")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .trim();

/** Every `<code class="tok">` inside one stretch of markup, as plain text. */
const toks = (frag) => [...frag.matchAll(/<code class="tok">([\s\S]*?)<\/code>/g)].map((m) => text(m[1]));

/** A documented token with a real value in place of the sheet's placeholder. */
const real = (tok) => tok.replace(/npub1…/g, NPUB);

// Read off query.js's source, not a list repeated here.
const scopes = /const SCOPES = "([^"]+)"/.exec(query);
assert.ok(scopes, "query.js no longer declares SCOPES — this test cannot read the scope prefixes");
const known = new Set([
  ...scopes[1].split("|"),
  ...["who", "when"].map((g) => new RegExp(`\\(\\?<${g}>\\(\\?:([a-z|]+)\\):\\)`).exec(query)).flatMap((m) => {
    assert.ok(m, "query.js's token regex no longer names its from:/to: and since:/until: groups");
    return m[1].split("|");
  }),
  "group",
]);
// The reads above ask only for the groups this file knows by name.
const groups = [...new Set([...query.matchAll(/\(\?<([a-z]+)>/g)].map((m) => m[1]))].sort();
assert.deepStrictEqual(
  groups,
  ["day", "ext", "grp", "gid", "key", "lead", "when", "who", "sid"].sort(),
  `query.js's tokenizer grew or lost a named group (${groups.join(", ")}) — the syntax sheet has to keep up with it`,
);

assert.ok(known.has("from") && known.has("until") && known.has("group") && known.has("podcast:item:guid"), `read the wrong prefixes off query.js: ${[...known].join(", ")}`);

const sheetAt = html.indexOf('<dialog id="help"');
const sheet = html.slice(sheetAt, html.indexOf("</dialog>", sheetAt));
assert.ok(sheet.length > 500, "index.html has no <dialog id=\"help\"> to check");
const btn = html.slice(html.indexOf("<button id=\"helpbtn\""), html.indexOf("</button>", html.indexOf("<button id=\"helpbtn\"")));
assert.ok(btn, "nothing on the page opens the syntax sheet");
assert.ok(/aria-label="[^"]+"/.test(btn), "the syntax button draws an icon and nothing else — it needs an aria-label");

const sections = [...sheet.matchAll(/<section class="help-sec">([\s\S]*?)<\/section>/g)].map((m) => ({
  title: text(/<h3>([\s\S]*?)<\/h3>/.exec(m[1])[1]),
  tokens: toks(m[1]),
}));
assert.ok(sections.length >= 5, `the sheet lost its sections: ${sections.map((s) => s.title).join(", ")}`);

const RANKING = "Ranking and order";
const store = sections.find((s) => s.title === RANKING);
assert.ok(store, `the sheet no longer has a "${RANKING}" section — the two languages are what it is FOR`);
const page = sections.filter((s) => s !== store).flatMap((s) => s.tokens);

/** The prefix a documented token claims, or null for one that claims none. */
function claimed(tok) {
  // Longest first, or `podcast:item:guid:<guid>` reads as the `podcast:guid` family.
  for (const p of [...known].sort((a, b) => b.length - a.length)) if (tok.startsWith(p + ":")) return p;
  const at = tok.indexOf(":");
  return at === -1 ? null : tok.slice(0, at);
}

for (const prefix of known) {
  assert.ok(
    page.some((t) => t.startsWith(prefix + ":")),
    `query.js lifts \`${prefix}:\` and the syntax sheet never mentions it`,
  );
}
assert.ok(page.some((t) => t.startsWith("#")), "the sheet documents no #hashtag");

for (const tok of page) {
  // Hashtags are the one token with no prefix; they have their own loop below.
  if (tok.startsWith("#")) continue;
  const prefix = claimed(tok);
  if (prefix == null) {
    // `-word`, `"exact phrase"`, plain words: NIP-50's, and must reach the store as text.
    assert.strictEqual(parseQuery(real(tok)).terms, tok, `\`${tok}\` is documented as words but the page rewrites it`);
    continue;
  }
  assert.ok(known.has(prefix), `the sheet documents \`${prefix}:\`, which query.js does not lift — it would be searched for as words`);
  const seg = tokenize(real(tok)).filter((s) => s.type !== "text");
  assert.strictEqual(seg.length, 1, `\`${tok}\` is documented as a filter and the tokenizer reads it as ${seg.length} tokens`);
}
for (const tok of page.filter((t) => t.startsWith("#"))) {
  assert.deepStrictEqual(tokenize(tok).map((s) => s.type), ["tag"], `\`${tok}\` is documented as a hashtag and does not tokenize as one`);
}

for (const tok of store.tokens) {
  assert.strictEqual(parseQuery(tok).terms, tok, `\`${tok}\` is documented as a NIP-50 token and the page lifts it out of the query instead`);
}

// The Filters menu writes the sort tokens; the sheet explains them.
const select = html.slice(html.indexOf('<select id="sort"'), html.indexOf("</select>", html.indexOf('<select id="sort"')));
const options = [...select.matchAll(/value="([^"]*)"/g)].map((m) => m[1]).filter(Boolean);
assert.ok(options.length >= 4, `read no sort options out of index.html: ${options.join(", ")}`);
const documented = new Set(store.tokens.filter((t) => t.startsWith("sort:")).map((t) => t.slice("sort:".length)));
for (const v of options) assert.ok(documented.has(v), `the Filters menu offers \`sort:${v}\` and the sheet does not explain it`);
for (const v of documented) assert.ok(options.includes(v), `the sheet documents \`sort:${v}\`, which the Filters menu cannot produce`);

assert.ok(store.tokens.includes("include:spam"), "the spam switch writes `include:spam` and the sheet does not say so");
assert.ok(store.tokens.some((t) => t.startsWith("observer:")), "\"Ranking as\" writes `observer:` and the sheet does not say so");

// The closing example mixes the two languages, as a real query does.
const note = html.slice(html.indexOf('<p class="help-note">'), html.indexOf("</p>", html.indexOf('<p class="help-note">')));
const example = real(toks(note)[0]);
assert.ok(example, "the sheet's closing example is gone");
const q = parseQuery(example);
assert.deepStrictEqual(
  [q.hashtags.length, q.authors.length, q.since != null],
  [1, 1, true],
  `the sheet's example does not filter the way it says it does: ${example}`,
);
assert.strictEqual(q.terms, "sort:recent", `the sheet's example leaves ${JSON.stringify(q.terms)} as search words`);

// The store ignores an npub observer silently rather than refusing it, so the sheet must say hex.
for (const tok of store.tokens) {
  if (!tok.startsWith("observer:")) continue;
  assert.ok(/hex/i.test(tok), `\`${tok}\` must say hex: the store ignores an npub observer silently`);
}
assert.ok(
  store.tokens.some((t) => t.startsWith("observer:")),
  "the sheet stopped documenting `observer:` — the check above went vacuous with it",
);

// `?` opens the sheet over the Filters panel; the panel's Escape handler must stand down or one
// press dismisses both.
const esc = app.slice(app.indexOf("// One Escape handler"), app.indexOf("// ---- the syntax sheet"));
assert.ok(esc.length > 200, "app.js's Escape handler moved — this pin cannot see it any more");
assert.ok(esc.includes("$help.open"), "the Filters panel's Escape handler must stand down while the syntax sheet is up");

// Escape, the backdrop and the focus trap all come with showModal() and nothing else.
for (const id of ["help", "helpbtn", "helpclose"]) {
  assert.ok(app.includes(`getElementById("${id}")`), `app.js never reaches for #${id}`);
}
assert.ok(app.includes("showModal()"), "the sheet is opened non-modally — Escape and the backdrop stop working");

console.log(`syntax sheet: ${known.size} prefixes documented, ${store.tokens.length} NIP-50 tokens passed through, ${options.length} sort values in step with the menu`);
