// The syntax sheet is only worth having if it is TRUE, and nothing else on the
// page can tell that it has stopped being true.
//
// A prefix is documented in prose and implemented in a regex, and the two are
// edited on different days. Both directions fail silently: a family query.js
// learns that the sheet never mentions is a feature nobody finds, and a token
// the sheet names that query.js does not lift is worse — it stays in the query
// as WORDS, so `lang:en` would quietly search for the string "lang:en" and
// come back with nothing while the page insisted it was a filter.
//
// So the bijection is asserted here the way filters.test.mjs asserts the panel
// against the badge: what the sheet claims, read out of index.html, against
// what the tokenizer really does, asked of query.js itself rather than of a
// copy of its source. Adding a prefix to query.js fails this test until the
// sheet names it; naming one the sheet cannot back up fails it too.
//
// The sheet's own split is the other half. Its sections are two languages —
// the page's prefixes, which become NIP-01 filter fields and never reach the
// store as text, and NIP-50's own tokens, which reach it as text and nothing
// else. A token filed under the wrong heading is a sentence that is exactly
// backwards, and the only way to catch it is to run the parser over it.

import assert from "assert";
import { readFileSync } from "node:fs";
import { parseQuery, tokenize } from "../../relay/src/main/resources/web/shared/query.js";

const html = readFileSync(new URL("../../relay/src/main/resources/index.html", import.meta.url), "utf8");
const app = readFileSync(new URL("../../relay/src/main/resources/web/app.js", import.meta.url), "utf8");
const query = readFileSync(new URL("../../relay/src/main/resources/web/shared/query.js", import.meta.url), "utf8");

// A real npub, minted by the page's own encoder — the same one query.test.mjs
// uses. The sheet draws an npub as `npub1…` because a 63-character key in a
// help row is a line of noise, so every check below swaps the ellipsis for
// this before asking the tokenizer anything.
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

// ---- what the tokenizer knows, read off query.js ---------------------------
//
// From the SOURCE, not from a list repeated here: a list in this file is a
// third place the prefixes are written down, and the one that would go stale
// without failing anything.
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
// A whole NEW family would be invisible to the two reads above — they ask for
// the groups this test already knows the names of, so `(?<lang>(?:lang):)`
// added to the tokenizer would leave `known` unchanged and every check below
// passing over a prefix the sheet has never heard of. So the group names are
// pinned as a set: adding one fails here, which is the prompt to teach this
// file and the sheet about it together.
const groups = [...new Set([...query.matchAll(/\(\?<([a-z]+)>/g)].map((m) => m[1]))].sort();
assert.deepStrictEqual(
  groups,
  ["day", "ext", "grp", "gid", "key", "lead", "when", "who", "sid"].sort(),
  `query.js's tokenizer grew or lost a named group (${groups.join(", ")}) — the syntax sheet has to keep up with it`,
);

// The families the tokenizer knows about, spot-checked so a regex change that
// silently stops matching cannot make the rest of this file vacuous.
assert.ok(known.has("from") && known.has("until") && known.has("group") && known.has("podcast:item:guid"), `read the wrong prefixes off query.js: ${[...known].join(", ")}`);

// ---- the sheet -------------------------------------------------------------
const sheetAt = html.indexOf('<dialog id="help"');
const sheet = html.slice(sheetAt, html.indexOf("</dialog>", sheetAt));
assert.ok(sheet.length > 500, "index.html has no <dialog id=\"help\"> to check");
const btn = html.slice(html.indexOf("<button id=\"helpbtn\""), html.indexOf("</button>", html.indexOf("<button id=\"helpbtn\"")));
assert.ok(btn, "nothing on the page opens the syntax sheet");
// It is a mark with no word beside it — the markup says why — so the only
// thing naming it for a screen reader is the attribute, and an icon-only
// button that loses its label is a button announced as "button".
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
  // Longest first, or `podcast:item:guid:<guid>` reads as the `podcast:guid`
  // family and a sheet documenting only one of the three would pass.
  for (const p of [...known].sort((a, b) => b.length - a.length)) if (tok.startsWith(p + ":")) return p;
  const at = tok.indexOf(":");
  return at === -1 ? null : tok.slice(0, at);
}

// ---- every prefix the tokenizer lifts is on the sheet ----------------------
for (const prefix of known) {
  assert.ok(
    page.some((t) => t.startsWith(prefix + ":")),
    `query.js lifts \`${prefix}:\` and the syntax sheet never mentions it`,
  );
}
assert.ok(page.some((t) => t.startsWith("#")), "the sheet documents no #hashtag");

// ---- and every prefix the sheet names is one the tokenizer lifts -----------
//
// Both halves of the claim, per token: it has to BE a known prefix (a spelling
// nobody implemented fails here, not in front of a reader), and running it
// through the tokenizer has to actually produce a token rather than text.
for (const tok of page) {
  // Hashtags have their own loop below: they are the one token with no prefix
  // to name, so everything this one asks about a colon is meaningless for them.
  if (tok.startsWith("#")) continue;
  const prefix = claimed(tok);
  if (prefix == null) {
    // Not a prefix at all — `-word`, `"exact phrase"`, two plain words. Those
    // are NIP-50's and must reach the store as the text they are.
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

// ---- the ranking tokens are the OTHER language ------------------------------
//
// They are the store's, so the page must leave every one of them alone: the
// test is that the token survives parseQuery into the search string, character
// for character. `sort:recent` filed under Topics would fail here.
for (const tok of store.tokens) {
  assert.strictEqual(parseQuery(tok).terms, tok, `\`${tok}\` is documented as a NIP-50 token and the page lifts it out of the query instead`);
}

// ---- the sort values are the Filters menu's, exactly ------------------------
//
// The menu writes these tokens; the sheet explains them. Two lists of the same
// six strings, in two places, is the drift filters.test.mjs was written for one
// row up — so it is asserted here too, in both directions.
const select = html.slice(html.indexOf('<select id="sort"'), html.indexOf("</select>", html.indexOf('<select id="sort"')));
const options = [...select.matchAll(/value="([^"]*)"/g)].map((m) => m[1]).filter(Boolean);
assert.ok(options.length >= 4, `read no sort options out of index.html: ${options.join(", ")}`);
const documented = new Set(store.tokens.filter((t) => t.startsWith("sort:")).map((t) => t.slice("sort:".length)));
for (const v of options) assert.ok(documented.has(v), `the Filters menu offers \`sort:${v}\` and the sheet does not explain it`);
for (const v of documented) assert.ok(options.includes(v), `the sheet documents \`sort:${v}\`, which the Filters menu cannot produce`);

// The other two controls in that panel write a token each. Same argument, no
// value list to compare — just that the sheet names them at all.
assert.ok(store.tokens.includes("include:spam"), "the spam switch writes `include:spam` and the sheet does not say so");
assert.ok(store.tokens.some((t) => t.startsWith("observer:")), "\"Ranking as\" writes `observer:` and the sheet does not say so");

// ---- the sheet's own worked example ----------------------------------------
//
// The one line that shows the two languages MIXED, which is how a real query
// uses them. Both halves are asserted: every prefix in it becomes a token, and
// what is left over is the NIP-50 tail and nothing else.
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

// ---- the observer token is HEX, and the sheet must not suggest otherwise ----
//
// MEASURED against wss://search-staging.brainstorm.world with a real 10040
// observer: `observer:<64 hex>` reorders the answer, and `observer:<npub>` for
// the SAME key returns byte-identical results to sending no observer at all.
// The store ignores the bech32 form rather than refusing it, so a reader who
// copies the npub this page shows everywhere — `?as=npub1…` is in its own URL —
// gets an unranked answer and no error. An example spelt that way here would be
// the sheet teaching the one mistake it exists to prevent.
for (const tok of store.tokens) {
  if (!tok.startsWith("observer:")) continue;
  assert.ok(/hex/i.test(tok), `\`${tok}\` must say hex: the store ignores an npub observer silently`);
}
assert.ok(
  store.tokens.some((t) => t.startsWith("observer:")),
  "the sheet stopped documenting `observer:` — the check above went vacuous with it",
);

// ---- one Escape dismisses one thing ----------------------------------------
//
// `?` opens the sheet from the Filters panel's own summary button (a button is
// not a field, so isTyping() lets the key through), which leaves that panel
// open BEHIND the modal. The <dialog> closes itself on Escape; the panel's own
// Escape handler is a document listener that still runs on the same press, and
// without a guard it closed the panel too — two things dismissed by one key,
// which is the thing that handler's own comment says not to do.
const esc = app.slice(app.indexOf("// ONE Escape handler"), app.indexOf("// ---- the syntax sheet"));
assert.ok(esc.length > 200, "app.js's Escape handler moved — this pin cannot see it any more");
assert.ok(esc.includes("$help.open"), "the Filters panel's Escape handler must stand down while the syntax sheet is up");

// ---- and the page really opens it ------------------------------------------
//
// A sheet nothing opens is a sheet nobody reads, and the markup alone cannot
// say whether the button is wired. Three ids and the call that makes it modal
// — which is what the <dialog> is chosen FOR: Escape, the backdrop and the
// focus ring all come with showModal() and with nothing else.
for (const id of ["help", "helpbtn", "helpclose"]) {
  assert.ok(app.includes(`getElementById("${id}")`), `app.js never reaches for #${id}`);
}
assert.ok(app.includes("showModal()"), "the sheet is opened non-modally — Escape and the backdrop stop working");

console.log(`syntax sheet: ${known.size} prefixes documented, ${store.tokens.length} NIP-50 tokens passed through, ${options.length} sort values in step with the menu`);
