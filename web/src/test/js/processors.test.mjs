// THE GLOSSARY LOOKUP — what a chip's tooltip is allowed to come back as.
//
// Every number the status pages draw can hang the document's own definition off
// itself, and several of those lookups use a key the DOCUMENT chose rather than
// one written here: a funnel slice's key, a stream's phase word. `TERMS` is
// parsed JSON, so it is a plain object with Object.prototype behind it, and a
// member named `constructor` or `toString` would come back as a FUNCTION and be
// assigned to an element's `title`.
//
// The same rule `funnelOf` in shared/sync.js already holds, asserted there for
// reasons and hosts. This holds it for the vocabulary.
import assert from "node:assert/strict";
import { setTerms, term } from "../../main/resources/web/shared/processors.js";

const ok = (name) => console.log(`  ✓ ${name}`);

setTerms({ queued: "How many events are waiting to be written.", empty: "" });

assert.equal(term("queued"), "How many events are waiting to be written.");
ok("a member the document defines gets its definition");

for (const key of ["constructor", "toString", "valueOf", "hasOwnProperty", "__proto__", "isPrototypeOf"]) {
  const got = term(key);
  assert.equal(typeof got, "string", `term("${key}") must be a string, got ${typeof got}`);
  assert.equal(got, "", `term("${key}") must be empty — it is not a term this document carries`);
}
ok("a member named after something on Object.prototype is not a term");

assert.equal(term("neverHeardOf"), "");
ok("a member the document does not carry gets nothing at all");

// An empty definition and an absent one must render identically: `processorFact`
// only sets `title` when this answers truthy, because an EMPTY title still
// paints the help cursor over a tooltip the browser then declines to show.
assert.equal(term("empty"), "");
ok("a member defined as the empty string is as good as absent");

// The glossary is per document — two pages read one process, and the second
// must not inherit the first's words for members it does not publish.
setTerms(null);
assert.equal(term("queued"), "");
ok("a document with no terms clears the last one's");
