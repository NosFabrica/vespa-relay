// The glossary lookup: `TERMS` is parsed JSON with Object.prototype behind it,
// and several keys come from the document, so a member named `constructor`
// must come back as an empty string, never a function.
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

// `processorFact` sets `title` only when this is truthy: an empty title still
// paints the help cursor over a tooltip the browser declines to show.
assert.equal(term("empty"), "");
ok("a member defined as the empty string is as good as absent");

setTerms(null);
assert.equal(term("queued"), "");
ok("a document with no terms clears the last one's");
