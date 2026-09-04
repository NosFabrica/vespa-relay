// The keyboard that walks the results list: which presses are a move, and
// where a move lands. The search box is a contenteditable div, so a guard
// that only tests tag names eats letters out of the query.

import assert from "assert";
import { isTyping, navKey, stepIndex } from "../../main/resources/web/shared/keynav.js";

const key = (k, mods = {}) => ({ key: k, ...mods });
const el = (tagName, editable = false) => ({ tagName, isContentEditable: editable });

assert.strictEqual(isTyping(el("INPUT")), true);
assert.strictEqual(isTyping(el("TEXTAREA")), true);
assert.strictEqual(isTyping(el("SELECT")), true);
assert.strictEqual(isTyping(el("DIV", true)), true, "the search box is a contenteditable DIV — a tag test alone misses it");
assert.strictEqual(isTyping(el("DIV")), false);
assert.strictEqual(isTyping(el("BODY")), false);
assert.strictEqual(isTyping(null), false, "no focus at all is not somebody typing");

const body = el("BODY");
assert.strictEqual(navKey(key("j"), body), "next");
assert.strictEqual(navKey(key("k"), body), "prev");
assert.strictEqual(navKey(key("Enter"), body), "open");
assert.strictEqual(navKey(key("x"), body), null);
assert.strictEqual(navKey(key("ArrowDown"), body), null, "the arrows still scroll the page");

// Tab-to-a-chip-and-Enter predates the cursor and must keep working.
for (const tag of ["BUTTON", "A", "SUMMARY"]) {
  assert.strictEqual(navKey(key("Enter"), el(tag)), null, `Enter belongs to the focused ${tag}`);
  assert.strictEqual(navKey(key("j"), el(tag)), "next", `…but ${tag} does nothing with a letter`);
}

for (const k of ["j", "k", "Enter"]) {
  assert.strictEqual(navKey(key(k), el("DIV", true)), null, `"${k}" in the search box is the search box's`);
  assert.strictEqual(navKey(key(k), el("INPUT")), null, `"${k}" in an input is the input's`);
}

for (const mod of ["metaKey", "ctrlKey", "altKey"]) {
  assert.strictEqual(navKey(key("j", { [mod]: true }), body), null, `${mod}+j is not ours`);
  assert.strictEqual(navKey(key("k", { [mod]: true }), body), null, `${mod}+k is not ours`);
}

assert.strictEqual(stepIndex(null, 0, 1), null, "an empty list has nowhere to be");
assert.strictEqual(stepIndex(3, 0, 1), null);

// -1 is the DOM's answer to "which card carries the cursor" when none does.
assert.strictEqual(stepIndex(null, 5, 1), 0);
assert.strictEqual(stepIndex(null, 5, -1), 4);
assert.strictEqual(stepIndex(-1, 5, 1), 0);
assert.strictEqual(stepIndex(-1, 5, -1), 4);

assert.strictEqual(stepIndex(0, 5, 1), 1);
assert.strictEqual(stepIndex(4, 5, -1), 3);

// Clamped, not wrapped: the list is several screens long.
assert.strictEqual(stepIndex(4, 5, 1), 4, "the last card is the last card");
assert.strictEqual(stepIndex(0, 5, -1), 0, "the first card is the first card");
assert.strictEqual(stepIndex(0, 1, 1), 0);

console.log("keynav: j/k/Enter mean a move only outside a text field, and a move clamps at both ends");
