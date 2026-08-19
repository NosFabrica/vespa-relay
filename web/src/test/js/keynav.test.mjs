// The keyboard that walks the results list: which presses are a move, and
// where a move lands.
//
// Both halves fail silently in the direction that is hardest to notice. A
// guard that is one test too narrow does not break navigation — it eats a
// letter out of somebody's query, on a page whose search box is a
// contenteditable div and therefore invisible to the obvious tag test. That
// exact bug shipped once already, in the "/" shortcut, which is why the rule
// is one function with one caller-independent contract and why it is asserted
// here rather than trusted.

import assert from "assert";
import { isTyping, navKey, stepIndex } from "../../main/resources/web/shared/keynav.js";

const key = (k, mods = {}) => ({ key: k, ...mods });
const el = (tagName, editable = false) => ({ tagName, isContentEditable: editable });

// ---- where a keypress is a letter -----------------------------------------
assert.strictEqual(isTyping(el("INPUT")), true);
assert.strictEqual(isTyping(el("TEXTAREA")), true);
assert.strictEqual(isTyping(el("SELECT")), true);
assert.strictEqual(isTyping(el("DIV", true)), true, "the search box is a contenteditable DIV — a tag test alone misses it");
assert.strictEqual(isTyping(el("DIV")), false);
assert.strictEqual(isTyping(el("BODY")), false);
assert.strictEqual(isTyping(null), false, "no focus at all is not somebody typing");

// ---- …and therefore what a press means -------------------------------------
const body = el("BODY");
assert.strictEqual(navKey(key("j"), body), "next");
assert.strictEqual(navKey(key("k"), body), "prev");
assert.strictEqual(navKey(key("Enter"), body), "open");
assert.strictEqual(navKey(key("x"), body), null);
assert.strictEqual(navKey(key("ArrowDown"), body), null, "the arrows still scroll the page");

// Enter is not ours when the thing with the focus ring is one Enter presses.
// Tab-to-a-chip-and-Enter is how this page worked from the keyboard before
// there was a cursor at all, and a cursor must not take it away.
for (const tag of ["BUTTON", "A", "SUMMARY"]) {
  assert.strictEqual(navKey(key("Enter"), el(tag)), null, `Enter belongs to the focused ${tag}`);
  assert.strictEqual(navKey(key("j"), el(tag)), "next", `…but ${tag} does nothing with a letter`);
}

// The whole point of the guard: the same three presses inside the field are
// the query being typed, not the list being walked.
for (const k of ["j", "k", "Enter"]) {
  assert.strictEqual(navKey(key(k), el("DIV", true)), null, `"${k}" in the search box is the search box's`);
  assert.strictEqual(navKey(key(k), el("INPUT")), null, `"${k}" in an input is the input's`);
}

// A modified press belongs to the browser or to a screen reader.
for (const mod of ["metaKey", "ctrlKey", "altKey"]) {
  assert.strictEqual(navKey(key("j", { [mod]: true }), body), null, `${mod}+j is not ours`);
  assert.strictEqual(navKey(key("k", { [mod]: true }), body), null, `${mod}+k is not ours`);
}

// ---- where a move lands ----------------------------------------------------
assert.strictEqual(stepIndex(null, 0, 1), null, "an empty list has nowhere to be");
assert.strictEqual(stepIndex(3, 0, 1), null);

// From nowhere: forward is the top, back is the bottom — so `k` means
// something before `j` has ever been pressed. -1 is the DOM answer to "which
// card carries the cursor" when none does, and reads the same as null.
assert.strictEqual(stepIndex(null, 5, 1), 0);
assert.strictEqual(stepIndex(null, 5, -1), 4);
assert.strictEqual(stepIndex(-1, 5, 1), 0);
assert.strictEqual(stepIndex(-1, 5, -1), 4);

assert.strictEqual(stepIndex(0, 5, 1), 1);
assert.strictEqual(stepIndex(4, 5, -1), 3);

// Clamped, not wrapped: this list is several screens long, and running off
// the end of it must not teleport the reader past everything they were
// walking through.
assert.strictEqual(stepIndex(4, 5, 1), 4, "the last card is the last card");
assert.strictEqual(stepIndex(0, 5, -1), 0, "the first card is the first card");
assert.strictEqual(stepIndex(0, 1, 1), 0);

console.log("keynav: j/k/Enter mean a move only outside a text field, and a move clamps at both ends");
