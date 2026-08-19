// `j` down, `k` up, Enter to open — the keyboard every reader-shaped app on
// the web already shares (mail, timelines, code review, issue lists), so
// somebody who knows it arrives on this page already knowing it.
//
// The two halves worth pinning live here rather than in app.js, which cannot
// be imported without a DOM: which keypresses are a move at all, and where a
// move lands. tools/webtest/keynav.test.mjs holds both. What is left in app.js
// is the part that needs the page — which cards exist, and drawing the cursor
// on one.

/**
 * Where a keypress is a LETTER rather than a command.
 *
 * `isContentEditable`, not just the tag list: this page's search box IS a div
 * (searchfield.js says why an input could not draw a person), so a tag test
 * alone reads the `j` somebody typed into their query as a navigation and eats
 * it. That already happened once to the "/" shortcut, which is the other
 * caller here — one rule, asked in one place, rather than two copies that can
 * disagree about what counts as typing.
 */
export function isTyping(el) {
  return !!el && (el.isContentEditable === true || /^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName || ""));
}

/**
 * What a keydown means to a list: "next", "prev", "open", or null for the
 * overwhelming majority of keys, which mean nothing here.
 *
 * [active] is the element focus is on — passed in rather than read off
 * `document`, so the guard above is testable without a browser.
 *
 * Modified presses are never ours. ⌘K and Ctrl-J are the browser's, and an
 * Alt chord is somebody's screen reader; taking any of them would break a key
 * that already had a job for a key that is only a convenience.
 */
export function navKey(e, active) {
  if (e.metaKey || e.ctrlKey || e.altKey) return null;
  if (isTyping(active)) return null;
  if (e.key === "j") return "next";
  if (e.key === "k") return "prev";
  // Enter is the shared key, and every one of these already answers it: a kind
  // chip, the json toggle, the Filters disclosure, any link a card draws. A
  // list cursor that swallowed it would make Tab-then-Enter — the way the page
  // was operable from the keyboard before any of this existed — open a card
  // instead of pressing the button under the ring. j and k are safe on them
  // because those elements do nothing with a letter.
  if (e.key === "Enter") return ACTIVATES_ON_ENTER.test((active && active.tagName) || "") ? null : "open";
  return null;
}

const ACTIVATES_ON_ENTER = /^(BUTTON|A|SUMMARY)$/;

/**
 * Where a move lands: the row a press of j/k selects, given the row it is on
 * ([current], or null for nowhere yet) and how many there are.
 *
 * From nowhere, forward is the first row and back is the last — the same
 * answer the type-ahead popup's arrows give, and the only one that lets `k`
 * mean anything before `j` has been pressed.
 *
 * Then it CLAMPS, where that popup wraps. The two lists are different shapes:
 * the popup is eight rows in one box, all of them on screen at once, so a wrap
 * is a move the eye follows. A results list is forty cards over several
 * screens, and wrapping off the end of it teleports the reader past everything
 * they were walking through, with a scroll they did not ask for to prove it.
 */
export function stepIndex(current, len, delta) {
  if (len <= 0) return null;
  if (current == null || current < 0) return delta > 0 ? 0 : len - 1;
  return Math.max(0, Math.min(len - 1, current + delta));
}
