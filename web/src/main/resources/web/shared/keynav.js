// `j` down, `k` up, Enter to open: the keyboard every reader-shaped app
// shares. The two halves that can be tested without a DOM live here, which
// keypresses are a move and where a move lands; web/src/test/js/keynav.test.mjs
// holds both, and app.js keeps the part that needs the page.

/**
 * Where a keypress is a letter rather than a command. `isContentEditable` as
 * well as the tag list, because the search box is a div; one rule for this and
 * the "/" shortcut, so the two cannot disagree about what counts as typing.
 */
export function isTyping(el) {
  return !!el && (el.isContentEditable === true || /^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName || ""));
}

/**
 * What a keydown means to a list: "next", "prev", "open", or null. [active]
 * is the focused element, passed in so this is testable without a browser.
 * Modified presses are never ours: ⌘K and Ctrl-J are the browser's and an Alt
 * chord is somebody's screen reader.
 */
export function navKey(e, active) {
  if (e.metaKey || e.ctrlKey || e.altKey) return null;
  if (isTyping(active)) return null;
  if (e.key === "j") return "next";
  if (e.key === "k") return "prev";
  // A button, link or summary under focus already answers Enter, and a cursor
  // that swallowed it would break Tab-then-Enter. j and k are safe on them.
  if (e.key === "Enter") return ACTIVATES_ON_ENTER.test((active && active.tagName) || "") ? null : "open";
  return null;
}

const ACTIVATES_ON_ENTER = /^(BUTTON|A|SUMMARY)$/;

/**
 * Where a move lands, given the row it is on ([current], or null for nowhere
 * yet) and how many there are. From nowhere, forward is the first row and back
 * is the last. Then it clamps where the type-ahead popup wraps: a results list
 * runs over several screens, and a wrap would teleport the reader.
 */
export function stepIndex(current, len, delta) {
  if (len <= 0) return null;
  if (current == null || current < 0) return delta > 0 ? 0 : len - 1;
  return Math.max(0, Math.min(len - 1, current + delta));
}
