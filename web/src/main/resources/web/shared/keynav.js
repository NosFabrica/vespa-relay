// `j` down, `k` up, Enter to open. The DOM-free halves live here: which keypresses are a
// move and where a move lands. app.js keeps the part that needs the page.

/** Where a keypress is a letter rather than a command. The search box is a contenteditable div. */
export function isTyping(el) {
  return !!el && (el.isContentEditable === true || /^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName || ""));
}

/** What a keydown means to a list: "next", "prev", "open", or null. [active] is the focused element. */
export function navKey(e, active) {
  if (e.metaKey || e.ctrlKey || e.altKey) return null;
  if (isTyping(active)) return null;
  if (e.key === "j") return "next";
  if (e.key === "k") return "prev";
  // A button, link or summary under focus already answers Enter; Tab-then-Enter must keep working.
  if (e.key === "Enter") return ACTIVATES_ON_ENTER.test((active && active.tagName) || "") ? null : "open";
  return null;
}

const ACTIVATES_ON_ENTER = /^(BUTTON|A|SUMMARY)$/;

/**
 * Where a move lands from [current] (null for nowhere yet). Clamps rather than wraps: a
 * results list runs over several screens, and a wrap would teleport the reader.
 */
export function stepIndex(current, len, delta) {
  if (len <= 0) return null;
  if (current == null || current < 0) return delta > 0 ? 0 : len - 1;
  return Math.max(0, Math.min(len - 1, current + delta));
}
