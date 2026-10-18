// What a picture or a video takes with it when it fails to load. A renderer marks the element
// with `data-broken`; one capture-phase listener acts on it, because the page's policy admits
// no inline `onerror`.

import { BLANK } from "./avatar.js";

/** The `data-broken` values: the element itself goes, the frame around it goes, or a face falls back. */
export const BROKEN = Object.freeze({ self: "self", frame: "frame", face: "face" });

/** Undo a failed load on [el] as its `data-broken` says; false when it carries none. */
export function onBroken(el) {
  const what = el && el.dataset && el.dataset.broken;
  if (what === BROKEN.self) el.remove();
  else if (what === BROKEN.frame) el.parentElement && el.parentElement.remove();
  else if (what === BROKEN.face) {
    el.classList.add("gen");
    // The blank cannot fail, but a loop on it would never end.
    if (el.getAttribute("src") !== BLANK) el.setAttribute("src", BLANK);
  } else return false;
  return true;
}

/** Watch [doc] for failed loads. `error` does not bubble, so only the capture phase sees it. */
export function watchBrokenMedia(doc = document) {
  doc.addEventListener("error", (e) => onBroken(e.target), true);
}
