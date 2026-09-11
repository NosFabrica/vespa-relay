// Interactive stories: a prologue, the scenes it branches into, and a reader's place in one.
//
// What makes the kind what it is are the `option` tags — `["option", <label>, <address>]` — so
// the card is the scene's text over the choices that lead out of it, each a link to the scene it
// opens. The reference client draws them as buttons; here they are the list of ways on.

import { esc, clip, titleOf, summaryOf, imageOf } from "../shared/format.js";
import { shortAddr } from "../shared/nip19.js";
import {
  register, registerRow, shell, titleHtml, bodyHtml, refRows, addrHref, tagOf, tagsOf, oneLine, plural,
} from "./base.js";

/** The ways out of a scene: the label the story wrote, and the scene it opens. */
const optionsOf = (ev) =>
  tagsOf(ev, "option").filter((t) => t[2]).map((t) => ({ kind: "a", value: t[2], label: oneLine(t[1]) }));

/** 30296 / 30297 — a prologue or one scene: the text, then the choices out of it. */
function sceneCard(ev, opts) {
  const options = optionsOf(ev);
  const img = imageOf(ev);
  const full = opts && opts.full;
  const inner =
    (full && img ? `<div class="embed"><img src="${esc(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.parentElement.remove()" /></div>` : "") +
    `<div class="result-main">
      <div class="text">
        ${titleHtml(opts, titleOf(ev), 140)}
        ${bodyHtml(opts, summaryOf(ev), 300, true)}
        ${bodyHtml(opts, ev.content, full ? 0 : 400)}
        ${options.length ? `<div class="result-body">${esc(plural(options.length, "way on", "ways on"))}</div>` : ""}
      </div>
      ${!full && img ? `<img class="thumb cover" src="${esc(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()" />` : ""}
    </div>` +
    refRows(options, opts);
  return shell(ev, opts, inner);
}

/** Where a reader has got to: the scene the state points at, linked. */
function sceneLink(ev, name) {
  const addr = tagOf(ev, name);
  if (!addr) return null;
  const href = addrHref(addr);
  return href ? `<a href="${href}">${esc(clip(shortAddr(addr), 60))}</a>` : `<span class="mono">${esc(clip(shortAddr(addr), 60))}</span>`;
}

/** 30298 — one reader's place in one story: which story, how far, and whether they finished. */
function readingStateCard(ev, opts) {
  const status = oneLine(tagOf(ev, "status"));
  const inner =
    titleHtml(opts, titleOf(ev), 140) +
    (status ? `<div class="pill-row"><span class="status-pill lead">${esc(clip(status, 24))}</span></div>` : "") +
    bodyHtml(opts, summaryOf(ev) || ev.content, 300, true);
  return shell(ev, opts, inner, [
    // `A` is the story itself; the lowercase `a` is the scene this reader stopped at.
    ["story", sceneLink(ev, "A")],
    ["at", sceneLink(ev, "a")],
  ]);
}

register([30296, 30297], sceneCard);
register([30298], readingStateCard);

registerRow([30296, 30297], (ev) => ({
  name: titleOf(ev),
  sub: [summaryOf(ev) || ev.content, optionsOf(ev).length ? plural(optionsOf(ev).length, "way on", "ways on") : ""]
    .filter(Boolean).join(" · "),
}));
registerRow([30298], (ev) => ({
  name: titleOf(ev) || "a story",
  sub: [oneLine(tagOf(ev, "status")), summaryOf(ev) || ev.content].filter(Boolean).join(" · "),
}));
