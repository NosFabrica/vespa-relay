// The renderer assembly: importing a family module registers its kinds, and
// card() dispatches bespoke kind -> generic floor. Adding a renderer never
// edits this dispatch — a new family file registers itself and appears here
// as one import line. Every registered kind is held to a fixture in the
// render test, so "covers all kinds" stays a checked claim rather than a
// hope.

import { esc, clip, titleOf, summaryOf } from "./shared/format.js";
import { shortNpub } from "./shared/nip19.js";
import { authorOf, displayName, parseProfile } from "./shared/profiles.js";
import { renderers, avatarHtml, badgeHtml } from "./cards/base.js";
import { genericCard } from "./cards/generic.js";
import "./cards/profile.js";
import "./cards/note.js";
import "./cards/people.js";
import "./cards/social.js";
import "./cards/lists.js";
import "./cards/article.js";
import "./cards/media.js";
import "./cards/code.js";
import "./cards/live.js";
import "./cards/market.js";
import "./cards/apps.js";
import "./cards/relays.js";

/**
 * One event to one card. `opts.full` is the permalink depth — nothing
 * clipped, full dates, media playable; without it the card is the preview a
 * results list wants. Dispatch is on the EVENT's kind, never the identifier
 * that led here: a note1… id can name an article, and it should render as
 * one.
 */
export const card = (ev, opts) => (renderers.get(ev.kind) || genericCard)(ev, opts);

/** The type-ahead row: works for every kind (profiles show their own face). */
export function popupRow(ev, idx) {
  let pic, name, sub;
  if (ev.kind === 0) {
    const p = parseProfile(ev);
    pic = p.picture; name = displayName(p) || shortNpub(ev.pubkey); sub = p.about;
  } else {
    const a = authorOf(ev);
    pic = a.picture; name = titleOf(ev) || clip(ev.content, 60) || a.name; sub = titleOf(ev) ? (summaryOf(ev) || clip(ev.content, 60)) : a.name;
  }
  return `
    <div class="popup-item" data-idx="${idx}" role="option">
      ${avatarHtml(pic, ev.pubkey)}
      <div class="row-main">
        <div class="row-name">${esc(clip(name, 80))}</div>
        <div class="row-about">${esc(clip(sub || "", 90))}</div>
      </div>
      ${badgeHtml(ev)}
    </div>`;
}
