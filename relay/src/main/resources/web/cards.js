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

/**
 * The pubkeys a card writes a NAME for — what a page must load profiles for
 * before rendering, or the card falls back to an npub.
 *
 * It lives here, beside the dispatch, because it is knowledge about the CARDS:
 * both pages had their own scan of the tags, neither knew what the renderers
 * actually name, and the two answers drifted the moment a new family named
 * somebody from a slot the scans did not cover. A zap names its sender from
 * inside a stringified event, which no tag scan will ever reach.
 *
 * Faces are deliberately NOT here. A face strip shows a picture, a follow list
 * carries thousands of them, and both pages already load those separately —
 * this is the narrow set whose absence shows up as hex-shaped text.
 */
export function namedPubkeys(ev) {
  const out = new Set();
  const add = (v) => { if (/^[0-9a-f]{64}$/.test(v || "")) out.add(v); };
  for (const t of ev.tags || []) {
    // A 30382's `d` is its subject. A 30383's is an event id and a 30384's an
    // address, which is why this is keyed by kind and not by shape.
    if (t[0] === "d" && ev.kind === 30382) add(t[1]);
    else if (/^\d+:/.test(t[0] || "")) add(t[1]);           // a 10040's service column
    else if (t[0] === "p" && NAMES_P_TAGS.has(ev.kind)) add(t[1]);
  }
  // NIP-57 puts the zap REQUEST, stringified, in the receipt's `description`,
  // and the sender is that inner event's author — the one person named on
  // this page who appears nowhere in the outer event's tags.
  if (ev.kind === 9735) {
    try { add(JSON.parse((ev.tags || []).find((t) => t[0] === "description")?.[1] || "{}").pubkey); } catch (e) { /* a malformed receipt names nobody */ }
  }
  return [...out];
}

/** The kinds whose card names the person a `p` tag points at, one per event. */
const NAMES_P_TAGS = new Set([9734, 9735, 1984]);

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
