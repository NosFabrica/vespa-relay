// The renderer assembly. Importing a family module registers its kinds, and
// card() dispatches bespoke kind, then the generic floor. Two renderings
// dispatch here: the card, and the type-ahead row the search field draws
// above it. A family registers both, and the render test holds the two
// registries to the same key set and every kind to a fixture.

import { esc, clip } from "./shared/format.js";
import { authorOf } from "./shared/profiles.js";
import { replyPerson } from "./shared/parents.js";
import { avatarHtml } from "./shared/avatar.js";
import { renderers, rows, oneLine, badgeHtml, gridPeople, namedPeople, tagOf, tagsWhere } from "./cards/base.js";
import { genericCard, genericRow } from "./cards/generic.js";
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
import "./cards/trust.js";

/**
 * One event to one card. `opts.full` is the permalink depth; without it the
 * card is a results-list preview. Dispatch is on the event's kind, never the
 * identifier that led here: a note1 id can name an article.
 */
export const card = (ev, opts) => (renderers.get(ev.kind) || genericCard)(ev, opts);

/**
 * The pubkeys a card writes a name for at this depth: what a page must load
 * profiles for before rendering. Faces need no lookup and are not here.
 * `opts` must be the opts the card will be drawn with.
 */
export function namedPubkeys(ev, opts) {
  const out = new Set();
  const add = (v) => { if (/^[0-9a-f]{64}$/.test(v || "")) out.add(v); };
  for (const t of tagsWhere(ev, (name) => name === "d" || name === "p" || /^\d+:/.test(name))) {
    // A 30382's `d` is its subject; a 30383's is an event id and a 30384's an address.
    if (t[0] === "d") { if (ev.kind === 30382) add(t[1]); }
    else if (t[0] === "p") { if (NAMES_P_TAGS.has(ev.kind)) add(t[1]); }
    else add(t[1]);                                        // a 10040's service column
  }
  for (const pk of gridPeople(ev, opts)) add(pk);
  // Names from slots no tag scan covers, such as a repository's `maintainers` values.
  for (const pk of namedPeople(ev, opts)) add(pk);
  add(replyPerson(ev));
  // A zap receipt's sender is the author of the stringified request in `description`.
  if (ev.kind === 9735) {
    try { add(JSON.parse(tagOf(ev, "description") || "{}").pubkey); } catch (e) { /* a malformed receipt names nobody */ }
  }
  return [...out];
}

/** The kinds whose card names the person a `p` tag points at, one per event. */
const NAMES_P_TAGS = new Set([9734, 9735, 1984]);

/**
 * What a type-ahead row says, with the ladder every family leans on applied
 * once. The name never falls back to the content; a family with nothing to
 * say leaves the row leading with the author. A sub that repeats the name is
 * dropped, whatever family produced it.
 */
export function rowOf(ev) {
  const a = authorOf(ev);
  const r = (rows.get(ev.kind) || genericRow)(ev) || {};
  const name = oneLine(r.name) || a.name;
  const sub = oneLine(r.sub);
  return {
    pic: oneLine(r.pic) || a.picture,
    name,
    sub: sub && sub !== name ? sub : (r.self || name === a.name ? "" : a.name),
  };
}

/** The type-ahead row: one line for what the event is, one for what it says. */
export function popupRow(ev, idx) {
  const { pic, name, sub } = rowOf(ev);
  return `
    <div class="popup-item" data-idx="${idx}" role="option">
      ${avatarHtml(pic, ev.pubkey)}
      <div class="row-main">
        <div class="row-name">${esc(clip(name, 80))}</div>
        <div class="row-about">${esc(clip(sub, 90))}</div>
      </div>
      ${badgeHtml(ev)}
    </div>`;
}
