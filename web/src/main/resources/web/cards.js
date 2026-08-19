// The renderer assembly: importing a family module registers its kinds, and
// card() dispatches bespoke kind -> generic floor. Adding a renderer never
// edits this dispatch — a new family file registers itself and appears here
// as one import line. Every registered kind is held to a fixture in the
// render test, so "covers all kinds" stays a checked claim rather than a
// hope.
//
// TWO renderings dispatch here, not one: the card, and the type-ahead row the
// search field draws above it. A family registers both beside each other, and
// the test holds the two registries to the same key set — which is what a row
// reading `{"about":"","name":"Test group","picture":""}`, beside a card
// drawing that same channel properly, cost to learn.

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
 * A bare FACE is still not here — a poll's winners and a community's
 * moderators are pictures, and a picture needs no lookup to be right. What a
 * list's people grid draws is not that: it draws names, so gridPeople() is
 * declared, capped at the number the card can actually draw. Bounded by the
 * grid rather than by the list, because that is the difference between a
 * follow list costing a couple of dozen profiles and costing eight thousand.
 *
 * `opts` is the RENDER's opts, and it is the same one the card will be drawn
 * with — the answer is "who does this card name at this depth", not "who could
 * it ever name". The results list only ever renders previews; asking it for
 * the permalink's set fetched three times the profiles any card on the page
 * could show, on every search.
 */
export function namedPubkeys(ev, opts) {
  const out = new Set();
  const add = (v) => { if (/^[0-9a-f]{64}$/.test(v || "")) out.add(v); };
  for (const t of tagsWhere(ev, (name) => name === "d" || name === "p" || /^\d+:/.test(name))) {
    // A 30382's `d` is its subject. A 30383's is an event id and a 30384's an
    // address, which is why this is keyed by kind and not by shape.
    if (t[0] === "d") { if (ev.kind === 30382) add(t[1]); }
    else if (t[0] === "p") { if (NAMES_P_TAGS.has(ev.kind)) add(t[1]); }
    else add(t[1]);                                        // a 10040's service column
  }
  // Everyone a list's grid puts a name under, and nobody past its last cell.
  for (const pk of gridPeople(ev, opts)) add(pk);
  // …and whoever else a renderer says it names from a slot no tag scan covers:
  // a repository's maintainers ride as the VALUES of one `maintainers` tag.
  for (const pk of namedPeople(ev, opts)) add(pk);
  // Who a reply answers. Not reachable by any scan of this event either: the
  // person is named by an `e` tag's optional fifth element, by an address's
  // middle field, or by a lookup of the parent — three slots, one answer, and
  // parents.js is what knows which kinds ask the question at all.
  add(replyPerson(ev));
  // NIP-57 puts the zap REQUEST, stringified, in the receipt's `description`,
  // and the sender is that inner event's author — the one person named on
  // this page who appears nowhere in the outer event's tags.
  if (ev.kind === 9735) {
    try { add(JSON.parse(tagOf(ev, "description") || "{}").pubkey); } catch (e) { /* a malformed receipt names nobody */ }
  }
  return [...out];
}

/** The kinds whose card names the person a `p` tag points at, one per event. */
const NAMES_P_TAGS = new Set([9734, 9735, 1984]);

/**
 * What a type-ahead row SAYS — the same dispatch the card takes, at the size a
 * list of eight rows has room for, with the ladder every family leans on
 * applied once here rather than thirty times over.
 *
 * The NAME never falls back to the content: that fallback is what put a
 * channel's `{"about":"","name":"Test group",…}` in the row while its card drew
 * the channel. A family with nothing to say about a particular event says
 * nothing, and the row leads with the person who posted it — always true, if
 * not always the most interesting thing on offer.
 *
 * The SUB then carries that person, unless the row already used them (a kind 3
 * says "follows 2,431 people" UNDER the author's name, never beside it) or the
 * event simply IS them: base.js's `self`.
 *
 * And a second line that repeats the first is not a second line. Half the
 * families draw their two from slots an author is free to fill with the same
 * words — a channel named "Bitcoin India" described as "Bitcoin India", an
 * article whose summary is its title — and the families that can see it coming
 * already guard it (media.js drops a caption that is the title). Here it holds
 * for every kind, on events no fixture anticipated.
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
