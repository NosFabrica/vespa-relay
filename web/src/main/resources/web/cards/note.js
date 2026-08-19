// The text family: notes, threads and chat messages (1, 11, 9, 42, 1311),
// highlights (9802), channel metadata (40/41, whose payload is a
// profile-shaped JSON content) and the NIP-29 group record (39000, which is
// the same idea again with the fields in tags and a relay holding the pen).

import { esc } from "../shared/format.js";
import { shortNote } from "../shared/nip19.js";
import { avatarHtml } from "../shared/avatar.js";
import { register, registerRow, shell, bodyHtml, replyLine, extLink, noteHref, tagOf, tagsOf, jsonContent, clipIf, titleHtml, groupHref } from "./base.js";

/**
 * A note, and — when it is a reply — who it answers, above the text where the
 * context belongs.
 *
 * There is no `note: note1qqq…` row any more. It was the card's only route to
 * its own page, which is why it survived being useless to read for so long;
 * the card itself and its byline date lead there now, so the row was left
 * saying nothing to nobody. The id is still one click away under "json".
 */
function noteCard(ev, opts) {
  return shell(ev, opts, replyLine(ev) + bodyHtml(opts, ev.content, 500));
}

/** 9802 — the content IS the quote; what it was clipped from rides in tags. */
function highlightCard(ev, opts) {
  const quote = clipIf(opts, ev.content, 500);
  const comment = tagOf(ev, "comment");
  const sourceUrl = tagOf(ev, "r");
  const sourceEvent = tagOf(ev, "e");
  const props = [];
  if (sourceUrl) props.push(["source", extLink(sourceUrl)]);
  else if (sourceEvent && /^[0-9a-f]{64}$/.test(sourceEvent)) props.push(["source", `<a class="mono" href="${noteHref(sourceEvent)}">${esc(shortNote(sourceEvent))}</a>`]);
  const inner =
    (quote ? `<blockquote class="quote">${esc(quote)}</blockquote>` : "") +
    (comment ? bodyHtml(opts, comment, 300) : "");
  return shell(ev, opts, inner, props);
}

/** 40/41 — channel create/update: a mini profile drawn from JSON content. */
function channelCard(ev, opts) {
  const c = jsonContent(ev);
  const inner = `
    <div class="result-main">
      ${c.picture ? avatarHtml(c.picture, ev.pubkey) : ""}
      <div class="text">
        ${c.name ? `<h2 class="result-title"><a href="${noteHref(ev.id)}">${esc(clipIf(opts, c.name, 120))}</a></h2>` : ""}
        ${bodyHtml(opts, c.about || "", 400)}
      </div>
    </div>`;
  return shell(ev, opts, inner);
}

// The NIP-29 access flags, in the order they answer "can I read this, can I
// post, can I join" — presence IS the value, which is why they are listed
// rather than read as key/value pairs.
const GROUP_FLAGS = [
  ["private", "members only"], ["public", "public"],
  ["restricted", "members post"], ["open", "open to post"],
  ["closed", "invite only"], ["hidden", "hidden"],
];

/**
 * The access flags this group actually carries. Presence of the tag NAME is the
 * whole value, so this reads names rather than going through tagOf — a
 * `["private"]` tag has no element 1 to read.
 */
const groupAccess = (ev) => GROUP_FLAGS.filter(([f]) => tagsOf(ev, f).length).map(([, label]) => label);

/**
 * 39000 — a NIP-29 group, as its HOST RELAY describes it.
 *
 * The channel card's shape with two differences that matter, both of them
 * about who is speaking. The byline is the RELAY, not a person: NIP-29 has the
 * relay sign this record with its own key, so the face above this card is a
 * server describing a room it runs. And the title is a LINK — into this page's
 * own `group:` search — because unlike a channel's metadata this record has
 * somewhere to send the reader: everything posted in the group.
 *
 * The id is shown as a property rather than left to the json toggle. It is not
 * decoration: the id is what a search actually filters on, it is what the
 * reader will paste somewhere else, and a group whose NAME is "General" tells
 * you nothing about which `general` this is.
 */
function groupCard(ev, opts) {
  const id = tagOf(ev, "d");
  const picture = tagOf(ev, "picture");
  const access = groupAccess(ev);
  const inner = `
    <div class="result-main">
      ${picture ? avatarHtml(picture, ev.pubkey) : ""}
      <div class="text">
        ${titleHtml(opts, tagOf(ev, "name") || id, 120, id ? groupHref(id) : null)}
        ${bodyHtml(opts, tagOf(ev, "about") || "", 400)}
      </div>
    </div>`;
  const props = [];
  if (id) props.push(["group id", `<span class="mono">${esc(id)}</span>`]);
  if (access.length) props.push(["access", esc(access.join(" · "))]);
  return shell(ev, opts, inner, props);
}

// 9, 42 and 1311 are chat messages — NIP-C7 rooms, NIP-28 channels, NIP-53
// live streams. Three transports, one shape: a line of text whose context is
// the room it was said in, which the byline and the json toggle already carry.
register([1, 11, 9, 42, 1311], noteCard);
register([9802], highlightCard);
register([40, 41], channelCard);
register([39000], groupCard);

// The rows. For the text kinds the words ARE the event, so the row is the
// content and the second line is who said it — the one family where the old
// generic ladder was already right.
registerRow([1, 11, 9, 42, 1311], (ev) => ({ name: ev.content }));
registerRow([9802], (ev) => ({ name: ev.content, sub: tagOf(ev, "comment") || tagOf(ev, "r") }));
// THE ROW THIS WORK STARTED FROM. A channel keeps its name, description and
// picture in a profile-shaped JSON content, so "the content" was the whole
// document: the row printed `{"about":"","name":"Test group","picture":""}`
// beside a card that had drawn the channel properly all along.
registerRow([40, 41], (ev) => {
  const c = jsonContent(ev);
  return { name: c.name, sub: c.about, pic: c.picture };
});
// The same idea with the fields in tags — and with the access flags as the
// second line when the relay wrote no description, since "members only · invite
// only" is what a reader picking a group out of eight rows is deciding on.
registerRow([39000], (ev) => ({
  name: tagOf(ev, "name") || tagOf(ev, "d"),
  sub: tagOf(ev, "about") || groupAccess(ev).join(" · "),
  pic: tagOf(ev, "picture"),
}));
