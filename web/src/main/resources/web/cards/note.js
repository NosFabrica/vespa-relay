// The text family: notes, threads and chat messages, highlights, channel metadata (40/41,
// a profile-shaped JSON content) and the NIP-29 group record (39000).

import { esc } from "../shared/format.js";
import { shortNote } from "../shared/nip19.js";
import { avatarHtml } from "../shared/avatar.js";
import { register, registerRow, shell, bodyHtml, replyLine, extLink, noteHref, tagOf, tagsOf, jsonContent, clipIf, titleHtml, groupHref } from "./base.js";

/** A note, with who it answers above the text when it is a reply. */
function noteCard(ev, opts) {
  return shell(ev, opts, replyLine(ev) + bodyHtml(opts, ev.content, 500));
}

/** 9802 — the content is the quote; what it was clipped from rides in tags. */
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

// The NIP-29 access flags, in reading order. The tag's presence is the value.
const GROUP_FLAGS = [
  ["private", "members only"], ["public", "public"],
  ["restricted", "members post"], ["open", "open to post"],
  ["closed", "invite only"], ["hidden", "hidden"],
];

/**
 * The access flags this group carries. A `["private"]` tag has no element 1, so this reads names,
 * not tagOf.
 */
const groupAccess = (ev) => GROUP_FLAGS.filter(([f]) => tagsOf(ev, f).length).map(([, label]) => label);

/**
 * 39000 — a NIP-29 group as its host relay describes it. The title links into the `group:`
 * search, and the id is a property because a name like "General" does not say which one.
 */
function groupCard(ev, opts) {
  const id = tagOf(ev, "d");
  const picture = tagOf(ev, "picture");
  const access = groupAccess(ev);
  const inner = `
    <div class="result-main">
      ${picture ? avatarHtml(picture, ev.pubkey) : ""}
      <div class="text">
        ${titleHtml(opts, tagOf(ev, "name") || id, 120, groupHref(id))}
        ${bodyHtml(opts, tagOf(ev, "about") || "", 400)}
      </div>
    </div>`;
  const props = [];
  if (id) props.push(["group id", `<span class="mono">${esc(id)}</span>`]);
  if (access.length) props.push(["access", esc(access.join(" · "))]);
  return shell(ev, opts, inner, props);
}

// 9, 42 and 1311 are chat messages (NIP-C7 rooms, NIP-28 channels, NIP-53 streams): one shape.
register([1, 11, 9, 42, 1311], noteCard);
register([9802], highlightCard);
register([40, 41], channelCard);
register([39000], groupCard);

// For the text kinds the words are the event, so the row is the content.
registerRow([1, 11, 9, 42, 1311], (ev) => ({ name: ev.content }));
registerRow([9802], (ev) => ({ name: ev.content, sub: tagOf(ev, "comment") || tagOf(ev, "r") }));
// A channel's content is a JSON document; the row reads its fields, never the document.
registerRow([40, 41], (ev) => {
  const c = jsonContent(ev);
  return { name: c.name, sub: c.about, pic: c.picture };
});
// The access flags are the second line when the relay wrote no description.
registerRow([39000], (ev) => ({
  name: tagOf(ev, "name") || tagOf(ev, "d"),
  sub: tagOf(ev, "about") || groupAccess(ev).join(" · "),
  pic: tagOf(ev, "picture"),
}));
