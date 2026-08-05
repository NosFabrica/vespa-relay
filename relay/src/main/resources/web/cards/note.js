// The text family: notes, threads and chat messages (1, 11, 9, 42, 1311),
// highlights (9802), and channel metadata (40/41, whose payload is a
// profile-shaped JSON content).

import { esc } from "../shared/format.js";
import { noteId, shortNote } from "../shared/nip19.js";
import { register, shell, bodyHtml, replyLine, noteHref, tagOf, jsonContent, avatarHtml, clipIf } from "./base.js";

/**
 * A note, and — when it is a reply — who it answers, above the text where the
 * context belongs rather than as a row under it.
 *
 * The `note` row stays. It reads as redundant beside the reply line and it is
 * not: in a results list it is the ONLY route from a card to its own page
 * (nothing else in the frame links the event itself), so replacing it with the
 * reply link would have made every reply a dead end at exactly the moment
 * replies became worth following.
 */
function noteCard(ev, opts) {
  const inner = replyLine(ev) + bodyHtml(opts, ev.content, 500);
  const props = [["note", `<a class="mono" href="${noteHref(ev.id)}" title="${esc(noteId(ev.id))}">${esc(shortNote(ev.id))}</a>`]];
  return shell(ev, opts, inner, props);
}

/** 9802 — the content IS the quote; what it was clipped from rides in tags. */
function highlightCard(ev, opts) {
  const quote = clipIf(opts, ev.content, 500);
  const comment = tagOf(ev, "comment");
  const sourceUrl = tagOf(ev, "r");
  const sourceEvent = tagOf(ev, "e");
  const props = [];
  if (sourceUrl) props.push(["source", `<a href="${esc(sourceUrl)}" target="_blank" rel="noopener noreferrer">${esc(sourceUrl)}</a>`]);
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

// 9, 42 and 1311 are chat messages — NIP-C7 rooms, NIP-28 channels, NIP-53
// live streams. Three transports, one shape: a line of text whose context is
// the room it was said in, which the byline and the json toggle already carry.
register([1, 11, 9, 42, 1311], noteCard);
register([9802], highlightCard);
register([40, 41], channelCard);
