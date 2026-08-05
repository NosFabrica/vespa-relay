// The text family: notes, threads and chat messages (1, 11, 9, 42, 1311),
// highlights (9802), and channel metadata (40/41, whose payload is a
// profile-shaped JSON content).

import { esc } from "../shared/format.js";
import { shortNote } from "../shared/nip19.js";
import { register, shell, bodyHtml, replyLine, noteHref, tagOf, jsonContent, avatarHtml, clipIf } from "./base.js";

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
