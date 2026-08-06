// The media family. The permalink shows the media itself — an <img> or a
// <video controls> — because for these kinds the file IS the event. A picture
// keeps its small thumbnail in the results list so a list stays a list; a
// VIDEO does not, and that difference is deliberate (see videoCard).
// URLs come from NIP-92 imeta first, then the legacy url/image tags.

import { esc, titleOf, imageOf } from "../shared/format.js";
import { register, shell, titleHtml, bodyHtml, replyLine, emojiGrid, chipRow, extLink, imetaField, tagOf, tagsOf, clipIf } from "./base.js";

const mediaUrl = (ev) => imetaField(ev, "url") || tagOf(ev, "url");
const poster = (ev) => imetaField(ev, "image") || imageOf(ev);
/** imeta first, then the same field as a top-level tag — NIP-71 allows both. */
const mediaField = (ev, name) => imetaField(ev, name) || tagOf(ev, name);

const imgEmbed = (url) =>
  `<div class="embed"><img src="${esc(url)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.parentElement.remove()" /></div>`;
const thumbFallback = (url) =>
  url ? `<img class="thumb" src="${esc(url)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()" />` : "";

/**
 * 20 — picture-first. One body, computed once: the first version of these
 * two cards composed "embed-or-preview-block" + "full-mode body" as separate
 * terms, and the corner cases double-rendered — a full-depth video with no
 * url printed its text twice, and a preview picture with no url printed
 * nothing at all.
 */
function pictureCard(ev, opts) {
  const url = mediaUrl(ev) || imageOf(ev);
  const body = bodyHtml(opts, titleOf(ev) || ev.content, 200);
  const inner = opts && opts.full
    ? (url ? imgEmbed(url) : "") + body
    : `<div class="result-main"><div class="text">${body}</div>${thumbFallback(url)}</div>`;
  return shell(ev, opts, inner);
}

/**
 * 21/22/34235/34236 — video. The video itself, at BOTH depths.
 *
 * Every other family keeps its preview small on the principle that a results
 * list is a list. For video that principle rendered nothing: a kind 22 names
 * no `image`, so the 92px poster slot beside the text was empty, and the text
 * was the title ladder falling through to the `d` tag — a byline over
 * `f56d739a-09c9-4f0b-ba82-f8c21e1a6b8e`, with the caption the event was
 * carrying in `content` never reaching the page. The file IS the event here,
 * so the frame is the card and the list is worth scrolling.
 *
 * Three details are what keep a feed of these calm rather than janky:
 * `dim` sizes the frame before a byte is fetched (a portrait video reserves a
 * portrait box, so nothing below it moves when the metadata lands); `#t=0.1`
 * asks for the first frame when the event names no poster, because Safari
 * paints a black rectangle otherwise; and `preload` drops to `none` behind a
 * poster, since a picture we already have is not worth a second request.
 */
function videoCard(ev, opts) {
  const url = mediaUrl(ev);
  const title = titleOf(ev);
  // The caption: what the author wrote, then the media's own description, and
  // only last the NIP-31 `alt` — which is boilerplate by design ("Vertical
  // Video"), a line for clients that cannot render the kind at all rather than
  // anything about this video. A title already has its own line above, so it
  // never repeats itself down here — which is all the body of a titled video's
  // card used to be.
  const caption = ev.content || tagOf(ev, "summary", "description") || imetaField(ev, "alt") || tagOf(ev, "alt");
  const tags = tagsOf(ev, "t").filter((t) => t[1]).map((t) => `#${t[1]}`);
  const inner =
    titleHtml(opts, title, 140) +
    (url ? videoFrame(ev, url, opts) : "") +
    bodyHtml(opts, caption === title ? "" : caption, 300) +
    chipRow(tags, opts);
  return shell(ev, opts, inner, opts && opts.full
    ? [["url", extLink(url)], ["duration", esc(fmtDuration(mediaField(ev, "duration")))], ["size", fmtBytes(mediaField(ev, "size"))]]
    : []);
}

function videoFrame(ev, url, opts) {
  const p = poster(ev);
  const dur = fmtDuration(mediaField(ev, "duration"));
  const src = (p || url.includes("#")) ? url : `${url}#t=0.1`;
  const shape = frameStyle(ev, opts);
  return `<div class="media-frame${shape ? " sized" : ""}"${shape}>` +
    `<video controls playsinline preload="${p ? "none" : "metadata"}"${p ? ` poster="${esc(p)}"` : ""} src="${esc(src)}" onerror="this.parentElement.remove()"></video>` +
    (dur ? `<span class="media-chip">${esc(dur)}</span>` : "") +
    "</div>";
}

/**
 * The frame's shape, from NIP-92 `dim`, as an inline style — and the one place
 * on these cards where an event's own numbers reach a `style` attribute, which
 * is why the parse is a strict full-string match on two short runs of digits
 * and everything else yields no attribute at all rather than a sanitised one.
 *
 * `max-width` is the height cap the stylesheet applies, converted through the
 * ratio into the width that cap implies. Without it a 1088x1920 phone video
 * is a full-width black band with a narrow slot of picture down the middle;
 * with it, the card is the shape of the video.
 */
function frameStyle(ev, opts) {
  const m = /^(\d{1,5})x(\d{1,5})$/.exec(mediaField(ev, "dim") || "");
  const w = m ? Number(m[1]) : 0, h = m ? Number(m[2]) : 0;
  if (!w || !h) return "";
  // The floor is for the shapes nobody films in: `dim 100x4000` is a legal tag
  // and the width its ratio implies is nine pixels, which is a card with a
  // hairline where the video should be. Below the floor the frame keeps the
  // floor's width and letterboxes, which is at least a thing you can watch.
  const cap = opts && opts.full
    ? `${Math.max(12, 70 * w / h).toFixed(1)}vh`
    : `${Math.max(96, Math.round(360 * w / h))}px`;
  return ` style="aspect-ratio: ${w} / ${h}; max-width: min(100%, ${cap})"`;
}

/** NIP-71 `duration` is a count of seconds; a video's length reads as 0:42. */
const fmtDuration = (secs) => {
  const n = Math.round(Number(secs));
  if (!Number.isFinite(n) || n <= 0) return null;
  const two = (x) => String(x).padStart(2, "0");
  return n >= 3600
    ? `${Math.floor(n / 3600)}:${two(Math.floor(n / 60) % 60)}:${two(n % 60)}`
    : `${Math.floor(n / 60)}:${two(n % 60)}`;
};

const fmtBytes = (n) => {
  n = Number(n);
  if (!Number.isFinite(n) || n <= 0) return null;
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
};

/** 1063 — file metadata: what it is, how big, where; preview when it is an image. */
function fileCard(ev, opts) {
  const url = tagOf(ev, "url");
  const mime = tagOf(ev, "m") || "";
  const full = opts && opts.full;
  const inner =
    (full && url && mime.startsWith("image/") ? imgEmbed(url) : "") +
    bodyHtml(opts, ev.content || titleOf(ev), 300);
  return shell(ev, opts, inner, [
    ["file", extLink(url)],
    ["type", mime ? esc(mime) : null],
    ["size", fmtBytes(tagOf(ev, "size"))],
  ]);
}

/**
 * 1986 — audio: playable when a url is present. A 1244 is a voice message
 * REPLY and carries nothing but the audio, so who it answers is the only text
 * on the card and the only way to place it in a conversation.
 */
function audioCard(ev, opts) {
  const url = mediaUrl(ev);
  const inner =
    replyLine(ev) +
    (opts && opts.full && url ? `<div class="embed"><audio controls preload="metadata" src="${esc(url)}"></audio></div>` : "") +
    bodyHtml(opts, ev.content || titleOf(ev), 300);
  return shell(ev, opts, inner);
}

/** 30005 — a video set: title + how many it holds. */
function videoSetCard(ev, opts) {
  const n = tagsOf(ev, "a").length + tagsOf(ev, "e").length;
  const inner =
    (titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 120))}</h2>` : "") +
    `<div class="result-body">${n} video${n === 1 ? "" : "s"}</div>`;
  return shell(ev, opts, inner);
}

/** 30030 — an emoji pack: the emoji, visible, which is the whole point of one. */
function emojiPackCard(ev, opts) {
  const emoji = tagsOf(ev, "emoji").filter((t) => t[1] && t[2]).map((t) => [t[1], t[2]]);
  const inner =
    (titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 120))}</h2>` : "") +
    `<div class="result-body">${emoji.length} emoji</div>` +
    emojiGrid(emoji, opts);
  return shell(ev, opts, inner);
}

register([20], pictureCard);
register([21, 22, 34235, 34236], videoCard);
register([1063], fileCard);
// 1222/1244 are NIP-A0 voice messages and their replies: an imeta audio url
// with the spoken words nowhere in the event, so the player IS the content.
register([1986, 1222, 1244], audioCard);
register([30005], videoSetCard);
register([30030], emojiPackCard);
