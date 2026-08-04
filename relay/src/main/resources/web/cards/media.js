// The media family. The permalink shows the media itself — an <img> or a
// <video controls> — because for these kinds the file IS the event; the
// search preview keeps the small thumbnail so a results list stays a list.
// URLs come from NIP-92 imeta first, then the legacy url/image tags.

import { esc, titleOf, summaryOf, imageOf } from "../shared/format.js";
import { register, shell, bodyHtml, imetaField, tagOf, tagsOf, clipIf } from "./base.js";

const mediaUrl = (ev) => imetaField(ev, "url") || tagOf(ev, "url");
const poster = (ev) => imetaField(ev, "image") || imageOf(ev);

const imgEmbed = (url) =>
  `<div class="embed"><img src="${esc(url)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.parentElement.remove()" /></div>`;
const thumbFallback = (url) =>
  url ? `<img class="thumb" src="${esc(url)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()" />` : "";

/** 20 — picture-first. */
function pictureCard(ev, opts) {
  const url = mediaUrl(ev) || imageOf(ev);
  const full = opts && opts.full;
  const inner =
    (url ? (full ? imgEmbed(url) : `<div class="result-main"><div class="text">${bodyHtml(opts, titleOf(ev) || ev.content, 200)}</div>${thumbFallback(url)}</div>`) : "") +
    (full ? bodyHtml(opts, titleOf(ev) || ev.content, 200) : "");
  return shell(ev, opts, inner);
}

/** 21/22/34235/34236 — video: playable on the permalink, poster in the list. */
function videoCard(ev, opts) {
  const url = mediaUrl(ev);
  const full = opts && opts.full;
  const embed = full && url
    ? `<div class="embed"><video controls preload="metadata" src="${esc(url)}"${poster(ev) ? ` poster="${esc(poster(ev))}"` : ""}></video></div>`
    : `<div class="result-main"><div class="text">${bodyHtml(opts, titleOf(ev) || summaryOf(ev) || ev.content, 300)}</div>${thumbFallback(poster(ev))}</div>`;
  const inner = embed + (full ? bodyHtml(opts, titleOf(ev) || summaryOf(ev) || ev.content, 300) : "");
  return shell(ev, opts, inner, full && url ? [["url", `<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">${esc(url)}</a>`]] : []);
}

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
    ["file", url ? `<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">${esc(url)}</a>` : null],
    ["type", mime ? esc(mime) : null],
    ["size", fmtBytes(tagOf(ev, "size"))],
  ]);
}

/** 1986 — audio: playable when a url is present. */
function audioCard(ev, opts) {
  const url = mediaUrl(ev);
  const inner =
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
  const emoji = tagsOf(ev, "emoji").filter((t) => t[1] && t[2]);
  const shown = opts && opts.full ? emoji : emoji.slice(0, 16);
  const inner =
    (titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 120))}</h2>` : "") +
    `<div class="result-body">${emoji.length} emoji</div>` +
    (shown.length
      ? `<div class="emoji-grid">${shown.map((t) => `<img src="${esc(t[2])}" alt=":${esc(t[1])}:" title=":${esc(t[1])}:" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()" />`).join("")}</div>`
      : "");
  return shell(ev, opts, inner);
}

register([20], pictureCard);
register([21, 22, 34235, 34236], videoCard);
register([1063], fileCard);
register([1986], audioCard);
register([30005], videoSetCard);
register([30030], emojiPackCard);
