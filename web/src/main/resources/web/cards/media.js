// The media family. The media itself is on the card at both depths, because here the file
// is the event. URLs come from NIP-92 imeta first, then the legacy url/image tags.

import { esc, titleOf, summaryOf, imageOf } from "../shared/format.js";
import {
  register, registerRow, shell, titleHtml, bodyHtml, replyLine, emojiGrid, chipRow, hashtagHref, extLink,
  imetas, tagOf, tagsOf, clipIf, plural,
} from "./base.js";

/** The file a single-file card is about: its first imeta, read whole, never fields across several. */
const EMPTY = Object.freeze(Object.create(null));
const fileOf = (ev) => imetas(ev)[0] || EMPTY;
/** An imeta field, else the same field as a top-level tag. */
const fieldOf = (m, ev, name) => m[name] || tagOf(ev, name);
/** The event's topics as `#` chips, each once, case-insensitively. */
function topics(ev) {
  const seen = new Set(), out = [];
  for (const t of tagsOf(ev, "t")) {
    const v = String(t[1] || "").replace(/^#+/, "").trim();
    if (!v || seen.has(v.toLowerCase())) continue;
    seen.add(v.toLowerCase());
    out.push(`#${v}`);
  }
  return out;
}

/** The author's text, then the media's description, and last the NIP-31 `alt`. */
const captionOf = (ev, media) =>
  ev.content || tagOf(ev, "summary", "description") || (media && media.alt) || tagOf(ev, "alt");

/** 20 — a NIP-68 picture post, an album of one imeta per image. */
function pictureCard(ev, opts) {
  const pics = imetas(ev).filter((m) => m.url);
  const legacy = tagOf(ev, "url") || imageOf(ev);
  const shown = pics.length ? pics : (legacy ? [{ url: legacy }] : []);
  const title = titleOf(ev);
  const caption = captionOf(ev, shown[0]);
  const inner =
    titleHtml(opts, title, 140) +
    (shown.length === 1 ? mediaFrame(frameStyle(shown[0].dim, opts), pictureImg(shown[0])) : pictureGrid(shown, opts)) +
    bodyHtml(opts, caption === title ? "" : caption, 300) +
    chipRow(topics(ev), opts, hashtagHref);
  return shell(ev, opts, inner);
}

/**
 * One picture. `whenBroken` is what a dead url removes: the frame around a lone picture,
 * the cell inside a grid.
 */
const pictureImg = (m, whenBroken = "this.parentElement.remove()") =>
  `<img src="${esc(m.url)}" alt="${esc(m.alt || "")}" loading="lazy" referrerpolicy="no-referrer" onerror="${whenBroken}" />`;

/** The album: four in the list, all of them on the permalink. */
function pictureGrid(pics, opts) {
  if (!pics.length) return "";
  const shown = opts && opts.full ? pics : pics.slice(0, 4);
  const more = pics.length - shown.length;
  return `<div class="media-grid">${shown.map((m) => pictureImg(m, "this.remove()")).join("")}</div>` +
    (more > 0 ? `<div class="muted-note">…and ${more} more</div>` : "");
}

/** 21/22/34235/34236 — video: the player itself, at both depths. */
function videoCard(ev, opts) {
  const m = fileOf(ev);
  const url = fieldOf(m, ev, "url");
  const title = titleOf(ev);
  const caption = captionOf(ev, m);
  const inner =
    titleHtml(opts, title, 140) +
    (url ? videoFrame(m, ev, url, opts) : "") +
    bodyHtml(opts, caption === title ? "" : caption, 300) +
    chipRow(topics(ev), opts, hashtagHref);
  return shell(ev, opts, inner, opts && opts.full
    ? [["url", extLink(url)], ["duration", esc(fmtDuration(fieldOf(m, ev, "duration")))], ["size", fmtBytes(fieldOf(m, ev, "size"))]]
    : []);
}

/** The player, framed by `dim` before a byte loads. `#t=0.1` without a poster, or Safari paints black. */
function videoFrame(m, ev, url, opts) {
  const p = m.image || imageOf(ev);
  const dur = fmtDuration(fieldOf(m, ev, "duration"));
  const src = (p || url.includes("#")) ? url : `${url}#t=0.1`;
  // `data-src`, not `src`: app.js promotes it when the card nears the viewport.
  return mediaFrame(frameStyle(fieldOf(m, ev, "dim"), opts),
    `<video controls playsinline preload="${p ? "none" : "metadata"}"${p ? ` poster="${esc(p)}"` : ""} data-src="${esc(src)}" onerror="this.parentElement.remove()"></video>` +
    (dur ? `<span class="media-chip">${esc(dur)}</span>` : ""));
}

/** The one media box a picture and a video share; `.sized` when `dim` said so. */
const mediaFrame = (shape, inner) => `<div class="media-frame${shape ? " sized" : ""}"${shape}>${inner}</div>`;

/**
 * The frame's shape from `dim` as an inline style. The one place an event's numbers reach
 * a `style` attribute, so anything but two short digit runs yields no attribute at all.
 */
function frameStyle(dim, opts) {
  const m = /^(\d{1,5})x(\d{1,5})$/.exec(dim || "");
  const w = m ? Number(m[1]) : 0, h = m ? Number(m[2]) : 0;
  if (!w || !h) return "";
  // The floor keeps an absurd ratio at a watchable width; the frame letterboxes below it.
  const cap = opts && opts.full
    ? `${Math.max(12, 70 * w / h).toFixed(1)}vh`
    : `${Math.max(96, Math.round(360 * w / h))}px`;
  return ` style="aspect-ratio: ${w} / ${h}; max-width: min(100%, ${cap})"`;
}

/** Seconds as 0:42. */
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

/** 1063 — file metadata, with the file itself when it is an image. */
function fileCard(ev, opts) {
  const url = tagOf(ev, "url");
  const mime = tagOf(ev, "m") || "";
  const full = opts && opts.full;
  const inner =
    (full && url && mime.startsWith("image/")
      ? mediaFrame(frameStyle(tagOf(ev, "dim"), opts), pictureImg({ url, alt: tagOf(ev, "alt") }))
      : "") +
    bodyHtml(opts, ev.content || titleOf(ev), 300);
  return shell(ev, opts, inner, [
    ["file", extLink(url)],
    ["type", mime ? esc(mime) : null],
    ["size", fmtBytes(tagOf(ev, "size"))],
  ]);
}

/** 1986 — audio; a 1244 voice reply carries nothing but the audio, so the reply line is its text. */
function audioCard(ev, opts) {
  const url = fieldOf(fileOf(ev), ev, "url");
  const inner =
    replyLine(ev) +
    (opts && opts.full && url ? `<div class="embed"><audio controls preload="metadata" src="${esc(url)}"></audio></div>` : "") +
    bodyHtml(opts, ev.content || titleOf(ev), 300);
  return shell(ev, opts, inner);
}

/** How many videos a set names, by address or by id. */
const setSize = (ev) => tagsOf(ev, "a").length + tagsOf(ev, "e").length;

/** 30005 — a video set. */
function videoSetCard(ev, opts) {
  const inner =
    (titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 120))}</h2>` : "") +
    bodyHtml(opts, summaryOf(ev), 300, true) +
    `<div class="result-body">${esc(plural(setSize(ev), "video"))}</div>`;
  return shell(ev, opts, inner);
}

/** The (shortcode, url) pairs a pack defines; a tag missing either half is skipped. */
const emojiOf = (ev) => tagsOf(ev, "emoji").filter((t) => t[1] && t[2]).map((t) => [t[1], t[2]]);

/** 30030 — an emoji pack. */
function emojiPackCard(ev, opts) {
  const emoji = emojiOf(ev);
  const inner =
    (titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 120))}</h2>` : "") +
    bodyHtml(opts, summaryOf(ev), 300, true) +
    `<div class="result-body">${esc(plural(emoji.length, "emoji", "emoji"))}</div>` +
    emojiGrid(emoji, opts);
  return shell(ev, opts, inner);
}

register([20], pictureCard);
register([21, 22, 34235, 34236], videoCard);
register([1063], fileCard);
// 1222/1244 are NIP-A0 voice messages and their replies.
register([1986, 1222, 1244], audioCard);
register([30005], videoSetCard);
register([30030], emojiPackCard);

/** The title, else the caption; cards.js drops a sub that repeats the name. */
const captionRow = (ev) => {
  const title = titleOf(ev);
  const caption = captionOf(ev, fileOf(ev));
  return { name: title || caption, sub: title ? caption : "" };
};
registerRow([20], captionRow);
// A caption-less clip's second line is its length.
registerRow([21, 22, 34235, 34236], (ev) => {
  const row = captionRow(ev);
  return { ...row, sub: row.sub || fmtDuration(fieldOf(fileOf(ev), ev, "duration")) };
});
registerRow([1063], (ev) => ({
  name: ev.content || titleOf(ev),
  sub: [tagOf(ev, "m"), fmtBytes(tagOf(ev, "size"))].filter(Boolean).join(" · "),
}));
registerRow([1986, 1222, 1244], (ev) => ({
  name: ev.content || titleOf(ev),
  sub: fmtDuration(fieldOf(fileOf(ev), ev, "duration")),
}));
// Counted the way lists.js counts its sets.
registerRow([30005], (ev) => ({
  name: titleOf(ev),
  sub: [plural(setSize(ev), "video"), summaryOf(ev)].filter(Boolean).join(" · "),
}));
registerRow([30030], (ev) => ({
  name: titleOf(ev),
  sub: [plural(emojiOf(ev).length, "emoji", "emoji"), summaryOf(ev)].filter(Boolean).join(" · "),
}));
