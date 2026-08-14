// The media family. The media itself is on the card at BOTH depths — an
// <img>, a grid of them, or a <video controls> — because for these kinds the
// file IS the event, and a 92px thumbnail beside two lines of text is a card
// about a picture rather than the picture. Every other family still previews
// small; this is the one place where the media is the point.
// URLs come from NIP-92 imeta first, then the legacy url/image tags.

import { esc, titleOf, summaryOf, imageOf } from "../shared/format.js";
import {
  register, registerRow, shell, titleHtml, bodyHtml, replyLine, emojiGrid, chipRow, hashtagHref, extLink,
  imetas, tagOf, tagsOf, clipIf, plural,
} from "./base.js";

/**
 * The file a single-file card is about: its FIRST imeta, read once and passed
 * around whole.
 *
 * One imeta, not "the first tag that happens to carry this field", because a
 * NIP-71 video may list several — an mp4 and a webm of the same thing, each
 * with its own url, dim and duration. Reading field by field across all of
 * them can shape the frame from one file and play another.
 */
const EMPTY = Object.freeze(Object.create(null));
const fileOf = (ev) => imetas(ev)[0] || EMPTY;
/** …falling back to the same field as a top-level tag, which NIP-71 also allows. */
const fieldOf = (m, ev, name) => m[name] || tagOf(ev, name);
/**
 * The topics an event names, as the chips a reader can search from — each one
 * once. `t` tags repeat in the wild (a client writing both the cased and the
 * lowercased spelling is the common one), and two identical chips side by side
 * read as two different topics that happen to look alike. Some clients write
 * the `#` into the tag value and most do not, so it is normalised off here and
 * put back once.
 */
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

/**
 * What a media card says in words: the author's own text, then the media's
 * description, and only last the NIP-31 `alt` — which is boilerplate by
 * design ("Vertical Video"), a line for clients that cannot render the kind
 * at all rather than anything about this file.
 */
const captionOf = (ev, media) =>
  ev.content || tagOf(ev, "summary", "description") || (media && media.alt) || tagOf(ev, "alt");

/**
 * 20 — pictures, and a NIP-68 picture post is an ALBUM: one imeta per image,
 * where reading only the first showed one photo of nine and the search preview
 * showed that one at 92px. A single picture gets the same frame a video does,
 * shaped by its `dim`; several get a grid, capped in the list and complete on
 * the permalink, because a results list is still a list.
 *
 * The imeta `alt` becomes the image's alt text, which is what it was written
 * for — the card was passing `alt=""` while the event carried the description.
 */
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
 * One picture, carrying the description the event wrote for it. `whenBroken`
 * is what a dead url takes with it: the frame around a lone picture (an empty
 * bordered box is worse than no box), the cell alone inside a grid.
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
  const m = fileOf(ev);
  const url = fieldOf(m, ev, "url");
  const title = titleOf(ev);
  // A title already has its own line above, so the caption never repeats it
  // down here — which is all the body of a titled video's card used to be.
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

function videoFrame(m, ev, url, opts) {
  const p = m.image || imageOf(ev);
  const dur = fmtDuration(fieldOf(m, ev, "duration"));
  const src = (p || url.includes("#")) ? url : `${url}#t=0.1`;
  // `data-src`, not `src`: app.js promotes it when the card comes near the
  // viewport. `preload="metadata"` on every card in a list of sixty short
  // videos is sixty range requests before the reader has passed the second.
  return mediaFrame(frameStyle(fieldOf(m, ev, "dim"), opts),
    `<video controls playsinline preload="${p ? "none" : "metadata"}"${p ? ` poster="${esc(p)}"` : ""} data-src="${esc(src)}" onerror="this.parentElement.remove()"></video>` +
    (dur ? `<span class="media-chip">${esc(dur)}</span>` : ""));
}

/** The one media box a picture and a video share — `.sized` when `dim` said so. */
const mediaFrame = (shape, inner) => `<div class="media-frame${shape ? " sized" : ""}"${shape}>${inner}</div>`;

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
function frameStyle(dim, opts) {
  const m = /^(\d{1,5})x(\d{1,5})$/.exec(dim || "");
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

/**
 * 1063 — file metadata: what it is, how big, where; the file itself when it is
 * one this page can show. A 1063 is a NIP-94 description of the same bytes a
 * kind 20 would carry, tags and all, so it uses the same frame — the alternative
 * was a second image embed that sized itself differently for no reason a reader
 * could see.
 */
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

/**
 * 1986 — audio: playable when a url is present. A 1244 is a voice message
 * REPLY and carries nothing but the audio, so who it answers is the only text
 * on the card and the only way to place it in a conversation.
 */
function audioCard(ev, opts) {
  const url = fieldOf(fileOf(ev), ev, "url");
  const inner =
    replyLine(ev) +
    (opts && opts.full && url ? `<div class="embed"><audio controls preload="metadata" src="${esc(url)}"></audio></div>` : "") +
    bodyHtml(opts, ev.content || titleOf(ev), 300);
  return shell(ev, opts, inner);
}

/** How many videos a set names, by address and by id alike. */
const setSize = (ev) => tagsOf(ev, "a").length + tagsOf(ev, "e").length;

/** 30005 — a video set: title + how many it holds. */
function videoSetCard(ev, opts) {
  const inner =
    (titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 120))}</h2>` : "") +
    `<div class="result-body">${esc(plural(setSize(ev), "video"))}</div>`;
  return shell(ev, opts, inner);
}

/** The (shortcode, url) pairs a pack defines — both halves, or it draws nothing. */
const emojiOf = (ev) => tagsOf(ev, "emoji").filter((t) => t[1] && t[2]).map((t) => [t[1], t[2]]);

/** 30030 — an emoji pack: the emoji, visible, which is the whole point of one. */
function emojiPackCard(ev, opts) {
  const emoji = emojiOf(ev);
  const inner =
    (titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 120))}</h2>` : "") +
    `<div class="result-body">${esc(plural(emoji.length, "emoji", "emoji"))}</div>` +
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

/**
 * The row for a file that has something to say: its title, else the words the
 * author wrote about it — and never both, since the caption a titled post
 * repeats verbatim is what the card drops for the same reason.
 *
 * A media event carries no prose at all often enough that the fallback matters:
 * the row is then the author, and the second line is what the file IS.
 */
const captionRow = (ev) => {
  const title = titleOf(ev);
  const caption = captionOf(ev, fileOf(ev));
  return { name: title || caption, sub: title && caption !== title ? caption : "" };
};
registerRow([20], captionRow);
registerRow([21, 22, 34235, 34236], (ev) => ({
  ...captionRow(ev),
  sub: captionRow(ev).sub || fmtDuration(fieldOf(fileOf(ev), ev, "duration")),
}));
registerRow([1063], (ev) => ({
  name: ev.content || titleOf(ev),
  sub: [tagOf(ev, "m"), fmtBytes(tagOf(ev, "size"))].filter(Boolean).join(" · "),
}));
// A voice message has the spoken words nowhere in the event, so its length is
// the only thing there is to say about it beyond who recorded it.
registerRow([1986, 1222, 1244], (ev) => ({
  name: ev.content || titleOf(ev),
  sub: fmtDuration(fieldOf(fileOf(ev), ev, "duration")),
}));
// The two NIP-51 sets this family renders, counted the way lists.js counts its
// own — and carrying their description after the count when they have one.
registerRow([30005], (ev) => ({
  name: titleOf(ev),
  sub: [plural(setSize(ev), "video"), summaryOf(ev)].filter(Boolean).join(" · "),
}));
registerRow([30030], (ev) => ({
  name: titleOf(ev),
  sub: [plural(emojiOf(ev).length, "emoji", "emoji"), summaryOf(ev)].filter(Boolean).join(" · "),
}));
