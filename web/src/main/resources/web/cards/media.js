// The media family. The media itself is on the card at both depths, because here the file
// is the event. URLs come from NIP-92 imeta first, then the legacy url/image tags. The file
// headers at the end of this file describe bytes that live somewhere else — in a sibling event
// (1065), on a server (1163, 1808) or in a swarm (2003) — so what they can show is a preview
// and a promise, never the thing itself.

import { esc, clip, titleOf, summaryOf, imageOf } from "../shared/format.js";
import {
  register, registerRow, shell, titleHtml, bodyHtml, replyLine, emojiGrid, chipRow, hashtagHref,
  extLink, relayRows, refRows, imetas, tagOf, tagsOf, topicsOf, clipIf, audioEmbed, noteHref,
  fmtBytes, fmtDuration, plural,
} from "./base.js";
import { shortNote } from "../shared/nip19.js";

/** The file a single-file card is about: its first imeta, read whole, never fields across several. */
const EMPTY = Object.freeze(Object.create(null));
const fileOf = (ev) => imetas(ev)[0] || EMPTY;
/** An imeta field, else the same field as a top-level tag. */
const fieldOf = (m, ev, name) => m[name] || tagOf(ev, name);

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
    chipRow(topicsOf(ev), opts, hashtagHref);
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
    chipRow(topicsOf(ev), opts, hashtagHref);
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
    (opts && opts.full ? audioEmbed(url) : "") +
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

/**
 * 32176 — a Blossom piece index: one file, the chunks it is split into, and the servers holding
 * them. Reassembling the pieces is a download client's job, so what this card offers is the
 * whole-file url (`r`) the publisher may have included beside them.
 *
 * `size` is published as a string and treated as one: it is a byte count in every event seen,
 * but a publisher who wrote something else there has not said how big the file is, and guessing
 * units would be inventing one.
 */
function pieceIndexCard(ev, opts) {
  const size = tagOf(ev, "size");
  const hash = tagOf(ev, "x");
  const pieces = tagsOf(ev, "b").filter((t) => t[1]);
  const servers = tagsOf(ev, "blossom").map((t) => t[1]).filter(Boolean);
  const img = imageOf(ev);
  const full = opts && opts.full;
  const inner =
    titleHtml(opts, titleOf(ev), 140) +
    (full && img ? mediaFrame("", pictureImg({ url: img })) : "") +
    bodyHtml(opts, summaryOf(ev) || ev.content, 300, true) +
    `<div class="result-body">${esc(plural(pieces.length, "piece"))}</div>` +
    (full && servers.length ? relayRows(servers.map((url) => ({ url })), opts) : "");
  return shell(ev, opts, inner, [
    ["file", extLink(tagOf(ev, "r"))],
    ["type", tagOf(ev, "type") ? esc(tagOf(ev, "type")) : null],
    ["size", fmtBytes(size) || (size ? esc(clip(size, 40)) : null)],
    ["hash", hash ? `<span class="mono">${esc(clip(hash, 20))}</span>` : null],
  ]);
}

/** The file facts these headers share, in the order a reader wants them. */
const fileFacts = (ev) => [
  ["type", tagOf(ev, "m") ? esc(clip(tagOf(ev, "m"), 40)) : null],
  ["size", fmtBytes(tagOf(ev, "size"))],
  ["dimensions", tagOf(ev, "dim") ? esc(clip(tagOf(ev, "dim"), 24)) : null],
  ["hash", tagOf(ev, "x") ? `<span class="mono">${esc(clip(tagOf(ev, "x"), 20))}</span>` : null],
];

/**
 * 1065 — a NIP-95 file header. The bytes are not here: they are in a sibling kind-1064 event
 * this one names, which is why the card shows the preview image the header carries rather than
 * the file, and links the event that holds it.
 */
function fileStorageCard(ev, opts) {
  const preview = tagOf(ev, "image", "thumb");
  const data = tagsOf(ev, "e").map((t) => t[1]).filter((v) => /^[0-9a-f]{64}$/.test(v || ""));
  const full = opts && opts.full;
  const inner =
    (full && preview ? mediaFrame(frameStyle(tagOf(ev, "dim"), opts), pictureImg({ url: preview })) : "") +
    titleHtml(opts, titleOf(ev), 140) +
    bodyHtml(opts, summaryOf(ev) || ev.content, 300, true) +
    refRows(data.map((id) => ({ kind: "e", value: id })), opts);
  return shell(ev, opts, inner, [
    ...fileFacts(ev),
    ["stored by", tagOf(ev, "service") ? esc(clip(tagOf(ev, "service"), 60)) : null],
  ]);
}

/** 1163 — a gallery entry: a picture the author pinned to their profile, and where it came from. */
function galleryCard(ev, opts) {
  const url = tagOf(ev, "url") || tagOf(ev, "image");
  const from = tagsOf(ev, "e").map((t) => t[1]).find((v) => /^[0-9a-f]{64}$/.test(v || ""));
  const inner =
    (url ? mediaFrame(frameStyle(tagOf(ev, "dim"), opts), pictureImg({ url, alt: tagOf(ev, "alt") })) : "") +
    bodyHtml(opts, summaryOf(ev) || ev.content, 200) +
    (from ? `<div class="result-body">from <a class="mono" href="${noteHref(from)}">${esc(shortNote(from))}</a></div>` : "");
  return shell(ev, opts, inner, fileFacts(ev));
}

/** 1808 — an audio header: a track that streams from one url and downloads from another. */
function audioHeaderCard(ev, opts) {
  const stream = tagOf(ev, "stream_url");
  const download = tagOf(ev, "download_url");
  const inner =
    titleHtml(opts, titleOf(ev), 140) +
    (opts && opts.full ? audioEmbed(stream || download) : "") +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner, [
    ["stream", extLink(stream)],
    ["download", extLink(download)],
  ]);
}

/** What a torrent ships: `["file", <name>, <bytes>]`, one per file. */
const torrentFiles = (ev) => tagsOf(ev, "file").filter((t) => t[1]).map((t) => ({ name: t[1], bytes: t[2] }));

/** The whole torrent's size: every file's byte count, where the publisher gave one. */
const torrentBytes = (ev) => torrentFiles(ev).reduce((sum, f) => {
  const n = Number(f.bytes);
  return sum + (Number.isFinite(n) && n > 0 ? n : 0);
}, 0);

/**
 * 2003 — a torrent. The swarm is reached by a `magnet:` link, which is not a scheme this page
 * will put in an href, so the info hash is shown as the thing to copy instead.
 */
function torrentCard(ev, opts) {
  const files = torrentFiles(ev);
  const bytes = torrentBytes(ev);
  const trackers = tagsOf(ev, "tracker").map((t) => t[1]).filter(Boolean);
  const shown = opts && opts.full ? files : files.slice(0, 6);
  const more = files.length - shown.length;
  const inner =
    titleHtml(opts, titleOf(ev), 140) +
    `<div class="result-body">${esc(plural(files.length, "file"))}${bytes ? ` · ${esc(fmtBytes(bytes))}` : ""}</div>` +
    bodyHtml(opts, ev.content, 300) +
    (shown.length
      ? `<ul class="ref-list">${shown.map((f) => `<li>${esc(clip(f.name, 90))}${fmtBytes(f.bytes) ? ` <span class="muted-note">${esc(fmtBytes(f.bytes))}</span>` : ""}</li>`).join("")}` +
        `${more > 0 ? `<li class="muted-note">…and ${more} more</li>` : ""}</ul>`
      : "") +
    chipRow(topicsOf(ev), opts, hashtagHref);
  return shell(ev, opts, inner, [
    ["info hash", tagOf(ev, "btih", "x") ? `<span class="mono">${esc(clip(tagOf(ev, "btih", "x"), 40))}</span>` : null],
    ["trackers", trackers.length ? String(trackers.length) : null],
  ]);
}

/** 2004 — a comment on a torrent: a note whose parent is the torrent it answers. */
function torrentCommentCard(ev, opts) {
  return shell(ev, opts, replyLine(ev) + bodyHtml(opts, ev.content, 400));
}

register([20], pictureCard);
register([21, 22, 34235, 34236], videoCard);
register([1063], fileCard);
// 1222/1244 are NIP-A0 voice messages and their replies.
register([1986, 1222, 1244], audioCard);
register([30005], videoSetCard);
register([30030], emojiPackCard);
register([32176], pieceIndexCard);
register([1065], fileStorageCard);
register([1163], galleryCard);
register([1808], audioHeaderCard);
register([2003], torrentCard);
register([2004], torrentCommentCard);

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
// A file is what it is and how big: the pieces are the kind's business, not the reader's first line.
registerRow([32176], (ev) => ({
  name: titleOf(ev),
  sub: [tagOf(ev, "type"), fmtBytes(tagOf(ev, "size")), summaryOf(ev) || ev.content].filter(Boolean).join(" · "),
}));
registerRow([1065], (ev) => ({
  name: titleOf(ev) || summaryOf(ev) || ev.content,
  sub: [tagOf(ev, "m"), fmtBytes(tagOf(ev, "size")), tagOf(ev, "service")].filter(Boolean).join(" · "),
}));
registerRow([1163], (ev) => ({
  name: summaryOf(ev) || ev.content || tagOf(ev, "url"),
  sub: [tagOf(ev, "m"), fmtBytes(tagOf(ev, "size"))].filter(Boolean).join(" · "),
}));
registerRow([1808], (ev) => ({ name: titleOf(ev), sub: ev.content }));
registerRow([2003], (ev) => ({
  name: titleOf(ev),
  sub: [plural(torrentFiles(ev).length, "file"), fmtBytes(torrentBytes(ev)), ev.content].filter(Boolean).join(" · "),
}));
registerRow([2004], (ev) => ({ name: ev.content }));
