// Podcasts and music: a show and its episodes, a trailer, a track and a playlist. Two podcast
// vocabularies are in the store and they disagree about who the author is, which is the one
// thing a reader has to be told apart:
//
// | kind | | |
// |---|---|---|
// | 10154 | NIP-F4 show | the podcast IS its own keypair, and this replaces its `kind:0` |
// | 54    | NIP-F4 episode | a regular event by that same podcast key |
// | 30054 | Podcasting 2.0 episode | addressable, signed by the HUMAN creator, edited in place |
// | 30055 | Podcasting 2.0 trailer | the same, for a show's trailer |
// | 31337 | audio track | its only free text is a `subject` |
// | 36787 | music track | title and artist, with the file in `url` |
// | 34139 | music playlist | `a` tags naming 36787s, in order |
//
// The audio itself plays on the permalink only: a results list of ten episodes must not open
// ten connections to ten media hosts.

import { esc, clip, titleOf, summaryOf, imageOf, mdExcerpt } from "../shared/format.js";
import {
  register, registerRow, registerNamedPeople, shell, titleHtml, bodyHtml, refRows, chipRow,
  topicsOf, hashtagHref, personLink, faceStrip, extLink, audioEmbed, tagOf, tagsOf, oneLine,
  fmtBytes, fmtDuration, plural,
} from "./base.js";

const HEX64 = /^[0-9a-f]{64}$/;

/** The cover, at the depth it belongs to: a banner on the permalink, a thumb beside the text in the list. */
const coverBanner = (img) => (img
  ? `<div class="embed"><img src="${esc(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.parentElement.remove()" /></div>`
  : "");
const coverThumb = (img) => (img
  ? `<img class="thumb cover" src="${esc(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()" />`
  : "");

/**
 * The files a card can play, in the spelling its kind uses: an episode lists `audio` tags
 * (`["audio", <url>, <mime>]`, one per encoding), a trailer and a music track carry a single
 * `url`, an audio track a `media`.
 */
function mediaOf(ev) {
  const audio = tagsOf(ev, "audio").map((t) => t[1]).filter(Boolean);
  if (audio.length) return audio;
  const single = tagOf(ev, "url", "media");
  return single ? [single] : [];
}

/** The players, permalink only, one per encoding the publisher listed. */
const playersHtml = (ev, opts) => (opts && opts.full ? mediaOf(ev).map(audioEmbed).join("") : "");

// ---- 10154, the show -------------------------------------------------------

/** A show's claimed authors: `p` tags whose second slot may name a role. Hex only. */
const claimedAuthors = (ev) =>
  tagsOf(ev, "p").filter((t) => HEX64.test(t[1] || "")).map((t) => ({ pubkey: t[1], role: oneLine(t[2]) }));

/**
 * A known flood: thousands of identical headless-test shows, all with this exact title,
 * description and body. Saying so beats a reader wondering why one show is published a thousand
 * times — and the fingerprint is exact, so nothing real matches it.
 */
const isMockFeed = (ev) =>
  ev.content === "Headless test feed" &&
  tagOf(ev, "title") === "Mock Podcast" &&
  tagOf(ev, "description") === "Headless test feed";

/**
 * 10154 — a podcast show. The podcast is its own keypair, so this event, not the pubkey's
 * `kind:0`, is what the show is called.
 */
function showCard(ev, opts) {
  const authors = claimedAuthors(ev);
  const sites = tagsOf(ev, "website").map((t) => t[1]).filter(Boolean);
  const full = opts && opts.full;
  const inner =
    (full ? coverBanner(imageOf(ev)) : "") +
    `<div class="result-main">
      <div class="text">
        ${titleHtml(opts, titleOf(ev), 140)}
        ${bodyHtml(opts, summaryOf(ev) || ev.content, 400, true)}
        ${isMockFeed(ev) ? `<div class="result-body muted">a headless test feed — this exact show is published in bulk</div>` : ""}
      </div>
      ${full ? "" : coverThumb(imageOf(ev))}
    </div>` +
    // "Claims", not "by": the spec warns these are unverified until the named key counter-signs.
    (authors.length
      ? `<div class="result-body" title="claimed by the show, and unverified — only the named key can confirm it">claims ` +
        authors.map((a) => personLink(a.pubkey) + (a.role ? ` <span class="muted-note">${esc(clip(a.role, 24))}</span>` : "")).join(", ") +
        `</div>`
      : "");
  return shell(ev, opts, inner, [["websites", sites.map((u) => extLink(u)).filter(Boolean).join(" · ") || null]]);
}

// ---- 54 / 30054 / 30055, the episodes --------------------------------------

/** "S2 E14", or whichever half the publisher gave. A trailer carries a season and no number. */
const seasonLine = (ev) => {
  const season = oneLine(tagOf(ev, "season"));
  const number = oneLine(tagOf(ev, "episode"));
  return [season && `S${season}`, number && `E${number}`].filter(Boolean).join(" ");
};

/** 54 / 30054 / 30055 — an episode or a trailer: notes over a player, whichever vocabulary published it. */
function episodeCard(ev, opts) {
  const title = titleOf(ev);
  const summary = summaryOf(ev);
  const full = opts && opts.full;
  const inner =
    (full ? coverBanner(imageOf(ev)) : "") +
    `<div class="result-main">
      <div class="text">
        ${titleHtml(opts, title, 140)}
        ${full
          ? (summary ? `<div class="result-body muted">${esc(summary)}</div>` : "") + bodyHtml(opts, ev.content, 0)
          : bodyHtml(opts, summary || mdExcerpt(ev.content, title), 400, true)}
      </div>
      ${full ? "" : coverThumb(imageOf(ev))}
    </div>` +
    playersHtml(ev, opts) +
    chipRow(topicsOf(ev), opts, hashtagHref);
  return shell(ev, opts, inner, [
    [tagOf(ev, "episode") ? "episode" : "season", seasonLine(ev) ? esc(seasonLine(ev)) : null],
    ["duration", fmtDuration(tagOf(ev, "duration"))],
    // An RFC2822 string kept as published, for whoever regenerates the feed.
    ["published", tagOf(ev, "pubdate") ? esc(clip(tagOf(ev, "pubdate"), 60)) : null],
    ["transcript", extLink(tagOf(ev, "transcript"))],
    ["chapters", extLink(tagOf(ev, "chapters"))],
    ["video", extLink(tagOf(ev, "video"))],
    ["size", fmtBytes(tagOf(ev, "length"))],
    ["type", tagOf(ev, "type") ? esc(clip(tagOf(ev, "type"), 40)) : null],
  ]);
}

// ---- 31337 / 36787 / 34139, the music --------------------------------------

/** 31337 — an audio track. Its `subject` is the only text it carries, so that is its title. */
function audioTrackCard(ev, opts) {
  const cover = tagOf(ev, "cover") || imageOf(ev);
  const full = opts && opts.full;
  const inner =
    (full ? coverBanner(cover) : "") +
    `<div class="result-main">
      <div class="text">
        ${titleHtml(opts, tagOf(ev, "subject"), 140)}
        ${bodyHtml(opts, ev.content, 300)}
      </div>
      ${full ? "" : coverThumb(cover)}
    </div>` +
    playersHtml(ev, opts) +
    faceStrip(tagsOf(ev, "p").map((t) => t[1]).filter((pk) => HEX64.test(pk || "")), full ? 24 : 12);
  return shell(ev, opts, inner, [
    ["type", tagOf(ev, "c") ? esc(clip(tagOf(ev, "c"), 40)) : null],
    ["price", tagOf(ev, "price") ? esc(clip(tagOf(ev, "price"), 40)) : null],
  ]);
}

/** "Artist — Album", the line under a track's title. */
const trackLine = (ev) => [tagOf(ev, "artist"), tagOf(ev, "album")].map(oneLine).filter(Boolean).join(" — ");

/** 36787 — a music track: title over artist, with the file itself on the permalink. */
function musicTrackCard(ev, opts) {
  const line = trackLine(ev);
  const full = opts && opts.full;
  const inner =
    (full ? coverBanner(imageOf(ev)) : "") +
    `<div class="result-main">
      <div class="text">
        ${titleHtml(opts, titleOf(ev), 140)}
        ${line ? `<div class="result-body muted">${esc(clip(line, 200))}</div>` : ""}
        ${bodyHtml(opts, ev.content, 300)}
      </div>
      ${full ? "" : coverThumb(imageOf(ev))}
    </div>` +
    playersHtml(ev, opts) +
    chipRow(topicsOf(ev), opts, hashtagHref);
  return shell(ev, opts, inner, [
    ["track", tagOf(ev, "track_number") ? esc(clip(tagOf(ev, "track_number"), 12)) : null],
    ["released", tagOf(ev, "released") ? esc(clip(tagOf(ev, "released"), 40)) : null],
    ["duration", fmtDuration(tagOf(ev, "duration"))],
    ["format", [tagOf(ev, "format"), tagOf(ev, "bitrate"), tagOf(ev, "sample_rate")]
      .map(oneLine).filter(Boolean).map((v) => esc(clip(v, 24))).join(" · ") || null],
    ["language", tagOf(ev, "language") ? esc(clip(tagOf(ev, "language"), 24)) : null],
    ["explicit", oneLine(tagOf(ev, "explicit")).toLowerCase() === "true" ? "yes" : null],
    ["video", extLink(tagOf(ev, "video"))],
  ]);
}

/** What a playlist holds: `a` tags naming music tracks. Anything else it points at is not a track. */
const tracksOf = (ev) =>
  tagsOf(ev, "a").map((t) => t[1]).filter((a) => /^36787:[0-9a-f]{64}:/.test(String(a || "")));

/** The flags a playlist may carry; `private` wins, so a playlist claiming both is private. */
const flagsOf = (ev) => {
  const on = (name) => oneLine(tagOf(ev, name)).toLowerCase() === "true";
  return [on("private") ? "private" : "public", on("collaborative") ? "collaborative" : ""].filter(Boolean);
};

/** 34139 — a music playlist: its tracks, in the order it lists them. */
function playlistCard(ev, opts) {
  const tracks = tracksOf(ev);
  const full = opts && opts.full;
  const inner =
    (full ? coverBanner(imageOf(ev)) : "") +
    `<div class="result-main">
      <div class="text">
        ${titleHtml(opts, titleOf(ev), 140)}
        ${bodyHtml(opts, summaryOf(ev) || ev.content, 300, true)}
        <div class="result-body">${esc(plural(tracks.length, "track"))}</div>
      </div>
      ${full ? "" : coverThumb(imageOf(ev))}
    </div>` +
    refRows(tracks.map((a) => ({ kind: "a", value: a })), opts) +
    chipRow(flagsOf(ev), opts);
  return shell(ev, opts, inner);
}

register([10154], showCard);
register([54, 30054, 30055], episodeCard);
register([31337], audioTrackCard);
register([36787], musicTrackCard);
register([34139], playlistCard);
// A show's claimed authors are named by this card alone; no scan of `p` tags reaches them.
registerNamedPeople([10154], (ev) => claimedAuthors(ev).map((a) => a.pubkey));

registerRow([10154], (ev) => ({
  name: titleOf(ev),
  sub: [isMockFeed(ev) ? "a headless test feed" : "", summaryOf(ev) || ev.content].filter(Boolean).join(" · "),
  pic: imageOf(ev),
}));
// An episode's second line is where it sits in the show, then what it is about.
registerRow([54, 30054, 30055], (ev) => ({
  name: titleOf(ev),
  sub: [seasonLine(ev), fmtDuration(tagOf(ev, "duration")), summaryOf(ev) || mdExcerpt(ev.content, titleOf(ev))]
    .filter(Boolean).join(" · "),
  pic: imageOf(ev),
}));
registerRow([31337], (ev) => ({ name: tagOf(ev, "subject"), sub: tagOf(ev, "c") || ev.content }));
registerRow([36787], (ev) => ({
  name: titleOf(ev),
  sub: [trackLine(ev), fmtDuration(tagOf(ev, "duration"))].filter(Boolean).join(" · "),
  pic: imageOf(ev),
}));
registerRow([34139], (ev) => ({
  name: titleOf(ev),
  sub: [plural(tracksOf(ev).length, "track"), summaryOf(ev) || ev.content].filter(Boolean).join(" · "),
  pic: imageOf(ev),
}));
