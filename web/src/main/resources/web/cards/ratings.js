// Reviews and ratings: a review of a relay (31987) and the rate-anything entity rating (34259).
// Neither is a merged NIP. Both are one score out of five over an optional written review, so
// they share the star row, and both are addressable — `(pubkey, d)` is the identity, which means
// one rating per author per subject and a newer one replaces it.

import { esc, clip } from "../shared/format.js";
import { shortAddr, shortNote } from "../shared/nip19.js";
import {
  register, registerRow, registerNamedPeople, shell, titleHtml, bodyHtml, chipRow, personLink,
  noteHref, addrHref, hostOf, tagOf, tagsOf, oneLine, plural,
} from "./base.js";

const HEX64 = /^[0-9a-f]{64}$/;
/** Every scale here is out of five; the published 0..1 fraction is `stars / 5`. */
const MAX_STARS = 5;

/** A score as ★★★★☆ with the number beside it. A rating we cannot read draws nothing — not zero stars, which would misreport the author. */
function starsHtml(stars) {
  if (stars == null) return "";
  const n = Math.max(0, Math.min(MAX_STARS, stars));
  const filled = Math.round(n);
  const exact = Number.isInteger(n) ? String(n) : n.toFixed(1);
  return `<span class="stars" title="${esc(exact)} out of ${MAX_STARS}">` +
    `<span class="star-glyphs">${"★".repeat(filled)}${"☆".repeat(MAX_STARS - filled)}</span> ` +
    `<span class="star-n">${esc(exact)}</span></span>`;
}

/** The same score as a row can say it: "4.5★". */
const starsText = (stars) => (stars == null ? "" : `${Number.isInteger(stars) ? stars : stars.toFixed(1)}★`);

// ---- 31987, a relay review -------------------------------------------------

/**
 * The `rating` tags, overall and per aspect. This kind is only ever published on the 0..1 scale,
 * so a value outside it is malformed rather than another convention to guess at.
 */
const scoresOf = (ev) => tagsOf(ev, "rating")
  .map((t) => ({ value: Number(t[1]), category: oneLine(t[2]) }))
  .filter((r) => Number.isFinite(r.value) && r.value >= 0 && r.value <= 1);

/** The overall score: the `rating` tag that names no aspect. */
const overallOf = (ev) => {
  const r = scoresOf(ev).find((s) => !s.category);
  return r ? r.value * MAX_STARS : null;
};

/** The reviewed relay: its `d`, which is the addressable identity, else a `relay` tag. */
const relayOf = (ev) => tagOf(ev, "d", "relay");

/** 31987 — a review of one relay, headed by the relay rather than by the reviewer. */
function relayReviewCard(ev, opts) {
  const url = relayOf(ev);
  const aspects = scoresOf(ev).filter((s) => s.category);
  const inner =
    titleHtml(opts, hostOf(url), 140) +
    `<div class="result-body">${starsHtml(overallOf(ev))}</div>` +
    bodyHtml(opts, ev.content, 400) +
    chipRow(aspects.map((s) => `${s.category} ${starsText(s.value * MAX_STARS)}`), opts);
  return shell(ev, opts, inner, [["relay", url ? `<span class="mono">${esc(clip(url, 80))}</span>` : null]]);
}

// ---- 34259, a rating of anything -------------------------------------------

/** What sort of thing is rated. An absent `m` means a nostr event, which is the spec's default, not a guess. */
const markOf = (ev) => oneLine(tagOf(ev, "m")) || "event";

/**
 * The rated id with its `<mark>:` prefix removed — the spec asks for one on ids that are not
 * unique alone ("hashtag:<tag>"). Only stripped when the prefix is this event's own mark, so a
 * `d` that carries colons for other reasons (an address, say) survives intact.
 */
function subjectOf(ev) {
  const d = oneLine(tagOf(ev, "d"));
  const mark = oneLine(tagOf(ev, "m"));
  return mark && d.startsWith(`${mark}:`) ? d.slice(mark.length + 1) : d;
}

/**
 * The stars, out of [MAX_STARS], or null when nothing here can be read as a score.
 *
 * Two incompatible conventions are in the wild and the `rating` tag alone cannot always separate
 * them: `["rating", "1"]` is a full score to a client publishing the 0..1 fraction and one star
 * out of five to one publishing a raw count. So an `s` tag inside 1..5 wins outright, being the
 * author's own star count; otherwise a `rating` in 0..1 is read as a fraction and scaled up,
 * which resolves the ambiguous `"1"` toward the documented scale; otherwise a `rating` in 1..5
 * is read as a raw count.
 */
function ratedStars(ev) {
  const s = Number.parseInt(tagOf(ev, "s"), 10);
  if (Number.isInteger(s) && s >= 1 && s <= MAX_STARS) return s;
  // An absent tag is not a zero: `Number(null)` is 0, which would publish a one-star verdict
  // this author never gave.
  const rating = tagOf(ev, "rating");
  if (rating === null) return null;
  const raw = Number(rating);
  if (!Number.isFinite(raw) || raw < 0) return null;
  if (raw <= 1) return raw * MAX_STARS;
  return raw <= MAX_STARS ? raw : null;
}

/**
 * What was rated, as `{href, label}`: the coordinate in `a`/`A`, else the event id in `e`, else
 * the `d` itself, which for a nostr subject is the id and for everything else is the thing's own
 * name. Never a bare 64-hex on screen — that places nothing for a reader.
 */
function subjectLink(ev) {
  const addr = tagOf(ev, "a", "A");
  if (addr) return { href: addrHref(addr), label: shortAddr(addr) };
  const id = tagsOf(ev, "e").map((t) => t[1]).find((v) => HEX64.test(v || ""));
  if (id) return { href: noteHref(id), label: shortNote(id) };
  const subject = subjectOf(ev);
  if (HEX64.test(subject)) return { href: noteHref(subject), label: shortNote(subject) };
  return { href: addrHref(subject), label: subject };
}

/** Who wrote what was rated, when the rating names them. */
const ratedAuthor = (ev) => (tagsOf(ev, "p").map((t) => t[1]).find((pk) => HEX64.test(pk || "")) || null);

/** 34259 — a rating of anything: a book, a relay, a hashtag, an event. */
function entityRatingCard(ev, opts) {
  const mark = markOf(ev);
  const { href, label } = subjectLink(ev);
  const author = ratedAuthor(ev);
  const subject = label
    ? (href ? `<a href="${href}">${esc(clip(label, 60))}</a>` : `<span class="mono">${esc(clip(label, 60))}</span>`)
    : `a ${esc(mark)}`;
  const inner =
    `<div class="result-body">${starsHtml(ratedStars(ev))} rated ${subject}${author ? ` by ${personLink(author)}` : ""}</div>` +
    bodyHtml(opts, ev.content, 400) +
    chipRow([mark], opts);
  return shell(ev, opts, inner, [
    ["kind rated", tagOf(ev, "k") ? esc(clip(tagOf(ev, "k"), 20)) : null],
  ]);
}

register([31987], relayReviewCard);
register([34259], entityRatingCard);
// The rated event's author is named by this card alone; no scan of `p` tags reaches them.
registerNamedPeople([34259], (ev) => [ratedAuthor(ev)].filter(Boolean));

// A review's first line is what it reviewed; the score and the words go under it.
registerRow([31987], (ev) => {
  const aspects = scoresOf(ev).filter((s) => s.category);
  return {
    name: hostOf(relayOf(ev)) || "a relay",
    sub: [starsText(overallOf(ev)), ev.content, aspects.length ? plural(aspects.length, "aspect") : ""]
      .filter(Boolean).join(" · "),
  };
});
registerRow([34259], (ev) => {
  const { label } = subjectLink(ev);
  const stars = starsText(ratedStars(ev));
  return {
    name: `rated ${clip(label, 60) || `a ${markOf(ev)}`}${stars ? ` ${stars}` : ""}`,
    sub: ev.content,
  };
});
