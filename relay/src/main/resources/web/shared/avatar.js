// A person's picture, everywhere this page draws one.
//
// A face is never just an `<img>`. It is three things that have to agree
// wherever they appear: the picture, a GENERATED face for somebody who has
// none (or whose host is down) keyed off the pubkey, so a faceless account is
// still stable and distinct rather than a grey hole — and the score chip the
// active lens fills in, which is the question this whole relay is organised
// around. Four modules had grown their own version of that; they differed only
// in how big the face was, and in which of the three parts they had forgotten.
//
// So: one function, and SIZE is an argument. The pixels themselves live in the
// stylesheet's `--av` table (index.html), one line per size, because a face
// also has to shrink with the layout it sits in and CSS is where that is
// decided — this module names the size, it does not measure it.

import { esc } from "./format.js";

/** A pubkey-derived hue, so a missing picture is still a stable, distinct face. */
export const hueOf = (seed) => (parseInt(String(seed || "").slice(0, 4), 16) || 0) % 360;

/** 1x1 transparent gif — what a broken picture is swapped for, in place. */
export const BLANK = "data:image/gif;base64,R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==";

/**
 * The sizes a face is drawn at, as the stylesheet knows them:
 *
 *   xs    the search field's `from:`/`to:` pill
 *   sm    a card's byline
 *   md    a face strip (a community's moderators, a poll's winners)
 *   lg    a result row or a card's own picture — the default
 *   xl    the profile card's header
 *   xxl   a cell of a list's people grid, where the face IS the content
 *   fill  as large as the box it is placed in (the toolbar's "me" button)
 *
 * An unknown name would silently draw a face with no width at all, so it is
 * rejected here rather than in a screenshot.
 */
export const SIZES = ["xs", "sm", "md", "lg", "xl", "xxl", "fill"];

/**
 * One face: `pic` if there is one, a generated face if not or if it fails to
 * load, and the score chip on top of either.
 *
 * `pubkey` is both the chip's subject and the generated face's seed, so a card
 * drawing somebody else's picture (a channel's, an app's icon) still scores the
 * AUTHOR — the person the reader is being asked to trust.
 *
 * The chip is left empty here and painted after the fact by the page's
 * paintScores(): the score is a second round trip, and a face should not wait
 * on it.
 */
export function avatarHtml(pic, pubkey, size = "lg") {
  if (!SIZES.includes(size)) throw new Error(`unknown avatar size: ${size}`);
  const style = `style="--h:${hueOf(pubkey)}"`;
  const face = pic
    // decoding="async" as well as loading="lazy": lazy decides WHEN the bytes
    // are fetched, not who decodes them. A full results page lands forty faces
    // at once, and decoding them synchronously blocks the same main thread that
    // is rendering the list they belong to.
    ? `<img class="avatar" ${style} src="${esc(pic)}" alt="" loading="lazy" decoding="async" referrerpolicy="no-referrer"
         onerror="this.classList.add('gen');this.src='${BLANK}'" />`
    : `<div class="avatar gen" ${style}></div>`;
  return `<span class="av-wrap av-${size}">${face}<span class="score-chip" data-pk="${esc(pubkey || "")}"></span></span>`;
}
