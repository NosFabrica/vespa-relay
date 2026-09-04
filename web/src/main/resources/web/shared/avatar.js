// A person's picture, everywhere this page draws one: the picture, a
// generated face keyed off the pubkey for somebody who has none, and the
// score chip the active lens fills in. One function, with size as an
// argument; the pixels live in the stylesheet's `--av` table (index.html),
// because a face also has to shrink with the layout it sits in.

import { esc } from "./format.js";

/** A pubkey-derived hue, so a missing picture is still a stable, distinct face. */
export const hueOf = (seed) => (parseInt(String(seed || "").slice(0, 4), 16) || 0) % 360;

/** 1x1 transparent gif, what a broken picture is swapped for in place. */
export const BLANK = "data:image/gif;base64,R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==";

/**
 * The sizes a face is drawn at, as the stylesheet knows them:
 *
 *   xs    the search field's `from:`/`to:` pill
 *   sm    a card's byline
 *   md    a face strip (a community's moderators, a poll's winners)
 *   lg    a result row or a card's own picture, the default
 *   xl    the profile card's header
 *   xxl   a cell of a list's people grid, where the face is the content
 *   fill  as large as the box it is placed in (the toolbar's "me" button)
 *
 * An unknown name would silently draw a face with no width, so it is rejected.
 */
export const SIZES = ["xs", "sm", "md", "lg", "xl", "xxl", "fill"];

/**
 * One face: `pic` if there is one, a generated face if not or if it fails to
 * load, and the score chip on top of either. `pubkey` is both the chip's
 * subject and the seed, so a card drawing somebody else's picture still scores
 * the author. The chip is painted later by the page's paintScores(), since the
 * score is a second round trip a face should not wait on.
 */
export function avatarHtml(pic, pubkey, size = "lg") {
  if (!SIZES.includes(size)) throw new Error(`unknown avatar size: ${size}`);
  const style = `style="--h:${hueOf(pubkey)}"`;
  const face = pic
    // decoding="async" as well as loading="lazy": lazy decides when the bytes
    // are fetched, not who decodes them, and forty faces decoded synchronously
    // block the thread rendering the list.
    ? `<img class="avatar" ${style} src="${esc(pic)}" alt="" loading="lazy" decoding="async" referrerpolicy="no-referrer"
         onerror="this.classList.add('gen');this.src='${BLANK}'" />`
    : `<div class="avatar gen" ${style}></div>`;
  return `<span class="av-wrap av-${size}">${face}<span class="score-chip" data-pk="${esc(pubkey || "")}"></span></span>`;
}
