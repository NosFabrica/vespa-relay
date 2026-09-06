// A person's picture everywhere the page draws one: the picture, a generated face keyed off
// the pubkey when there is none, and the score chip the active lens fills in. The pixel
// sizes live in the stylesheet's `--av` table (index.html).

import { esc } from "./format.js";

/** A pubkey-derived hue, so a missing picture is still a stable, distinct face. */
export const hueOf = (seed) => (parseInt(String(seed || "").slice(0, 4), 16) || 0) % 360;

/** 1x1 transparent gif, what a broken picture is swapped for in place. */
export const BLANK = "data:image/gif;base64,R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==";

/**
 * The sizes a face is drawn at, as the stylesheet's `--av` table names them, from a search
 * pill (xs) to a people-grid cell (xxl); `fill` takes the box it is placed in. An unknown
 * name would silently draw a face with no width, so it is rejected.
 */
export const SIZES = ["xs", "sm", "md", "lg", "xl", "xxl", "fill"];

/**
 * One face: `pic` if there is one, a generated face otherwise or when it fails to load, and
 * the score chip on top of either. `pubkey` is both the chip's subject and the seed; the
 * chip itself is painted later by the page's paintScores().
 */
export function avatarHtml(pic, pubkey, size = "lg") {
  if (!SIZES.includes(size)) throw new Error(`unknown avatar size: ${size}`);
  const style = `style="--h:${hueOf(pubkey)}"`;
  const face = pic
    // decoding="async" as well as loading="lazy": lazy decides when the bytes are fetched,
    // not who decodes them.
    ? `<img class="avatar" ${style} src="${esc(pic)}" alt="" loading="lazy" decoding="async" referrerpolicy="no-referrer"
         onerror="this.classList.add('gen');this.src='${BLANK}'" />`
    : `<div class="avatar gen" ${style}></div>`;
  return `<span class="av-wrap av-${size}">${face}<span class="score-chip" data-pk="${esc(pubkey || "")}"></span></span>`;
}
