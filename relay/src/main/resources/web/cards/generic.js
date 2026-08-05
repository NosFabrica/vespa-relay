// The floor: the card that makes "all indexed kinds" render before anyone has
// written a bespoke renderer. Byline, the event's title-ish tag, its
// summary-ish tag (else the content), an image when the tags carry one —
// exactly the fields the search extractors index, so what matched is what
// shows. Not registered: cards.js falls back to it for any kind the registry
// does not name, which is what keeps an unknown kind honest instead of blank.

import { esc, clip, titleOf, summaryOf, imageOf } from "../shared/format.js";
import { shell, bodyHtml, clipIf, noteHref } from "./base.js";

export function genericCard(ev, opts) {
  const title = titleOf(ev);
  const summary = summaryOf(ev);
  const body = opts && opts.full
    ? [summary, ev.content].filter(Boolean).join("\n\n")
    : (summary || clip(ev.content, 400));
  const img = imageOf(ev);
  const inner = `
    <div class="result-main">
      <div class="text">
        ${title ? `<h2 class="result-title"><a href="${noteHref(ev.id)}">${esc(clipIf(opts, title, 120))}</a></h2>` : ""}
        ${bodyHtml(opts, body, 400, !!summary && !(opts && opts.full))}
      </div>
      ${img ? `<img class="thumb" src="${esc(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()" />` : ""}
    </div>`;
  // No `id` row: it was here because a title-less card had no other link to
  // its own page, and the card frame carries that now — byline date and the
  // card itself. What the row actually said, a truncated bech32, is under
  // "json" for anyone who wants it.
  return shell(ev, opts, inner);
}
