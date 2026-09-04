// The floor: the card every indexed kind gets before anyone writes a bespoke
// renderer. Byline, the title-ish tag, the summary-ish tag (else the
// content), an image when the tags carry one: the fields the search
// extractors index, so what matched is what shows. Not registered; cards.js
// falls back to it for any kind the registry does not name.

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
  return shell(ev, opts, inner);
}

/**
 * The floor's type-ahead row: the card's fields, in order, minus the markup.
 * The content fallback is honest here, on both lines: an unknown kind's
 * content is all anybody knows about it.
 */
export const genericRow = (ev) => {
  const title = titleOf(ev);
  return { name: title || ev.content, sub: summaryOf(ev) || (title ? ev.content : "") };
};
