// The long-form family: articles, drafts, wikis, curations. Content is
// NIP-23 markdown; it renders here as escaped pre-wrap text on purpose — a
// hand-rolled markdown renderer is exactly the kind of surface where an
// escaping mistake becomes an XSS in a page that renders strangers' events,
// so plain-but-safe wins until a renderer earns its audit.

import { esc, titleOf, summaryOf, imageOf } from "../shared/format.js";
import { register, shell, bodyHtml, noteHref, tagOf, tagsOf, clipIf, fmtTs } from "./base.js";

function articleCard(ev, opts) {
  const title = titleOf(ev);
  const summary = summaryOf(ev);
  const img = imageOf(ev);
  const published = tagOf(ev, "published_at");
  const full = opts && opts.full;
  const inner =
    (full && img ? `<div class="embed"><img src="${esc(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.parentElement.remove()" /></div>` : "") +
    `<div class="result-main">
      <div class="text">
        ${title ? `<h2 class="result-title"><a href="${noteHref(ev.id)}">${esc(clipIf(opts, title, 120))}</a></h2>` : ""}
        ${full
          ? (summary ? `<div class="result-body muted">${esc(summary)}</div>` : "") + bodyHtml(opts, ev.content, 0)
          : bodyHtml(opts, summary || ev.content, 400, !!summary)}
      </div>
      ${!full && img ? `<img class="thumb" src="${esc(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()" />` : ""}
    </div>`;
  return shell(ev, opts, inner, [["published", published ? fmtTs(published) : null]]);
}

/** 30004 — a curation: the title and what it collects. */
function curationCard(ev, opts) {
  const title = titleOf(ev);
  const picks = tagsOf(ev, "a").length + tagsOf(ev, "e").length;
  const inner =
    (title ? `<h2 class="result-title">${esc(clipIf(opts, title, 120))}</h2>` : "") +
    `<div class="result-body">${picks} item${picks === 1 ? "" : "s"} curated</div>` +
    bodyHtml(opts, summaryOf(ev) || ev.content, 300);
  return shell(ev, opts, inner);
}

register([30023, 30024, 30818], articleCard);
register([30004], curationCard);
