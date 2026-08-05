// The long-form family: articles, drafts, wikis, curations. Content is
// NIP-23 markdown; it renders here as escaped pre-wrap text on purpose — a
// hand-rolled markdown renderer is exactly the kind of surface where an
// escaping mistake becomes an XSS in a page that renders strangers' events,
// so plain-but-safe wins until a renderer earns its audit.

import { esc, titleOf, summaryOf, imageOf } from "../shared/format.js";
import { register, shell, titleHtml, bodyHtml, refRows, noteHref, tagOf, tagsOf, clipIf, fmtTs } from "./base.js";

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
  return shell(ev, opts, inner, [["published", published ? esc(fmtTs(published)) : null]]);
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

/**
 * 30040 — a curated publication index: a book's table of contents. Its `a`
 * tags are ORDERED sections (30041s), so the card lists them as the contents
 * they are; the prose lives in the sections, not here, which is why this one
 * cannot share articleCard's body-first template.
 */
function publicationCard(ev, opts) {
  const sections = tagsOf(ev, "a").map((t) => t[1]).filter(Boolean);
  const inner =
    titleHtml(opts, titleOf(ev), 140, noteHref(ev.id)) +
    bodyHtml(opts, summaryOf(ev), 300, true) +
    `<div class="result-body">${sections.length} section${sections.length === 1 ? "" : "s"}</div>` +
    refRows(sections.map((a) => ({ kind: "a", value: a })), opts);
  return shell(ev, opts, inner, [
    ["author", tagOf(ev, "author") ? esc(tagOf(ev, "author")) : null],
    ["version", tagOf(ev, "version") ? esc(tagOf(ev, "version")) : null],
  ]);
}

// 30041 is a publication SECTION — a title over prose, which is an article in
// every way that matters to this page.
register([30023, 30024, 30818, 30041], articleCard);
register([30004], curationCard);
register([30040], publicationCard);
