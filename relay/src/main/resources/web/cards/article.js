// The long-form family: articles, drafts, wikis, curations. Content is
// NIP-23 markdown; it renders here as escaped pre-wrap text on purpose — a
// hand-rolled markdown renderer is exactly the kind of surface where an
// escaping mistake becomes an XSS in a page that renders strangers' events,
// so plain-but-safe wins until a renderer earns its audit.
//
// The PREVIEW's summary line is the one place that costs something, because
// there the marks are most of the four lines on offer. format.js's mdExcerpt
// pays it without moving the line above: it reduces markdown to TEXT, never to
// HTML, so what this card interpolates is still a string it escapes.

import { esc, titleOf, summaryOf, imageOf, mdExcerpt } from "../shared/format.js";
import { register, registerRow, shell, titleHtml, bodyHtml, refRows, noteHref, tagOf, tagsOf, clipIf, fmtTs, plural } from "./base.js";

/**
 * An article, at both depths — and in preview the three things a long-form
 * card is for: **cover, title, summary**, in that reading order.
 *
 * Each of the three used to be almost that and not quite:
 *
 * The COVER is a 1200×630-ish banner, and it was drawn in the shared square
 * `.thumb` frame that channel pictures and generic images use, centred against
 * the text. `object-fit: cover` on a square then threw away a third of every
 * banner from each side. `.thumb.cover` is the landscape frame, topped out
 * with the title.
 *
 * The SUMMARY was the `summary` tag when there was one and the raw markdown
 * body when there was not — see mdExcerpt for what that looked like — and the
 * two rendered in different VOICES, the tag muted and the fallback in body
 * text. One slot, one voice: whichever it came from, this line is the standing
 * in for the article, not the article.
 *
 * `published_at` stays on the permalink. On a card whose byline already reads
 * "2d ago" it was a second date for the same event, in a props table under a
 * preview that is supposed to be three things.
 */
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
          : bodyHtml(opts, summary || mdExcerpt(ev.content, title), 400, true)}
      </div>
      ${!full && img ? `<img class="thumb cover" src="${esc(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()" />` : ""}
    </div>`;
  return shell(ev, opts, inner, full ? [["published", published ? esc(fmtTs(published)) : null]] : []);
}

/** How many things a curation collects, by address and by id alike. */
const picksOf = (ev) => tagsOf(ev, "a").length + tagsOf(ev, "e").length;

/** 30004 — a curation: the title and what it collects. */
function curationCard(ev, opts) {
  const title = titleOf(ev);
  const inner =
    (title ? `<h2 class="result-title">${esc(clipIf(opts, title, 120))}</h2>` : "") +
    `<div class="result-body">${esc(plural(picksOf(ev), "item"))} curated</div>` +
    bodyHtml(opts, summaryOf(ev) || ev.content, 300);
  return shell(ev, opts, inner);
}

/** A publication's ORDERED sections — its `a` tags, each naming a 30041. */
const sectionAddrs = (ev) => tagsOf(ev, "a").map((t) => t[1]).filter(Boolean);

/**
 * 30040 — a curated publication index: a book's table of contents. Its `a`
 * tags are ORDERED sections (30041s), so the card lists them as the contents
 * they are; the prose lives in the sections, not here, which is why this one
 * cannot share articleCard's body-first template.
 */
function publicationCard(ev, opts) {
  const sections = sectionAddrs(ev);
  const inner =
    titleHtml(opts, titleOf(ev), 140, noteHref(ev.id)) +
    bodyHtml(opts, summaryOf(ev), 300, true) +
    `<div class="result-body">${esc(plural(sections.length, "section"))}</div>` +
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

// The rows. mdExcerpt is here for the same reason it is on the card: most
// articles carry no `summary`, and the fallback is the BODY — so a row of
// search results led with `## Somebody is paying for this`, marks and all, in
// the ninety characters it had.
registerRow([30023, 30024, 30818, 30041], (ev) => ({
  name: titleOf(ev),
  sub: summaryOf(ev) || mdExcerpt(ev.content, titleOf(ev)),
}));
registerRow([30004], (ev) => ({
  name: titleOf(ev),
  sub: [`${plural(picksOf(ev), "item")} curated`, summaryOf(ev) || ev.content].filter(Boolean).join(" · "),
}));
registerRow([30040], (ev) => ({ name: titleOf(ev), sub: summaryOf(ev) || plural(sectionAddrs(ev).length, "section") }));
