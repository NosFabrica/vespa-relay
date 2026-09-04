// The long-form family: articles, drafts, wikis, curations. NIP-23 markdown
// renders as escaped pre-wrap text: a hand-rolled markdown renderer is where
// an escaping mistake becomes an XSS in a page that renders strangers'
// events. The preview's summary line goes through format.js's mdExcerpt,
// which reduces markdown to text, never to HTML.

import { esc, titleOf, summaryOf, imageOf, mdExcerpt } from "../shared/format.js";
import { register, registerRow, shell, titleHtml, bodyHtml, refRows, noteHref, tagOf, tagsOf, clipIf, fmtTs, plural } from "./base.js";

/**
 * An article: cover, title, summary in preview, the whole body on the
 * permalink. The summary line is one slot in one voice whether it came from
 * the `summary` tag or the body. `published_at` is a permalink-only row; the
 * byline already dates the preview.
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

/** A publication's ordered sections: its `a` tags, each naming a 30041. */
const sectionAddrs = (ev) => tagsOf(ev, "a").map((t) => t[1]).filter(Boolean);

/**
 * 30040 — a curated publication index: a table of contents whose `a` tags
 * are ordered sections. The prose lives in the sections, not here.
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

// 30041 is a publication section: a title over prose, which is an article here.
register([30023, 30024, 30818, 30041], articleCard);
register([30004], curationCard);
register([30040], publicationCard);

// Most articles carry no `summary`, so the row's second line is the same excerpt the card shows.
registerRow([30023, 30024, 30818, 30041], (ev) => ({
  name: titleOf(ev),
  sub: summaryOf(ev) || mdExcerpt(ev.content, titleOf(ev)),
}));
registerRow([30004], (ev) => ({
  name: titleOf(ev),
  sub: [`${plural(picksOf(ev), "item")} curated`, summaryOf(ev) || ev.content].filter(Boolean).join(" · "),
}));
registerRow([30040], (ev) => ({ name: titleOf(ev), sub: summaryOf(ev) || plural(sectionAddrs(ev).length, "section") }));
