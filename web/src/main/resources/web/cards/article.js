// The long-form family: articles, drafts, wikis, curations, publications and the shelves and
// courses built out of them, plus the one request that acts on a wiki rather than being one.
// Markdown renders as escaped pre-wrap text, never as HTML; the summary line goes through
// format.js's mdExcerpt.

import { esc, clip, titleOf, summaryOf, imageOf, mdExcerpt } from "../shared/format.js";
import { shortAddr, shortNote } from "../shared/nip19.js";
import {
  register, registerRow, registerNamedPeople, shell, titleHtml, bodyHtml, refRows, chipRow,
  hashtagHref, personLink, extLink, noteHref, addrHref, tagOf, tagsOf, tagsWhere, topicsOf, oneLine,
  clipIf, fmtTs, fmtBytes, plural,
} from "./base.js";

const HEX64 = /^[0-9a-f]{64}$/;

/** The cover, at the depth it belongs to: a banner on the permalink, a landscape thumb in the list. */
const coverBanner = (img) => (img
  ? `<div class="embed"><img src="${esc(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.parentElement.remove()" /></div>`
  : "");
const coverThumb = (img) => (img
  ? `<img class="thumb cover" src="${esc(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()" />`
  : "");

/**
 * An article: cover, title, summary in preview, the whole body on the permalink.
 * `published_at` is a permalink-only row; the byline already dates the preview.
 */
function articleCard(ev, opts) {
  const title = titleOf(ev);
  const summary = summaryOf(ev);
  const img = imageOf(ev);
  const published = tagOf(ev, "published_at");
  const full = opts && opts.full;
  const inner =
    (full ? coverBanner(img) : "") +
    `<div class="result-main">
      <div class="text">
        ${title ? `<h2 class="result-title"><a href="${noteHref(ev.id)}">${esc(clipIf(opts, title, 120))}</a></h2>` : ""}
        ${full
          ? (summary ? `<div class="result-body muted">${esc(summary)}</div>` : "") + bodyHtml(opts, ev.content, 0)
          : bodyHtml(opts, summary || mdExcerpt(ev.content, title), 400, true)}
      </div>
      ${full ? "" : coverThumb(img)}
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

/** How deep a table of contents may nest; a part holding chapters is level 1 holding level 2. */
const MAX_LEVEL = 6;

/** A nesting level as published: a plain integer, clamped to the levels that exist. Anything else is level 1. */
const levelOf = (v) => (/^-?\d+$/.test(v) ? Math.min(MAX_LEVEL, Math.max(1, Number(v))) : 1);

/**
 * Whether slot 2 is a relay hint rather than a title. Schemeless hosts count: publishers write a
 * bare `relay.example.com` there, and reading that as a title puts a hostname in the contents.
 */
const looksLikeRelay = (v) =>
  /^(?:wss?|https?):\/\//i.test(v) || /^[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+(?::\d+)?(?:\/\S*)?$/.test(v);

/**
 * A table of contents, in tag order, which is the order it is meant to be read in.
 *
 * NKBIP-01 documents `["a", "<kind:pubkey:d>", "<relay hint>", "<event id>"]`, but what the
 * publishing clients emit is looser, and reading only the documented shape loses most of a real
 * book:
 *
 * - `e` entries are sections too, interleaved with the `a` tags in tag order.
 * - Slot 2 is as often the entry's own title as it is a relay hint — the valuable case, since it
 *   names every chapter before one section event has been fetched.
 * - Slot 3 may be a nesting level rather than the documented event id: a small integer is a
 *   level, a 64-hex value is the revision the entry is pinned to.
 *
 * Under an `a`, a slot 2 that is a plausible level is that level's stray spelling rather than a
 * title; under an `e` it is kept, because an `e` has no coordinate to fall back on and a chapter
 * really can be called "1984". Uppercase `A`/`E` are NOT entries at all: on a derivative work
 * they name the original, so the match is case-sensitive.
 */
const contentsOf = (ev) =>
  tagsWhere(ev, (name) => name === "a" || name === "e")
    .filter((t) => t[1])
    .map((t) => {
      const slot2 = oneLine(t[2]);
      const titled = slot2 && !looksLikeRelay(slot2) && (t[0] === "e" || !/^\d+$/.test(slot2));
      return { kind: t[0], value: t[1], label: titled ? slot2 : "", level: levelOf(oneLine(t[3])) };
    });

/** 30040 — a curated publication index: a table of contents whose entries are its ordered sections. */
function publicationCard(ev, opts) {
  const sections = contentsOf(ev);
  const inner =
    titleHtml(opts, titleOf(ev), 140, noteHref(ev.id)) +
    bodyHtml(opts, summaryOf(ev), 300, true) +
    `<div class="result-body">${esc(plural(sections.length, "section"))}</div>` +
    refRows(sections, opts);
  return shell(ev, opts, inner, [
    ["author", tagOf(ev, "author") ? esc(tagOf(ev, "author")) : null],
    ["version", tagOf(ev, "version") ? esc(tagOf(ev, "version")) : null],
  ]);
}

/** 30045 — a bookshelf: a personal, ordered shelf of publications, listed the way an index lists sections. */
function shelfCard(ev, opts) {
  const items = contentsOf(ev);
  const img = imageOf(ev);
  const full = opts && opts.full;
  const inner =
    (full ? coverBanner(img) : "") +
    `<div class="result-main">
      <div class="text">
        ${titleHtml(opts, titleOf(ev), 140)}
        ${bodyHtml(opts, summaryOf(ev) || ev.content, 300, true)}
        <div class="result-body">${esc(plural(items.length, "item"))}</div>
      </div>
      ${full ? "" : coverThumb(img)}
    </div>` +
    refRows(items, opts);
  return shell(ev, opts, inner);
}

/**
 * The people who made a learning resource, as the two publishing vocabularies spell them: the
 * book publishers write `author`/`artist`, the schema.org ones a structured creator of which
 * only the name is worth showing.
 */
const creatorOf = (ev) => tagOf(ev, "author", "artist", "creator:name");

/**
 * One schema.org facet, as it is published: `["about:prefLabel:de", "Biologie"]`, repeated per
 * value and per language. Only one language comes back — an event routinely carries six, and
 * printing them all reads as gibberish — and the values are deduped in publication order.
 */
function facetLabels(ev, facet, preferred) {
  const prefix = `${facet}:prefLabel:`;
  const byLanguage = new Map();
  for (const t of tagsWhere(ev, (name) => name.startsWith(prefix))) {
    const value = oneLine(t[1]);
    if (!value) continue;
    const language = String(t[0]).slice(prefix.length);
    if (!byLanguage.has(language)) byLanguage.set(language, new Set());
    byLanguage.get(language).add(value);
  }
  if (!byLanguage.size) return [];
  const language = byLanguage.has(preferred) ? preferred : byLanguage.keys().next().value;
  return [...byLanguage.get(language)];
}

/**
 * Every facet worth showing, in the order a reader wants them: what kind of thing it is, who it
 * is for, what it is about. Deduped ACROSS facets, because the vocabularies overlap — one
 * resource carries "Informatik" under two ids and again under a second facet — and a chip row
 * that repeats itself reads as a bug.
 */
function facetsOf(ev) {
  const preferred = oneLine(tagOf(ev, "inLanguage"));
  const out = new Set();
  for (const facet of ["learningResourceType", "educationalLevel", "about"]) {
    for (const label of facetLabels(ev, facet, preferred)) out.add(label);
  }
  return [...out];
}

/**
 * 30142 — a learning resource: a course, a tutorial, a lesson. Long enough to read rather than
 * skim, so the body is the article treatment; what is bespoke is the schema.org furniture around
 * it, which is where every other publisher's `title` and `summary` are spelled `name` and
 * `description` (format.js reads both) and where the subjects and levels live.
 */
function learningResourceCard(ev, opts) {
  const title = titleOf(ev);
  const summary = summaryOf(ev);
  const img = imageOf(ev);
  const full = opts && opts.full;
  const free = oneLine(tagOf(ev, "isAccessibleForFree")).toLowerCase();
  const inner =
    (full ? coverBanner(img) : "") +
    `<div class="result-main">
      <div class="text">
        ${titleHtml(opts, title, 140)}
        ${full
          ? (summary ? `<div class="result-body muted">${esc(summary)}</div>` : "") + bodyHtml(opts, ev.content, 0)
          : bodyHtml(opts, summary || mdExcerpt(ev.content, title), 400, true)}
      </div>
      ${full ? "" : coverThumb(img)}
    </div>` +
    chipRow(facetsOf(ev), opts) +
    chipRow(topicsOf(ev), opts, hashtagHref);
  return shell(ev, opts, inner, [
    ["by", creatorOf(ev) ? esc(creatorOf(ev)) : null],
    ["published", tagOf(ev, "published", "published_on", "datePublished", "release_date")
      ? esc(tagOf(ev, "published", "published_on", "datePublished", "release_date")) : null],
    ["language", tagOf(ev, "inLanguage") ? esc(tagOf(ev, "inLanguage")) : null],
    ["licence", tagOf(ev, "license:id") ? esc(tagOf(ev, "license:id")) : null],
    // An absent flag is not a "no": only the two values it can carry say anything.
    ["free", free === "true" ? "yes" : free === "false" ? "no" : null],
    ["file", extLink(tagOf(ev, "encoding:contentUrl"))],
    ["file type", tagOf(ev, "encoding:encodingFormat") ? esc(tagOf(ev, "encoding:encodingFormat")) : null],
    ["file size", fmtBytes(tagOf(ev, "encoding:contentSize"))],
  ]);
}

/** `source` is NIP-54's marker for the version to merge; the client publishing these writes `fork`. */
const MERGE_SOURCE = new Set(["source", "fork"]);

/**
 * The two versions a merge request names: the fork to merge, which carries the marker, and the
 * version it grew from, which is the unmarked `e`. Publishers are inconsistent about marking the
 * base at all, so it is read as "the other one" rather than by a marker of its own.
 */
function versionsOf(ev) {
  const ids = tagsOf(ev, "e").filter((t) => HEX64.test(t[1] || ""));
  const source = ids.find((t) => MERGE_SOURCE.has(String(t[3] || "")));
  const base = ids.find((t) => t !== source && !String(t[3] || ""));
  return { source: source && source[1], base: base && base[1] };
}

/** Who is being asked to merge: the first `p`, which is the target article's author. */
const askedOf = (ev) => (tagsOf(ev, "p").map((t) => t[1]).find((pk) => HEX64.test(pk || "")) || null);

const noteLink = (id) => `<a class="mono" href="${noteHref(id)}">${esc(shortNote(id))}</a>`;

/**
 * 818 — a request to merge a forked wiki article back into the original (NIP-54, Appendix 1).
 * It is an ask, not a document: the card leads with who is being asked and what for, and the
 * body is the explanation the requester wrote.
 */
function mergeRequestCard(ev, opts) {
  const { source, base } = versionsOf(ev);
  const target = tagOf(ev, "a");
  const href = target ? addrHref(target) : null;
  const into = target
    ? (href ? `<a href="${href}">${esc(clip(shortAddr(target), 60))}</a>` : `<span class="mono">${esc(clip(shortAddr(target), 60))}</span>`)
    : "an article";
  const asked = askedOf(ev);
  const inner =
    `<div class="result-body">asks ${asked ? personLink(asked) : "its author"} to merge into ${into}</div>` +
    // A request with no version to merge cannot be acted on; saying nothing would read as one that can.
    (source ? "" : `<div class="result-body muted">no version to merge</div>`) +
    bodyHtml(opts, ev.content, 400);
  return shell(ev, opts, inner, [
    ["merging", source ? noteLink(source) : null],
    ["based on", base ? noteLink(base) : null],
  ]);
}

// 30041 is a publication section: a title over prose, which is an article here.
// 30817 is a NIP's own text, published on nostr: a titled document, which is this card.
register([30023, 30024, 30818, 30041, 30817], articleCard);
register([30004], curationCard);
register([30040], publicationCard);
register([30045], shelfCard);
register([30142], learningResourceCard);
register([818], mergeRequestCard);
// The person a merge request asks reaches no scan of `p` tags: only this kind's card names them.
registerNamedPeople([818], (ev) => [askedOf(ev)].filter(Boolean));

// Most articles carry no `summary`, so the row's second line is the same excerpt the card shows.
registerRow([30023, 30024, 30818, 30041, 30817], (ev) => ({
  name: titleOf(ev),
  sub: summaryOf(ev) || mdExcerpt(ev.content, titleOf(ev)),
}));
registerRow([30004], (ev) => ({
  name: titleOf(ev),
  sub: [`${plural(picksOf(ev), "item")} curated`, summaryOf(ev) || ev.content].filter(Boolean).join(" · "),
}));
registerRow([30040], (ev) => ({ name: titleOf(ev), sub: summaryOf(ev) || plural(contentsOf(ev).length, "section") }));
registerRow([30045], (ev) => ({
  name: titleOf(ev),
  sub: [plural(contentsOf(ev).length, "item"), summaryOf(ev) || ev.content].filter(Boolean).join(" · "),
}));
registerRow([30142], (ev) => ({
  name: titleOf(ev),
  sub: [creatorOf(ev), summaryOf(ev) || mdExcerpt(ev.content, titleOf(ev))].filter(Boolean).join(" · "),
}));
// What the request is for, over why: the address it would change, never one of the two revisions.
registerRow([818], (ev) => {
  const target = tagOf(ev, "a");
  return { name: `asks to merge${target ? ` into ${clip(shortAddr(target), 60)}` : ""}`, sub: ev.content };
});
