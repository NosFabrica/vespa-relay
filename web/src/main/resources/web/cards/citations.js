// Citations: a reference to a source, published as its own event so other events can point at
// it. Not defined by any NIP — the shape is what the publishing clients emit, and it is flat:
// every field is a `[name, value]` tag, which is why one renderer serves all three kinds and
// only the reference line and the facts under it differ.
//
// | kind | | |
// |---|---|---|
// | 31 | a url | `u`, and `open_timestamp` for the NIP-03 attestation of when it was read |
// | 32 | something printed | `published_in` (volume in its second slot), `page_range`, `doi` |
// | 33 | an LLM prompt | `llm`, the prompt itself in `content` |
//
// Kind 30 — a citation of another nostr event — is not here: the number is a chess move to the
// other vocabulary using it, and which one a 30 belongs to is not a question a renderer settles.

import { esc, clip } from "../shared/format.js";
import {
  register, registerRow, shell, titleHtml, bodyHtml, extLink, noteHref, tagOf, tagsOf, oneLine,
} from "./base.js";

/** The url a citation points at. The draft builders write `u`; a publisher following the manifest wrote `url`. */
const urlOf = (ev) => tagOf(ev, "u", "url");

/** The `published_in` tag carries the volume in its second slot rather than in a tag of its own. */
const volumeOf = (ev) => oneLine((tagsOf(ev, "published_in").find((t) => t[2]) || [])[2]);

/**
 * What heads the card: the citer's own title, else whatever else identifies the source. A
 * citation that names none of them is headed by nothing and reads as the note it is.
 */
function headingOf(ev) {
  const title = tagOf(ev, "title");
  if (title) return title;
  if (ev.kind === 31) return urlOf(ev);
  if (ev.kind === 32) return tagOf(ev, "chapter_title", "published_in", "author");
  return tagOf(ev, "llm", "author");
}

/**
 * The bibliographic line: who wrote it, where it appeared, who published it, when. One run,
 * because that is how a reference reads and because a row each would be a table of blanks.
 */
const citeLine = (ev) => [
  tagOf(ev, "author"),
  ev.kind === 32 ? [tagOf(ev, "published_in"), volumeOf(ev)].filter(Boolean).join(" ") : null,
  tagOf(ev, "published_by"),
  tagOf(ev, "location"),
  tagOf(ev, "published_on"),
].map(oneLine).filter(Boolean).join(" · ");

/** The facts a reader looks up rather than reads, per kind; the url is a link, so it is built last. */
const FACTS = {
  31: [["timestamped by", "open_timestamp"]],
  32: [["pages", "page_range"], ["chapter", "chapter_title"], ["editor", "editor"], ["doi", "doi"]],
  33: [["model", "llm"]],
};

/**
 * The table, minus whatever the heading already is: an untitled citation is headed by its url or
 * its model, and a row repeating it is the same fact twice.
 */
const factsOf = (ev, heading) => {
  const url = urlOf(ev);
  const fact = (name) => (tagOf(ev, name) && tagOf(ev, name) !== heading ? esc(tagOf(ev, name)) : null);
  return [
    [ev.kind === 33 ? "conversation" : "url", url === heading ? null : extLink(url)],
    ...(FACTS[ev.kind] || []).map(([label, name]) => [label, fact(name)]),
    ["version", fact("version")],
    ["accessed", fact("accessed_on")],
  ];
};

/** 31 / 32 / 33 — one citation. The source is the heading and the line under it; the body is the citer's. */
function citationCard(ev, opts) {
  const heading = headingOf(ev);
  const cite = citeLine(ev);
  const summary = tagOf(ev, "summary");
  const inner =
    titleHtml(opts, heading, 140, noteHref(ev.id)) +
    (cite ? `<div class="result-body muted">${esc(clip(cite, 200))}</div>` : "") +
    bodyHtml(opts, summary, 300, true) +
    // A prompt's `content` IS the source it cites, so it is never clipped away as a side note.
    bodyHtml(opts, ev.content, ev.kind === 33 ? 600 : 400);
  return shell(ev, opts, inner, factsOf(ev, heading));
}

register([31, 32, 33], citationCard);

// The source on one line, and under it what this citation says about it — for a prompt, the
// prompt, which is the only part of it a reader would recognise.
registerRow([31, 32, 33], (ev) => ({
  name: headingOf(ev),
  sub: [citeLine(ev), tagOf(ev, "summary") || ev.content].filter(Boolean).join(" · "),
}));
