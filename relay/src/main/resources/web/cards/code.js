// The code & git family. Snippets and patches keep their whitespace in a
// scrolling <pre> — a diff with re-wrapped lines is not a diff any more.

import { esc, titleOf, summaryOf } from "../shared/format.js";
import { register, shell, bodyHtml, tagOf, tagsOf, clipIf } from "./base.js";

const preBlock = (opts, text, n = 2000) =>
  text ? `<pre class="codeblock">${esc(clipIf(opts, text, n))}</pre>` : "";

/** 1337 — a code snippet: the code, plus what language it claims to be. */
function snippetCard(ev, opts) {
  const inner = preBlock(opts, ev.content, 1200);
  return shell(ev, opts, inner, [
    ["name", tagOf(ev, "name") ? esc(tagOf(ev, "name")) : null],
    ["language", tagOf(ev, "l") ? esc(tagOf(ev, "l")) : null],
  ]);
}

/** 1617 — a patch: subject line up top, the diff underneath. */
function patchCard(ev, opts) {
  const subject = tagOf(ev, "subject") || (ev.content || "").split("\n")[0];
  const inner =
    (subject ? `<h2 class="result-title">${esc(clipIf(opts, subject, 140))}</h2>` : "") +
    preBlock(opts, ev.content, 1200);
  return shell(ev, opts, inner);
}

/** 1621 — an issue: a subject and prose, like a note that names a repo. */
function issueCard(ev, opts) {
  const subject = tagOf(ev, "subject");
  const inner =
    (subject ? `<h2 class="result-title">${esc(clipIf(opts, subject, 140))}</h2>` : "") +
    bodyHtml(opts, ev.content, 500);
  return shell(ev, opts, inner);
}

/** 30617 — a repository announcement: name, description, where to get it. */
function repoCard(ev, opts) {
  const name = tagOf(ev, "name") || titleOf(ev);
  const link = (url) => url ? `<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">${esc(url)}</a>` : null;
  const inner =
    (name ? `<h2 class="result-title">${esc(clipIf(opts, name, 120))}</h2>` : "") +
    bodyHtml(opts, tagOf(ev, "description") || summaryOf(ev) || ev.content, 400);
  return shell(ev, opts, inner, [
    ["web", link(tagOf(ev, "web"))],
    ["clone", link(tagOf(ev, "clone"))],
  ]);
}

/** 30063 — a release: what shipped, and the artifacts it points at. */
function releaseCard(ev, opts) {
  const links = tagsOf(ev, "url").map((t) => t[1]).filter(Boolean);
  const inner =
    (titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 140))}</h2>` : "") +
    bodyHtml(opts, ev.content, 500);
  return shell(ev, opts, inner,
    links.slice(0, opts && opts.full ? links.length : 3).map((u) => ["artifact", `<a href="${esc(u)}" target="_blank" rel="noopener noreferrer">${esc(u)}</a>`]));
}

register([1337], snippetCard);
register([1617], patchCard);
register([1621], issueCard);
register([30617], repoCard);
register([30063], releaseCard);
