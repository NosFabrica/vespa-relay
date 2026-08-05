// The code & git family. Snippets and patches keep their whitespace in a
// scrolling <pre> — a diff with re-wrapped lines is not a diff any more.

import { esc, titleOf, summaryOf } from "../shared/format.js";
import { shortNote } from "../shared/nip19.js";
import { register, shell, bodyHtml, replyLine, extLink, noteHref, tagOf, tagsOf, tagsWhere, clipIf, chipRow } from "./base.js";

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

/**
 * 1621 — an issue: a subject and prose, like a note that names a repo. A 1622
 * takes the same template as a git REPLY, which is why the reply line is here:
 * those carry no subject, so without it the card is prose with no thread.
 */
function issueCard(ev, opts) {
  const subject = tagOf(ev, "subject");
  const inner =
    (subject ? `<h2 class="result-title">${esc(clipIf(opts, subject, 140))}</h2>` : "") +
    replyLine(ev) +
    bodyHtml(opts, ev.content, 500);
  return shell(ev, opts, inner);
}

/** 30617 — a repository announcement: name, description, where to get it. */
function repoCard(ev, opts) {
  const name = tagOf(ev, "name") || titleOf(ev);
  const inner =
    (name ? `<h2 class="result-title">${esc(clipIf(opts, name, 120))}</h2>` : "") +
    bodyHtml(opts, tagOf(ev, "description") || summaryOf(ev) || ev.content, 400);
  return shell(ev, opts, inner, [
    ["web", extLink(tagOf(ev, "web"))],
    ["clone", extLink(tagOf(ev, "clone"))],
  ]);
}

/** 30063 — a release: what shipped, and the artifacts it points at. */
function releaseCard(ev, opts) {
  const links = tagsOf(ev, "url").map((t) => t[1]).filter(Boolean);
  const inner =
    (titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 140))}</h2>` : "") +
    bodyHtml(opts, ev.content, 500);
  return shell(ev, opts, inner,
    links.slice(0, opts && opts.full ? links.length : 3).map((u) => ["artifact", extLink(u)]));
}

/**
 * 1630-1633 — a NIP-34 status. The KIND is the status: which one it is cannot
 * be read off any tag, so the map below is the whole meaning of the event and
 * a card that omitted it would be showing a comment with no verdict attached.
 */
const GIT_STATUS = { 1630: "open", 1631: "applied or merged", 1632: "closed", 1633: "draft" };
function gitStatusCard(ev, opts) {
  const target = tagsOf(ev, "e").map((t) => t[1]).find((v) => /^[0-9a-f]{64}$/.test(v));
  const inner =
    `<div class="result-body">marked <b>${esc(GIT_STATUS[ev.kind])}</b>${target ? ` — <a class="mono" href="${noteHref(target)}">${esc(shortNote(target))}</a>` : ""}</div>` +
    bodyHtml(opts, ev.content, 400);
  return shell(ev, opts, inner);
}

/**
 * 30618 — repository state: one tag per ref, `["refs/heads/master", <commit>]`.
 * The tag NAME is the branch, which is why this cannot ride on repoCard.
 */
function repoStateCard(ev, opts) {
  const refs = tagsWhere(ev, (name) => name.startsWith("refs/"));
  const heads = refs.filter((t) => t[0].startsWith("refs/heads/")).map((t) => t[0].slice("refs/heads/".length));
  const tags = refs.filter((t) => t[0].startsWith("refs/tags/")).map((t) => t[0].slice("refs/tags/".length));
  const inner =
    `<div class="result-body">${heads.length} branch${heads.length === 1 ? "" : "es"} · ${tags.length} tag${tags.length === 1 ? "" : "s"}</div>` +
    chipRow([...heads, ...tags], opts);
  return shell(ev, opts, inner, [["HEAD", tagOf(ev, "HEAD") ? esc(tagOf(ev, "HEAD")) : null]]);
}

register([1337], snippetCard);
register([1617], patchCard);
// 1618/1619 are pull requests and their updates: a subject over prose, not a
// diff, so they take the issue template rather than the patch one — a <pre>
// around a paragraph is a description with its line breaks frozen.
register([1621, 1618, 1619, 1622], issueCard);
register([1630, 1631, 1632, 1633], gitStatusCard);
register([30617], repoCard);
register([30618], repoStateCard);
register([30063], releaseCard);
