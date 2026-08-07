// The code & git family: snippets, and the whole of NIP-34 — repositories and
// their state, patches, issues, pull requests, statuses and releases.
//
// Three rules run through all of it, and each one replaced something that was
// on screen and wrong.
//
// **A patch is not its content.** NIP-34 puts the output of `git format-patch`
// in `content`, which is an RFC2822 mail: a header block, the commit message,
// `---`, git's own diffstat, then the diff. Rendered whole, the card's title
// was the line `From <40 hex> Mon Sep 17 00:00:00 2001` — a constant git has
// printed since 2005, on every patch ever made, above a wall of headers. So
// the mail is PARSED: the subject becomes the title, the commit message
// becomes prose, and only the diff goes in the code block.
//
// **Code is clipped by LINES, never by characters.** A 1200-character clip
// lands mid-token, and a diff cut mid-line is not a diff any more — `-  if
// (x)` and `- if (x` say different things. The preview takes whole lines and
// says how many it left.
//
// **Every git event says which repository it is in.** A patch, an issue, a
// status and a release each carry an `a` tag naming their 30617, and not one
// of these cards used to show it: a page of issues from four projects looked
// like a page of issues from one. It is the first line of the card now, and
// the two kinds that carry no `a` (a repo's own state, a release's `d`) get it
// derived rather than dropped.

import { esc, clip, titleOf, summaryOf } from "../shared/format.js";
import { shortNote, shortAddr } from "../shared/nip19.js";
import {
  register, shell, bodyHtml, replyLine, extLink, eventHref, addrHref, personLink, registerNamedPeople,
  tagOf, tagsOf, tagsWhere, clipIf, chipRow, hashtagHref, uniquePubkeys,
} from "./base.js";

// ---- what a git event belongs to ------------------------------------------

/** Every value of every tag with this name — `["clone", <url>, <url>]` and repeats alike. */
const multiTag = (ev, name) => tagsOf(ev, name).flatMap((t) => t.slice(1)).filter((v) => typeof v === "string" && v);

/**
 * The 30617 address this event belongs to.
 *
 * Matched on the KIND prefix rather than "the first `a` tag": a NIP-22 comment
 * on a patch carries the patch's address in an `a` too, and "in <that patch>"
 * under a card whose byline already says so is not the repository line.
 *
 * The two kinds that name their repo without an `a` get it built here, because
 * both name it perfectly well in another slot and a link is worth more than a
 * purist's blank: a 30618's `d` IS the repo id, published by the repo's own
 * author, and a release's `d` is `<repo-id>@<version>` by NIP-34's own rule.
 */
export function repoAddr(ev) {
  const named = tagsOf(ev, "a").map((t) => t[1]).find((v) => /^30617:[0-9a-f]{64}:./.test(String(v || "")));
  if (named) return named;
  const d = tagOf(ev, "d");
  if (!d || !/^[0-9a-f]{64}$/.test(ev.pubkey || "")) return null;
  if (ev.kind === 30618) return `30617:${ev.pubkey}:${d}`;
  if (ev.kind === 30063 && d.includes("@")) return `30617:${ev.pubkey}:${d.slice(0, d.lastIndexOf("@"))}`;
  return null;
}

/**
 * "in <repo>" — the line every git card leads with, or "" when nothing names
 * one, or when the page is ALREADY that repository: `opts.within` is the
 * address the cards are being drawn under, and a repo's page repeating "in
 * vespa-relay" on all twenty of its own issues is a line saying nothing
 * twenty times.
 */
function repoLine(ev, opts) {
  const a = repoAddr(ev);
  if (!a || (opts && opts.within === a)) return "";
  const href = addrHref(a);
  const label = clip(shortAddr(a), 60);
  return `<div class="repo-line">in ${href ? `<a href="${href}">${esc(label)}</a>` : `<span class="mono">${esc(label)}</span>`}</div>`;
}

// ---- code blocks -----------------------------------------------------------

/** How many lines of code or diff a PREVIEW shows before it says how many it didn't. */
const CODE_LINES = 14;

/**
 * Whole lines, at either depth — plus a per-line ceiling in preview, since one
 * minified line is a card as wide as the corpus and the block scrolls sideways
 * rather than wrapping.
 *
 * Not `clip()`, which trims: leading whitespace is INDENTATION here, and the
 * shared clipper flattened every preview of every snippet against the margin.
 */
const LINE_CHARS = 200;
function clipLines(opts, text, n) {
  const lines = String(text || "").replace(/\s+$/, "").split("\n");
  if (opts && opts.full) return { text: lines.join("\n"), more: 0 };
  return {
    text: lines.slice(0, n).map((l) => (l.length > LINE_CHARS ? l.slice(0, LINE_CHARS - 1) + "…" : l)).join("\n"),
    more: Math.max(0, lines.length - n),
  };
}

/**
 * Which of a diff's voices a line speaks in. Order matters twice: `+++`/`---`
 * are file headers and not a one-line addition and deletion, and `--- ` is
 * tested before the bare `-`.
 */
function diffClass(ln) {
  if (/^(diff --git |index |new file|deleted file|old mode|new mode|similarity |rename |copy |Binary files )/.test(ln)) return "d-file";
  if (ln.startsWith("+++") || ln.startsWith("---")) return "d-file";
  if (ln.startsWith("@@")) return "d-hunk";
  if (ln.startsWith("+")) return "d-add";
  if (ln.startsWith("-")) return "d-del";
  return "";
}

/**
 * The diff, tinted. TEXT colour only, never a filled row: the block scrolls
 * sideways, and a background stops at the fold — so a wide diff would show
 * green lines that go grey halfway across.
 */
const diffHtml = (text) =>
  text.split("\n").map((ln) => {
    const cls = diffClass(ln);
    return cls ? `<span class="${cls}">${esc(ln)}</span>` : esc(ln);
  }).join("\n");

/**
 * A code block, optionally with the file's name ON it.
 *
 * The header bar is where every code host in the world puts a filename, and it
 * is the reason a snippet's `name` and `l` left the props table: a filename
 * under the code it names, in a two-column table beside the word "name", is a
 * fact about the card rather than a label on the file.
 */
function codeBlock(opts, text, { name = null, lang = null, diff = false } = {}) {
  const { text: shown, more } = clipLines(opts, text, CODE_LINES);
  if (!shown) return "";
  const head = name || lang
    ? `<div class="code-head">${name ? `<span class="code-name">${esc(clip(name, 60))}</span>` : ""}` +
      `${lang ? `<span class="code-lang">${esc(clip(lang, 24))}</span>` : ""}</div>`
    : "";
  return head +
    `<pre class="codeblock${head ? " headed" : ""}${diff ? " diff" : ""}">${diff ? diffHtml(shown) : esc(shown)}</pre>` +
    (more ? `<div class="muted-note">…${more} more line${more === 1 ? "" : "s"}</div>` : "");
}

// ---- the format-patch mail -------------------------------------------------

/**
 * `git format-patch` output, taken apart: `{subject, markers, message, diff}`.
 *
 * Exported because tools/webtest/cards.test.mjs holds it against real patches —
 * this is the one place in the card layer doing real parsing, and every field
 * of a patch card is downstream of it.
 *
 * What it has to survive: a patch that is only a diff (some clients send one),
 * a subject folded across lines the way RFC2822 allows, a `---` inside the
 * commit message, and a header block that is not a header block at all.
 * Nothing here throws on a stranger's string; the fallbacks are the `subject`
 * tag and the raw content, which is what the card showed before.
 */
export function parsePatch(text) {
  const lines = String(text || "").replace(/\r\n/g, "\n").split("\n");
  const head = Object.create(null);   // a stranger's header names, so no prototype
  let i = 0;
  // A mail only if it opens like one. `From <sha> <date>` is git's own first
  // line; anything else is a bare diff and keeps every line it has.
  if (/^From [0-9a-f]{7,40} /.test(lines[0] || "")) {
    let name = null;
    for (i = 1; i < lines.length; i++) {
      if (lines[i] === "") { i++; break; }
      if (/^[ \t]/.test(lines[i]) && name) { head[name] += " " + lines[i].trim(); continue; }
      const m = /^([A-Za-z][A-Za-z-]*):[ \t]*(.*)$/.exec(lines[i]);
      name = m ? m[1].toLowerCase() : null;
      if (m && !(name in head)) head[name] = m[2];
    }
  }
  // Where the diff starts. `diff --git` is git's; the `--- a/…` pair is what a
  // hand-rolled or `diff -u` patch opens with.
  let at = lines.length;
  for (let n = i; n < lines.length; n++) {
    if (lines[n].startsWith("diff --git ") ||
        (/^--- /.test(lines[n]) && /^\+\+\+ /.test(lines[n + 1] || ""))) { at = n; break; }
  }
  // The message ends at git's `---` separator, whose diffstat we do not keep:
  // the one below is counted from the diff being shown, so it cannot disagree
  // with it. Searched from the diff BACKWARDS — a `---` rule inside the commit
  // message is prose, and the separator is the last one before the patch.
  let end = at;
  for (let n = at - 1; n >= i; n--) if (lines[n] === "---") { end = n; break; }
  const subject = stripMarkers(head.subject || "");
  return {
    subject: subject.text,
    markers: subject.markers,
    message: lines.slice(i, end).join("\n").trim(),
    diff: lines.slice(at).join("\n").trim(),
  };
}

/**
 * `[PATCH v2 2/3] router: yield ingest` -> `{markers: ["PATCH v2 2/3"], text:
 * "router: yield ingest"}`.
 *
 * Leading brackets are the mail convention for metadata about the patch rather
 * than words in its subject, and this series is exactly what a reader scanning
 * a list of patches wants at a glance — so they come off the title and go back
 * on as pills. Capped, because the brackets are a stranger's.
 */
function stripMarkers(subject) {
  let text = String(subject || "").trim();
  const markers = [];
  let m;
  while (markers.length < 3 && (m = /^\[([^\]]{1,40})\]\s*/.exec(text))) {
    markers.push(m[1].trim());
    text = text.slice(m[0].length);
  }
  return { markers, text };
}

/**
 * What the diff being shown adds and removes — counted here rather than read
 * off git's own `--- 2 files changed…` block, so the numbers describe the lines
 * on the card even when the patch arrived without a stat block at all.
 */
function diffStat(diff) {
  let files = 0, headers = 0, add = 0, del = 0;
  for (const ln of String(diff || "").split("\n")) {
    if (ln.startsWith("diff --git ")) files++;
    else if (ln.startsWith("+++")) headers++;
    else if (ln.startsWith("---")) continue;
    else if (ln.startsWith("+")) add++;
    else if (ln.startsWith("-")) del++;
  }
  files = files || headers;   // a plain `diff -u` names its files only in the +++ line
  return files || add || del ? { files, add, del } : null;
}

const statLine = (st) =>
  `<div class="diffstat">${st.files ? `<span>${st.files} file${st.files === 1 ? "" : "s"}</span>` : ""}` +
  `<span class="d-add">+${st.add}</span><span class="d-del">−${st.del}</span></div>`;

/** A commit id, at the length every git tool shows it at. */
const shortSha = (v) => (/^[0-9a-f]{7,64}$/.test(String(v || "")) ? `<span class="mono">${esc(String(v).slice(0, 7))}</span>` : null);

// ---- the cards -------------------------------------------------------------

/**
 * A title with the pills that qualify it — a patch's series marker.
 *
 * The pills survive a card with no title: a patch whose mail carried no
 * `Subject` still knows it is the root of a series, and hanging that fact off
 * the title meant the one patch most in need of the marker was the one that
 * dropped it.
 */
const titleWith = (opts, text, pills = "", n = 140) =>
  text ? `<h2 class="result-title">${esc(clipIf(opts, text, n))}${pills}</h2>`
    : pills ? `<div class="pill-row">${pills}</div>` : "";

const pill = (label, tone = "", title = "") =>
  `<span class="status-pill${tone ? ` ${tone}` : ""}"${title ? ` title="${esc(title)}"` : ""}>${esc(label)}</span>`;

/** 1337 — a code snippet: the file, named, with whatever it says about itself. */
function snippetCard(ev, opts) {
  const inner =
    bodyHtml(opts, tagOf(ev, "description"), 300, true) +
    codeBlock(opts, ev.content, { name: tagOf(ev, "name"), lang: tagOf(ev, "l", "language") });
  return shell(ev, opts, inner, [
    ["runtime", tagOf(ev, "runtime") ? esc(clip(tagOf(ev, "runtime"), 60)) : null],
    ["license", tagOf(ev, "license") ? esc(clip(tagOf(ev, "license"), 40)) : null],
    ["repo", extLink(tagOf(ev, "repo"))],
  ]);
}

/**
 * 1617 — a patch: the commit's subject, what it changes, and the diff.
 *
 * The order is a reviewer's: which repository, what it is called, how big it
 * is, why it was written, and only then the change itself. The `subject` tag
 * is the fallback rather than the source — clients that send one usually send
 * the same words the mail already carries, and clients that don't used to
 * leave the title to git's `From <sha>` line.
 */
function patchCard(ev, opts) {
  const p = parsePatch(ev.content);
  const subject = p.subject || tagOf(ev, "subject") || "";
  // NIP-34 marks the first patch of a series with `["t", "root"]` — the same
  // thing the mail's own `[PATCH 1/3]` says, in the tag a relay can filter on.
  // So it is the FALLBACK, never a second pill beside it: two markers saying
  // one thing, one of them able to contradict the other, is worse than either.
  const marks = p.markers.length
    ? p.markers
    : tagsOf(ev, "t").map((t) => t[1]).filter((v) => v === "root" || v === "root-revision");
  const st = diffStat(p.diff);
  // What goes in the block. The parse recognised nothing at all — no mail, no
  // diff — for content this card cannot read, and showing it verbatim beats
  // showing a card with a byline and nothing under it. Guarded on the WHOLE
  // parse rather than on the subject: a message with no diff is already
  // rendered as prose above, and content would draw it a second time.
  const block = p.diff || (p.subject || p.message || p.markers.length ? "" : ev.content);
  const inner =
    repoLine(ev, opts) +
    titleWith(opts, subject, marks.slice(0, 3).map((m) => pill(clip(m, 40))).join("")) +
    (st ? statLine(st) : "") +
    bodyHtml(opts, p.message, 400) +
    codeBlock(opts, block, { diff: true });
  return shell(ev, opts, inner, opts && opts.full ? [
    ["commit", shortSha(tagOf(ev, "commit"))],
    ["parent", shortSha(tagOf(ev, "parent-commit"))],
  ] : []);
}

/**
 * 1621 — an issue: a subject and prose, in a repository, under its labels. A
 * 1622 takes the same template as a git REPLY, which is why the reply line is
 * here: those carry no subject, so without it the card is prose with no thread.
 *
 * The labels are `t` tags, and they link to the search for that hashtag like
 * every other chip on this page does — a NIP-34 label and a `#bug` on a note
 * are the same tag, so they are the same search.
 */
function issueCard(ev, opts) {
  const labels = [...new Set(tagsOf(ev, "t").map((t) => t[1]).filter(Boolean))];
  const inner =
    repoLine(ev, opts) +
    titleWith(opts, tagOf(ev, "subject")) +
    replyLine(ev) +
    bodyHtml(opts, ev.content, 500) +
    chipRow(labels, opts, hashtagHref);
  return shell(ev, opts, inner);
}

/**
 * 1630-1633 — a NIP-34 status. The KIND is the status: which one it is cannot
 * be read off any tag, so the table below is the whole meaning of the event,
 * and a card that omitted it would be showing a comment with no verdict.
 *
 * A pill rather than a bold word, for the reason live.js states about a
 * stream's `status`: "open" versus "closed" is the entire question anybody
 * clicking one of these has, and the page already speaks in pills when the
 * answer IS the event. 1631's pill is short and its full phrase is the hover —
 * NIP-34 calls it "applied or merged" because a patch is applied and a pull
 * request is merged, and neither word is wrong enough to drop the other.
 */
const GIT_STATUS = {
  1630: { label: "open", tone: "open", full: "open" },
  1631: { label: "merged", tone: "merged", full: "applied or merged" },
  1632: { label: "closed", tone: "closed", full: "closed" },
  1633: { label: "draft", tone: "draft", full: "draft" },
};
function gitStatusCard(ev, opts) {
  const s = GIT_STATUS[ev.kind];
  // The ROOT `e` tag is what the status is about; NIP-34 lets a status also
  // name the patch revisions and the earlier statuses it supersedes, and the
  // first hex `e` on the event is as likely to be one of those as the target.
  const es = tagsWhere(ev, (n) => n === "e").filter((t) => /^[0-9a-f]{64}$/.test(t[1] || ""));
  const t = es.find((x) => x[3] === "root") || es[0];
  const inner =
    repoLine(ev, opts) +
    `<div class="result-body">${pill(s.label, `lead ${s.tone}`, s.full)}` +
    (t ? ` on <a class="mono" href="${eventHref(t[1], { relay: t[2] })}">${esc(shortNote(t[1]))}</a>` : "") +
    `</div>` +
    bodyHtml(opts, ev.content, 400) +
    (opts && opts.full && es.length > 1
      ? `<div class="muted-note">supersedes ${es.length - 1} earlier event${es.length === 2 ? "" : "s"}</div>` : "");
  return shell(ev, opts, inner, opts && opts.full ? [
    ["merge commit", shortSha(tagOf(ev, "merge-commit"))],
    ["applied as", shortSha(tagOf(ev, "applied-as-commits"))],
  ] : []);
}

/**
 * The people a repository declares — the VALUES of one `maintainers` tag, not
 * one tag each, which is why this needs declaring at all: `namedPubkeys` scans
 * `p` tags and would have rendered every one of these as an npub.
 *
 * Registered as the very function the card draws with, so "who is named" and
 * "whose profile is loaded" cannot become two answers. The author is dropped:
 * a repo's own byline is already them, and "maintained by Alice" under a card
 * that says Alice posted it is a line that adds nothing.
 */
const MAINTAINERS = { preview: 3, full: 12 };
const maintainersShown = (ev, opts) =>
  uniquePubkeys(multiTag(ev, "maintainers")).filter((pk) => pk !== ev.pubkey)
    .slice(0, opts && opts.full ? MAINTAINERS.full : MAINTAINERS.preview);
registerNamedPeople([30617], maintainersShown);

/** 30617 — a repository announcement: what it is, who keeps it, where to get it. */
function repoCard(ev, opts) {
  const all = uniquePubkeys(multiTag(ev, "maintainers")).filter((pk) => pk !== ev.pubkey);
  const shown = maintainersShown(ev, opts);
  const more = all.length - shown.length;
  const topics = [...new Set(tagsOf(ev, "t").map((t) => t[1]).filter(Boolean))];
  const inner =
    titleWith(opts, tagOf(ev, "name") || titleOf(ev) || tagOf(ev, "d"), "", 120) +
    bodyHtml(opts, tagOf(ev, "description") || summaryOf(ev) || ev.content, 400) +
    (shown.length
      ? `<div class="meta-line">maintained with ${shown.map(personLink).join(", ")}${more > 0 ? ` and ${more} more` : ""}</div>`
      : "") +
    chipRow(topics, opts, hashtagHref);
  // Every clone url, because a repo mirrored to two hosts is a repo with two
  // clone urls and the second one is the one that works when the first is down.
  const urls = (name) => multiTag(ev, name).slice(0, opts && opts.full ? 8 : 2).map((u) => [name, extLink(u)]);
  return shell(ev, opts, inner, [...urls("web"), ...urls("clone")]);
}

/**
 * 30618 — repository state: one tag per ref, `["refs/heads/master", <commit>]`.
 * The tag NAME is the branch, which is why this cannot ride on repoCard.
 *
 * Branches and tags are drawn as two labelled groups rather than one row of
 * identical chips: `main`, `v0.9.3` and `claude/git-cards` in a single row
 * tells a reader nothing about which of those is a release, and that row was
 * the whole card. The default branch is marked, since `HEAD` on its own —
 * printed raw as `ref: refs/heads/main` — was a props row nobody can act on.
 */
function repoStateCard(ev, opts) {
  const under = (prefix) => tagsWhere(ev, (n) => n.startsWith(prefix))
    .map((t) => ({ name: t[0].slice(prefix.length), commit: t[1] }))
    .filter((r) => r.name);
  const heads = under("refs/heads/");
  const tags = under("refs/tags/");
  const headRef = /^ref:\s*refs\/heads\/(.+)$/.exec(String(tagOf(ev, "HEAD") || "").trim());
  const head = headRef ? headRef[1] : null;
  // The counts label the groups rather than sitting on a line of their own:
  // "2 branches · 2 tags" above two headed groups is the same two numbers
  // twice, and the version with the heads is the one you can act on.
  const inner =
    repoLine(ev, opts) +
    refSection(`${heads.length} branch${heads.length === 1 ? "" : "es"}`, heads, opts, head) +
    refSection(`${tags.length} tag${tags.length === 1 ? "" : "s"}`, tags, opts, null) +
    (heads.length || tags.length ? "" : `<div class="result-body muted">no refs</div>`);
  return shell(ev, opts, inner);
}

/** How many refs a group shows before it counts the rest. */
const REF_CHIPS = { preview: 6, full: 60 };

function refSection(label, refs, opts, head) {
  if (!refs.length) return "";
  const shown = refs.slice(0, opts && opts.full ? REF_CHIPS.full : REF_CHIPS.preview);
  const more = refs.length - shown.length;
  const chip = (r) => {
    const isHead = head != null && r.name === head;
    return `<span class="tag-chip ref-chip${isHead ? " head" : ""}"${isHead ? ' title="the default branch"' : ""}>` +
      `${esc(clip(r.name, 48))}${shortSha(r.commit) || ""}</span>`;
  };
  return `<div class="list-section"><div class="section-head">${esc(label)}</div>` +
    `<div class="chip-row">${shown.map(chip).join("")}${more > 0 ? `<span class="tag-chip more">+${more}</span>` : ""}</div></div>`;
}

/**
 * 30063 — a release: what shipped, and the artifacts it points at.
 *
 * The artifacts are a LIST of file names, not one props row per url repeating
 * the word "artifact" beside a path ellipsised in the middle. NIP-34 puts the
 * version in `d` as `<repo-id>@<version>`, which is the title whenever the
 * publishing client left the `title` tag off.
 */
function releaseCard(ev, opts) {
  const d = tagOf(ev, "d") || "";
  const version = titleOf(ev) || (d.includes("@") ? d.slice(d.lastIndexOf("@") + 1) : "");
  const urls = multiTag(ev, "url");
  const shown = urls.slice(0, opts && opts.full ? urls.length : 3);
  const more = urls.length - shown.length;
  const inner =
    repoLine(ev, opts) +
    titleWith(opts, version) +
    bodyHtml(opts, ev.content, 500) +
    (shown.length
      ? `<div class="list-section"><div class="section-head">${urls.length} artifact${urls.length === 1 ? "" : "s"}</div>` +
        `<ul class="ref-list">${shown.map((u) => `<li>${extLink(u, fileName(u))}</li>`).join("")}` +
        `${more > 0 ? `<li class="muted-note">…and ${more} more</li>` : ""}</ul></div>`
      : "");
  return shell(ev, opts, inner);
}

/**
 * The last segment of a url, as a name for the thing behind it. The url itself
 * stays in the link's `href` and the reader's status bar; what belongs on the
 * page is `vespa-relay-0.9.3.jar`, not 74 characters of CDN path with the half
 * that identifies the file ellipsised away.
 */
function fileName(url) {
  try {
    const p = decodeURIComponent(new URL(url).pathname);
    const last = p.slice(p.lastIndexOf("/") + 1);
    return last ? clip(last, 60) : clip(url, 60);
  } catch (e) { return clip(String(url), 60); }
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
