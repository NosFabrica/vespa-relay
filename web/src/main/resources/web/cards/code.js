// The code & git family: snippets, and the whole of NIP-34 — repositories and
// their state, patches, issues, pull requests, statuses and releases.
//
// A patch's `content` is `git format-patch` output, an RFC2822 mail, and is
// parsed: the subject becomes the title, the commit message prose, and only
// the diff goes in the code block. Code is clipped by whole lines, never by
// characters. Every git event leads with the repository it belongs to.

import { esc, clip, titleOf, summaryOf } from "../shared/format.js";
import { shortNote, shortAddr } from "../shared/nip19.js";
import {
  register, registerRow, shell, bodyHtml, replyLine, extLink, eventHref, addrHref, personLink, registerNamedPeople,
  tagOf, tagsOf, tagsWhere, clipIf, chipRow, hashtagHref, uniquePubkeys, plural,
} from "./base.js";

// ---- what a git event belongs to ------------------------------------------

/** Every value of every tag with this name — `["clone", <url>, <url>]` and repeats alike. */
const multiTag = (ev, name) => tagsOf(ev, name).flatMap((t) => t.slice(1)).filter((v) => typeof v === "string" && v);

/**
 * The 30617 address this event belongs to. Matched on the kind prefix, not
 * "the first `a`": a NIP-22 comment on a patch carries the patch's address
 * too. A 30618's `d` is the repo id and a release's `d` is
 * `<repo-id>@<version>`, so those two get the address derived.
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

/** The repository's name, the identifier out of its address. "" when nothing here names one. */
const repoName = (ev) => {
  const a = repoAddr(ev);
  return a ? clip(shortAddr(a), 60) : "";
};

/**
 * "in <repo>" — the line every git card leads with. "" when nothing names a
 * repo, or when `opts.within` says the page is already that repository.
 */
function repoLine(ev, opts) {
  const a = repoAddr(ev);
  if (!a || (opts && opts.within === a)) return "";
  const href = addrHref(a);
  const label = repoName(ev);
  return `<div class="repo-line">in ${href ? `<a href="${href}">${esc(label)}</a>` : `<span class="mono">${esc(label)}</span>`}</div>`;
}

// ---- code blocks -----------------------------------------------------------

/** How many lines of code or diff a preview shows before it says how many it left. */
const CODE_LINES = 14;

/**
 * Whole lines at either depth, plus a per-line ceiling in preview so one
 * minified line cannot make the block scroll sideways. Not `clip()`, which
 * would trim the indentation. Trailing blank lines are counted back over
 * whole lines rather than matched with `/\s+$/`, which is quadratic on a long
 * run of whitespace inside a line.
 */
const LINE_CHARS = 200;
function clipLines(opts, src, n) {
  const all = Array.isArray(src) ? src : String(src || "").split("\n");
  let end = all.length;
  while (end > 0 && !all[end - 1].trim()) end--;
  if (opts && opts.full) return { lines: all.slice(0, end), more: 0 };
  const lines = all.slice(0, Math.min(n, end))
    .map((l) => (l.length > LINE_CHARS ? l.slice(0, LINE_CHARS - 1) + "…" : l));
  return { lines, more: end - lines.length };
}

/** The `diff --git` preamble lines, which are about the file rather than in it. */
const DIFF_META = /^(index |new file|deleted file|old mode|new mode|similarity |rename |copy |Binary files )/;

/**
 * A diff read in one pass: its size, and which voice each of its first
 * [upTo] lines speaks in. `inHunk` is what makes `+++`/`---` readable:
 * outside a hunk they are file headers, inside one they are ordinary added
 * and removed lines whose text starts with `--`.
 */
function readDiff(lines, upTo) {
  let files = 0, headers = 0, add = 0, del = 0, inHunk = false;
  const classes = [];
  for (let n = 0; n < lines.length; n++) {
    const ln = lines[n];
    let cls = "";
    if (ln.startsWith("diff --git ")) { files++; inHunk = false; cls = "d-file"; }
    else if (ln.startsWith("@@")) { inHunk = true; cls = "d-hunk"; }
    // A bare `diff -u` has no `diff --git` to reset us, so the `--- `/`+++ ` pair ends the hunk.
    else if (ln.startsWith("--- ") && String(lines[n + 1] || "").startsWith("+++ ")) { inHunk = false; cls = "d-file"; }
    else if (!inHunk) {
      if (ln.startsWith("+++")) { headers++; cls = "d-file"; }
      else if (ln.startsWith("---") || DIFF_META.test(ln)) cls = "d-file";
    } else if (ln.startsWith("+")) { add++; cls = "d-add"; }
    else if (ln.startsWith("-")) { del++; cls = "d-del"; }
    if (n < upTo) classes.push(cls);
  }
  // A plain `diff -u` names its files only in the `+++` line.
  return { files: files || headers, add, del, classes };
}

/**
 * A code block, optionally with the file's name on it and its lines tinted.
 * `classes` tints text colour only, never a filled row: the block scrolls
 * sideways and a background would stop at the fold.
 */
function codeBlock(opts, src, { name = null, lang = null, classes = null } = {}) {
  const { lines, more } = clipLines(opts, src, CODE_LINES);
  if (!lines.length) return "";
  const head = name || lang
    ? `<div class="code-head">${name ? `<span class="code-name">${esc(clip(name, 60))}</span>` : ""}` +
      `${lang ? `<span class="code-lang">${esc(clip(lang, 24))}</span>` : ""}</div>`
    : "";
  const body = classes
    ? lines.map((ln, n) => (classes[n] ? `<span class="${classes[n]}">${esc(ln)}</span>` : esc(ln))).join("\n")
    : esc(lines.join("\n"));
  return head +
    `<pre class="codeblock${head ? " headed" : ""}${classes ? " diff" : ""}">${body}</pre>` +
    (more ? `<div class="muted-note">…${more} more line${more === 1 ? "" : "s"}</div>` : "");
}

// ---- the format-patch mail -------------------------------------------------

/**
 * `git format-patch` output, taken apart: `{subject, markers, message,
 * diffLines}`. Survives a bare diff, a subject folded across lines, a `---`
 * inside the commit message, and a header block that is not one. Never
 * throws; cards.test.mjs holds it against real patches.
 */
export function parsePatch(text) {
  // Split once and stay in lines: `diffLines` is a view of this array, so a
  // megabyte patch is one copy rather than one per stage.
  const lines = String(text || "").split(/\r?\n/);
  const head = Object.create(null);   // a stranger's header names, so no prototype
  let i = 0;
  // A mail only if it opens with git's own `From <sha> <date>` line; anything else is a bare diff.
  if (/^From [0-9a-f]{7,40} /.test(lines[0] || "")) {
    let name = null;
    for (i = 1; i < lines.length; i++) {
      // The blank line ends the headers, and so does the diff, for a mail that never wrote one.
      if (lines[i] === "" || isDiffStart(lines, i)) { if (lines[i] === "") i++; break; }
      if (/^[ \t]/.test(lines[i]) && name) { head[name] += " " + lines[i].trim(); continue; }
      const m = /^([A-Za-z][A-Za-z-]*):[ \t]*(.*)$/.exec(lines[i]);
      // A repeated header keeps the first value and takes no continuation.
      name = m && !(m[1].toLowerCase() in head) ? m[1].toLowerCase() : null;
      if (name) head[name] = m[2];
    }
  }
  let at = lines.length;
  for (let n = i; n < lines.length; n++) if (isDiffStart(lines, n)) { at = n; break; }
  // The message ends at git's `---` separator, searched backwards from the
  // diff: a `---` rule inside the commit message is prose. Git's diffstat
  // after it is dropped; the card's stat is counted from the diff shown.
  let end = at;
  for (let n = at - 1; n >= i; n--) if (lines[n] === "---") { end = n; break; }
  const subject = stripMarkers(head.subject || "");
  return {
    subject: subject.text,
    markers: subject.markers,
    message: lines.slice(i, end).join("\n").trim(),
    diffLines: lines.slice(at),
  };
}

/** Where a patch stops being prose: git's own header, or a `--- `/`+++ ` pair. */
const isDiffStart = (lines, n) =>
  lines[n].startsWith("diff --git ") ||
  (lines[n].startsWith("--- ") && String(lines[n + 1] || "").startsWith("+++ "));

/**
 * `[PATCH v2 2/3] router: yield ingest` -> `{markers: ["PATCH v2 2/3"], text:
 * "router: yield ingest"}`. Leading brackets are metadata about the patch and
 * become pills. Capped, because the brackets are a stranger's.
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

const statLine = (st) =>
  `<div class="diffstat">${st.files ? `<span>${st.files} file${st.files === 1 ? "" : "s"}</span>` : ""}` +
  `<span class="d-add">+${st.add}</span><span class="d-del">−${st.del}</span></div>`;

/** A commit id, at the length every git tool shows it at. */
const shortSha = (v) => (/^[0-9a-f]{7,64}$/.test(String(v || "")) ? `<span class="mono">${esc(String(v).slice(0, 7))}</span>` : null);

// ---- the cards -------------------------------------------------------------

/** A title with the pills that qualify it. The pills survive a card with no title. */
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
 * 1617 — a patch, in a reviewer's order: which repository, the subject, how
 * big, why, then the diff. The `subject` tag is the fallback, not the source.
 */
function patchCard(ev, opts) {
  const p = parsePatch(ev.content);
  const subject = p.subject || tagOf(ev, "subject") || "";
  // NIP-34's `["t", "root"]` says what the mail's `[PATCH 1/3]` says, so it is
  // the fallback, never a second pill that could contradict the first.
  const marks = p.markers.length
    ? p.markers
    : tagsOf(ev, "t").map((t) => t[1]).filter((v) => SERIES_MARKS.has(v));
  // Content the parse recognised nothing in (no mail, no diff) is shown
  // verbatim. Guarded on the whole parse: a message with no diff is already
  // rendered as prose above.
  const block = p.diffLines.length || p.subject || p.message || p.markers.length
    ? p.diffLines
    : String(ev.content || "").split(/\r?\n/);
  const st = readDiff(block, opts && opts.full ? block.length : CODE_LINES);
  const isDiff = !!(st.files || st.add || st.del);
  const inner =
    repoLine(ev, opts) +
    titleWith(opts, subject, marks.slice(0, 3).map((m) => pill(clip(m, 40))).join("")) +
    (isDiff ? statLine(st) : "") +
    bodyHtml(opts, p.message, 400) +
    // Tinted only when it is a diff; a prose line opening with a dash is not a removal.
    codeBlock(opts, block, { classes: isDiff ? st.classes : null });
  return shell(ev, opts, inner, opts && opts.full ? [
    ["commit", shortSha(tagOf(ev, "commit"))],
    ["parent", shortSha(tagOf(ev, "parent-commit"))],
  ] : []);
}

/** NIP-34's structural `t` values: a patch's series position, not a label. A pull request carries them too. */
const SERIES_MARKS = new Set(["root", "root-revision"]);

/**
 * 1621 — an issue: a subject and prose, in a repository, under its labels.
 * A 1622 reply takes the same template and carries no subject, hence the
 * reply line. Labels are `t` tags and link to the hashtag search.
 */
function issueCard(ev, opts) {
  const labels = [...new Set(tagsOf(ev, "t").map((t) => t[1]).filter((v) => v && !SERIES_MARKS.has(v)))];
  const inner =
    repoLine(ev, opts) +
    titleWith(opts, tagOf(ev, "subject")) +
    replyLine(ev) +
    bodyHtml(opts, ev.content, 500) +
    chipRow(labels, opts, hashtagHref);
  return shell(ev, opts, inner);
}

/**
 * 1630-1633 — a NIP-34 status. The kind is the status; no tag says which.
 * 1631's pill is short and its full phrase is the hover, since a patch is
 * applied and a pull request is merged.
 */
const GIT_STATUS = {
  1630: { label: "open", tone: "open", full: "open" },
  1631: { label: "merged", tone: "merged", full: "applied or merged" },
  1632: { label: "closed", tone: "closed", full: "closed" },
  1633: { label: "draft", tone: "draft", full: "draft" },
};
function gitStatusCard(ev, opts) {
  const s = GIT_STATUS[ev.kind];
  // The root `e` is what the status is about; the others name revisions and superseded statuses.
  const es = tagsOf(ev, "e").filter((t) => /^[0-9a-f]{64}$/.test(t[1] || ""));
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
 * The people a repository declares: the values of one `maintainers` tag,
 * which no scan of `p` tags reaches. Registered as the very function the card
 * draws with. The author is dropped; the byline is already them.
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
  // Every clone url: a mirrored repo's second url is the one that works when the first is down.
  const urls = (name) => multiTag(ev, name).slice(0, opts && opts.full ? 8 : 2).map((u) => [name, extLink(u)]);
  return shell(ev, opts, inner, [...urls("web"), ...urls("clone")]);
}

/** The refs under one prefix — the tag name is the branch, hence tagsWhere. */
const refsUnder = (ev, prefix) => tagsWhere(ev, (n) => n.startsWith(prefix))
  .map((t) => ({ name: t[0].slice(prefix.length), commit: t[1] }))
  .filter((r) => r.name);

/**
 * 30618 — repository state: one tag per ref, `["refs/heads/master", <commit>]`.
 * Branches and tags are two labelled groups, with the default branch marked
 * from `HEAD`'s `ref: refs/heads/<name>`.
 */
function repoStateCard(ev, opts) {
  const heads = refsUnder(ev, "refs/heads/");
  const tags = refsUnder(ev, "refs/tags/");
  const headRef = /^ref:\s*refs\/heads\/(.+)$/.exec(String(tagOf(ev, "HEAD") || "").trim());
  const head = headRef ? headRef[1] : null;
  // The counts label the groups rather than sitting on a line of their own.
  const inner =
    repoLine(ev, opts) +
    refSection(plural(heads.length, "branch", "branches"), heads, opts, head) +
    refSection(plural(tags.length, "tag"), tags, opts, null) +
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

/** What shipped: the `title` tag, else the version half of `<repo-id>@<version>`. */
const releaseVersion = (ev) => {
  const d = tagOf(ev, "d") || "";
  return titleOf(ev) || (d.includes("@") ? d.slice(d.lastIndexOf("@") + 1) : "");
};

/** 30063 — a release: what shipped, and the artifacts it points at, as a list of file names. */
function releaseCard(ev, opts) {
  const version = releaseVersion(ev);
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

/** The last segment of a url, as a name for the thing behind it; the url itself stays in the `href`. */
function fileName(url) {
  try {
    const p = decodeURIComponent(new URL(url).pathname);
    const last = p.slice(p.lastIndexOf("/") + 1);
    return last ? clip(last, 60) : clip(url, 60);
  } catch (e) { return clip(String(url), 60); }
}

register([1337], snippetCard);
register([1617], patchCard);
// 1618/1619 pull requests are a subject over prose, not a diff, so they take the issue template.
register([1621, 1618, 1619, 1622], issueCard);
register([1630, 1631, 1632, 1633], gitStatusCard);
register([30617], repoCard);
register([30618], repoStateCard);
register([30063], releaseCard);

// The rows. The second line is the repository on every kind that names one.
registerRow([1337], (ev) => ({
  name: tagOf(ev, "name") || tagOf(ev, "description"),
  sub: tagOf(ev, "l", "language") || tagOf(ev, "runtime"),
}));
registerRow([1617], (ev) => ({ name: parsePatch(ev.content).subject || tagOf(ev, "subject"), sub: repoName(ev) }));
registerRow([1621, 1618, 1619, 1622], (ev) => ({ name: tagOf(ev, "subject") || ev.content, sub: repoName(ev) }));
registerRow([1630, 1631, 1632, 1633], (ev) => ({ name: GIT_STATUS[ev.kind].full, sub: repoName(ev) || ev.content }));
registerRow([30617], (ev) => ({
  name: tagOf(ev, "name") || titleOf(ev) || tagOf(ev, "d"),
  sub: tagOf(ev, "description") || summaryOf(ev) || ev.content,
}));
// Only the groups that have refs, which is the card's rule too.
registerRow([30618], (ev) => {
  const heads = refsUnder(ev, "refs/heads/").length;
  const tags = refsUnder(ev, "refs/tags/").length;
  const groups = [heads ? plural(heads, "branch", "branches") : "", tags ? plural(tags, "tag") : ""].filter(Boolean);
  return { name: repoName(ev), sub: groups.join(" · ") || "no refs" };
});
registerRow([30063], (ev) => ({ name: releaseVersion(ev), sub: repoName(ev) || ev.content }));
