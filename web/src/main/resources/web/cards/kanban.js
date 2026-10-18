// Kanban boards and cards (NIP PR #1665, draft): a board is a titled set of ordered columns, a card
// a task that names its board with an `a` tag and its column with `s`.
//
// 30301 is shared, and this file renders all of it because a renderer is chosen per kind:
// WalletScrutiny publishes reproducible-build verifications there, and an encrypted planner app
// its tasks. Each event is told apart by its tags in quartz's order (`EventFactory`): `i` +
// `status` is a verification, a `title` or a named `col` a board, and the rest none of them.
// 30302 is shared with Fieldbook's team memberships the same way.

import { esc, clip, titleOf, HEX64 } from "../shared/format.js";
import { shortNote, shortAddr } from "../shared/nip19.js";
import {
  register, registerRow, registerBadge, registerNamedPeople, shell, titleHtml, bodyHtml, chipRow,
  faceStrip, extLink, addrHref, eventHref, tagOf, tagsOf, oneLine, plural, jsonContent, notOursCard,
  uniquePubkeys,
} from "./base.js";

// ---- telling 30301 apart ----------------------------------------------------

/** A WalletScrutiny verification: a product id (`i`) and a verdict (`status`). */
const isVerification = (ev) =>
  tagsOf(ev, "i").some((t) => t[1]) && tagsOf(ev, "status").some((t) => t[1]);

/** A board: a title, or a column with both an id and a name. */
const isBoard = (ev) => !!oneLine(tagOf(ev, "title")) || tagsOf(ev, "col").some((t) => t[1] && t[2]);

/** Which of the three a 30301 is, verification first as quartz checks it. */
const kind30301 = (ev) => (isVerification(ev) ? "verification" : isBoard(ev) ? "board" : "other");

/** A card: a title, or an `a` naming a 30301 board. */
const isCard = (ev) =>
  !!oneLine(tagOf(ev, "title")) || tagsOf(ev, "a").some((t) => /^30301:[0-9a-f]{64}:/.test(t[1] || ""));

// ---- the board ----------------------------------------------------------------

/** A board's or card's own words: its `description` (markdown, read as text), never the NIP-31 `alt`. */
const descriptionOf = (ev) => String(tagOf(ev, "description") || "").trim();

/** The columns by their `order`, the unordered last, ties in tag order (quartz's `sortedColumns`). */
function columnsOf(ev) {
  const cols = tagsOf(ev, "col").filter((t) => t[1] && t[2]).map((t, i) => ({
    name: oneLine(t[2]),
    order: /^-?\d+$/.test(t[3] || "") ? Number(t[3]) : Infinity,
    i,
  }));
  return cols.sort((a, b) => a.order - b.order || a.i - b.i).map((c) => c.name).filter(Boolean);
}

/** The people who may add and edit cards besides the author: the board's `p` tags. */
const maintainersOf = (ev) => uniquePubkeys(tagsOf(ev, "p").map((t) => t[1])).filter((pk) => pk !== ev.pubkey);

/** 30301 — a Kanban board: its title, its columns in order, and who maintains it. */
function boardCard(ev, opts) {
  const cols = columnsOf(ev);
  const maintainers = maintainersOf(ev);
  const inner =
    titleHtml(opts, titleOf(ev), 140) +
    bodyHtml(opts, descriptionOf(ev), 300, true) +
    (cols.length ? `<div class="result-body">${esc(plural(cols.length, "column"))}</div>` + chipRow(cols, opts) : "") +
    (maintainers.length
      ? `<div class="result-body muted">${esc(plural(maintainers.length, "maintainer"))}</div>` + faceStrip(maintainers)
      : "");
  return shell(ev, opts, inner);
}

// ---- the verification ---------------------------------------------------------

/** WalletScrutiny's verdicts in words, and the ones a reader should see tinted. */
const VERDICT = {
  reproducible: { label: "reproducible", tone: "open" },
  not_reproducible: { label: "not reproducible", tone: "closed" },
  ftbfs: { label: "fails to build", tone: "closed" },
  nosource: { label: "no public source", tone: "closed" },
  notag: { label: "no source tag", tone: "" },
  obfuscated: { label: "obfuscated", tone: "closed" },
  warning: { label: "warning", tone: "" },
  spam: { label: "spam", tone: "" },
};

/** The verdict, in words; a code WalletScrutiny adds later still shows, untinted, as written. */
const verdictOf = (ev) => {
  const code = oneLine(tagOf(ev, "status")).toLowerCase();
  return VERDICT[code] || { label: code.replace(/_/g, " "), tone: "" };
};

/** What was built: "<product> <version> · <platform>". */
const builtLine = (ev) =>
  [[oneLine(tagOf(ev, "i")), oneLine(tagOf(ev, "version"))].filter(Boolean).join(" "), oneLine(tagOf(ev, "platform"))]
    .filter(Boolean).join(" · ");

/**
 * The content is JSON, `{description, content}`: a one-line description and the markdown report
 * (quartz's `BuildVerificationContent`). A content that is not that JSON is read as the report.
 */
function reportOf(ev) {
  const j = jsonContent(ev);
  const text = (v) => (typeof v === "string" ? v.trim() : "");
  const parsed = !!(text(j.description) || text(j.content));
  return { description: text(j.description), report: parsed ? text(j.content) : String(ev.content || "").trim() };
}

/** 30301 — a reproducible-build verification: the verdict, what was built, and the report. */
function verificationCard(ev, opts) {
  const v = verdictOf(ev);
  const { description, report } = reportOf(ev);
  const built = builtLine(ev);
  const files = tagsOf(ev, "x").filter((t) => HEX64.test(t[1] || "")).length;
  const basedOn = tagsOf(ev, "based-on").find((t) => HEX64.test(t[1] || ""));
  const inner =
    `<div class="pill-row"><span class="status-pill lead${v.tone ? ` ${v.tone}` : ""}">${esc(clip(v.label, 24))}</span>` +
    (built ? `<span>${esc(clip(built, 100))}</span>` : "") + `</div>` +
    titleHtml(opts, description, 140) +
    bodyHtml(opts, report, 500);
  return shell(ev, opts, inner, [
    ["files", files ? esc(plural(files, "file")) : null],
    ["issues", extLink(tagOf(ev, "issue-tracker-url"))],
    ["based on", basedOn ? `<a class="mono" href="${eventHref(basedOn[1], { relay: basedOn[2] })}">${esc(shortNote(basedOn[1]))}</a>` : null],
  ]);
}

// ---- the card -------------------------------------------------------------------

/** The board a card belongs to: its `a` naming a 30301. */
const boardOf = (ev) => tagsOf(ev, "a").map((t) => t[1]).find((v) => /^30301:[0-9a-f]{64}:./.test(v || "")) || null;

/** "on <board d>", linked. */
function boardLine(ev) {
  const board = boardOf(ev);
  const href = board && addrHref(board);
  return href ? `<div class="result-body muted">on <a href="${href}">${esc(clip(shortAddr(board), 60))}</a></div>` : "";
}

/** What a tracker card mirrors (it has a `k`): the event, or the addressable, it follows. */
function trackedLine(ev) {
  const k = oneLine(tagOf(ev, "k"));
  if (!/^\d+$/.test(k)) return "";
  const e = tagsOf(ev, "e").find((t) => HEX64.test(t[1] || ""));
  const a = tagsOf(ev, "a").map((t) => t[1]).find((v) => /^\d+:[0-9a-f]{64}:/.test(v || "") && !v.startsWith("30301:"));
  const link = e
    ? `<a class="mono" href="${eventHref(e[1], { relay: e[2] })}">${esc(shortNote(e[1]))}</a>`
    : a && addrHref(a) ? `<a href="${addrHref(a)}">${esc(clip(shortAddr(a), 60))}</a>` : "";
  return `<div class="result-body muted">tracks a kind ${esc(k)} event${link ? ` ${link}` : ""}</div>`;
}

/** 30302 — a Kanban card: its column, its board, the task, and who it is assigned to. */
function cardCard(ev, opts) {
  const status = oneLine(tagOf(ev, "s"));
  const assignees = uniquePubkeys(tagsOf(ev, "p").map((t) => t[1]));
  const links = tagsOf(ev, "u").map((t) => t[1]).filter(Boolean);
  const inner =
    (status ? `<div class="pill-row"><span class="status-pill lead">${esc(clip(status, 24))}</span></div>` : "") +
    titleHtml(opts, titleOf(ev), 140) +
    boardLine(ev) +
    trackedLine(ev) +
    bodyHtml(opts, descriptionOf(ev), 400) +
    (assignees.length ? `<div class="result-body muted">assigned to ${esc(plural(assignees.length, "person", "people"))}</div>` + faceStrip(assignees) : "");
  return shell(ev, opts, inner, [
    ["attachments", links.length ? links.slice(0, opts && opts.full ? 12 : 3).map((u) => extLink(u)).filter(Boolean).join("<br>") || null : null],
  ]);
}

// ---- registration ------------------------------------------------------------------

const CARDS_30301 = { verification: verificationCard, board: boardCard, other: (ev, opts) => notOursCard(ev, opts, "a Kanban board or a build verification") };

register([30301], (ev, opts) => CARDS_30301[kind30301(ev)](ev, opts));
register([30302], (ev, opts) => (isCard(ev) ? cardCard(ev, opts) : notOursCard(ev, opts, "a Kanban card")));
registerBadge([30301], (ev) => ({ verification: "build verification", board: "", other: "kind 30301" })[kind30301(ev)]);
registerBadge([30302], (ev) => (isCard(ev) ? "" : "kind 30302"));
registerNamedPeople([30301], (ev) => (kind30301(ev) === "board" ? maintainersOf(ev) : []));
registerNamedPeople([30302], (ev) => (isCard(ev) ? uniquePubkeys(tagsOf(ev, "p").map((t) => t[1])) : []));

registerRow([30301], (ev) => {
  const which = kind30301(ev);
  if (which === "verification") {
    return { name: `${verdictOf(ev).label}${builtLine(ev) ? ` · ${builtLine(ev)}` : ""}`, sub: reportOf(ev).description };
  }
  if (which === "board") {
    const cols = columnsOf(ev);
    return { name: titleOf(ev) || "a Kanban board", sub: [cols.length ? cols.slice(0, 6).join(" → ") : "", descriptionOf(ev)].filter(Boolean).join(" · ") };
  }
  return { name: "a kind 30301 event, not a board or a verification" };
});
registerRow([30302], (ev) => (isCard(ev)
  ? { name: titleOf(ev) || "a Kanban card", sub: [oneLine(tagOf(ev, "s")), descriptionOf(ev)].filter(Boolean).join(" · ") }
  : { name: "a kind 30302 event, not a Kanban card" }));
