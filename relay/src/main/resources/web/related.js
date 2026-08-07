// What a git permalink shows BELOW its card.
//
// A NIP-34 event is one turn of a conversation this relay already holds the
// rest of, and the permalink used to show the turn alone: a repository page
// was a name, a description and two urls — nothing about the project — and an
// issue page was a bug report with no answer to the only question anybody
// opens one to ask, which is whether it is still open. Both facts are one REQ
// away, indexed under tags the store already answers on (`#a` for everything
// that belongs to a repository, `#e` for everything that answers an event).
//
// The rules are split from the fetching on purpose: what to ASK and how to
// SHAPE what comes back are pure functions, held by tools/webtest/related.test.mjs.
//
// It renders the SAME cards as everything else, at preview depth — so a repo's
// issues below its card look exactly like that issue in a search result, open
// on click, walk under j/k and toggle their own json, none of which this module
// implements: app.js delegates all three off #results, and these cards are in
// it. Depth is the one thing this file decides — the page is about the event
// above, and twenty full-depth patches under it would bury it in diffs.

import { card, namedPubkeys } from "./cards.js";
import { esc } from "./shared/format.js";
import { when } from "./shared/format.js";
import { profiles, displayName, enrichProfiles } from "./shared/profiles.js";
import { keyHref, noteHref } from "./cards/base.js";
import { repoAddr } from "./cards/code.js";
import { shortNpub, npub } from "./shared/nip19.js";

/** The kinds that belong TO a repository, and each gets a section of its own. */
const REPO_SECTIONS = [
  { head: "issues", kinds: [1621] },
  { head: "patches", kinds: [1617] },
  { head: "pull requests", kinds: [1618, 1619] },
  { head: "releases", kinds: [30063] },
];
const REPO_ITEM_KINDS = REPO_SECTIONS.flatMap((s) => s.kinds);

/** The kinds a status or a reply can be ABOUT. */
const THREAD_KINDS = [1617, 1618, 1619, 1621];
/** NIP-34's four verdicts, newest of which is the current one. */
const STATUS_KINDS = [1630, 1631, 1632, 1633];
/**
 * How an answer is written: NIP-34's own reply, a NIP-22 comment, a plain note.
 * Not `parents.js`'s REPLY_KINDS, which answers a different question — "which
 * kinds LEAD with who they answer" — and would drag in channel messages and
 * voice replies that no issue thread contains.
 */
const ANSWER_KINDS = [1622, 1111, 1];

/** How much of each list a page draws before it says how much it left. */
const SECTION_CAP = 20;
/** How much the relay is asked for — and so where a count stops being exact. */
const ASK_LIMIT = 120;
/** The one ask, and it must not hold the page: the card is already on screen. */
export const RELATED_TIMEOUT_MS = 6000;

/**
 * The NIP-01 filters that answer "what else belongs to this", or null for a
 * kind that asks nothing.
 *
 * A repository takes two, ORed in one REQ (NIP-01 ORs a subscription's
 * filters): everything tagged with its address, and its own state — which is
 * addressed by `d` and carries no `a` at all, so no `#a` ask would ever
 * reach it.
 */
export function relatedAsk(ev) {
  if (!ev || !/^[0-9a-f]{64}$/.test(ev.pubkey || "")) return null;
  if (ev.kind === 30617) {
    const d = (ev.tags || []).find((t) => Array.isArray(t) && t[0] === "d" && t[1]);
    if (!d) return null;
    return [
      { "#a": [`30617:${ev.pubkey}:${d[1]}`], kinds: REPO_ITEM_KINDS, limit: ASK_LIMIT },
      { kinds: [30618], authors: [ev.pubkey], "#d": [d[1]], limit: 1 },
    ];
  }
  if (THREAD_KINDS.includes(ev.kind) && /^[0-9a-f]{64}$/.test(ev.id || "")) {
    return [{ "#e": [ev.id], kinds: [...STATUS_KINDS, ...ANSWER_KINDS], limit: ASK_LIMIT }];
  }
  return null;
}

/**
 * What came back, as the page's shape: `{status, sections, events}`.
 *
 * `events` is every event that will be drawn, flat — the page hands it to the
 * json toggle and to the profile loader, and both want the set rather than the
 * layout.
 *
 * Two orders, deliberately different. A repository's lists are NEWEST first,
 * because they are lists and the newest issue is the live one. A thread is
 * OLDEST first, because it is a conversation and a conversation read backwards
 * is a different conversation.
 */
export function relatedShape(ev, events) {
  const seen = new Set([ev.id]);
  const fresh = [];
  for (const e of events || []) {
    if (!e || !e.id || seen.has(e.id) || !Number.isFinite(Number(e.created_at))) continue;
    if (onlyMentions(e, ev.id)) continue;
    seen.add(e.id);
    fresh.push(e);
  }
  // Whether the relay had more to say. `#e`/`#a` asks are capped, and a count
  // taken off a capped read is a claim the page cannot support: "20 issues"
  // when the ask stopped at its limit means "at least 20", and the heads say
  // so rather than quietly rounding a project's backlog down to the number
  // that fitted. `complete` is the client's EOSE flag — a TIMED-OUT read is
  // truncated too, and for a reason the reader has even less way to see.
  const partial = (events || []).length >= ASK_LIMIT || (events && events.complete === false);
  const count = (n, one, many) => `${n}${partial ? "+" : ""} ${n === 1 && !partial ? one : many}`;
  const newestFirst = [...fresh].sort((a, b) => b.created_at - a.created_at);
  // Which repository these cards are already UNDER. Every one of them belongs
  // to it — that is what the ask selected on — so the line each card draws to
  // say so is the one fact this page has already established, repeated once
  // per card. It is passed to the renderer rather than stripped afterwards:
  // the card decides what it draws, and it can only decide with the context.
  const within = ev.kind === 30617 ? selfAddr(ev) : repoAddr(ev);

  if (ev.kind === 30617) {
    const sections = [];
    // The repository's own state leads: branches and tags are what a person
    // arriving at a project wants before its issue list.
    const state = newestFirst.find((e) => e.kind === 30618);
    if (state) sections.push({ head: "state", events: [state], more: 0 });
    for (const s of REPO_SECTIONS) {
      const inSection = newestFirst.filter((e) => s.kinds.includes(e.kind));
      if (!inSection.length) continue;
      sections.push({
        head: count(inSection.length, singular(s.head), s.head),
        events: inSection.slice(0, SECTION_CAP),
        more: Math.max(0, inSection.length - SECTION_CAP),
      });
    }
    return { within, status: null, sections, events: sections.flatMap((s) => s.events) };
  }

  // A thread: its verdict, then its answers.
  const status = newestFirst.find((e) => STATUS_KINDS.includes(e.kind)) || null;
  const replies = fresh
    .filter((e) => ANSWER_KINDS.includes(e.kind))
    .sort((a, b) => a.created_at - b.created_at);
  const sections = replies.length
    ? [{
        head: count(replies.length, "reply", "replies"),
        events: replies.slice(0, SECTION_CAP),
        more: Math.max(0, replies.length - SECTION_CAP),
      }]
    : [];
  return { within, status, sections, events: [...(status ? [status] : []), ...sections.flatMap((s) => s.events)] };
}

const singular = (s) => (s === "issues" ? "issue" : s === "patches" ? "patch" : s === "releases" ? "release" : s.replace(/s$/, ""));

/**
 * An event that only MENTIONS the one this page is about.
 *
 * A `#e` ask returns everything carrying the id, and NIP-10 marks a reference
 * that is a citation rather than an answer. Listing those as replies puts
 * somebody quoting an issue elsewhere into its thread, under a heading
 * counting them as answers to it.
 */
function onlyMentions(e, id) {
  const es = (e.tags || []).filter((t) => Array.isArray(t) && t[0] === "e" && t[1] === id);
  return es.length > 0 && es.every((t) => String(t[3] || "").toLowerCase() === "mention");
}

/** A 30617's own address — what its page IS, and so what its cards are within. */
function selfAddr(ev) {
  const d = (ev.tags || []).find((t) => Array.isArray(t) && t[0] === "d" && t[1]);
  return d ? `30617:${ev.pubkey}:${d[1]}` : null;
}

/** The pubkeys the related cards will NAME — their authors, and whoever they mention. */
export const relatedPeople = (shape) =>
  [...new Set(shape.events.flatMap((e) => [e.pubkey, ...namedPubkeys(e, undefined)]))]
    .filter((pk) => /^[0-9a-f]{64}$/.test(pk || ""));

/**
 * The verdict, as the line an issue page owes its reader.
 *
 * It sits directly under the card rather than inside the list below, because
 * it is not another event on the page — it is the answer to the question the
 * page is about, and NIP-34 writes it as a separate event only because the
 * issue itself is immutable. Who closed it and when are part of that answer:
 * "closed" with nobody behind it invites the next question immediately.
 */
const VERDICT = { 1630: ["open", "open"], 1631: ["merged", "applied or merged"], 1632: ["closed", "closed"], 1633: ["draft", "draft"] };
function statusHtml(status) {
  const v = VERDICT[status.kind];
  if (!v) return "";
  const nm = displayName(profiles.get(status.pubkey));
  const who = `<a${nm ? "" : ' class="mono"'} href="${keyHref(status.pubkey)}" title="${esc(npub(status.pubkey))}">${esc(nm || shortNpub(status.pubkey))}</a>`;
  return `<div class="resolution"><span class="status-pill lead ${esc(v[0])}" title="${esc(v[1])}">${esc(v[0])}</span>` +
    ` by ${who} · <a href="${noteHref(status.id)}">${esc(when(status))}</a></div>`;
}

/** The whole block. Empty string when nothing came back, so nothing is drawn. */
export function relatedHtml(shape) {
  if (!shape || (!shape.status && !shape.sections.length)) return "";
  const opts = shape.within ? { within: shape.within } : undefined;
  const section = (s) =>
    `<div class="rel-section"><div class="section-head">${esc(s.head)}</div>` +
    s.events.map((e) => card(e, opts)).join("") +
    (s.more > 0 ? `<div class="muted-note">…and ${s.more} more</div>` : "") +
    `</div>`;
  return `<div class="related">${shape.status ? statusHtml(shape.status) : ""}${shape.sections.map(section).join("")}</div>`;
}

/**
 * Ask, shape, and load the names the cards need — everything but the DOM.
 *
 * A short budget and a swallowed failure on purpose: the permalink is already
 * on screen by the time this runs, and a repository whose issue list timed out
 * is a page missing a list, not a page that failed.
 */
export async function loadRelated(ev, conn) {
  const ask = relatedAsk(ev);
  if (!ask) return null;
  let events;
  try { events = await conn.req(ask, RELATED_TIMEOUT_MS); } catch (e) { return null; }
  const shape = relatedShape(ev, events);
  if (!shape.events.length) return shape;
  await enrichProfiles(relatedPeople(shape));
  return shape;
}
