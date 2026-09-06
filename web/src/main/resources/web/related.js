// What a git permalink shows below its card: the rest of the NIP-34 conversation this
// relay holds, under `#a` for a repository and `#e` for an event. The ask and the shape
// are pure functions; the cards are a search's own, at preview depth.

import { card, namedPubkeys } from "./cards.js";
import { esc } from "./shared/format.js";
import { when } from "./shared/format.js";
import { profiles, displayName, enrichProfiles } from "./shared/profiles.js";
import { keyHref, noteHref } from "./cards/base.js";
import { repoAddr } from "./cards/code.js";
import { shortNpub, npub } from "./shared/nip19.js";

/** The kinds that belong to a repository, each with a section of its own. */
const REPO_SECTIONS = [
  { head: "issues", kinds: [1621] },
  { head: "patches", kinds: [1617] },
  { head: "pull requests", kinds: [1618, 1619] },
  { head: "releases", kinds: [30063] },
];
const REPO_ITEM_KINDS = REPO_SECTIONS.flatMap((s) => s.kinds);

/** The kinds a status or a reply can be about. */
const THREAD_KINDS = [1617, 1618, 1619, 1621];
/** NIP-34's four verdicts; the newest is the current one. */
const STATUS_KINDS = [1630, 1631, 1632, 1633];
/** How an answer is written. Not parents.js's REPLY_KINDS, which no issue thread needs. */
const ANSWER_KINDS = [1622, 1111, 1];

const SECTION_CAP = 20;
/** Where a count stops being exact. */
const ASK_LIMIT = 120;
export const RELATED_TIMEOUT_MS = 6000;

/**
 * The filters that answer "what else belongs to this", or null for a kind that asks
 * nothing. A repository takes two: its state is addressed by `d` and carries no `a`.
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
 * What came back as `{within, status, sections, events}`; `events` is everything drawn,
 * flat. A repository's lists are newest first; a thread is oldest first.
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
  // A count off a capped or timed-out read is "at least", and the heads say so.
  const partial = (events || []).length >= ASK_LIMIT || (events && events.complete === false);
  const count = (n, one, many) => `${n}${partial ? "+" : ""} ${n === 1 && !partial ? one : many}`;
  const newestFirst = [...fresh].sort((a, b) => b.created_at - a.created_at);
  // Which repository these cards are already under, so each card can leave that line out.
  const within = ev.kind === 30617 ? selfAddr(ev) : repoAddr(ev);

  if (ev.kind === 30617) {
    const sections = [];
    // The repository's own state leads: branches and tags before the issue list.
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

/** An event that only cites the one this page is about; a `mention` is not a reply. */
function onlyMentions(e, id) {
  const es = (e.tags || []).filter((t) => Array.isArray(t) && t[0] === "e" && t[1] === id);
  return es.length > 0 && es.every((t) => String(t[3] || "").toLowerCase() === "mention");
}

/** A 30617's own address, which its cards are within. */
function selfAddr(ev) {
  const d = (ev.tags || []).find((t) => Array.isArray(t) && t[0] === "d" && t[1]);
  return d ? `30617:${ev.pubkey}:${d[1]}` : null;
}

/** The pubkeys the related cards will name: their authors, and whoever they mention. */
export const relatedPeople = (shape) =>
  [...new Set(shape.events.flatMap((e) => [e.pubkey, ...namedPubkeys(e, undefined)]))]
    .filter((pk) => /^[0-9a-f]{64}$/.test(pk || ""));

/** The verdict line under an issue's card: the state, who set it and when. */
const VERDICT = { 1630: ["open", "open"], 1631: ["merged", "applied or merged"], 1632: ["closed", "closed"], 1633: ["draft", "draft"] };
function statusHtml(status) {
  const v = VERDICT[status.kind];
  if (!v) return "";
  const nm = displayName(profiles.get(status.pubkey));
  const who = `<a${nm ? "" : ' class="mono"'} href="${keyHref(status.pubkey)}" title="${esc(npub(status.pubkey))}">${esc(nm || shortNpub(status.pubkey))}</a>`;
  return `<div class="resolution"><span class="status-pill lead ${esc(v[0])}" title="${esc(v[1])}">${esc(v[0])}</span>` +
    ` by ${who} · <a href="${noteHref(status.id)}">${esc(when(status))}</a></div>`;
}

/** The whole block; "" when nothing came back. */
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

/** Ask, shape, and load the names the cards need. A failed ask is a page missing a list, not a failed page. */
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
