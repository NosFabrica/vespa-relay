// The Tapestry Trusted List family — 30392-30395: a curated membership,
// computed under one point of view and published as an event.
//
// These get a real card for the same reason 10040 and 30382 do (people.js says
// it): they are what this relay is organised around. A Trusted List is also
// the one kind whose permalink another card LINKS TO — the provenance pill on
// a spliced result opens the list that put it there — so a page reading "kind
// 30392" over a hex blob would be the relay failing to explain the reason it
// just gave.
//
// FOUR KINDS, ONE RENDERER, because only the MEMBER TAG changes: `p` on 30392
// is a pubkey, `e` on 30393 an event id, `a` on 30394 an address, `i` on 30395
// a NIP-73 external identifier. The kind decides which to read — the same rule
// scoreCard follows for an assertion's `d`, and for the same reason: reading
// all four as pubkeys would draw a link to a person who does not exist.

import { esc, titleOf } from "../shared/format.js";
import { shortAddr, shortNote } from "../shared/nip19.js";
import {
  register, registerRow, registerPeopleGrid, shell, peopleGrid, peopleOf,
  noteHref, addrHref, tagOf, tagsOf, plural,
} from "./base.js";

/**
 * Which tag holds the membership, per kind — the family's whole dispatch.
 *
 * Keyed by kind rather than sniffed from the tags: a list may legitimately
 * carry `p` tags that are NOT membership (30393's `observer`, say, names whose
 * point of view it was computed under), so "which tag is the members" is a
 * question only the kind can answer.
 */
const MEMBER_TAG = { 30392: "p", 30393: "e", 30394: "a", 30395: "i" };

/** What one member is CALLED, in the vocabulary of the kind that holds it. */
const MEMBER_NOUN = { 30392: "member", 30393: "event", 30394: "article", 30395: "identifier" };

const membersOf = (ev) => tagsOf(ev, MEMBER_TAG[ev.kind] || "p").map((t) => t[1]).filter(Boolean);

/**
 * THE FACTS A LIST IS, drawn as the props table rather than as prose.
 *
 * `metric` names the computation, `observer` the point of view it ran under,
 * `min-rank` and `cutoff` where it drew the line, `rigor` how hard it looked.
 * None of them is searchable (quartz indexes a list's `title` and nothing
 * else), and that is the point of putting them here: the card is where a
 * reader finds out what a list they cannot search for actually measured.
 */
const FACTS = ["metric", "observer", "source-tag", "min-rank", "cutoff", "rigor"];

/**
 * A member score, when the publisher assigned one: `["p", <key>, <hint>, <score>]`.
 *
 * Quartz reads index 3 as a 0..100 PERCENTAGE and drops anything outside that
 * range rather than clamping it, so a publisher counting on another scale
 * reads back as unscored. This mirrors that: a number this scale cannot
 * express is not drawn as though it could.
 */
const scoreOf = (tag) => {
  const n = Number.parseInt(tag[3], 10);
  return Number.isInteger(n) && n >= 0 && n <= 100 ? n : null;
};

/** The non-pubkey members, one link per row — an id, an address, an external identifier. */
function memberRows(ev, opts) {
  const tags = tagsOf(ev, MEMBER_TAG[ev.kind]).filter((t) => t[1]);
  const cap = opts && opts.full ? 40 : 8;
  const shown = tags.slice(0, cap);
  const more = tags.length - shown.length;
  const cells = shown.map((t) => {
    const v = t[1];
    const score = scoreOf(t);
    const mark = score == null ? "" : ` <span class="member-score" title="the score this publisher assigned">${score}</span>`;
    if (ev.kind === 30393) return `<li><a class="mono" href="${noteHref(v)}">${esc(shortNote(v))}</a>${mark}</li>`;
    if (ev.kind === 30394) {
      const href = addrHref(v);
      return `<li>${href ? `<a href="${href}">${esc(shortAddr(v))}</a>` : `<span class="mono">${esc(v)}</span>`}${mark}</li>`;
    }
    return `<li><span class="mono">${esc(v)}</span>${mark}</li>`;
  });
  if (!cells.length) return "";
  return `<ul class="relay-list">${cells.join("")}${more > 0 ? `<li class="muted-note">+${more.toLocaleString()} more</li>` : ""}</ul>`;
}

/**
 * 30392-30395 — one Trusted List.
 *
 * The TITLE leads, because it is the only thing about a list this relay can
 * search for, and so the only thing that can have brought a reader here by
 * name. An untitled list is legal and indexes the empty string; it gets its
 * `d` instead of a blank heading, which is at least an identifier a reader can
 * carry back to the publisher.
 */
function trustedListCard(ev, opts) {
  const members = membersOf(ev);
  const noun = MEMBER_NOUN[ev.kind] || "member";
  const heading = titleOf(ev) || tagOf(ev, "d") || "";
  const facts = FACTS.map((name) => [name, tagOf(ev, name)]).filter(([, v]) => v).map(([k, v]) => [k, esc(v)]);
  const inner =
    (heading ? `<h2 class="result-title">${esc(heading)}</h2>` : "") +
    `<div class="result-body">${esc(plural(members.length, noun))}</div>` +
    (ev.kind === 30392 ? peopleGrid(members, opts) : memberRows(ev, opts));
  return shell(ev, opts, inner, facts);
}

register([30392, 30393, 30394, 30395], trustedListCard);
// 30392's members ARE people, so the page owes itself their profiles before it
// draws the grid — the same declaration every other people-holding kind makes.
registerPeopleGrid([30392]);

// The row leads with the list's NAME, which is the half a reader recognises,
// and counts underneath. `metric` rides along because two lists can share a
// title and differ only in what they measured — "Verified Human" is published
// twice under two observers on staging today, and a row that said only the
// title would offer the reader two identical lines.
registerRow([30392, 30393, 30394, 30395], (ev) => ({
  name: titleOf(ev) || tagOf(ev, "d") || "",
  sub: [plural(membersOf(ev).length, MEMBER_NOUN[ev.kind] || "member"), tagOf(ev, "metric")].filter(Boolean).join(" · "),
}));
