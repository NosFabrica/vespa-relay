// The Tapestry Trusted List family, 30392-30395: a membership computed under one point
// of view. One renderer for four kinds; the kind decides which tag holds the members.

import { esc, titleOf } from "../shared/format.js";
import { shortAddr, shortNote } from "../shared/nip19.js";
import {
  register, registerRow, registerPeopleGrid, shell, peopleGrid, peopleOf,
  noteHref, addrHref, tagOf, tagsOf, plural,
} from "./base.js";

/** Keyed by kind, never sniffed: a 30393 carries `p` tags (its `observer`) that are not members. */
const MEMBER_TAG = { 30392: "p", 30393: "e", 30394: "a", 30395: "i" };

/** What one member is called, per kind. */
const MEMBER_NOUN = { 30392: "member", 30393: "event", 30394: "article", 30395: "identifier" };

/** The membership; 30392 goes through `peopleOf` so the faces and the fetched profiles are one set. */
const membersOf = (ev) =>
  ev.kind === 30392 ? peopleOf(ev) : tagsOf(ev, MEMBER_TAG[ev.kind] || "p").map((t) => t[1]).filter(Boolean);

/** The tags a list states in its props table; none of them is searchable. */
const FACTS = ["metric", "observer", "source-tag", "min-rank", "cutoff", "rigor"];

/** The score at index 3 of a member tag; outside 0..100 it is unscored, not clamped. */
const scoreOf = (tag) => {
  const n = Number.parseInt(tag[3], 10);
  return Number.isInteger(n) && n >= 0 && n <= 100 ? n : null;
};

/** The non-pubkey members, one link per row. */
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

/** 30392-30395 — one Trusted List. An untitled list is headed by its `d`. */
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
registerPeopleGrid([30392]);

// `metric` rides along because two lists can share a title and differ only in what they measured.
registerRow([30392, 30393, 30394, 30395], (ev) => ({
  name: titleOf(ev) || tagOf(ev, "d") || "",
  sub: [plural(membersOf(ev).length, MEMBER_NOUN[ev.kind] || "member"), tagOf(ev, "metric")].filter(Boolean).join(" · "),
}));
