// The graph family: who somebody follows, which relays they read, and this
// relay's OWN working kinds — 10040 observer declarations and 30382 score
// cards. Those two get real renderers precisely because they are what this
// relay is organised around: a 10040 permalink that read "kind 10040" with a
// hex blob would be the relay failing to explain itself.

import { esc, titleOf, summaryOf } from "../shared/format.js";
import { shortNote, shortNpub, shortAddr } from "../shared/nip19.js";
import { displayName, profiles } from "../shared/profiles.js";
import {
  register, registerRow, registerPeopleGrid, shell, peopleGrid, peopleOf, personLink, noteHref, addrHref,
  relayRows, tagsOf, tagsWhere, tagOf, clipIf, plural,
} from "./base.js";

/** 3 — the follow list: a count and the first faces and names, not 800 rows. */
function followsCard(ev, opts) {
  const pks = peopleOf(ev);
  const inner =
    `<div class="result-body">follows <b>${pks.length.toLocaleString()}</b> ${pks.length === 1 ? "person" : "people"}</div>` +
    peopleGrid(pks, opts);
  return shell(ev, opts, inner);
}

/** 30000 — a named follow set: the d/title plus the same grid. */
function followSetCard(ev, opts) {
  const pks = peopleOf(ev);
  const title = titleOf(ev);
  const inner =
    (title ? `<h2 class="result-title">${esc(clipIf(opts, title, 120))}</h2>` : "") +
    `<div class="result-body">${pks.length.toLocaleString()} ${pks.length === 1 ? "member" : "members"}</div>` +
    peopleGrid(pks, opts);
  return shell(ev, opts, inner);
}

/** 10002 — NIP-65: r tags, each optionally marked read or write. */
function relayListCard(ev, opts) {
  const rows = tagsOf(ev, "r").map((t) => ({ url: t[1] || "", note: t[2] || "read + write" }));
  const inner = `<div class="result-body">${rows.length} relay${rows.length === 1 ? "" : "s"}</div>` + relayRows(rows, opts);
  return shell(ev, opts, inner);
}

/** 30002 — a named relay set. */
function relaySetCard(ev, opts) {
  const rows = tagsOf(ev, "relay").map((t) => ({ url: t[1] || "", note: "" }));
  const title = titleOf(ev);
  const inner =
    (title ? `<h2 class="result-title">${esc(clipIf(opts, title, 120))}</h2>` : "") +
    relayRows(rows, opts);
  return shell(ev, opts, inner);
}

/** The score dimensions a 10040 names — `["30382:rank", <service>, <relay>]`. */
const dimensionsOf = (ev) => tagsWhere(ev, (name, t) => /^\d+:/.test(name) && t[1]);

/** 10040 — NIP-85: per dimension, the service trusted and the relay serving it. */
function observerCard(ev, opts) {
  const dims = dimensionsOf(ev);
  const rows = dims.map((t) =>
    `<li><span class="mono">${esc(t[0])}</span> → ${personLink(t[1])}${t[2] ? ` <span class="muted-note">${esc(t[2].replace(/^wss?:\/\//, ""))}</span>` : ""}</li>`);
  const inner =
    `<div class="result-body">trusts ${dims.length} score dimension${dims.length === 1 ? "" : "s"}</div>` +
    (rows.length ? `<ul class="relay-list">${rows.join("")}</ul>` : "");
  return shell(ev, opts, inner);
}

/**
 * 30382/30383/30384 — NIP-85 assertions. One renderer, because only the `d`
 * changes meaning: a pubkey for 30382, an event id for 30383, an `a` address
 * for 30384. Reading all three as a pubkey — which the pubkey-only version of
 * this card did — turns an event id into a link to a person who does not
 * exist, so the subject is resolved by KIND, not by shape.
 */
function scoreCard(ev, opts) {
  const subject = tagOf(ev, "d");
  const rank = tagOf(ev, "rank");
  const subjectLink = subjectLinkFor(ev.kind, subject);
  const extras = tagsWhere(ev, (name, t) => name !== "d" && name !== "rank" && t[1] && /^[\d.]+$/.test(t[1]))
    .map((t) => [t[0], esc(t[1])]);
  const inner =
    `<div class="result-body">scores ${subjectLink}</div>` +
    (rank != null ? `<div class="rank-big" title="the rank this service assigns">${esc(rank)}</div>` : "");
  return shell(ev, opts, inner, extras);
}

/** The thing an assertion is about, named the way its kind defines it. */
function subjectLinkFor(kind, subject) {
  if (!subject) return "(no subject)";
  if (kind === 30383 && /^[0-9a-f]{64}$/.test(subject)) {
    return `<a class="mono" href="${noteHref(subject)}">${esc(subjectName(kind, subject))}</a>`;
  }
  if (kind === 30384) {
    const href = addrHref(subject);
    return href ? `<a href="${href}">${esc(subjectName(kind, subject))}</a>` : esc(subject);
  }
  return /^[0-9a-f]{64}$/.test(subject) ? personLink(subject) : esc(subject);
}

/**
 * The same subject as the WORDS alone — what a type-ahead row has room for.
 *
 * The person branch is personLink's ladder without the anchor (name, else a
 * short npub, never hex), which is the same trade peopleGrid's cells make: a
 * row is one line of text, and a link inside a row that is already one big
 * click target has nowhere to go that the row does not.
 */
function subjectName(kind, subject) {
  if (!subject) return "(no subject)";
  if (kind === 30384) return shortAddr(subject);       // an address is never bare hex
  if (!/^[0-9a-f]{64}$/.test(subject)) return subject;
  return kind === 30383 ? shortNote(subject) : displayName(profiles.get(subject)) || shortNpub(subject);
}

register([3], followsCard);
// 39089/39092 are follow sets under another name — a named group of pubkeys
// meant to be followed together. Same tags, same card, no second template.
register([30000, 39089, 39092], followSetCard);
registerPeopleGrid([3, 30000, 39089, 39092]);
register([10002], relayListCard);
register([30002], relaySetCard);
register([10040], observerCard);
register([30382, 30383, 30384], scoreCard);

// The rows. Every card in this family COUNTS something, and none of them
// carries a line of prose to count as a title — so each row led with the
// author's name and then said it again underneath. The count is the card's
// first line and it is the row's too; the author's name is the second, which
// cards.js fills in when a row leaves it empty.
// A NAMED set carries its description on the same line, after the count: a
// starter pack called "Test Group" and described as "Test Group for Amethyst"
// is two facts, and the row has room for both once the count stops being the
// author's name repeated.
registerRow([3], (ev) => ({ sub: `follows ${plural(peopleOf(ev).length, "person", "people")}` }));
registerRow([30000, 39089, 39092], (ev) => ({
  name: titleOf(ev),
  sub: [plural(peopleOf(ev).length, "member"), summaryOf(ev)].filter(Boolean).join(" · "),
}));
registerRow([10002], (ev) => ({ sub: plural(tagsOf(ev, "r").length, "relay") }));
registerRow([30002], (ev) => ({
  name: titleOf(ev),
  sub: [plural(tagsOf(ev, "relay").length, "relay"), summaryOf(ev)].filter(Boolean).join(" · "),
}));
registerRow([10040], (ev) => ({ sub: `trusts ${plural(dimensionsOf(ev).length, "score dimension")}` }));
// An assertion's `d` is its subject and its `rank` is the verdict — the two
// facts the card leads with, in the two lines a row has. The subject used to
// reach the row as a 64-character hex blob (a `d` that is opaque falls out of
// titleOf), which named nobody and filled the line.
registerRow([30382, 30383, 30384], (ev) => ({
  name: `scores ${subjectName(ev.kind, tagOf(ev, "d"))}`,
  sub: tagOf(ev, "rank") ? `rank ${tagOf(ev, "rank")}` : "",
}));
