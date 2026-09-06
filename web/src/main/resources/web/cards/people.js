// The graph family: who somebody follows, which relays they read, and this
// relay's own working kinds, 10040 observer declarations and 30382 score cards.

import { esc, titleOf, summaryOf } from "../shared/format.js";
import { shortNote, shortNpub, shortAddr } from "../shared/nip19.js";
import { displayName, profiles } from "../shared/profiles.js";
import {
  register, registerRow, registerPeopleGrid, shell, bodyHtml, peopleGrid, peopleOf, personLink, noteHref,
  addrHref, relayRows, tagsOf, tagsWhere, tagOf, clipIf, plural,
} from "./base.js";

/** 3 — the follow list: a count and the first faces and names, not 800 rows. */
function followsCard(ev, opts) {
  const pks = peopleOf(ev);
  const inner =
    `<div class="result-body">follows <b>${pks.length.toLocaleString()}</b> ${pks.length === 1 ? "person" : "people"}</div>` +
    peopleGrid(pks, opts);
  return shell(ev, opts, inner);
}

/** 30000 — a named follow set: title, muted description, and the same grid. */
function followSetCard(ev, opts) {
  const pks = peopleOf(ev);
  const title = titleOf(ev);
  const inner =
    (title ? `<h2 class="result-title">${esc(clipIf(opts, title, 120))}</h2>` : "") +
    bodyHtml(opts, summaryOf(ev), 300, true) +
    `<div class="result-body">${esc(plural(pks.length, "member"))}</div>` +
    peopleGrid(pks, opts);
  return shell(ev, opts, inner);
}

/** 10002 — NIP-65: r tags, each optionally marked read or write. */
function relayListCard(ev, opts) {
  const rows = tagsOf(ev, "r").map((t) => ({ url: t[1] || "", note: t[2] || "read + write" }));
  const inner = `<div class="result-body">${esc(plural(rows.length, "relay"))}</div>` + relayRows(rows, opts);
  return shell(ev, opts, inner);
}

/** 30002 — a named relay set, and whatever it says it is for. */
function relaySetCard(ev, opts) {
  const urls = tagsOf(ev, "relay").map((t) => ({ url: t[1] || "", note: "" }));
  const title = titleOf(ev);
  const inner =
    (title ? `<h2 class="result-title">${esc(clipIf(opts, title, 120))}</h2>` : "") +
    bodyHtml(opts, summaryOf(ev), 300, true) +
    relayRows(urls, opts);
  return shell(ev, opts, inner);
}

/** The score dimensions a 10040 names: `["30382:rank", <service>, <relay>]`. */
const dimensionsOf = (ev) => tagsWhere(ev, (name, t) => /^\d+:/.test(name) && t[1]);

/** 10040 — NIP-85: per dimension, the service trusted and the relay serving it. */
function observerCard(ev, opts) {
  const dims = dimensionsOf(ev);
  const rows = dims.map((t) =>
    `<li><span class="mono">${esc(t[0])}</span> → ${personLink(t[1])}${t[2] ? ` <span class="muted-note">${esc(t[2].replace(/^wss?:\/\//, ""))}</span>` : ""}</li>`);
  const inner =
    `<div class="result-body">trusts ${esc(plural(dims.length, "score dimension"))}</div>` +
    (rows.length ? `<ul class="relay-list">${rows.join("")}</ul>` : "");
  return shell(ev, opts, inner);
}

/**
 * 30382/30383/30384 — NIP-85 assertions. One renderer; the subject is resolved by kind, not by
 * shape, since an event id is hex too.
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

/** The same subject as words alone, for a row: a name, else a short npub, never hex. */
function subjectName(kind, subject) {
  if (!subject) return "(no subject)";
  if (kind === 30384) return shortAddr(subject);       // an address is never bare hex
  if (!/^[0-9a-f]{64}$/.test(subject)) return subject;
  return kind === 30383 ? shortNote(subject) : displayName(profiles.get(subject)) || shortNpub(subject);
}

register([3], followsCard);
// 39089/39092 are follow sets under another name: same tags, same card.
register([30000, 39089, 39092], followSetCard);
registerPeopleGrid([3, 30000, 39089, 39092]);
register([10002], relayListCard);
register([30002], relaySetCard);
register([10040], observerCard);
register([30382, 30383, 30384], scoreCard);

// Every card here counts something and none has a title, so the count is the row's line.
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
// An assertion's `d` is its subject and its `rank` the verdict: the card's two facts, in the row's
// two lines.
registerRow([30382, 30383, 30384], (ev) => ({
  name: `scores ${subjectName(ev.kind, tagOf(ev, "d"))}`,
  sub: tagOf(ev, "rank") ? `rank ${tagOf(ev, "rank")}` : "",
}));
