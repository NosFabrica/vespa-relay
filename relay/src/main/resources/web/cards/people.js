// The graph family: who somebody follows, which relays they read, and this
// relay's OWN working kinds — 10040 observer declarations and 30382 score
// cards. Those two get real renderers precisely because they are what this
// relay is organised around: a 10040 permalink that read "kind 10040" with a
// hex blob would be the relay failing to explain itself.

import { esc, titleOf } from "../shared/format.js";
import { shortNote, shortAddr } from "../shared/nip19.js";
import { register, shell, faceStrip, personLink, noteHref, addrHref, relayRows, tagsOf, tagsWhere, tagOf, clipIf } from "./base.js";

const pTags = (ev) => tagsOf(ev, "p").map((t) => t[1]).filter((pk) => /^[0-9a-f]{64}$/.test(pk));

/** 3 — the follow list: a count and a strip of faces, not 800 rows. */
function followsCard(ev, opts) {
  const pks = pTags(ev);
  const inner =
    `<div class="result-body">follows <b>${pks.length.toLocaleString()}</b> ${pks.length === 1 ? "person" : "people"}</div>` +
    faceStrip(pks, opts && opts.full ? 24 : 12);
  return shell(ev, opts, inner);
}

/** 30000 — a named follow set: the d/title plus the same strip. */
function followSetCard(ev, opts) {
  const pks = pTags(ev);
  const title = titleOf(ev);
  const inner =
    (title ? `<h2 class="result-title">${esc(clipIf(opts, title, 120))}</h2>` : "") +
    `<div class="result-body">${pks.length.toLocaleString()} ${pks.length === 1 ? "member" : "members"}</div>` +
    faceStrip(pks, opts && opts.full ? 24 : 12);
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

/** 10040 — NIP-85: per dimension, the service trusted and the relay serving it. */
function observerCard(ev, opts) {
  const dims = tagsWhere(ev, (name, t) => /^\d+:/.test(name) && t[1]);
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
    return `<a class="mono" href="${noteHref(subject)}">${esc(shortNote(subject))}</a>`;
  }
  if (kind === 30384) {
    const href = addrHref(subject);
    return href ? `<a href="${href}">${esc(shortAddr(subject))}</a>` : esc(subject);
  }
  return /^[0-9a-f]{64}$/.test(subject) ? personLink(subject) : esc(subject);
}

register([3], followsCard);
// 39089/39092 are follow sets under another name — a named group of pubkeys
// meant to be followed together. Same tags, same card, no second template.
register([30000, 39089, 39092], followSetCard);
register([10002], relayListCard);
register([30002], relaySetCard);
register([10040], observerCard);
register([30382, 30383, 30384], scoreCard);
