// The graph family: who somebody follows, which relays they read, and this
// relay's OWN working kinds — 10040 observer declarations and 30382 score
// cards. Those two get real renderers precisely because they are what this
// relay is organised around: a 10040 permalink that read "kind 10040" with a
// hex blob would be the relay failing to explain itself.

import { esc, titleOf } from "../shared/format.js";
import { npub, shortNpub } from "../shared/nip19.js";
import { displayName, profiles } from "../shared/profiles.js";
import { register, shell, faceStrip, keyHref, tagsOf, tagOf, clipIf } from "./base.js";

const pTags = (ev) => tagsOf(ev, "p").map((t) => t[1]).filter((pk) => /^[0-9a-f]{64}$/.test(pk));

/** A person, linked: their name when the store knows one, a short npub only
    as the fallback, the full npub in the hover — and never, anywhere, hex. */
const personLink = (pk) => {
  const nm = displayName(profiles.get(pk));
  return `<a${nm ? "" : ' class="mono"'} href="${keyHref(pk)}" title="${esc(npub(pk))}">${esc(nm || shortNpub(pk))}</a>`;
};

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

/** A list of relay rows; full mode shows all, preview the first few. */
function relayRows(rows, opts) {
  const shown = opts && opts.full ? rows : rows.slice(0, 6);
  const more = rows.length - shown.length;
  return `<ul class="relay-list">${shown.map((r) => `<li><span class="mono">${esc(r.url)}</span>${r.note ? ` <span class="muted-note">${esc(r.note)}</span>` : ""}</li>`).join("")}${more > 0 ? `<li class="muted-note">…and ${more} more</li>` : ""}</ul>`;
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
  const dims = (ev.tags || []).filter((t) => /^\d+:/.test(t[0] || "") && t[1]);
  const rows = dims.map((t) =>
    `<li><span class="mono">${esc(t[0])}</span> → ${personLink(t[1])}${t[2] ? ` <span class="muted-note">${esc(t[2].replace(/^wss?:\/\//, ""))}</span>` : ""}</li>`);
  const inner =
    `<div class="result-body">trusts ${dims.length} score dimension${dims.length === 1 ? "" : "s"}</div>` +
    (rows.length ? `<ul class="relay-list">${rows.join("")}</ul>` : "");
  return shell(ev, opts, inner);
}

/** 30382 — one score: WHO it is about, and the rank under this service's lens. */
function scoreCard(ev, opts) {
  const subject = tagOf(ev, "d");
  const rank = tagOf(ev, "rank");
  const subjectLink = subject && /^[0-9a-f]{64}$/.test(subject)
    ? personLink(subject)
    : esc(subject || "(no subject)");
  const extras = (ev.tags || [])
    .filter((t) => t[0] !== "d" && t[0] !== "rank" && t[1] && /^[\d.]+$/.test(t[1]))
    .map((t) => [t[0], esc(t[1])]);
  const inner =
    `<div class="result-body">scores ${subjectLink}</div>` +
    (rank != null ? `<div class="rank-big" title="the rank this service assigns">${esc(rank)}</div>` : "");
  return shell(ev, opts, inner, extras);
}

register([3], followsCard);
register([30000], followSetCard);
register([10002], relayListCard);
register([30002], relaySetCard);
register([10040], observerCard);
register([30382], scoreCard);
