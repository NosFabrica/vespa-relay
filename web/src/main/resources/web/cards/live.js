// The time family: live streams and calendar events, plus the two things a stream throws off —
// a raid that sends its audience somewhere else, and a clip cut out of it. A 30311's status is a
// pill beside the badge.

import { esc, clip, titleOf, summaryOf, imageOf } from "../shared/format.js";
import { shortAddr } from "../shared/nip19.js";
import {
  register, registerRow, shell, titleHtml, bodyHtml, personLink, addrHref, extLink, videoEmbed,
  tagOf, tagsOf, tagsWhere, clipIf, fmtTs, plural,
} from "./base.js";

const HEX64 = /^[0-9a-f]{64}$/;

/** A 30311 address as a link, or its short form when it cannot be encoded. */
function streamLink(addr) {
  if (!addr) return null;
  const href = addrHref(addr);
  return href ? `<a href="${href}">${esc(clip(shortAddr(addr), 60))}</a>` : `<span class="mono">${esc(clip(shortAddr(addr), 60))}</span>`;
}

/** The streams a raid names: `a` tags pointing at a 30311, which is what makes one an end of a raid. */
const raidAddrs = (ev) => tagsWhere(ev, (name, t) => name === "a" && /^30311:[0-9a-f]{64}:/.test(String(t[1] || "")));

/** The one carrying a NIP-10 marker: a raid names its source `root` and its target `mention`. */
const markedAddr = (ev, marker) =>
  (raidAddrs(ev).find((t) => String(t[3] || "") === marker) || [])[1] || null;

/**
 * 1312 — a raid: a streamer handing their audience to another stream. Both ends are `a` tags and
 * ONLY the marker tells them apart, so an unmarked one is shown as a stream this raid names
 * without claiming which end it is — guessing would send a reader to the wrong room.
 */
function raidCard(ev, opts) {
  const to = markedAddr(ev, "mention");
  const from = markedAddr(ev, "root");
  const unmarked = to || from ? [] : raidAddrs(ev).map((t) => t[1]);
  const inner =
    `<div class="result-body">raids ${streamLink(to) || "another stream"}</div>` +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner, [
    ["from", streamLink(from)],
    ["stream", unmarked.length ? unmarked.map(streamLink).filter(Boolean).join(" · ") : null],
  ]);
}

/** 1313 — a clip cut from a stream: the video itself, and the stream it came out of. */
function clipCard(ev, opts) {
  const host = tagOf(ev, "p");
  const inner =
    titleHtml(opts, titleOf(ev), 140) +
    (opts && opts.full ? videoEmbed(tagOf(ev, "r")) : "") +
    bodyHtml(opts, ev.content || summaryOf(ev), 300);
  return shell(ev, opts, inner, [
    ["from", streamLink(tagOf(ev, "a"))],
    ["host", HEX64.test(host || "") ? personLink(host) : null],
    ["video", extLink(tagOf(ev, "r"))],
  ]);
}

/** 30311 — a live event: status, the stream, who is watching. */
function liveCard(ev, opts) {
  const status = (tagOf(ev, "status") || "").toLowerCase();
  const img = imageOf(ev);
  const full = opts && opts.full;
  const inner =
    (full && img ? `<div class="embed"><img src="${esc(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.parentElement.remove()" /></div>` : "") +
    `${titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 140))}${status ? ` <span class="status-pill ${esc(status)}">${esc(status)}</span>` : ""}</h2>` : ""}` +
    bodyHtml(opts, summaryOf(ev) || ev.content, 300);
  const participants = tagOf(ev, "current_participants");
  return shell(ev, opts, inner, [
    ["stream", extLink(tagOf(ev, "streaming"))],
    ["recording", extLink(tagOf(ev, "recording"))],
    ["starts", tagOf(ev, "starts") ? esc(fmtTs(tagOf(ev, "starts"))) : null],
    ["watching", participants ? esc(participants) : null],
  ]);
}

/** NIP-52 splits all-day (a YYYY-MM-DD string, passed through) from timed (unix seconds). */
const ts = (v) => (v && /^\d{9,}$/.test(v) ? fmtTs(v) : v);

/** 31922/31923 — calendar events, all-day and timed; only how `start`/`end` read differs. */
function calendarEventCard(ev, opts) {
  const inner =
    (titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 140))}</h2>` : "") +
    bodyHtml(opts, summaryOf(ev) || ev.content, 300);
  return shell(ev, opts, inner, [
    ["starts", tagOf(ev, "start") ? esc(ts(tagOf(ev, "start"))) : null],
    ["ends", tagOf(ev, "end") ? esc(ts(tagOf(ev, "end"))) : null],
    ["location", tagOf(ev, "location") ? esc(tagOf(ev, "location")) : null],
  ]);
}

/** 31924 — a calendar: how many events it collects. */
function calendarCard(ev, opts) {
  const inner =
    (titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 140))}</h2>` : "") +
    `<div class="result-body">${esc(plural(tagsOf(ev, "a").length, "event"))}</div>` +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner);
}

/**
 * 31925 — an RSVP: `status` is the answer (accepted/declined/tentative), the `a` tag what is being
 * answered.
 */
function rsvpCard(ev, opts) {
  const status = (tagOf(ev, "status") || "").toLowerCase();
  const target = tagOf(ev, "a");
  const href = target ? addrHref(target) : null;
  const inner =
    `<div class="result-body">${status ? `<span class="status-pill ${esc(status)}">${esc(status)}</span> ` : ""}` +
    `${target ? `for ${href ? `<a href="${href}">${esc(shortAddr(target))}</a>` : esc(shortAddr(target))}` : "rsvp"}</div>` +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner, [
    ["free/busy", tagOf(ev, "fb") ? esc(tagOf(ev, "fb")) : null],
  ]);
}

// 30312/30313 are NIP-53's rooms and conference events: the same vocabulary as a live event.
register([30311, 30312, 30313], liveCard);
register([1312], raidCard);
register([1313], clipCard);
register([31922, 31923], calendarEventCard);
register([31924], calendarCard);
register([31925], rsvpCard);

// `status` leads the second line for the reason it is a pill on the card.
registerRow([30311, 30312, 30313], (ev) => ({
  name: titleOf(ev),
  sub: [tagOf(ev, "status"), summaryOf(ev) || ev.content].filter(Boolean).join(" · "),
}));
// When and where, which for a calendar entry is most of what it is.
registerRow([31922, 31923], (ev) => ({
  name: titleOf(ev),
  sub: [ts(tagOf(ev, "start")), tagOf(ev, "location")].filter(Boolean).join(" · ")
    || summaryOf(ev) || ev.content,
}));
registerRow([31924], (ev) => ({ name: titleOf(ev), sub: plural(tagsOf(ev, "a").length, "event") }));
registerRow([31925], (ev) => ({
  name: tagOf(ev, "status") ? `rsvp: ${tagOf(ev, "status")}` : "rsvp",
  sub: ev.content,
}));
// A raid is about where it sends people; a clip, about what it shows.
registerRow([1312], (ev) => ({
  name: `raids ${shortAddr(markedAddr(ev, "mention") || "") || "another stream"}`,
  sub: ev.content,
}));
registerRow([1313], (ev) => ({ name: titleOf(ev), sub: ev.content || summaryOf(ev) }));
