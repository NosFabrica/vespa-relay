// The time family: live streams and calendar events. The one liberty taken
// with the badge system: a 30311's status tag is surfaced as a pill, because
// "live" versus "ended" is the entire question a person clicking one has.

import { esc, titleOf, summaryOf, imageOf } from "../shared/format.js";
import { shortAddr } from "../shared/nip19.js";
import { register, shell, bodyHtml, addrHref, tagOf, tagsOf, clipIf, fmtTs } from "./base.js";

/** 30311 — a live event: status, the stream, who is watching. */
function liveCard(ev, opts) {
  const status = (tagOf(ev, "status") || "").toLowerCase();
  const img = imageOf(ev);
  const full = opts && opts.full;
  const link = (url, label) => url ? `<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">${esc(label || url)}</a>` : null;
  const inner =
    (full && img ? `<div class="embed"><img src="${esc(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.parentElement.remove()" /></div>` : "") +
    `${titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 140))}${status ? ` <span class="status-pill ${esc(status)}">${esc(status)}</span>` : ""}</h2>` : ""}` +
    bodyHtml(opts, summaryOf(ev) || ev.content, 300);
  const participants = tagOf(ev, "current_participants");
  return shell(ev, opts, inner, [
    ["stream", link(tagOf(ev, "streaming"))],
    ["recording", link(tagOf(ev, "recording"))],
    ["starts", tagOf(ev, "starts") ? fmtTs(tagOf(ev, "starts")) : null],
    ["watching", participants ? esc(participants) : null],
  ]);
}

/**
 * 31922/31923 — calendar events. Two kinds because NIP-52 splits all-day
 * (dates as YYYY-MM-DD strings) from timed (unix seconds); one renderer,
 * because the difference is only how `start`/`end` want to be read.
 */
function calendarEventCard(ev, opts) {
  const ts = (v) => (v && /^\d{9,}$/.test(v) ? fmtTs(v) : v); // date strings pass through
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
  const n = tagsOf(ev, "a").length;
  const inner =
    (titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 140))}</h2>` : "") +
    `<div class="result-body">${n} event${n === 1 ? "" : "s"}</div>` +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner);
}

/**
 * 31925 — an RSVP. `status` is the answer (accepted/declined/tentative) and it
 * is the entire event; the `a` tag names what is being answered, so both ride
 * on the same line as the pill.
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

// 30312/30313 are NIP-53's interactive rooms and conference events: the same
// title/summary/status/streaming vocabulary as a live event, so the same card.
register([30311, 30312, 30313], liveCard);
register([31922, 31923], calendarEventCard);
register([31924], calendarCard);
register([31925], rsvpCard);
