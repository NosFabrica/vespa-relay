// The time family: live streams and calendar events. The one liberty taken
// with the badge system: a 30311's status tag is surfaced as a pill, because
// "live" versus "ended" is the entire question a person clicking one has.

import { esc, titleOf, summaryOf, imageOf } from "../shared/format.js";
import { register, shell, bodyHtml, tagOf, tagsOf, clipIf, fmtTs } from "./base.js";

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

register([30311], liveCard);
register([31922, 31923], calendarEventCard);
register([31924], calendarCard);
