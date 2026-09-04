// The time family: live streams and calendar events. A 30311's status tag
// is surfaced as a pill beside the badge: "live" versus "ended" is the whole
// question a person clicking one has.

import { esc, titleOf, summaryOf, imageOf } from "../shared/format.js";
import { shortAddr } from "../shared/nip19.js";
import { register, registerRow, shell, bodyHtml, addrHref, extLink, tagOf, tagsOf, clipIf, fmtTs, plural } from "./base.js";

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

/** 31925 — an RSVP: `status` is the answer (accepted/declined/tentative), the `a` tag what is being answered. */
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
