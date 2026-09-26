// The time family: live streams and calendar events, plus the two things a stream throws off —
// a raid that sends its audience somewhere else, and a clip cut out of it. A 30311's status is a
// pill beside the badge.

import { esc, clip, titleOf, summaryOf, imageOf } from "../shared/format.js";
import { shortAddr } from "../shared/nip19.js";
import {
  register, registerRow, registerNamedPeople, shell, titleHtml, bodyHtml, personLink, addrHref, extLink,
  videoEmbed, coverBanner, coverThumb, chipRow, topicsOf, hashtagHref, hostOf, oneLine,
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

/** The title with the event's `status` as a pill beside it. */
function statusTitle(ev, opts) {
  const status = (tagOf(ev, "status") || "").toLowerCase();
  const title = titleOf(ev);
  return title
    ? `<h2 class="result-title">${esc(clipIf(opts, title, 140))}${status ? ` <span class="status-pill ${esc(status)}">${esc(status)}</span>` : ""}</h2>`
    : "";
}

/** 30311 — a live event: status, the stream, who is watching. */
function liveCard(ev, opts) {
  const full = opts && opts.full;
  const inner =
    (full ? coverBanner(imageOf(ev)) : "") +
    statusTitle(ev, opts) +
    bodyHtml(opts, summaryOf(ev) || ev.content, 300);
  const participants = tagOf(ev, "current_participants");
  return shell(ev, opts, inner, [
    ["stream", extLink(tagOf(ev, "streaming"))],
    ["recording", extLink(tagOf(ev, "recording"))],
    ["starts", tagOf(ev, "starts") ? esc(fmtTs(tagOf(ev, "starts"))) : null],
    ["watching", participants ? esc(participants) : null],
  ]);
}

/**
 * A 30312 `p`'s role. NIP-53 puts it after the relay hint (`["p", pk, relay, role]`); some
 * clients write it in the hint's slot (`["p", pk, "owner"]`), which no relay url looks like.
 */
const roleOf = (t) => oneLine(t[3]) || (t[2] && !/^wss?:\/\//i.test(t[2]) ? oneLine(t[2]) : "");

/** The people a room lists, each once with the first role given. The byline is already the author. */
function roomPeople(ev) {
  const seen = new Map();
  for (const t of tagsOf(ev, "p")) {
    if (HEX64.test(t[1] || "") && t[1] !== ev.pubkey && !seen.has(t[1])) seen.set(t[1], roleOf(t));
  }
  return [...seen].map(([pk, role]) => ({ pk, role }));
}
const ROOM_PEOPLE = { preview: 3, full: 24 };
const roomPeopleShown = (ev, opts) => roomPeople(ev).slice(0, opts && opts.full ? ROOM_PEOPLE.full : ROOM_PEOPLE.preview);

/**
 * 30312 — a room: a standing place to meet rather than one broadcast. Its name is the `room`
 * tag (titleOf reads it), and what a reader wants from it is the `service` url that joins it.
 */
function roomCard(ev, opts) {
  const full = opts && opts.full;
  const img = imageOf(ev);
  const all = roomPeople(ev);
  const shown = roomPeopleShown(ev, opts);
  const more = all.length - shown.length;
  const person = (p) => personLink(p.pk) + (p.role ? ` <span class="muted-note">${esc(clip(p.role, 30))}</span>` : "");
  const relays = tagsOf(ev, "relays").flatMap((t) => t.slice(1)).filter((v) => typeof v === "string" && v.trim());
  const inner =
    (full ? coverBanner(img) : "") +
    `<div class="result-main"><div class="text">` +
    statusTitle(ev, opts) +
    bodyHtml(opts, summaryOf(ev) || ev.content, 300) +
    (shown.length
      ? `<div class="meta-line">with ${shown.map(person).join(", ")}${more > 0 ? ` and ${more} more` : ""}</div>`
      : "") +
    chipRow(topicsOf(ev), opts, hashtagHref) +
    `</div>${full ? "" : coverThumb(img)}</div>`;
  return shell(ev, opts, inner, [
    ["join", extLink(tagOf(ev, "service"))],
    ["endpoint", full ? extLink(tagOf(ev, "endpoint")) : null],
    ["relays", relays.length ? relays.map((r) => `<span class="mono">${esc(hostOf(r.trim()))}</span>`).join(" · ") : null],
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

// A 30313 conference event shares a live event's vocabulary; a 30312 room does not.
register([30311, 30313], liveCard);
register([30312], roomCard);
registerNamedPeople([30312], (ev, opts) => roomPeopleShown(ev, opts).map((p) => p.pk));
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
