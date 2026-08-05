// The reactive kinds: the events that are ABOUT other events. Reactions,
// reposts, zaps, comments, approvals, reports, labels and deletions all share
// one problem the generic floor cannot solve — their own content is a
// fragment ("+", "", a bolt11 invoice) and the interesting half is whatever
// they point at. So every card here leads with the RELATION and links the
// target, rather than printing a payload that means nothing on its own.
//
// They earn cards because a search over the whole corpus returns them by the
// thousand: a "kind 7" badge over an empty body was the most common shape of
// nothing this page could produce.

import { esc, clip, summaryOf, titleOf, imageOf } from "../shared/format.js";
import { shortNote, shortAddr } from "../shared/nip19.js";
import {
  register, shell, titleHtml, bodyHtml, replyLine, personLink, faceStrip, noteHref, addrHref,
  chipRow, tagOf, tagsOf, tagsWhere, jsonContent, fmtTs, extLink,
} from "./base.js";

/** The event a reactive kind points at: `e` by id, `a` by address, in that order. */
function targetLink(ev) {
  const e = tagsOf(ev, "e").map((t) => t[1]).filter((v) => /^[0-9a-f]{64}$/.test(v)).pop();
  if (e) return `<a class="mono" href="${noteHref(e)}">${esc(shortNote(e))}</a>`;
  const a = tagOf(ev, "a");
  if (!a) return null;
  const href = addrHref(a);
  return href ? `<a href="${href}">${esc(shortAddr(a))}</a>` : esc(shortAddr(a));
}

const relationLine = (verb, target) =>
  `<div class="result-body">${verb}${target ? ` ${target}` : ""}</div>`;

/**
 * 7 / 17 — a reaction. NIP-25 says "+" is a like, "-" a dislike, and anything
 * else is the reaction itself: a literal emoji, or a `:shortcode:` whose image
 * rides in an `emoji` tag. All three are shown as what they are — mapping the
 * custom ones to a generic "reacted" would throw away the only content the
 * event has.
 */
function reactionCard(ev, opts) {
  const c = (ev.content || "").trim();
  const shortcode = /^:([^:]+):$/.exec(c);
  const custom = shortcode && tagsOf(ev, "emoji").find((t) => t[1] === shortcode[1]);
  const glyph = custom
    ? `<img class="react-emoji" src="${esc(custom[2])}" alt=":${esc(shortcode[1])}:" title=":${esc(shortcode[1])}:" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()" />`
    : `<span class="react-glyph">${esc(clip(c || "+", 16))}</span>`;
  const verb = c === "+" || c === "" ? "liked" : c === "-" ? "disliked" : "reacted to";
  const target = targetLink(ev);
  const inner = `<div class="result-body">${c === "+" || c === "-" || c === "" ? "" : glyph + " "}${verb}${target ? ` ${target}` : ""}</div>` +
    (ev.kind === 17 && tagOf(ev, "r") ? `<div class="result-body">${extLink(tagOf(ev, "r"))}</div>` : "");
  return shell(ev, opts, inner);
}

/**
 * 6 / 16 — a repost. NIP-18 puts the whole reposted event in `content` as
 * JSON, so the card can show what was actually shared instead of the word
 * "repost" over a link — and falls back to the link when the content is empty,
 * which plenty of clients leave it.
 */
function repostCard(ev, opts) {
  const inner_ = jsonContent(ev);
  const quoted = inner_ && typeof inner_.content === "string" ? inner_.content : "";
  const target = targetLink(ev);
  const inner = relationLine("reposted", target) +
    (quoted ? `<blockquote class="quote">${esc(opts && opts.full ? quoted.trim() : clip(quoted, 300))}</blockquote>` : "");
  return shell(ev, opts, inner);
}

/**
 * 9735 — a zap receipt. The amount is not in this event: it is in the zap
 * REQUEST, which rides stringified in the `description` tag. Reading it there
 * is the difference between "1,000 sats" and a bolt11 invoice nobody can read,
 * and the comment the zapper typed is in that same nested event.
 */
function zapCard(ev, opts) {
  let req = {};
  try { req = JSON.parse(tagOf(ev, "description") || "{}") || {}; } catch (e) { req = {}; }
  const reqTag = (name) => ((req.tags || []).find((t) => t[0] === name) || [])[1];
  const msats = Number(tagOf(ev, "amount") || reqTag("amount"));
  const sats = Number.isFinite(msats) && msats > 0 ? Math.round(msats / 1000).toLocaleString() : null;
  const to = tagOf(ev, "p");
  const from = req.pubkey && /^[0-9a-f]{64}$/.test(req.pubkey) ? req.pubkey : null;
  const target = targetLink(ev);
  const comment = typeof req.content === "string" ? req.content : "";
  const inner =
    `<div class="result-body">${from ? `${personLink(from)} ` : ""}zapped${to && /^[0-9a-f]{64}$/.test(to) ? ` ${personLink(to)}` : ""}${target ? ` on ${target}` : ""}</div>` +
    (sats ? `<div class="price-line">${esc(sats)} sats</div>` : "") +
    bodyHtml(opts, comment, 300);
  return shell(ev, opts, inner);
}

/** 9734 — the zap request itself: the comment, and what it asks to pay for. */
function zapRequestCard(ev, opts) {
  const msats = Number(tagOf(ev, "amount"));
  const sats = Number.isFinite(msats) && msats > 0 ? Math.round(msats / 1000).toLocaleString() : null;
  const to = tagOf(ev, "p");
  const inner =
    `<div class="result-body">asks to zap${to && /^[0-9a-f]{64}$/.test(to) ? ` ${personLink(to)}` : ""}</div>` +
    (sats ? `<div class="price-line">${esc(sats)} sats</div>` : "") +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner);
}

/**
 * 1111 — a NIP-22 comment. It is a note whose whole point is what it replies
 * to, and the uppercase tags name the ROOT while the lowercase ones name the
 * immediate parent.
 *
 * The parent leads the card as a PERSON now — replyLine reads the same `e`/`a`
 * pair this used to print as two bech32 ids side by side, and a comment whose
 * card says "replying to note1qqq… under note1qqq…" told a reader nothing
 * twice. The root stays a row, and only when it differs from the parent: on a
 * direct comment the two are the same event, which is where the duplicate came
 * from.
 */
function commentCard(ev, opts) {
  const ref = (id, addr) => {
    if (id && /^[0-9a-f]{64}$/.test(id)) return `<a class="mono" href="${noteHref(id)}">${esc(shortNote(id))}</a>`;
    if (!addr) return null;
    const href = addrHref(addr);
    return href ? `<a href="${href}">${esc(shortAddr(addr))}</a>` : esc(shortAddr(addr));
  };
  const root = ref(tagOf(ev, "E"), tagOf(ev, "A")) || (tagOf(ev, "I") ? esc(tagOf(ev, "I")) : null);
  const parent = ref(tagOf(ev, "e"), tagOf(ev, "a"));
  const line = replyLine(ev);
  const inner = line + bodyHtml(opts, ev.content, 500);
  return shell(ev, opts, inner, [
    // The parent's own id, kept as a row only when the line above could not
    // name it — otherwise the card says the same thing in two registers.
    ["replying to", line ? null : parent],
    ["under", root && root !== parent ? root : null],
  ]);
}

/** 1068 — a poll: the question is the content, the choices are `option` tags. */
function pollCard(ev, opts) {
  const options = tagsOf(ev, "option").map((t) => t[2]).filter(Boolean);
  const ends = tagOf(ev, "endsAt");
  const inner =
    bodyHtml(opts, ev.content, 400) +
    (options.length ? `<ul class="ref-list">${(opts && opts.full ? options : options.slice(0, 6)).map((o) => `<li>${esc(clip(o, 120))}</li>`).join("")}</ul>` : "");
  return shell(ev, opts, inner, [
    ["choices", options.length ? String(options.length) : null],
    ["closes", ends ? fmtTs(ends) : null],
  ]);
}

/** 1018 — a poll response: which choices, on which poll. */
function pollResponseCard(ev, opts) {
  const picks = tagsOf(ev, "response").map((t) => t[1]).filter(Boolean);
  const inner = relationLine("voted on", targetLink(ev)) +
    (picks.length ? chipRow(picks, opts) : "");
  return shell(ev, opts, inner);
}

/**
 * 1984 — a report. The category is the THIRD element of the p/e tag that names
 * the reported thing ("spam", "nudity", …), which is the one field a moderator
 * reading a list of these actually filters on.
 */
function reportCard(ev, opts) {
  const flagged = tagsWhere(ev, (name, t) => (name === "p" || name === "e") && t[2])[0];
  const category = flagged ? flagged[2] : tagOf(ev, "report");
  const subject = flagged && flagged[0] === "p" && /^[0-9a-f]{64}$/.test(flagged[1])
    ? personLink(flagged[1])
    : targetLink(ev);
  const inner =
    `<div class="result-body">reports${subject ? ` ${subject}` : ""}${category ? ` as <b>${esc(category)}</b>` : ""}</div>` +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner);
}

/** 1985 — a NIP-32 label: the namespace, the labels, and what they were put on. */
function labelCard(ev, opts) {
  const ns = tagsOf(ev, "L").map((t) => t[1]).filter(Boolean);
  const labels = tagsOf(ev, "l").map((t) => t[1]).filter(Boolean);
  const inner =
    relationLine("labels", targetLink(ev)) +
    chipRow(labels, opts) +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner, [["namespace", ns.length ? esc(ns.join(", ")) : null]]);
}

/**
 * 5 — a deletion REQUEST, and the card says request on purpose. Whether the
 * events are gone is this relay's business, not the event's claim; what the
 * event carries is an ask and a reason.
 */
function deletionCard(ev, opts) {
  const n = tagsOf(ev, "e").length + tagsOf(ev, "a").length;
  const kinds = [...new Set(tagsOf(ev, "k").map((t) => t[1]).filter(Boolean))];
  const inner =
    `<div class="result-body">asks to delete ${n.toLocaleString()} event${n === 1 ? "" : "s"}${kinds.length ? ` of kind ${esc(kinds.join(", "))}` : ""}</div>` +
    bodyHtml(opts, ev.content, 300, true);
  return shell(ev, opts, inner);
}

/** 8 — a badge award: which badge, to whom. */
function badgeAwardCard(ev, opts) {
  const badge = tagOf(ev, "a");
  const href = badge ? addrHref(badge) : null;
  const winners = tagsOf(ev, "p").map((t) => t[1]).filter((pk) => /^[0-9a-f]{64}$/.test(pk));
  const inner =
    `<div class="result-body">awards ${badge ? (href ? `<a href="${href}">${esc(shortAddr(badge))}</a>` : esc(shortAddr(badge))) : "a badge"} to ${winners.length.toLocaleString()} recipient${winners.length === 1 ? "" : "s"}</div>` +
    faceStrip(winners, opts && opts.full ? 24 : 12);
  return shell(ev, opts, inner);
}

/** 4550 — a moderator approving a post into a NIP-72 community. */
function approvalCard(ev, opts) {
  const community = tagOf(ev, "a");
  const href = community ? addrHref(community) : null;
  const approved = jsonContent(ev);
  const quoted = approved && typeof approved.content === "string" ? approved.content : "";
  const inner =
    `<div class="result-body">approved a post${community ? ` in ${href ? `<a href="${href}">${esc(shortAddr(community))}</a>` : esc(shortAddr(community))}` : ""}</div>` +
    (quoted ? `<blockquote class="quote">${esc(opts && opts.full ? quoted.trim() : clip(quoted, 300))}</blockquote>` : "");
  return shell(ev, opts, inner);
}

/**
 * 34550 — a NIP-72 community definition. The `d` is the community's name and
 * there is usually no `title` tag, so titleOf's `d` fallback is doing real
 * work here rather than leaking an opaque identifier.
 */
function communityCard(ev, opts) {
  const img = imageOf(ev);
  // Every `p` on a 34550 IS a moderator — the role rides in a later element
  // whose position clients disagree on, so the tag's presence is the fact and
  // its shape is not worth guessing at.
  const mods = tagsOf(ev, "p").map((t) => t[1]).filter((pk) => /^[0-9a-f]{64}$/.test(pk));
  const full = opts && opts.full;
  const inner =
    (full && img ? `<div class="embed"><img src="${esc(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.parentElement.remove()" /></div>` : "") +
    titleHtml(opts, titleOf(ev), 140) +
    bodyHtml(opts, summaryOf(ev) || ev.content, 400) +
    (mods.length ? `<div class="result-body">${mods.length} moderator${mods.length === 1 ? "" : "s"}</div>` + faceStrip(mods, full ? 24 : 12) : "");
  return shell(ev, opts, inner);
}

/**
 * 30315 — a NIP-38 status. The `d` is which status it is ("general",
 * "music"), and a music status routinely carries only an `r` link with the
 * track — so the link is shown rather than an empty body.
 */
function statusCard(ev, opts) {
  const kindOfStatus = tagOf(ev, "d");
  const link = tagOf(ev, "r");
  const expiry = tagOf(ev, "expiration");
  const inner = bodyHtml(opts, ev.content, 300) || `<div class="result-body muted">cleared</div>`;
  return shell(ev, opts, inner, [
    ["status", kindOfStatus ? esc(kindOfStatus) : null],
    ["link", extLink(link)],
    ["expires", expiry ? fmtTs(expiry) : null],
  ]);
}

register([7, 17], reactionCard);
register([6, 16], repostCard);
register([9735], zapCard);
register([9734], zapRequestCard);
register([1111], commentCard);
register([1068], pollCard);
register([1018], pollResponseCard);
register([1984], reportCard);
register([1985], labelCard);
register([5], deletionCard);
register([8], badgeAwardCard);
register([4550], approvalCard);
register([34550], communityCard);
register([30315], statusCard);
