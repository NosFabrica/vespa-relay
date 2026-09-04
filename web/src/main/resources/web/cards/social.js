// The reactive kinds: reactions, reposts, zaps, comments, approvals, reports,
// labels and deletions. Their own content is a fragment ("+", "", a bolt11
// invoice) and the meaning is in what they point at, so every card here leads
// with the relation and links the target.

import { esc, clip, summaryOf, titleOf, imageOf } from "../shared/format.js";
import { shortNote, shortAddr } from "../shared/nip19.js";
import {
  register, registerRow, shell, titleHtml, bodyHtml, replyLine, personLink, faceStrip, noteHref, addrHref,
  chipRow, tagOf, tagsOf, tagsWhere, jsonContent, fmtTs, extLink, plural, satsOf,
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

/**
 * The same target as a noun for a row, in targetLink's precedence. An `a` is
 * "an entry", not "a note": it is as often an article or a stream. No target,
 * no noun.
 */
const targetNoun = (ev) => {
  if (tagsOf(ev, "e").some((t) => /^[0-9a-f]{64}$/.test(t[1]))) return " a note";
  return tagOf(ev, "a") ? " an entry" : "";
};

const relationLine = (verb, target) =>
  `<div class="result-body">${verb}${target ? ` ${target}` : ""}</div>`;

/** NIP-25's three cases, in the words the card and the row both use. */
const reactionVerb = (c) => (c === "+" || c === "" ? "liked" : c === "-" ? "disliked" : "reacted to");
/** Whether the reaction is a glyph rather than a bare vote. */
const isGlyph = (c) => !!c && c !== "+" && c !== "-";

/**
 * 7 / 17 — a reaction. "+" is a like, "-" a dislike, anything else is the
 * reaction itself: a literal emoji, or a `:shortcode:` whose image rides in
 * an `emoji` tag. All three are shown as what they are.
 */
function reactionCard(ev, opts) {
  const c = (ev.content || "").trim();
  const shortcode = /^:([^:]+):$/.exec(c);
  const custom = shortcode && tagsOf(ev, "emoji").find((t) => t[1] === shortcode[1]);
  const glyph = custom
    ? `<img class="react-emoji" src="${esc(custom[2])}" alt=":${esc(shortcode[1])}:" title=":${esc(shortcode[1])}:" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()" />`
    : `<span class="react-glyph">${esc(clip(c || "+", 16))}</span>`;
  const verb = reactionVerb(c);
  const target = targetLink(ev);
  const inner = `<div class="result-body">${isGlyph(c) ? `${glyph} ` : ""}${verb}${target ? ` ${target}` : ""}</div>` +
    (ev.kind === 17 && tagOf(ev, "r") ? `<div class="result-body">${extLink(tagOf(ev, "r"))}</div>` : "");
  return shell(ev, opts, inner);
}

/** The text of the event carried inside this one's content: a NIP-18 repost, a NIP-72 approved post. */
function quotedText(ev) {
  const inner = jsonContent(ev);
  return inner && typeof inner.content === "string" ? inner.content : "";
}

/** 6 / 16 — a repost: the reposted text when the content carries it, the link otherwise. */
function repostCard(ev, opts) {
  const quoted = quotedText(ev);
  const target = targetLink(ev);
  const inner = relationLine("reposted", target) +
    (quoted ? `<blockquote class="quote">${esc(opts && opts.full ? quoted.trim() : clip(quoted, 300))}</blockquote>` : "");
  return shell(ev, opts, inner);
}

/** The zap request a receipt carries, stringified, in its `description` tag. */
function zapRequest(ev) {
  try { return JSON.parse(tagOf(ev, "description") || "{}") || {}; } catch (e) { return {}; }
}
/** What the receipt is worth: the outer `amount` tag, else the request's own. */
const zapSats = (ev, req) =>
  satsOf(tagOf(ev, "amount") || ((req.tags || []).find((t) => Array.isArray(t) && t[0] === "amount") || [])[1]);

/**
 * 9735 — a zap receipt. The amount, the zapper and the comment are in the
 * nested zap request, not in this event's own fields.
 */
function zapCard(ev, opts) {
  const req = zapRequest(ev);
  const sats = zapSats(ev, req);
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
  const sats = satsOf(tagOf(ev, "amount"));
  const to = tagOf(ev, "p");
  const inner =
    `<div class="result-body">asks to zap${to && /^[0-9a-f]{64}$/.test(to) ? ` ${personLink(to)}` : ""}</div>` +
    (sats ? `<div class="price-line">${esc(sats)} sats</div>` : "") +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner);
}

/**
 * 1111 — a NIP-22 comment. Uppercase tags name the root, lowercase the
 * parent. The parent leads as a person via replyLine; the root is a row only
 * when it differs from the parent.
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
    // The parent's id is a row only when the reply line could not name it.
    ["replying to", line ? null : parent],
    ["under", root && root !== parent ? root : null],
  ]);
}

/** A poll's choices: `["option", <id>, <label>]`, so the label is element 2. */
const pollOptions = (ev) => tagsOf(ev, "option").map((t) => t[2]).filter(Boolean);

/** 1068 — a poll: the question is the content, the choices are `option` tags. */
function pollCard(ev, opts) {
  const options = pollOptions(ev);
  const ends = tagOf(ev, "endsAt");
  const inner =
    bodyHtml(opts, ev.content, 400) +
    (options.length ? `<ul class="ref-list">${(opts && opts.full ? options : options.slice(0, 6)).map((o) => `<li>${esc(clip(o, 120))}</li>`).join("")}</ul>` : "");
  return shell(ev, opts, inner, [
    ["choices", options.length ? String(options.length) : null],
    ["closes", ends ? esc(fmtTs(ends)) : null],
  ]);
}

/** 1018 — a poll response: which choices, on which poll. */
function pollResponseCard(ev, opts) {
  const picks = tagsOf(ev, "response").map((t) => t[1]).filter(Boolean);
  const inner = relationLine("voted on", targetLink(ev)) +
    (picks.length ? chipRow(picks, opts) : "");
  return shell(ev, opts, inner);
}

/** The p/e tag that names the reported thing, and therefore carries the category. */
const flaggedTag = (ev) => tagsWhere(ev, (name, t) => (name === "p" || name === "e") && t[2])[0];
/** "spam", "nudity", … — from a `report` tag when no p/e carried one. */
const reportCategory = (ev) => (flaggedTag(ev) || [])[2] || tagOf(ev, "report");

/** 1984 — a report. The category is the third element of the p/e tag naming the reported thing. */
function reportCard(ev, opts) {
  const flagged = flaggedTag(ev);
  const category = reportCategory(ev);
  const subject = flagged && flagged[0] === "p" && /^[0-9a-f]{64}$/.test(flagged[1])
    ? personLink(flagged[1])
    : targetLink(ev);
  const inner =
    `<div class="result-body">reports${subject ? ` ${subject}` : ""}${category ? ` as <b>${esc(category)}</b>` : ""}</div>` +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner);
}

/** The labels a 1985 puts on something: `l` values, the `L` being their namespace. */
const labelsOf = (ev) => tagsOf(ev, "l").map((t) => t[1]).filter(Boolean);

/** 1985 — a NIP-32 label: the namespace, the labels, and what they were put on. */
function labelCard(ev, opts) {
  const ns = tagsOf(ev, "L").map((t) => t[1]).filter(Boolean);
  const labels = labelsOf(ev);
  const inner =
    relationLine("labels", targetLink(ev)) +
    chipRow(labels, opts) +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner, [["namespace", ns.length ? esc(ns.join(", ")) : null]]);
}

/** How many events a deletion request names, by id and by address alike. */
const deletionCount = (ev) => tagsOf(ev, "e").length + tagsOf(ev, "a").length;

/**
 * 5 — a deletion request. The card says "asks to delete": whether the events
 * are gone is this relay's business, not the event's claim.
 */
function deletionCard(ev, opts) {
  const kinds = [...new Set(tagsOf(ev, "k").map((t) => t[1]).filter(Boolean))];
  const inner =
    `<div class="result-body">asks to delete ${plural(deletionCount(ev), "event")}${kinds.length ? ` of kind ${esc(kinds.join(", "))}` : ""}</div>` +
    bodyHtml(opts, ev.content, 300, true);
  return shell(ev, opts, inner);
}

/** Who a badge was awarded to: the `p` tags that are keys. */
const winnersOf = (ev) => tagsOf(ev, "p").map((t) => t[1]).filter((pk) => /^[0-9a-f]{64}$/.test(pk));

/** 8 — a badge award: which badge, to whom. */
function badgeAwardCard(ev, opts) {
  const badge = tagOf(ev, "a");
  const href = badge ? addrHref(badge) : null;
  const winners = winnersOf(ev);
  const inner =
    `<div class="result-body">awards ${badge ? (href ? `<a href="${href}">${esc(shortAddr(badge))}</a>` : esc(shortAddr(badge))) : "a badge"} to ${plural(winners.length, "recipient")}</div>` +
    faceStrip(winners, opts && opts.full ? 24 : 12);
  return shell(ev, opts, inner);
}

/** 4550 — a moderator approving a post into a NIP-72 community. */
function approvalCard(ev, opts) {
  const community = tagOf(ev, "a");
  const href = community ? addrHref(community) : null;
  const quoted = quotedText(ev);
  const inner =
    `<div class="result-body">approved a post${community ? ` in ${href ? `<a href="${href}">${esc(shortAddr(community))}</a>` : esc(shortAddr(community))}` : ""}</div>` +
    (quoted ? `<blockquote class="quote">${esc(opts && opts.full ? quoted.trim() : clip(quoted, 300))}</blockquote>` : "");
  return shell(ev, opts, inner);
}

/** 34550 — a NIP-72 community. The `d` is its name and there is usually no `title`, so titleOf's `d` fallback is the title. */
function communityCard(ev, opts) {
  const img = imageOf(ev);
  // Every `p` on a 34550 is a moderator; the role's position in the tag varies by client.
  const mods = tagsOf(ev, "p").map((t) => t[1]).filter((pk) => /^[0-9a-f]{64}$/.test(pk));
  const full = opts && opts.full;
  const inner =
    (full && img ? `<div class="embed"><img src="${esc(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.parentElement.remove()" /></div>` : "") +
    titleHtml(opts, titleOf(ev), 140) +
    bodyHtml(opts, summaryOf(ev) || ev.content, 400) +
    (mods.length ? `<div class="result-body">${mods.length} moderator${mods.length === 1 ? "" : "s"}</div>` + faceStrip(mods, full ? 24 : 12) : "");
  return shell(ev, opts, inner);
}

/** 30315 — a NIP-38 status. The `d` says which status ("general", "music"); a music status is often only an `r` link. */
function statusCard(ev, opts) {
  const kindOfStatus = tagOf(ev, "d");
  const link = tagOf(ev, "r");
  const expiry = tagOf(ev, "expiration");
  const inner = bodyHtml(opts, ev.content, 300) || `<div class="result-body muted">cleared</div>`;
  return shell(ev, opts, inner, [
    ["status", kindOfStatus ? esc(kindOfStatus) : null],
    ["link", extLink(link)],
    ["expires", expiry ? esc(fmtTs(expiry)) : null],
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

// The rows lead with the relation, as the cards do, minus the link.
registerRow([7, 17], (ev) => {
  const c = (ev.content || "").trim();
  // A `:shortcode:` image cannot ride in a line of text, so the row shows the code itself.
  return { name: isGlyph(c) ? `reacted ${clip(c, 24)}` : `${reactionVerb(c)}${targetNoun(ev)}` };
});
registerRow([6, 16], (ev) => ({ name: quotedText(ev) || `reposted${targetNoun(ev)}` }));
registerRow([9735], (ev) => {
  const req = zapRequest(ev);
  const sats = zapSats(ev, req);
  return { name: sats ? `zapped ${sats} sats` : "zapped", sub: typeof req.content === "string" ? req.content : "" };
});
registerRow([9734], (ev) => {
  const sats = satsOf(tagOf(ev, "amount"));
  return { name: sats ? `asks to zap ${sats} sats` : "asks to zap", sub: ev.content };
});
registerRow([1111], (ev) => ({ name: ev.content }));
registerRow([1068], (ev) => ({ name: ev.content, sub: plural(pollOptions(ev).length, "choice") }));
registerRow([1018], () => ({ name: "voted on a poll" }));
registerRow([1984], (ev) => {
  const category = reportCategory(ev);
  return { name: category ? `reports as ${category}` : "reports an event", sub: ev.content };
});
registerRow([1985], (ev) => {
  const labels = labelsOf(ev);
  return { name: labels.length ? `labels ${labels.join(", ")}` : "labels an event", sub: ev.content };
});
registerRow([5], (ev) => ({ name: `asks to delete ${plural(deletionCount(ev), "event")}`, sub: ev.content }));
registerRow([8], (ev) => ({ name: `awards a badge to ${plural(winnersOf(ev).length, "recipient")}` }));
registerRow([4550], (ev) => ({ name: "approved a post", sub: quotedText(ev) }));
registerRow([34550], (ev) => ({ name: titleOf(ev), sub: summaryOf(ev) || ev.content }));
// A status with no text is a status cleared, which is a fact worth a row.
registerRow([30315], (ev) => ({ name: ev.content || "cleared" }));
