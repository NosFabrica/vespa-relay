// The reactive kinds: reactions, reposts, zaps and the other payments, comments, approvals,
// reports, labels and deletions. Their content is a fragment, so every card leads with the
// relation and links the target.

import { esc, clip, summaryOf, titleOf, imageOf } from "../shared/format.js";
import { shortNote, shortAddr } from "../shared/nip19.js";
import {
  register, registerRow, shell, titleHtml, bodyHtml, replyLine, personLink, faceStrip, noteHref, addrHref,
  chipRow, markHref, tagOf, tagsOf, tagsWhere, jsonContent, oneLine, fmtTs, extLink, plural, satsOf,
} from "./base.js";

const HEX64 = /^[0-9a-f]{64}$/;

/** The event a reactive kind points at: `e` by id, `a` by address, in that order. */
function targetLink(ev) {
  const e = tagsOf(ev, "e").map((t) => t[1]).filter((v) => /^[0-9a-f]{64}$/.test(v)).pop();
  if (e) return `<a class="mono" href="${noteHref(e)}">${esc(shortNote(e))}</a>`;
  const a = tagOf(ev, "a");
  if (!a) return null;
  const href = addrHref(a);
  return href ? `<a href="${href}">${esc(shortAddr(a))}</a>` : esc(shortAddr(a));
}

/** The target as a noun for a row, in targetLink's precedence. An `a` is "an entry", never "a note". */
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

/** 7 / 17 — a reaction: a like, a dislike, or a glyph whose image may ride in an `emoji` tag. */
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

/** The text of the event carried inside this one's content. */
function quotedText(ev) {
  const inner = jsonContent(ev);
  return inner && typeof inner.content === "string" ? inner.content : "";
}

/** 6 / 16 — a repost. */
function repostCard(ev, opts) {
  const quoted = quotedText(ev);
  const target = targetLink(ev);
  const inner = relationLine("reposted", target) +
    (quoted ? `<blockquote class="quote">${esc(opts && opts.full ? quoted.trim() : clip(quoted, 300))}</blockquote>` : "");
  return shell(ev, opts, inner);
}

/** The request (9734) or the intent (9737) a receipt carries, stringified, in its `description` tag. */
function zapRequest(ev) {
  try { return JSON.parse(tagOf(ev, "description") || "{}") || {}; } catch (e) { return {}; }
}
/** What the receipt is worth: the outer `amount` tag, else the request's own. Millisats, both. */
const zapSats = (ev, req) =>
  satsOf(tagOf(ev, "amount") || ((req.tags || []).find((t) => Array.isArray(t) && t[0] === "amount") || [])[1]);

/**
 * Who paid: NIP-B1 names the payer in `P` on a publicly-attributed zap, and NIP-57 leaves it to
 * be read off the request it carries. A zap nobody signed for names nobody.
 */
const payerOf = (ev, req) => {
  const declared = tagOf(ev, "P");
  if (HEX64.test(declared || "")) return declared;
  return HEX64.test(req.pubkey || "") ? req.pubkey : null;
};

/** 9735 / 9736 — a settled zap. The zapper and the comment are in the nested request, not this event. */
function zapCard(ev, opts) {
  const req = zapRequest(ev);
  const sats = zapSats(ev, req);
  const to = tagOf(ev, "p");
  const from = payerOf(ev, req);
  const target = targetLink(ev);
  const comment = typeof req.content === "string" ? req.content : "";
  const inner =
    `<div class="result-body">${from ? `${personLink(from)} ` : ""}zapped${to && HEX64.test(to) ? ` ${personLink(to)}` : ""}${target ? ` on ${target}` : ""}</div>` +
    (sats ? `<div class="price-line">${esc(sats)} sats</div>` : "") +
    bodyHtml(opts, comment, 300);
  // A BOLT12 zap is paid against an offer rather than an invoice, so the offer is what it cites.
  return shell(ev, opts, inner, [["offer", monoValue(tagOf(ev, "offer"))]]);
}

/** 9734 / 9737 — the ask before the payment: NIP-57 calls it a request, NIP-B1 a signed intent. */
function zapRequestCard(ev, opts) {
  const sats = satsOf(tagOf(ev, "amount"));
  const to = tagOf(ev, "p");
  const inner =
    `<div class="result-body">${ev.kind === 9737 ? "intends to zap" : "asks to zap"}${to && HEX64.test(to) ? ` ${personLink(to)}` : ""}</div>` +
    (sats ? `<div class="price-line">${esc(sats)} sats</div>` : "") +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner, [["offer", monoValue(tagOf(ev, "offer"))]]);
}

/** A long opaque value — an offer, a txid, a hash — as something a reader can see is there. */
const monoValue = (v) => (v ? `<span class="mono">${esc(clip(String(v), 40))}</span>` : null);

/**
 * A nutzap's value: the `amount` of every cashu proof it carries, since the kind has no amount
 * tag at all. A proof this page cannot read counts nothing rather than guessing.
 */
function nutzapSats(ev) {
  let total = 0;
  for (const t of tagsOf(ev, "proof")) {
    try {
      const n = Number(JSON.parse(t[1]).amount);
      if (Number.isFinite(n) && n > 0) total += n;
    } catch (e) { /* not a proof this page can read */ }
  }
  return total > 0 ? total.toLocaleString() : null;
}

/**
 * What a payment is worth, in the unit ITS OWN kind counts in — the one thing these kinds
 * disagree about, and the way to be wrong by a factor of a thousand:
 *
 * - 9734/9735/9736/9737 carry millisats in `amount` (NIP-57 and NIP-B1 alike), so `satsOf`.
 * - 8333 carries SATS in the same tag name: NIP-BC counts the on-chain unit.
 * - 9321 carries no amount at all — it is the sum of its cashu proofs.
 */
function paidSats(ev) {
  if (ev.kind === 9321) return nutzapSats(ev);
  const n = Number(tagOf(ev, "amount"));
  return Number.isFinite(n) && n > 0 ? Math.round(n).toLocaleString() : null;
}

/** What the sum is denominated in: a nutzap says so, and everything else here is sats. */
const unitOf = (ev) => (ev.kind === 9321 ? oneLine(tagOf(ev, "unit")) || "sats" : "sats");

/** 8333 / 9321 — a payment that settled somewhere other than Lightning: on-chain, or in ecash. */
function paymentCard(ev, opts) {
  const sats = paidSats(ev);
  const to = tagOf(ev, "p");
  const target = targetLink(ev);
  const verb = ev.kind === 8333 ? "paid onchain" : "nutzapped";
  const inner =
    `<div class="result-body">${verb}${to && HEX64.test(to) ? ` ${personLink(to)}` : ""}${target ? ` on ${target}` : ""}</div>` +
    (sats ? `<div class="price-line">${esc(sats)} ${esc(clip(unitOf(ev), 16))}</div>` : "") +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner, ev.kind === 8333
    // The sender's own claim until someone checks the chain, which is why the txid is on the card.
    ? [["txid", monoValue(tagOf(ev, "i"))], ["block", tagOf(ev, "block") ? esc(clip(tagOf(ev, "block"), 40)) : null]]
    : [["mint", extLink(tagOf(ev, "u"))], ["proofs", tagsOf(ev, "proof").length ? String(tagsOf(ev, "proof").length) : null]]);
}

/** 1111 — a NIP-22 comment. The parent leads as a person; the root is a row only when it differs. */
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

/** A poll's choices; the label is element 2 of `["option", <id>, <label>]`. */
const pollOptions = (ev) => tagsOf(ev, "option").map((t) => t[2]).filter(Boolean);

/** 1068 — a poll. */
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

/** 1018 — a poll response. */
function pollResponseCard(ev, opts) {
  const picks = tagsOf(ev, "response").map((t) => t[1]).filter(Boolean);
  const inner = relationLine("voted on", targetLink(ev)) +
    (picks.length ? chipRow(picks, opts) : "");
  return shell(ev, opts, inner);
}

/** The p/e tag that names the reported thing; its third element is the category. */
const flaggedTag = (ev) => tagsWhere(ev, (name, t) => (name === "p" || name === "e") && t[2])[0];
/** "spam", "nudity"; from a `report` tag when no p/e carried one. */
const reportCategory = (ev) => (flaggedTag(ev) || [])[2] || tagOf(ev, "report");

/** 1984 — a report. */
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

/** The `l` values; `L` is their namespace. */
const labelsOf = (ev) => tagsOf(ev, "l").map((t) => t[1]).filter(Boolean);

/** 1985 — a NIP-32 label. */
function labelCard(ev, opts) {
  const ns = tagsOf(ev, "L").map((t) => t[1]).filter(Boolean);
  const labels = labelsOf(ev);
  const inner =
    relationLine("labels", targetLink(ev)) +
    chipRow(labels, opts, markHref) +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner, [["namespace", ns.length ? esc(ns.join(", ")) : null]]);
}

/** How many events a deletion request names, by id or by address. */
const deletionCount = (ev) => tagsOf(ev, "e").length + tagsOf(ev, "a").length;

/** 5 — a deletion request. "Asks to delete": whether the events are gone is the relay's business. */
function deletionCard(ev, opts) {
  const kinds = [...new Set(tagsOf(ev, "k").map((t) => t[1]).filter(Boolean))];
  const inner =
    `<div class="result-body">asks to delete ${plural(deletionCount(ev), "event")}${kinds.length ? ` of kind ${esc(kinds.join(", "))}` : ""}</div>` +
    bodyHtml(opts, ev.content, 300, true);
  return shell(ev, opts, inner);
}

/** The `p` tags that are keys. */
const winnersOf = (ev) => tagsOf(ev, "p").map((t) => t[1]).filter((pk) => /^[0-9a-f]{64}$/.test(pk));

/** 8 — a badge award. */
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

/** 34550 — a NIP-72 community; its `d` is the name, so titleOf's `d` fallback is the title. */
function communityCard(ev, opts) {
  const img = imageOf(ev);
  // Every `p` on a 34550 is a moderator.
  const mods = tagsOf(ev, "p").map((t) => t[1]).filter((pk) => /^[0-9a-f]{64}$/.test(pk));
  const full = opts && opts.full;
  const inner =
    (full && img ? `<div class="embed"><img src="${esc(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.parentElement.remove()" /></div>` : "") +
    titleHtml(opts, titleOf(ev), 140) +
    bodyHtml(opts, summaryOf(ev) || ev.content, 400) +
    (mods.length ? `<div class="result-body">${mods.length} moderator${mods.length === 1 ? "" : "s"}</div>` + faceStrip(mods, full ? 24 : 12) : "");
  return shell(ev, opts, inner);
}

/** 30315 — a NIP-38 status; a music status is often only an `r` link. */
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
register([9735, 9736], zapCard);
register([9734, 9737], zapRequestCard);
register([8333, 9321], paymentCard);
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
  // A `:shortcode:` image cannot ride in a line of text.
  return { name: isGlyph(c) ? `reacted ${clip(c, 24)}` : `${reactionVerb(c)}${targetNoun(ev)}` };
});
registerRow([6, 16], (ev) => ({ name: quotedText(ev) || `reposted${targetNoun(ev)}` }));
registerRow([9735, 9736], (ev) => {
  const req = zapRequest(ev);
  const sats = zapSats(ev, req);
  return { name: sats ? `zapped ${sats} sats` : "zapped", sub: typeof req.content === "string" ? req.content : "" };
});
registerRow([9734, 9737], (ev) => {
  const sats = satsOf(tagOf(ev, "amount"));
  const verb = ev.kind === 9737 ? "intends to zap" : "asks to zap";
  return { name: sats ? `${verb} ${sats} sats` : verb, sub: ev.content };
});
registerRow([8333, 9321], (ev) => {
  const sats = paidSats(ev);
  const verb = ev.kind === 8333 ? "paid onchain" : "nutzapped";
  return { name: sats ? `${verb} ${sats} ${unitOf(ev)}` : verb, sub: ev.content };
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
// A status with no text is a status cleared.
registerRow([30315], (ev) => ({ name: ev.content || "cleared" }));
