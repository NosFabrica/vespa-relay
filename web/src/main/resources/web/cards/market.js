// The marketplace family: NIP-99 listings keep everything in tags, NIP-15 stalls and
// products keep a JSON content. Nothing is invented for a missing field. The two ends of a
// trade are here too — a NIP-69 peer-to-peer order, and the mints a NIP-87 reader recommends.

import { esc, clip, titleOf, summaryOf, imageOf } from "../shared/format.js";
import {
  register, registerRow, shell, bodyHtml, chipRow, relayRows, refRows, extLink, tagsOf, tagOf,
  jsonContent, clipIf, oneLine, plural, satsOf,
} from "./base.js";

/** "250 USD", "9 EUR / month". Every part goes through oneLine first: `{"price": {}}` is legal. */
const priceText = (amount, currency, period) => {
  const a = oneLine(amount);
  const per = oneLine(period);
  return a ? `${a} ${oneLine(currency)}${per ? ` / ${per}` : ""}`.trim() : "";
};

const priceLine = (amount, currency, period) => {
  const text = priceText(amount, currency, period);
  return text ? `<div class="price-line">${esc(text)}</div>` : "";
};

const imgEmbed = (url) =>
  url ? `<div class="embed"><img src="${esc(url)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.parentElement.remove()" /></div>` : "";

/** 30402 — a classified listing. */
function listingCard(ev, opts) {
  const price = tagsOf(ev, "price")[0] || [];
  const full = opts && opts.full;
  const inner =
    (full ? imgEmbed(imageOf(ev)) : "") +
    (titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 140))}</h2>` : "") +
    priceLine(price[1], price[2], price[3]) +
    bodyHtml(opts, summaryOf(ev) || ev.content, 400);
  return shell(ev, opts, inner, [
    ["location", tagOf(ev, "location") ? esc(tagOf(ev, "location")) : null],
  ]);
}

/** 30018 — a product: JSON content {name, description, price, currency, images}. */
function productCard(ev, opts) {
  const c = jsonContent(ev);
  const full = opts && opts.full;
  const inner =
    (full ? imgEmbed(Array.isArray(c.images) ? c.images[0] : null) : "") +
    (c.name ? `<h2 class="result-title">${esc(clipIf(opts, c.name, 140))}</h2>` : "") +
    priceLine(c.price, c.currency) +
    bodyHtml(opts, c.description || "", 400);
  return shell(ev, opts, inner);
}

/** 30017 — a stall: the shop the products hang off. */
function stallCard(ev, opts) {
  const c = jsonContent(ev);
  const inner =
    (c.name ? `<h2 class="result-title">${esc(clipIf(opts, c.name, 140))}</h2>` : "") +
    bodyHtml(opts, c.description || "", 400);
  return shell(ev, opts, inner, [
    ["currency", c.currency ? esc(String(c.currency)) : null],
  ]);
}

/** 9041 — a zap goal: the target, in sats rather than raw millisats. */
function goalCard(ev, opts) {
  const sats = satsOf(tagOf(ev, "amount"));
  const inner =
    bodyHtml(opts, ev.content || summaryOf(ev), 300) +
    (sats ? `<div class="price-line">goal: ${sats} sats</div>` : "");
  return shell(ev, opts, inner);
}

/** 30009 — a badge definition: its image is the badge. */
function badgeCard(ev, opts) {
  const img = tagOf(ev, "image") || tagOf(ev, "thumb");
  const inner =
    (opts && opts.full ? imgEmbed(img) : "") +
    (tagOf(ev, "name") || titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, tagOf(ev, "name") || titleOf(ev), 140))}</h2>` : "") +
    bodyHtml(opts, tagOf(ev, "description") || ev.content, 300);
  return shell(ev, opts, inner);
}

// 30403 and 30020 are drafts carrying their published twin's shape.
/** A sat count with the separators every other amount on this page reads with; non-numeric, verbatim. */
const satCount = (v) => {
  const n = Number(oneLine(v));
  return Number.isFinite(n) && n > 0 ? n.toLocaleString() : oneLine(v);
};

/** NIP-69 writes the side in `k` and the state in `s`; both are closed vocabularies. */
const ORDER_SIDES = new Set(["buy", "sell"]);
const ORDER_STATES = new Set(["pending", "canceled", "in-progress", "success", "expired"]);
/** The states that get a tinted pill; the rest read as plain text so an unknown one still shows. */
const STATE_TONE = { pending: "open", "in-progress": "accepted", success: "merged", canceled: "closed", expired: "closed" };

/**
 * 38383 — a peer-to-peer order: somebody offering to buy or sell sats for fiat, on one of the
 * NIP-69 platforms. What a reader is scanning for is the side, the money on both ends and
 * whether the order is still open, so those are the line; the rest is the table.
 */
function orderCard(ev, opts) {
  const side = oneLine(tagOf(ev, "k")).toLowerCase();
  const state = oneLine(tagOf(ev, "s")).toLowerCase();
  const sats = satCount(tagOf(ev, "amt"));
  const fiat = oneLine(tagOf(ev, "fa"));
  const currency = oneLine(tagOf(ev, "f"));
  // `pm` is one tag carrying every method, not one tag each.
  const methods = tagsOf(ev, "pm").flatMap((t) => t.slice(1)).map(oneLine).filter(Boolean);
  const headline = [
    ORDER_SIDES.has(side) ? side : "",
    sats ? `${sats} sats` : "",
    fiat || currency ? `for ${[fiat, currency].filter(Boolean).join(" ")}` : "",
  ].filter(Boolean).join(" ");
  const inner =
    (state
      ? `<div class="pill-row"><span class="status-pill lead ${ORDER_STATES.has(state) ? STATE_TONE[state] || "" : ""}">${esc(clip(state, 24))}</span>` +
        `${headline ? `<span class="price-line">${esc(headline)}</span>` : ""}</div>`
      : (headline ? `<div class="price-line">${esc(headline)}</div>` : "")) +
    bodyHtml(opts, ev.content, 300) +
    chipRow(methods, opts);
  return shell(ev, opts, inner, [
    ["maker", tagOf(ev, "name") ? esc(clip(tagOf(ev, "name"), 60)) : null],
    ["premium", tagOf(ev, "premium") ? esc(clip(tagOf(ev, "premium"), 24)) : null],
    ["bond", tagOf(ev, "bond") ? esc(clip(tagOf(ev, "bond"), 24)) : null],
    ["platform", tagOf(ev, "y") ? esc(clip(tagOf(ev, "y"), 40)) : null],
    ["network", [tagOf(ev, "network"), tagOf(ev, "layer")].map(oneLine).filter(Boolean).map((v) => esc(clip(v, 24))).join(" · ") || null],
    ["rating", tagOf(ev, "rating") ? esc(clip(tagOf(ev, "rating"), 40)) : null],
  ]);
}

/** 38000 — a mint recommendation: which mints this reader vouches for, and of which sort. */
function mintRecommendationCard(ev, opts) {
  const urls = tagsOf(ev, "u").map((t) => t[1]).filter(Boolean);
  const addrs = tagsOf(ev, "a").map((t) => t[1]).filter(Boolean);
  // The `k` is the kind of mint announcement being seconded — 38172 cashu, 38173 fedimint.
  const of = oneLine(tagOf(ev, "k"));
  const inner =
    `<div class="result-body">recommends ${esc(plural(urls.length + addrs.length, "mint"))}${of ? ` of kind ${esc(clip(of, 12))}` : ""}</div>` +
    bodyHtml(opts, ev.content, 300) +
    relayRows(urls.map((url) => ({ url })), opts) +
    refRows(addrs.map((a) => ({ kind: "a", value: a })), opts);
  return shell(ev, opts, inner);
}

register([30402, 30403], listingCard);
register([30018, 30020], productCard);
register([30017], stallCard);
register([9041], goalCard);
register([30009], badgeCard);
register([38383], orderCard);
register([38000], mintRecommendationCard);

// The price rides in the sub line, never the title.
registerRow([30402, 30403], (ev) => {
  const price = tagsOf(ev, "price")[0] || [];
  return {
    name: titleOf(ev),
    sub: [priceText(price[1], price[2], price[3]), summaryOf(ev) || ev.content].filter(Boolean).join(" · "),
  };
});
registerRow([30018, 30020], (ev) => {
  const c = jsonContent(ev);
  return { name: c.name, sub: [priceText(c.price, c.currency), oneLine(c.description)].filter(Boolean).join(" · ") };
});
registerRow([30017], (ev) => {
  const c = jsonContent(ev);
  return { name: c.name, sub: c.description };
});
registerRow([9041], (ev) => {
  const sats = satsOf(tagOf(ev, "amount"));
  return { name: ev.content || summaryOf(ev), sub: sats ? `goal: ${sats} sats` : "" };
});
registerRow([30009], (ev) => ({
  name: tagOf(ev, "name") || titleOf(ev),
  sub: tagOf(ev, "description") || ev.content,
}));
// An order's line is what it offers and whether it is still open.
registerRow([38383], (ev) => {
  const side = oneLine(tagOf(ev, "k")).toLowerCase();
  const sats = satCount(tagOf(ev, "amt"));
  const fiat = [oneLine(tagOf(ev, "fa")), oneLine(tagOf(ev, "f"))].filter(Boolean).join(" ");
  return {
    name: [ORDER_SIDES.has(side) ? side : "order", sats ? `${sats} sats` : "", fiat ? `for ${fiat}` : ""].filter(Boolean).join(" "),
    sub: [oneLine(tagOf(ev, "s")), oneLine(tagOf(ev, "name")), oneLine(tagOf(ev, "y"))].filter(Boolean).join(" · "),
  };
});
registerRow([38000], (ev) => ({
  name: `recommends ${plural(tagsOf(ev, "u").length + tagsOf(ev, "a").length, "mint")}`,
  sub: ev.content,
}));
