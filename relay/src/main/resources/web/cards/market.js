// The marketplace family. Two payload styles meet here: NIP-99 listings keep
// everything in tags (price is ["price", amount, currency, period?]) while
// NIP-15 stalls/products keep a JSON content — both are rendered from what
// the event actually carries, with nothing invented for a missing field.

import { esc, titleOf, summaryOf, imageOf } from "../shared/format.js";
import { register, registerRow, shell, bodyHtml, tagsOf, tagOf, jsonContent, clipIf, oneLine, satsOf } from "./base.js";

/**
 * "250 USD", "9 EUR / month" — the price as WORDS, so the row can carry it too.
 *
 * Every part goes through oneLine because half of these come out of a JSON
 * content: `{"price": {}}` is a legal document and `${}` on it reads "[object
 * Object]", which is a price nobody quoted.
 */
const priceText = (amount, currency, period) => {
  const a = oneLine(amount);
  return a ? `${a} ${oneLine(currency)}${period ? ` / ${oneLine(period)}` : ""}`.trim() : "";
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

// 30403 is the draft of a 30402 and carries the identical tags; 30020 is a
// product sold at auction, whose JSON content is a product's plus a starting
// bid. Neither is a different card — a draft that rendered as "kind 30403"
// while its published twin rendered as a listing was the registry's gap, not
// the event's.
register([30402, 30403], listingCard);
register([30018, 30020], productCard);
register([30017], stallCard);
register([9041], goalCard);
register([30009], badgeCard);

// The rows, and the price is on every one that has a price: what a thing costs
// is half of why anybody clicks a listing, and it is not in the title.
registerRow([30402, 30403], (ev) => {
  const price = tagsOf(ev, "price")[0] || [];
  return {
    name: titleOf(ev),
    sub: [priceText(price[1], price[2], price[3]), summaryOf(ev) || ev.content].filter(Boolean).join(" · "),
  };
});
// NIP-15 keeps the whole product in a JSON content, which is what the row used
// to print: `{"name":"Widget","description":…,"price":10,…}` in place of the
// three words of it a reader wanted.
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
