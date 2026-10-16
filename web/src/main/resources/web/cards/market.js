// The marketplace family: NIP-99 listings keep everything in tags, and so do the Open Markets
// collections, shipping options and reviews around them; NIP-15 stalls and products keep a JSON
// content. Nothing is invented for a missing field. The two ends of a
// trade are here too — a NIP-69 peer-to-peer order, and the mints a NIP-87 reader recommends.
//
// Kind 38000 is three vocabularies on one number: NIP-87's mint recommendation, BAO Markets'
// prediction markets and an auditable-voting app's ballots, plus `d`-only spam. Quartz tells
// them apart by tags (EventFactory), and so does [kind38000], in the same order, so a market is
// never drawn as "recommends 0 mints".

import { esc, clip, titleOf, summaryOf, imageOf, OPAQUE_D } from "../shared/format.js";
import {
  register, registerRow, registerBadge, registerNamedPeople, shell, bodyHtml, chipRow, relayRows,
  refRows, faceStrip, hashtagHref, topicsOf, satCount, tagsOf, tagOf, jsonContent, clipIf, oneLine,
  fmtTs, plural, satsOf, personLink, noteHref, extLink, titleHtml, firstPerson, addrHref, multiTag,
  notOursCard,
} from "./base.js";
import { shortNote, shortAddr, naddr } from "../shared/nip19.js";

const HEX64 = /^[0-9a-f]{64}$/;

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

/** A listing's images in the order its `image` tags' sort values give, unsorted ones last. */
const imagesOf = (ev) => tagsOf(ev, "image")
  .map((t, i) => ({ url: oneLine(t[1]), rank: intOf(oneLine(t[3])) ?? Infinity, i }))
  .filter((m) => m.url)
  .sort((a, b) => a.rank - b.rank || a.i - b.i)
  .map((m) => m.url);

/** The first image as a banner, the rest as a grid beneath it. */
const gallery = (urls) => imgEmbed(urls[0]) + (urls.length > 1
  ? `<div class="media-grid">${urls.slice(1).map((u) => `<img src="${esc(u)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()" />`).join("")}</div>`
  : "");

/** The NIP-99 and Open Markets words a listing is flagged with; an absent or unknown one is no pill. */
const LISTING_PILLS = { sold: "closed", "pre-order": "tentative", hidden: "" };

/** What a listing says about its own availability: sold, pre-order or hidden, as written. */
function listingFlags(ev) {
  const status = oneLine(tagOf(ev, "status")).toLowerCase();
  const visibility = oneLine(tagOf(ev, "visibility")).toLowerCase();
  return [status === "sold" ? "sold" : "", visibility === "pre-order" || visibility === "hidden" ? visibility : ""]
    .filter(Boolean);
}

const flagPills = (flags) => (flags.length
  ? `<div class="pill-row">${flags.map((f) => `<span class="status-pill${LISTING_PILLS[f] ? ` ${LISTING_PILLS[f]}` : ""}">${esc(f)}</span>`).join("")}</div>`
  : "");

/** An address list as links, each labelled by its `d`. */
const addrLinks = (addrs) => addrs.map((a) => {
  const href = addrHref(a);
  const label = esc(clip(oneLine(shortAddr(a)), 40));
  return href ? `<a href="${href}">${label}</a>` : `<span class="mono">${label}</span>`;
}).join(", ");

/** `["weight", "1.2", "kg"]` and its kin as "1.2 kg", or "". */
const measureOf = (ev, name) => {
  const t = tagsOf(ev, name)[0] || [];
  return [oneLine(t[1]), oneLine(t[2])].filter(Boolean).join(" ");
};

const ADDRESS = /^(\d+):[0-9a-f]{64}:/;
/** A value that is an address, of [kind] when given; it is linked as written, since a `d` may hold any whitespace. */
const isAddress = (a, kind) => {
  const m = typeof a === "string" ? ADDRESS.exec(a) : null;
  return !!m && (kind == null || m[1] === String(kind));
};
const addrsOf = (ev, name, kind) => tagsOf(ev, name).map((t) => t[1]).filter((a) => isAddress(a, kind));

/** A listing's `type` tag as `[type, format]`; a format written in the type slot is still the format. */
const FORMATS = new Set(["physical", "digital"]);
function productTypeOf(ev) {
  const [, type, format] = (tagsOf(ev, "type")[0] || []).map((v) => oneLine(v).toLowerCase());
  return FORMATS.has(format) ? [type, format] : FORMATS.has(type) ? ["", type] : [type || "", ""];
}

/**
 * 30402 — a classified listing. The preview is the title, price, flags and one line of text; the
 * permalink adds every image, the description under the summary, and the shop's facts.
 */
function listingCard(ev, opts) {
  const price = tagsOf(ev, "price")[0] || [];
  const full = opts && opts.full;
  const summary = summaryOf(ev);
  const [type, format] = productTypeOf(ev);
  const stock = intOf(oneLine(tagOf(ev, "stock")));
  const parent = addrsOf(ev, "a", 30402);
  const shipping = addrsOf(ev, "shipping_option");
  const specs = full ? tagsOf(ev, "spec").map((t) => [oneLine(t[1]), oneLine(t[2])]).filter(([k, v]) => k && v) : [];
  const inner =
    (full ? gallery(imagesOf(ev)) : "") +
    (titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 140))}</h2>` : "") +
    flagPills(listingFlags(ev)) +
    priceLine(price[1], price[2], price[3]) +
    (full
      ? bodyHtml(opts, summary, 400) + bodyHtml(opts, summary === ev.content ? "" : ev.content, 400)
      : bodyHtml(opts, summary || ev.content, 400)) +
    chipRow(topicsOf(ev), opts, hashtagHref);
  return shell(ev, opts, inner, [
    ["location", tagOf(ev, "location") ? esc(tagOf(ev, "location")) : null],
    ["stock", stock != null && stock >= 0 ? esc(stock.toLocaleString()) : null],
    ["format", format ? esc(format) : null],
    ...(full ? [
      ["variant of", type === "variation" && parent.length ? addrLinks(parent.slice(0, 1)) : null],
      ["collections", addrLinks(addrsOf(ev, "a", 30405)) || null],
      ["shipping", addrLinks(shipping) || null],
      ["weight", measureOf(ev, "weight") ? esc(clip(measureOf(ev, "weight"), 40)) : null],
      ["dimensions", measureOf(ev, "dim") ? esc(clip(measureOf(ev, "dim"), 40)) : null],
      ...specs.map(([k, v]) => [clip(k, 40), esc(clip(v, 120))]),
    ] : []),
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
  const sats = satCount(tagOf(ev, "amt")) || oneLine(tagOf(ev, "amt"));
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

/**
 * A 38385 is a Mostro instance's terms when it says so: a `z` of `info` or a `mostro_version`,
 * quartz's `isMostroInfo`. Paygress, a bond app and a game publish other things on the number.
 */
const isMostroInfo = (ev) =>
  tagsOf(ev, "z").some((t) => t[1] === "info") || !!oneLine(tagOf(ev, "mostro_version"));

/** The instance's own name, the third slot of `y` (the second is the platform, "mostro"). */
const mostroName = (ev) => oneLine((tagsOf(ev, "y").find((t) => oneLine(t[2])) || [])[2]);

/** The trade fee, published as a fraction (`0.006`), read as a percentage: "0.6%". */
function mostroFee(ev) {
  const raw = oneLine(tagOf(ev, "fee"));
  if (!/^\d+(?:\.\d+)?$/.test(raw)) return "";
  return `${Number((Number(raw) * 100).toFixed(4))}%`;
}

/** The order-size window in sats, as `min_order_amount` / `max_order_amount` publish it. */
function mostroLimits(ev) {
  const sats = (name) => {
    const v = oneLine(tagOf(ev, name));
    return /^\d+$/.test(v) ? `${Number(v).toLocaleString()} sats` : "";
  };
  const lo = sats("min_order_amount");
  const hi = sats("max_order_amount");
  return lo && hi ? `orders ${lo} – ${hi}` : lo ? `orders from ${lo}` : hi ? `orders up to ${hi}` : "";
}

/**
 * The currencies an instance trades: one `fiat_currencies_accepted` tag, comma-separated. An empty
 * value is the spec's "every currency", which is not the same as an instance that names none.
 */
function mostroCurrencies(ev) {
  const t = tagsOf(ev, "fiat_currencies_accepted")[0];
  if (!t) return null;
  return String(t[1] || "").split(",").map(oneLine).filter(Boolean);
}

/**
 * 38385 — a Mostro instance's status and terms, republished on an interval: whether it is taking
 * orders, what it charges, the order sizes it accepts and the currencies it trades. Every value
 * is a tag; the content is empty by spec.
 */
function mostroInfoCard(ev, opts) {
  if (!isMostroInfo(ev)) return notOursCard(ev, opts, "a Mostro instance");
  // other_events.md: no `maintenance_mode` means the instance is open.
  const down = oneLine(tagOf(ev, "maintenance_mode")) === "true";
  const terms = [mostroFee(ev) && `fee ${mostroFee(ev)}`, mostroLimits(ev)].filter(Boolean).join(" · ");
  const currencies = mostroCurrencies(ev);
  const pow = oneLine(tagOf(ev, "pow"));
  const inner =
    titleHtml(opts, mostroName(ev) || "a Mostro instance", 140) +
    `<div class="pill-row"><span class="status-pill lead ${down ? "closed" : "open"}">${down ? "maintenance" : "open"}</span>` +
    (terms ? `<span class="price-line">${esc(terms)}</span>` : "") + `</div>` +
    (currencies && !currencies.length ? `<div class="result-body muted">trades every currency</div>` : "") +
    chipRow(currencies || [], opts);
  return shell(ev, opts, inner, [
    ["escrow", esc(clip(oneLine(tagOf(ev, "escrow_mode")) || "lightning", 24))],
    ["version", tagOf(ev, "mostro_version") ? esc(clip(oneLine(tagOf(ev, "mostro_version")), 24)) : null],
    ["protocol", tagOf(ev, "protocol_version") ? esc(clip(oneLine(tagOf(ev, "protocol_version")), 12)) : null],
    ["proof of work", /^[1-9]\d*$/.test(pow) ? esc(`${pow} bits`) : null],
    ["order expiry", /^\d+$/.test(oneLine(tagOf(ev, "expiration_hours"))) ? esc(plural(Number(tagOf(ev, "expiration_hours")), "hour")) : null],
    ["lightning node", tagOf(ev, "lnd_node_alias") ? esc(clip(oneLine(tagOf(ev, "lnd_node_alias")), 40)) : null],
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

/** 30019 — a marketplace: a shopfront over a set of merchants, with its look in the same JSON. */
function marketplaceCard(ev, opts) {
  const c = jsonContent(ev);
  const ui = c && typeof c.ui === "object" && c.ui ? c.ui : {};
  const merchants = Array.isArray(c.merchants) ? c.merchants.filter((pk) => /^[0-9a-f]{64}$/.test(pk || "")) : [];
  const banner = oneLine(ui.banner) || oneLine(ui.picture);
  const full = opts && opts.full;
  const inner =
    (full && banner ? imgEmbed(banner) : "") +
    (oneLine(c.name) ? `<h2 class="result-title">${esc(clipIf(opts, oneLine(c.name), 120))}</h2>` : "") +
    bodyHtml(opts, oneLine(c.about), 300, true) +
    `<div class="result-body">${esc(plural(merchants.length, "merchant"))}</div>` +
    faceStrip(merchants, full ? 24 : 12);
  return shell(ev, opts, inner);
}

/**
 * 33863 — a fundraiser. The goal is in sats and the progress toward it is not knowable here:
 * the zaps that count toward it are other events, and the `w` addresses are paid on-chain,
 * where this page cannot see. So the card states the target and the deadline, never a total.
 */
function fundraiserCard(ev, opts) {
  // `goal` is already in sats, so it is counted, not converted: satsOf would divide it away.
  const goal = satCount(tagOf(ev, "goal"));
  const addresses = tagsOf(ev, "w").map((t) => t[1]).filter(Boolean);
  const banner = tagOf(ev, "banner") || imageOf(ev);
  const full = opts && opts.full;
  const inner =
    (full && banner ? imgEmbed(banner) : "") +
    (titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 120))}</h2>` : "") +
    (goal ? `<div class="price-line">${esc(goal)} sats to raise</div>` : "") +
    bodyHtml(opts, summaryOf(ev) || ev.content, 400) +
    chipRow(topicsOf(ev), opts, hashtagHref);
  return shell(ev, opts, inner, [
    ["deadline", tagOf(ev, "deadline") ? esc(fmtTs(tagOf(ev, "deadline"))) : null],
    ["published", tagOf(ev, "published_at") ? esc(fmtTs(tagOf(ev, "published_at"))) : null],
    // Shown to be copied, never linked: this page has no way to pay one and no way to check one.
    ["onchain", addresses.length ? `<span class="mono">${esc(clip(addresses[0], 40))}</span>` : null],
  ]);
}

// ---- kind 38000, three apps on one number ----------------------------------

/** A tag value as quartz tests it: present (`size > 1`), as written, untrimmed. */
const valuesOf = (ev, name) => tagsOf(ev, name).filter((t) => typeof t[1] === "string").map((t) => t[1]);
const nonBlank = (ev, name) => valuesOf(ev, name).some((v) => v.trim());
/** Kotlin's toIntOrNull: an optional sign and digits, nothing around them. */
const intOf = (v) => (/^[+-]?\d+$/.test(v) ? Number(v) : null);

/**
 * Which 38000 this is, in quartz's order (EventFactory, isMintRecommendation, isBallot,
 * isPredictionMarket): a recommendation first, then a ballot, then a market.
 */
export function kind38000(ev) {
  const ks = valuesOf(ev, "k");
  if (ks.some((k) => intOf(k) === 38172 || intOf(k) === 38173)) return "mint";
  if (!ks.some((k) => k.trim()) && (nonBlank(ev, "u") ||
      valuesOf(ev, "a").some((v) => v.startsWith("38172:") || v.startsWith("38173:")))) return "mint";
  if (nonBlank(ev, "election")) return "ballot";
  if (nonBlank(ev, "market")) return "market";
  const outcomes = valuesOf(ev, "outcome").length;
  if (outcomes >= 2 || (valuesOf(ev, "type").length && valuesOf(ev, "end").length)) return "market";
  return "other";
}

/** A JSON object off a stranger's string, or null: the market keeps its details in `data` or in `content`. */
const objectOf = (s) => {
  if (!/^\s*\{/.test(String(s || ""))) return null;
  try { const o = JSON.parse(s); return o && typeof o === "object" && !Array.isArray(o) ? o : null; } catch (e) { return null; }
};

/** A JSON outcome list as `[id, label]` pairs: a bare string is both, an object its id and label. */
const jsonOutcomes = (d) => (Array.isArray(d && d.outcomes) ? d.outcomes : []).map((o) =>
  (typeof o === "string" ? [o, o] : o && typeof o === "object" ? [o.id || o.label || o.name, o.label || o.name] : []));

/**
 * A market's outcomes as labels: `["outcome", id, label]` tags first, else the JSON's list. A
 * tag with no label takes the JSON's for the same id, so `["outcome", "y"]` beside
 * `{id: "y", label: "Yes"}` reads "Yes", not the id.
 */
function outcomesOf(ev, details) {
  const labelled = new Map();
  for (const d of details) for (const [id, label] of jsonOutcomes(d)) {
    if (oneLine(id) && oneLine(label) && !labelled.has(oneLine(id))) labelled.set(oneLine(id), oneLine(label));
  }
  const seen = new Set(), out = [];
  const add = (id, label) => {
    id = oneLine(id); label = oneLine(label) || labelled.get(id) || id;
    if (id && !seen.has(id) && out.length < 32) { seen.add(id); out.push(label); }
  };
  for (const t of tagsOf(ev, "outcome")) add(t[1], t[2]);
  if (!out.length) {
    for (const d of details) {
      for (const [id, label] of jsonOutcomes(d)) add(id, label);
      if (out.length) break;
    }
  }
  return out;
}

/**
 * A BAO prediction market, read the way quartz's PredictionMarketEvent reads it: a `data` tag's
 * JSON wins, then the tags, then a JSON content. A content that is not JSON is the market's
 * social post, shown only when no description supersedes it.
 */
function marketOf(ev) {
  const data = objectOf(tagOf(ev, "data")) || {};
  const body = objectOf(ev.content);
  const c = body || {};
  const pick = (...vals) => vals.map(oneLine).find(Boolean) || "";
  const description = pick(data.description, c.description);
  const text = String(ev.content || "").trim();
  return {
    title: pick(data.title, data.question, tagOf(ev, "title"), c.title, c.question),
    description,
    post: !description && !body && text && text !== "null" && !/^[[{]/.test(text) ? text : "",
    outcomes: outcomesOf(ev, [data, c]),
    resolution: pick(tagOf(ev, "resolution"), data.resolution, c.resolution),
    cancelled: pick(tagOf(ev, "cancel_reason"), c.reason),
    status: pick(tagOf(ev, "status"), tagOf(ev, "state"), tagOf(ev, "s"), data.status, c.status),
    ends: pick(tagOf(ev, "end"), data.endDate, data.end, c.endDate, c.end),
    category: pick(tagOf(ev, "category"), tagOf(ev, "c"), data.category, c.category),
    network: pick(tagOf(ev, "network"), tagOf(ev, "n")),
    source: pick(tagOf(ev, "resolution_source"), data.resolutionSource, c.resolutionSource),
    minBet: satCount(pick(tagOf(ev, "min_bet"), data.minBetSats, c.minBetSats)),
    maxBet: satCount(pick(tagOf(ev, "max_bet"), data.maxBetSats, c.maxBetSats)),
    fee: pick(tagOf(ev, "fee_percent"), data.feePercent, c.feePercent),
  };
}

/** 38000 (BAO Markets) — a question, the outcomes a bet can land on, and how it settles. */
function predictionMarketCard(ev, opts) {
  const m = marketOf(ev);
  const pills = [
    m.status ? `<span class="status-pill">${esc(clip(m.status, 24))}</span>` : "",
    /^demo$/i.test(m.network) ? `<span class="status-pill">demo</span>` : "",
  ].join("");
  const bets = [m.minBet && `from ${m.minBet}`, m.maxBet && `to ${m.maxBet}`].filter(Boolean).join(" ");
  const inner =
    titleHtml(opts, m.title, 160) +
    (pills ? `<div class="pill-row">${pills}</div>` : "") +
    bodyHtml(opts, m.description || m.post, 400) +
    chipRow(m.outcomes, opts) +
    (m.resolution ? `<div class="result-body">resolved: <b>${esc(clip(m.resolution, 80))}</b></div>` : "") +
    (m.cancelled ? `<div class="result-body muted">cancelled: ${esc(clip(m.cancelled, 200))}</div>` : "");
  return shell(ev, opts, inner, [
    ["closes", m.ends ? esc(fmtTs(m.ends)) : null],
    ["category", m.category ? esc(clip(m.category, 40)) : null],
    ["bets", bets ? `${esc(bets)} sats` : null],
    ["fee", m.fee ? `${esc(clip(m.fee, 12))}%` : null],
    ["settled by", m.source ? extLink(m.source, clip(m.source, 60)) : null],
  ]);
}

/**
 * A ballot's answers, `[question, choice]`, from whichever of the three shapes the voting app
 * wrote: `responses` (question ids and values), a `ballot` map, or a lone `vote_choice`.
 */
function ballotAnswers(ev) {
  const c = jsonContent(ev);
  const scalar = (v) => (typeof v === "string" || typeof v === "number" || typeof v === "boolean" ? oneLine(String(v)) : "");
  if (Array.isArray(c.responses)) {
    return c.responses
      .map((r) => (r && typeof r === "object" ? [scalar(r.question_id) || scalar(r.question), scalar(r.value)] : []))
      .filter(([q, a]) => q && a);
  }
  if (c.ballot && typeof c.ballot === "object" && !Array.isArray(c.ballot)) {
    return Object.entries(c.ballot).map(([q, a]) => [oneLine(q), scalar(a)]).filter(([q, a]) => q && a);
  }
  return scalar(c.vote_choice) ? [["Vote", scalar(c.vote_choice)]] : [];
}

/** 38000 (a voting app) — one cast ballot: the election it counts in, and what it says. */
function ballotCard(ev, opts) {
  const answers = ballotAnswers(ev);
  const shown = opts && opts.full ? answers : answers.slice(0, 6);
  const proof = oneLine(tagOf(ev, "proof-hash", "proof_hash") || jsonContent(ev).proof_hash);
  const inner =
    `<div class="result-body">casts a ballot in <b>${esc(clip(oneLine(tagOf(ev, "election")), 80))}</b></div>` +
    (shown.length
      ? `<dl class="props">${shown.map(([q, a]) => `<dt>${esc(clip(q, 60))}</dt><dd>${esc(clip(a, 120))}</dd>`).join("")}</dl>`
      : "") +
    (answers.length > shown.length ? `<div class="muted-note">…and ${answers.length - shown.length} more</div>` : "");
  return shell(ev, opts, inner, [["proof", proof ? `<span class="mono">${esc(clip(proof, 24))}</span>` : null]]);
}

/**
 * 38000, none of the three — mostly `d`-only "test votes". Quartz keeps it addressable and indexes
 * nothing of it; the card says so rather than dressing it as a recommendation.
 */
function unrecognized38000Card(ev, opts) {
  const inner =
    `<div class="result-body muted">a kind 38000 event no app this page knows wrote</div>` +
    bodyHtml(opts, String(ev.content || "").trim().startsWith("{") ? "" : ev.content, 200, true);
  return shell(ev, opts, inner);
}

const KIND_38000 = {
  mint: {
    card: (ev, opts) => mintRecommendationCard(ev, opts),
    badge: "mint list",
    row: (ev) => ({ name: `recommends ${plural(tagsOf(ev, "u").length + tagsOf(ev, "a").length, "mint")}`, sub: ev.content }),
  },
  market: {
    card: (ev, opts) => predictionMarketCard(ev, opts),
    badge: "prediction market",
    row: (ev) => {
      const m = marketOf(ev);
      return { name: m.title || "a prediction market", sub: m.resolution ? `resolved: ${m.resolution}` : m.outcomes.join(" · ") };
    },
  },
  ballot: {
    card: (ev, opts) => ballotCard(ev, opts),
    badge: "ballot",
    row: (ev) => ({
      name: `casts a ballot in ${clip(oneLine(tagOf(ev, "election")), 60)}`,
      sub: ballotAnswers(ev).map(([q, a]) => `${q}: ${a}`).join(" · "),
    }),
  },
  other: {
    card: (ev, opts) => unrecognized38000Card(ev, opts),
    badge: "kind 38000",
    row: () => ({ name: "an unrecognized kind 38000 event" }),
  },
};

// ---- NIP-15's auction answer ------------------------------------------------

/** A 1022's JSON body: `{status, message, duration_extension}`, each field only if it is text. */
function bidConfirmationOf(ev) {
  const c = jsonContent(ev);
  return { status: oneLine(c.status), message: oneLine(c.message), extension: Number(c.duration_extension) };
}

/** The bidder a 1022 answers: its one `p`. */
const bidderOf = firstPerson;

/**
 * 1022 — a NIP-15 merchant answering a bid: accepted, rejected, pending or the winner. The first
 * `e` is the bid and the second the auction it was placed on.
 */
function bidConfirmationCard(ev, opts) {
  const c = bidConfirmationOf(ev);
  const [bid, auction] = tagsOf(ev, "e").map((t) => t[1]).filter((id) => HEX64.test(id || ""));
  const bidder = bidderOf(ev);
  const link = (id, noun) => (id ? `<a class="mono" href="${noteHref(id)}" title="${esc(noun)}">${esc(shortNote(id))}</a>` : esc(noun));
  const ext = Number.isFinite(c.extension) && c.extension > 0 ? Math.round(c.extension) : 0;
  const inner =
    `<div class="result-body">answers ${link(bid, "a bid")}${bidder ? ` by ${personLink(bidder)}` : ""}` +
    `${c.status ? `: <b>${esc(clip(c.status, 24))}</b>` : ""}</div>` +
    bodyHtml(opts, c.message, 300);
  return shell(ev, opts, inner, [
    ["auction", auction ? link(auction, "the auction") : null],
    ["extends by", ext ? `${esc(ext.toLocaleString())} s` : null],
  ]);
}

// ---- Open Markets: collections, shipping options, reviews --------------------

/** The products a collection holds: its `a` tags naming 30402s. */
const productsOf = (ev) => addrsOf(ev, "a", 30402);

/** A collection names itself or lists products; a 30405 with neither is somebody's cart. */
const isCollection = (ev) => !!oneLine(tagOf(ev, "title")) || tagsOf(ev, "a").some((t) => isAddress(t[1], 30402));

/** 30405 — a product collection: a merchant's named group of listings. */
function collectionCard(ev, opts) {
  if (!isCollection(ev)) return notOursCard(ev, opts, "a product collection");
  const full = opts && opts.full;
  const products = productsOf(ev);
  const summary = oneLine(tagOf(ev, "summary"));
  const shipping = addrsOf(ev, "shipping_option");
  const inner =
    (full ? imgEmbed(oneLine(tagOf(ev, "image"))) : "") +
    titleHtml(opts, oneLine(tagOf(ev, "title")), 140) +
    (full
      ? bodyHtml(opts, summary, 400) + bodyHtml(opts, summary === ev.content ? "" : ev.content, 400)
      : bodyHtml(opts, summary || ev.content, 300)) +
    `<div class="result-body">${esc(plural(products.length, "product"))}</div>` +
    (full ? refRows(products.map((a) => ({ kind: "a", value: a })), opts) : "");
  return shell(ev, opts, inner, [
    ["location", tagOf(ev, "location") ? esc(clip(tagOf(ev, "location"), 120)) : null],
    ["shipping", full ? addrLinks(shipping) || null : null],
  ]);
}

/**
 * A shipping option has a title, a service or a country, quartz's `isShippingOption`; Shopstr's
 * escrow records on 30406 have none of them.
 */
const isShippingOption = (ev) =>
  !!(oneLine(tagOf(ev, "title")) || oneLine(tagOf(ev, "service")) || oneLine(tagOf(ev, "country")));

const DURATION_UNITS = { H: ["hour", "hours"], D: ["day", "days"], W: ["week", "weeks"] };

/** `["duration", "24", "72", "H"]` as "24–72 hours", or "" when the window cannot be read. */
function durationText(ev) {
  const t = tagsOf(ev, "duration")[0] || [];
  const [a, b] = [oneLine(t[1]), oneLine(t[2])].map((v) => (/^\d+(?:\.\d+)?$/.test(v) ? v : ""));
  const unit = DURATION_UNITS[oneLine(t[3]).toUpperCase()];
  if (!unit || !(a || b)) return "";
  const [min, max] = a && b && Number(a) > Number(b) ? [b, a] : [a, b];
  const span = min && max && min !== max ? `${min}–${max}` : min || max;
  return `${span} ${span === "1" ? unit[0] : unit[1]}`;
}

/** A min/max pair of measures as one range: "up to 30 kg", "from 1 kg", "1 kg – 30 kg". */
const rangeText = (lo, hi) => (lo && hi ? `${lo} – ${hi}` : lo ? `from ${lo}` : hi ? `up to ${hi}` : "");

/** `["price-weight", "0.75", "USD", "kg"]` as "+0.75 USD per kg"; the currency is optional. */
function ratesOf(ev) {
  return ["price-weight", "price-volume", "price-distance"].map((name) => {
    const t = (tagsOf(ev, name)[0] || []).map(oneLine);
    while (t.length && !t[t.length - 1]) t.pop();
    if (!t[1] || t.length < 3) return "";
    return `+${t[1]}${t.length > 3 && t[2] ? ` ${t[2]}` : ""} per ${t[t.length - 1]}`;
  }).filter(Boolean);
}

/** 30406 — a shipping option: what it costs, how fast, and where it goes. */
function shippingCard(ev, opts) {
  if (!isShippingOption(ev)) return notOursCard(ev, opts, "a shipping option");
  const full = opts && opts.full;
  const price = tagsOf(ev, "price")[0] || [];
  const service = oneLine(tagOf(ev, "service")).toLowerCase();
  const countries = multiTag(ev, "country").map(oneLine).filter(Boolean);
  const regions = multiTag(ev, "region").map(oneLine).filter(Boolean);
  const where = oneLine(tagOf(ev, "location"));
  const inner =
    titleHtml(opts, oneLine(tagOf(ev, "title")), 140) +
    (service ? `<div class="pill-row"><span class="status-pill">${esc(clip(service, 24))}</span></div>` : "") +
    priceLine(price[1], price[2]) +
    bodyHtml(opts, ev.content === oneLine(tagOf(ev, "title")) ? "" : ev.content, 300);
  return shell(ev, opts, inner, [
    ["ships to", countries.length ? esc(clip(countries.join(", "), full ? 400 : 80)) : null],
    ["regions", regions.length ? esc(clip(regions.join(", "), full ? 400 : 80)) : null],
    ["delivery", durationText(ev) ? esc(durationText(ev)) : null],
    [service === "pickup" ? "pickup at" : "location", where ? esc(clip(where, 120)) : null],
    ...(full ? [
      ["carrier", tagOf(ev, "carrier") ? esc(clip(oneLine(tagOf(ev, "carrier")), 60)) : null],
      ["weight", rangeText(measureOf(ev, "weight-min"), measureOf(ev, "weight-max")) ? esc(clip(rangeText(measureOf(ev, "weight-min"), measureOf(ev, "weight-max")), 60)) : null],
      ["size", rangeText(measureOf(ev, "dim-min"), measureOf(ev, "dim-max")) ? esc(clip(rangeText(measureOf(ev, "dim-min"), measureOf(ev, "dim-max")), 60)) : null],
      ["rates", ratesOf(ev).length ? esc(ratesOf(ev).join(" · ")) : null],
    ] : []),
  ]);
}

/** A review's `d` names the product as `a:30402:<merchant>:<d>`. */
const REVIEWED = /^a:(30402:([0-9a-f]{64}):([\s\S]*))$/;
/** A score as a plain decimal: Number() alone also takes "0x1" and "1e0". */
const DECIMAL = /^\d*\.?\d+$/;

/**
 * A 31555 read the Open Markets way: the `thumb` rating, the category ratings beside it, and the
 * total, which is the thumb alone or half the thumb plus half the categories' mean. Scores outside
 * 0..1 are malformed and left out.
 */
function reviewOf(ev) {
  const d = tagOf(ev, "d");
  const m = typeof d === "string" ? REVIEWED.exec(d) : null;
  const linked = !!(m && naddr(m[1]));
  const scores = tagsOf(ev, "rating")
    .map((t) => ({ text: oneLine(t[1]), category: oneLine(t[2]) }))
    .filter((r) => DECIMAL.test(r.text) && r.category && Number(r.text) <= 1)
    .map((r) => ({ value: Number(r.text), category: r.category }));
  const thumbTag = scores.find((r) => r.category.toLowerCase() === "thumb");
  const categories = scores.filter((r) => r.category.toLowerCase() !== "thumb");
  const thumb = thumbTag ? thumbTag.value : null;
  const mean = categories.length ? categories.reduce((n, r) => n + r.value, 0) / categories.length : null;
  return {
    product: linked ? m[1] : null,
    merchant: linked ? m[2] : null,
    // An opaque `d` (a UUID, a hex id) is no name for the product.
    label: linked && oneLine(m[3]) && !OPAQUE_D.test(oneLine(m[3])) ? clip(oneLine(m[3]), 60) : "a listing",
    thumb,
    categories,
    total: thumb == null ? null : mean == null ? thumb : thumb * 0.5 + mean * 0.5,
  };
}

const percent = (v) => `${Math.round(v * 100)}%`;
/** The thumb's word; a half is neither way. */
const thumbWord = (v) => (v == null ? "" : v > 0.5 ? "thumbs up" : v < 0.5 ? "thumbs down" : "mixed");
const THUMB_TONE = { "thumbs up": "open", "thumbs down": "closed", mixed: "" };

/** 31555 — a product review: the verdict, its parts, the words, and the product it is about. */
function reviewCard(ev, opts) {
  const r = reviewOf(ev);
  const word = thumbWord(r.thumb);
  const product = r.product
    ? `<a href="${addrHref(r.product)}">${esc(r.label)}</a>${r.merchant ? ` from ${personLink(r.merchant)}` : ""}`
    : "a product";
  const inner =
    `<div class="result-body">reviews ${product}</div>` +
    (word
      ? `<div class="pill-row"><span class="status-pill lead${THUMB_TONE[word] ? ` ${THUMB_TONE[word]}` : ""}">${esc(word)}</span>` +
        `<span class="price-line">${esc(percent(r.total))} overall</span></div>`
      : "") +
    chipRow(r.categories.map((c) => `${c.category} ${percent(c.value)}`), opts) +
    bodyHtml(opts, ev.content, 400);
  return shell(ev, opts, inner);
}

register([30402, 30403], listingCard);
register([30018, 30020], productCard);
register([30017], stallCard);
register([9041], goalCard);
register([30009], badgeCard);
register([38383], orderCard);
register([30019], marketplaceCard);
register([33863], fundraiserCard);
register([38000], (ev, opts) => KIND_38000[kind38000(ev)].card(ev, opts));
register([1022], bidConfirmationCard);
register([30405], collectionCard);
register([30406], shippingCard);
register([31555], reviewCard);
register([38385], mostroInfoCard);
registerBadge([30405], (ev) => (isCollection(ev) ? "" : "kind 30405"));
registerBadge([30406], (ev) => (isShippingOption(ev) ? "" : "kind 30406"));
registerBadge([38385], (ev) => (isMostroInfo(ev) ? "" : "kind 38385"));
// The merchant is named from the address in `d`, which no scan of `p` tags reaches.
registerNamedPeople([31555], (ev) => [reviewOf(ev).merchant].filter(Boolean));
registerBadge([38000], (ev) => KIND_38000[kind38000(ev)].badge);
registerNamedPeople([1022], (ev) => [bidderOf(ev)].filter(Boolean));

// The price rides in the sub line, never the title.
registerRow([30402, 30403], (ev) => {
  const price = tagsOf(ev, "price")[0] || [];
  return {
    name: titleOf(ev),
    sub: [priceText(price[1], price[2], price[3]), ...listingFlags(ev), summaryOf(ev) || ev.content].filter(Boolean).join(" · "),
  };
});
registerRow([30405], (ev) => (isCollection(ev)
  ? { name: oneLine(tagOf(ev, "title")), sub: [plural(productsOf(ev).length, "product"), oneLine(tagOf(ev, "summary")) || oneLine(ev.content)].filter(Boolean).join(" · ") }
  : { name: "a kind 30405 event, not a collection" }));
registerRow([30406], (ev) => {
  if (!isShippingOption(ev)) return { name: "a kind 30406 event, not a shipping option" };
  const price = tagsOf(ev, "price")[0] || [];
  return {
    name: oneLine(tagOf(ev, "title")) || `${oneLine(tagOf(ev, "service"))} shipping`,
    sub: [priceText(price[1], price[2]), oneLine(tagOf(ev, "service")), durationText(ev), multiTag(ev, "country").map(oneLine).filter(Boolean).join(", ")]
      .filter(Boolean).join(" · "),
  };
});
// A review's first line is the product, the verdict and the words under it.
registerRow([31555], (ev) => {
  const r = reviewOf(ev);
  return {
    name: `reviews ${r.product ? r.label : "a product"}`,
    sub: [thumbWord(r.thumb), r.total == null ? "" : `${percent(r.total)} overall`, ev.content].filter(Boolean).join(" · "),
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
  const sats = satCount(tagOf(ev, "amt")) || oneLine(tagOf(ev, "amt"));
  const fiat = [oneLine(tagOf(ev, "fa")), oneLine(tagOf(ev, "f"))].filter(Boolean).join(" ");
  return {
    name: [ORDER_SIDES.has(side) ? side : "order", sats ? `${sats} sats` : "", fiat ? `for ${fiat}` : ""].filter(Boolean).join(" "),
    sub: [oneLine(tagOf(ev, "s")), oneLine(tagOf(ev, "name")), oneLine(tagOf(ev, "y"))].filter(Boolean).join(" · "),
  };
});
// An instance's line is its name, then whether it is open and what it charges.
registerRow([38385], (ev) => {
  if (!isMostroInfo(ev)) return { name: "a kind 38385 event, not a Mostro instance" };
  const currencies = mostroCurrencies(ev);
  return {
    name: mostroName(ev) || "a Mostro instance",
    sub: [
      oneLine(tagOf(ev, "maintenance_mode")) === "true" ? "maintenance" : "open",
      mostroFee(ev) && `fee ${mostroFee(ev)}`,
      currencies && (currencies.length ? currencies.slice(0, 6).join(", ") : "every currency"),
    ].filter(Boolean).join(" · "),
  };
});
registerRow([38000], (ev) => KIND_38000[kind38000(ev)].row(ev));
registerRow([1022], (ev) => {
  const c = bidConfirmationOf(ev);
  return { name: c.status ? `bid ${c.status}` : "answers a bid", sub: c.message };
});
registerRow([30019], (ev) => {
  const c = jsonContent(ev);
  const merchants = Array.isArray(c.merchants) ? c.merchants.length : 0;
  return { name: oneLine(c.name), sub: [plural(merchants, "merchant"), oneLine(c.about)].filter(Boolean).join(" · ") };
});
registerRow([33863], (ev) => {
  const goal = satCount(tagOf(ev, "goal"));
  return {
    name: titleOf(ev),
    sub: [goal && `${goal} sats to raise`, summaryOf(ev) || ev.content].filter(Boolean).join(" · "),
  };
});
