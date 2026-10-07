// The marketplace family: NIP-99 listings keep everything in tags, NIP-15 stalls and
// products keep a JSON content. Nothing is invented for a missing field. The two ends of a
// trade are here too — a NIP-69 peer-to-peer order, and the mints a NIP-87 reader recommends.
//
// Kind 38000 is three vocabularies on one number: NIP-87's mint recommendation, BAO Markets'
// prediction markets and an auditable-voting app's ballots, plus `d`-only spam. Quartz tells
// them apart by tags (EventFactory), and so does [kind38000], in the same order, so a market is
// never drawn as "recommends 0 mints".

import { esc, clip, titleOf, summaryOf, imageOf } from "../shared/format.js";
import {
  register, registerRow, registerBadge, registerNamedPeople, shell, bodyHtml, chipRow, relayRows,
  refRows, faceStrip, hashtagHref, topicsOf, satCount, tagsOf, tagOf, jsonContent, clipIf, oneLine,
  fmtTs, plural, satsOf, personLink, noteHref, extLink, titleHtml,
} from "./base.js";
import { shortNote } from "../shared/nip19.js";

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

const MINT_KINDS = new Set(["38172", "38173"]);
const nonBlank = (ev, name) => tagsOf(ev, name).some((t) => oneLine(t[1]));

/** Which 38000 this is, in quartz's order: a recommendation first, then a ballot, then a market. */
export function kind38000(ev) {
  const ks = tagsOf(ev, "k").map((t) => oneLine(t[1])).filter(Boolean);
  if (ks.some((k) => MINT_KINDS.has(k))) return "mint";
  if (!ks.length && (nonBlank(ev, "u") ||
      tagsOf(ev, "a").some((t) => /^3817[23]:/.test(oneLine(t[1]))))) return "mint";
  if (nonBlank(ev, "election")) return "ballot";
  if (nonBlank(ev, "market")) return "market";
  const outcomes = tagsOf(ev, "outcome").length;
  if (outcomes >= 2 || (tagsOf(ev, "type").length && tagsOf(ev, "end").length)) return "market";
  return "other";
}

/** A JSON object off a stranger's string, or null: the market keeps its details in `data` or in `content`. */
const objectOf = (s) => {
  if (!/^\s*\{/.test(String(s || ""))) return null;
  try { const o = JSON.parse(s); return o && typeof o === "object" && !Array.isArray(o) ? o : null; } catch (e) { return null; }
};

/** A market's outcomes as labels: `["outcome", id, label]` tags first, else the JSON's list. */
function outcomesOf(ev, details) {
  const seen = new Set(), out = [];
  const add = (id, label) => {
    id = oneLine(id); label = oneLine(label) || id;
    if (id && !seen.has(id) && out.length < 32) { seen.add(id); out.push(label); }
  };
  for (const t of tagsOf(ev, "outcome")) add(t[1], t[2]);
  if (!out.length) {
    for (const d of details) {
      for (const o of Array.isArray(d && d.outcomes) ? d.outcomes : []) {
        if (typeof o === "string") add(o, o);
        else if (o && typeof o === "object") add(o.id || o.label || o.name, o.label || o.name);
      }
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
    badge: "",
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
const bidderOf = (ev) => tagsOf(ev, "p").map((t) => t[1]).find((pk) => HEX64.test(pk || "")) || null;

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
registerBadge([38000], (ev) => KIND_38000[kind38000(ev)].badge);
registerNamedPeople([1022], (ev) => [bidderOf(ev)].filter(Boolean));

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
  const sats = satCount(tagOf(ev, "amt")) || oneLine(tagOf(ev, "amt"));
  const fiat = [oneLine(tagOf(ev, "fa")), oneLine(tagOf(ev, "f"))].filter(Boolean).join(" ");
  return {
    name: [ORDER_SIDES.has(side) ? side : "order", sats ? `${sats} sats` : "", fiat ? `for ${fiat}` : ""].filter(Boolean).join(" "),
    sub: [oneLine(tagOf(ev, "s")), oneLine(tagOf(ev, "name")), oneLine(tagOf(ev, "y"))].filter(Boolean).join(" · "),
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
