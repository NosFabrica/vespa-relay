// The relay's own panels on stats.html, drawn from [LiveCounts] as its answers arrive. Every
// number is a count this page asked the relay for; an ask that has not answered is an em dash,
// never a zero.

import { kindLabel } from "../shared/kinds.js";
import { ago, cardHead, el, fitTiles, fmt, hideTip, isoOf, moveTip, short, showTip, svgEl } from "../shared/page.js";
import { DAY, DAYS, HOUR, HOURS, PEOPLE_KINDS, dayOf, hourOf } from "./live.js";

/** Past this the newest-event tile is coloured. Not a verdict: a serve-only relay may be quiet. */
const STALE_CORPUS_SECONDS = 24 * 3600;

const MONTH = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/** "Oct 16" for a UTC day, "14:00" for a UTC hour; built from UTC fields, never the reader's zone. */
const dayTick = (t) => {
  const d = new Date(t * 1000);
  return `${MONTH[d.getUTCMonth()]} ${d.getUTCDate()}`;
};
const hourTick = (t) => `${String(new Date(t * 1000).getUTCHours()).padStart(2, "0")}:00`;

/** How many bars apart the x labels stand, measured from the widest label present. */
function tickStride(points, plotW) {
  const widest = points.reduce((m, p) => Math.max(m, (p.tick || "").length), 0);
  if (!widest) return points.length;
  const room = Math.max(2, Math.floor(plotW / (widest * 6.2 + 18)));
  return Math.max(1, Math.ceil(points.length / room));
}

/**
 * A bar chart of one series. A point whose value is null was not answered yet and draws no bar;
 * its hover says so, so a gap never reads as an empty hour.
 */
export function barChart(points, { label, height = 130 }) {
  // A fixed viewBox scaled by CSS, so nothing re-measures on resize.
  const W = 720, H = height, PAD_L = 34, PAD_B = 20, PAD_T = 8;
  const plotW = W - PAD_L, plotH = H - PAD_B - PAD_T;
  const svg = svgEl("svg", { class: "chart", viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": label });

  const max = Math.max(1, ...points.map((p) => p.value || 0));
  const step = Math.pow(10, Math.floor(Math.log10(max)));
  const tick = [1, 2, 5, 10].map((m) => m * step).find((t) => t >= max) || max;
  const y = (v) => PAD_T + plotH - (v / tick) * plotH;

  // Deduped by rendered label: a maximum of 1 puts the midpoint at 0.5, which also prints "1".
  const drawn = new Set();
  for (const v of [0, tick / 2, tick]) {
    const text = short(Math.round(v));
    if (drawn.has(text)) continue;
    drawn.add(text);
    svg.appendChild(svgEl("line", { class: "grid", x1: PAD_L, x2: W, y1: y(v), y2: y(v) }));
    const t = svgEl("text", { class: "tick", x: PAD_L - 6, y: y(v) + 3, "text-anchor": "end" });
    t.textContent = text;
    svg.appendChild(t);
  }

  const band = plotW / points.length;
  const w = Math.max(1, Math.min(24, band - 2, band * 0.72));
  points.forEach((p, i) => {
    const x = PAD_L + i * band + (band - w) / 2;
    const value = p.value || 0;
    const top = y(value), h = Math.max(value > 0 ? 1 : 0, PAD_T + plotH - top);
    // Rounded at the data end, square at the baseline.
    const r = Math.min(4, w / 2, h);
    const marks = [];
    if (h > 0) {
      marks.push(svgEl("rect", { class: p.partial ? "mark partial" : "mark", x, y: top, width: w, height: h, rx: r, ry: r }));
      if (h > r) marks.push(svgEl("rect", { class: p.partial ? "mark partial" : "mark", x, y: top + r, width: w, height: h - r }));
      marks.forEach((m) => svg.appendChild(m));
    }
    // The hit target is the whole band: a thin bar is unhoverable.
    const hit = svgEl("rect", { class: "hit", x: PAD_L + i * band, y: PAD_T, width: band, height: plotH });
    hit.addEventListener("pointerenter", (e) => {
      marks.forEach((m) => m.classList.add("on"));
      showTip(e, `${p.label} · ${p.value == null ? "not counted yet" : fmt(p.value)}${p.partial ? " so far" : ""}`);
    });
    hit.addEventListener("pointermove", moveTip);
    hit.addEventListener("pointerleave", () => {
      marks.forEach((m) => m.classList.remove("on"));
      hideTip();
    });
    svg.appendChild(hit);
  });

  // Labelled from the newest bar backwards, so the last bucket always carries a label.
  const stride = tickStride(points, plotW);
  for (let i = points.length - 1; i >= 0; i -= stride) {
    const p = points[i];
    if (!p.tick) continue;
    // The end labels are anchored at their band's outer edge, so each still sits under its own bar.
    const last = i === points.length - 1;
    const anchor = last ? "end" : i === 0 ? "start" : "middle";
    const x = last ? PAD_L + (i + 1) * band : i === 0 ? PAD_L + i * band : PAD_L + i * band + band / 2;
    const t = svgEl("text", { class: "tick", x, y: H - 6, "text-anchor": anchor });
    t.textContent = p.tick;
    svg.appendChild(t);
  }
  return svg;
}

/** A tile: the number, or an em dash with why on hover; a failed refresh keeps the last number. */
function tile(live, key, label, help, render = (v) => fmt(v)) {
  const t = el("div", "tile");
  const a = live.get(key);
  const v = el("div", "v", a && a.value != null ? render(a.value) : "—");
  t.appendChild(v);
  t.appendChild(el("div", "k", label));
  const notes = [help];
  if (a && a.error) notes.push(a.value != null ? `The last refresh failed (${a.error}); this is the previous answer.` : `Not answered: ${a.error}.`);
  else if (!a) notes.push("Asking…");
  t.title = notes.filter(Boolean).join(" ");
  return { t, v };
}

/** "≈ 21/s" from a minute's count. */
const perSecond = (n) => (n / 60 >= 10 ? `≈ ${fmt(Math.round(n / 60))}/s` : `≈ ${(n / 60).toFixed(1)}/s`);

/** The headline: how fresh the store is and how fast it is filling, re-asked every few seconds. */
function nowPanel(live, socketUrl) {
  const card = el("div", "panel");
  const body = el("div");
  card.appendChild(body);
  const update = () => {
    body.replaceChildren();
    cardHead(body, "Right now", null, null);
    const row = el("div", "tiles");

    const newest = tile(live, "newest", "Newest event", "The newest created_at this relay holds, bounded at the present.", (t) => ago(t));
    const at = live.value("newest");
    if (at != null) {
      if (Math.floor(Date.now() / 1000) - at > STALE_CORPUS_SECONDS) {
        newest.v.classList.add("stale");
        newest.t.title = "The newest event this relay holds is over a day old: the mirror has stopped, or this deployment serves without one.";
      } else {
        newest.t.title = isoOf(at);
      }
    }
    row.appendChild(newest.t);
    const minute = tile(live, "last.60", "Last minute", "Events created in the last 60 seconds that this relay holds now.");
    row.appendChild(minute.t);
    row.appendChild(tile(live, "last.3600", "Last hour", "Events created in the last hour that this relay holds now.").t);
    row.appendChild(tile(live, "last.86400", "Last 24 hours", "Events created in the last 24 hours that this relay holds now.").t);
    body.appendChild(fitTiles(row));
    const m = live.value("last.60");
    const facts = [];
    if (m != null) facts.push(`${perSecond(m)} over the last minute`);
    facts.push(`counted live over ${socketUrl}`);
    if (live.paused) facts.push("paused while this tab is hidden");
    body.appendChild(el("p", "card-sub", facts.join(" · ") + "."));
    if (live.connError) body.appendChild(el("p", "err", `Cannot reach this relay's websocket: ${live.connError}. Retrying.`));
  };
  return { card, update };
}

/**
 * One chart card over a run of fixed buckets. The open bucket is drawn partial: it is still
 * filling, and mirrored history keeps filling closed ones for a while too.
 */
function bucketCard(live, { title, sub, prefix, n, width, startOf, tick, label }) {
  const card = el("div", "card");
  let last = null;
  const update = () => {
    const now = Math.floor(Date.now() / 1000);
    const open = startOf(now);
    const points = [];
    for (let i = n - 1; i >= 0; i--) {
      const start = open - i * width;
      points.push({ label: label(start), tick: tick(start), value: live.value(`${prefix}.${start}`), partial: i === 0 });
    }
    const key = JSON.stringify(points);
    if (key === last) return;
    last = key;
    // A chart rebuilt under the pointer would strand its tooltip.
    hideTip();
    card.replaceChildren();
    const answered = points.filter((p) => p.value != null).length;
    cardHead(card, title, answered < n ? `${sub} · counting ${answered} of ${n}…` : sub, null);
    card.appendChild(barChart(points, { label: title }));
  };
  return { card, update };
}

/**
 * Every kind the router mirrors, plus any seen among the newest events, counted over the last
 * day. Kinds with nothing in the window fold into one line, so the table reads as what is moving.
 */
function kindsCard(live) {
  const card = el("div", "card");
  const head = el("div");
  const controls = el("div", "bar-row");
  const filter = el("input");
  filter.type = "search";
  filter.name = "kinds";
  filter.placeholder = "filter by kind number or name";
  filter.setAttribute("aria-label", "filter kinds");
  const count = el("span", "status");
  controls.append(filter, count);
  const wrap = el("div", "wrap tall");
  const quiet = el("p", "card-sub");
  card.append(head, controls, wrap, quiet);
  let rows = [];

  const applyFilter = () => {
    const q = filter.value.trim().toLowerCase();
    let shown = 0;
    for (const { tr, kind } of rows) {
      const hit = !q || String(kind).includes(q) || kindLabel(kind).toLowerCase().includes(q);
      tr.hidden = !hit;
      if (hit) shown++;
    }
    count.textContent = q ? `${fmt(shown)} of ${fmt(rows.length)}` : `${fmt(rows.length)} kinds`;
  };
  filter.addEventListener("input", applyFilter);

  let last = null;
  const update = () => {
    const kinds = live.kinds();
    const day = (k) => live.value(`kind.${k}.86400`);
    const hour = (k) => (day(k) === 0 ? 0 : live.value(`kind.${k}.3600`));
    const counted = kinds.filter((k) => day(k) != null);
    const moving = counted.filter((k) => day(k) > 0).sort((a, b) => day(b) - day(a) || a - b);
    const silent = counted.filter((k) => day(k) === 0);
    const key = JSON.stringify([moving.map((k) => [k, day(k), hour(k)]), silent.length, kinds.length]);
    if (key === last) return;
    last = key;

    head.replaceChildren();
    const scope = live.mirrored ? "the kinds this relay's router mirrors, plus any among the newest events" : "the kinds among the newest events";
    cardHead(head, "Kinds · last 24 hours", counted.length < kinds.length ? `Counting ${fmt(counted.length)} of ${fmt(kinds.length)} — ${scope}.` : `Every one of ${scope}.`, null);

    const max = moving.reduce((m, k) => Math.max(m, day(k)), 1);
    const table = el("table");
    const thead = el("thead");
    const hr = el("tr");
    for (const [text, cls] of [["Kind", ""], ["Name", ""], ["Last hour", "n"], ["Last 24h", "n"], ["", ""]]) hr.appendChild(el("th", cls, text));
    thead.appendChild(hr);
    table.appendChild(thead);
    const tbody = el("tbody");
    rows = moving.map((k) => {
      const tr = el("tr");
      tr.appendChild(el("td", "kind", String(k)));
      tr.appendChild(el("td", "name", kindLabel(k)));
      tr.appendChild(el("td", "n", hour(k) == null ? "—" : fmt(hour(k))));
      tr.appendChild(el("td", "n", fmt(day(k))));
      const share = el("td", "share");
      const bar = el("i");
      bar.style.width = Math.max(2, (day(k) / max) * 100) + "%";
      share.appendChild(bar);
      tr.appendChild(share);
      tbody.appendChild(tr);
      return { tr, kind: k };
    });
    table.appendChild(tbody);
    const scrolled = wrap.scrollTop;
    wrap.replaceChildren(table);
    wrap.scrollTop = scrolled;
    quiet.textContent = silent.length
      ? `${fmt(silent.length)} other kind${silent.length === 1 ? "" : "s"} had nothing in the last 24 hours: ${silent.join(", ")}.`
      : "";
    applyFilter();
  };
  return { card, update };
}

/**
 * Totals of kinds that hold one event per author, so each is a count of people. Whether ranked
 * search can use the observers' lists is /trust.html's question.
 */
function peoplePanel(live) {
  const card = el("div", "panel");
  const body = el("div");
  card.appendChild(body);
  const LABELS = {
    0: ["Profiles", "Pubkeys with a kind-0 profile. Replaceable: one per author."],
    3: ["Follow lists", "Pubkeys with a kind-3 follow list. Replaceable: one per author."],
    10002: ["Relay lists", "Pubkeys with a NIP-65 relay list. Replaceable: one per author."],
    10040: ["Trust observers", "Pubkeys with a kind-10040 list naming their NIP-85 score providers."],
  };
  const update = () => {
    body.replaceChildren();
    cardHead(body, "People and trust", null, null);
    const row = el("div", "tiles");
    for (const k of PEOPLE_KINDS) row.appendChild(tile(live, `people.${k}`, ...LABELS[k]).t);
    body.appendChild(fitTiles(row));
    const p = el("p", "card-sub");
    p.append("Whether the observers' providers reach ranked search is on ");
    const a = el("a", null, "the trust page");
    a.href = "./trust.html";
    p.append(a, ".");
    body.appendChild(p);
  };
  return { card, update };
}

/** The whole-store total: a walk over every document, so it is only ever asked by pressing. */
function storeCard(live) {
  const card = el("div", "card");
  const update = () => {
    card.replaceChildren();
    cardHead(card, "Whole store", null, null);
    const a = live.get("store");
    const row = el("div", "bar-row");
    const b = el("button", "btn", a ? "Count again" : "Count every event");
    const asking = live.countingStore;
    b.disabled = asking;
    b.addEventListener("click", () => live.countStore());
    row.appendChild(b);
    const status = el("span", "status");
    if (asking) status.textContent = "Counting…";
    else if (a && a.value != null) {
      status.textContent = `${fmt(a.value)} events, counted ${ago(a.at)} in ${(a.tookMs / 1000).toFixed(1)}s`;
      status.title = isoOf(a.at);
    } else if (a && a.error) status.textContent = `Not answered: ${a.error}.`;
    row.appendChild(status);
    card.appendChild(row);
    card.appendChild(el("p", "why",
      "Counting everything walks every document in the store, so it is not asked until you press. " +
      "The windows above are bounded by time and answer in milliseconds."));
  };
  return { card, update };
}

/**
 * The relay's live panels, in reading order, and one `update` that redraws them. Updates are
 * batched to a frame, since a page load answers several hundred asks.
 */
export function liveCards(live, socketUrl) {
  const parts = [
    nowPanel(live, socketUrl),
    // Headline totals, so they sit with the other headline numbers rather than under a long table.
    peoplePanel(live),
    bucketCard(live, {
      title: `Events per hour · last ${HOURS} hours`,
      sub: "UTC hours, by created_at",
      prefix: "hour",
      n: HOURS,
      width: HOUR,
      startOf: hourOf,
      tick: hourTick,
      label: (t) => `${dayTick(t)} ${hourTick(t)}–${hourTick(t).slice(0, 2)}:59 UTC`,
    }),
    bucketCard(live, {
      title: `Events per day · last ${DAYS} days`,
      sub: "UTC days, by created_at",
      prefix: "day",
      n: DAYS,
      width: DAY,
      startOf: dayOf,
      tick: dayTick,
      label: (t) => new Date(t * 1000).toISOString().slice(0, 10),
    }),
    kindsCard(live),
    storeCard(live),
  ];
  let queued = false;
  const update = () => {
    if (queued) return;
    queued = true;
    requestAnimationFrame(() => {
      queued = false;
      for (const p of parts) p.update();
    });
  };
  for (const p of parts) p.update();
  return { cards: parts.map((p) => p.card), update };
}
