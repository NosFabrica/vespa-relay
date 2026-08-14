// NIP-66: relay discovery records and the monitors that publish them. These
// two get cards for the same reason 10040 and 30382 do — this relay publishes
// them. It runs as a NIP-66 monitor (RelayIdentity's key signs the
// announcement), so a 30166 permalink reading "kind 30166" over a hex blob
// would be the relay failing to explain its own output.
//
// The discovery record is the interesting one: it is a statement ABOUT a
// relay, and every field that matters is a tag — the url is the `d`, the
// supported NIPs are `N` tags, the software is `s`. The NIP-11 document rides
// in `content` as JSON and is the only place a human-written name or
// description lives, so it is read rather than printed.

import { esc } from "../shared/format.js";
import { register, registerRow, shell, titleHtml, bodyHtml, chipRow, relayRows, jsonContent, tagOf, tagsOf } from "./base.js";

const hostOf = (url) => String(url || "").replace(/^wss?:\/\//i, "").replace(/\/+$/, "");

/** Seconds as a period a person reads: "30m", "6h", "1d". */
const everyN = (secs) => {
  const n = Number(secs);
  if (!Number.isFinite(n) || n <= 0) return null;
  if (n < 60) return `${Math.round(n)}s`;
  if (n < 3600) return `${Math.round(n / 60)}m`;
  if (n < 86400) return `${Math.round(n / 3600)}h`;
  return `${Math.round(n / 86400)}d`;
};

/**
 * 30166 — one relay, as this monitor found it. `N` tags are NIP numbers and
 * `T`/`R` the relay's type and requirements; they are shown as chips because
 * "supports 1, 11, 42, 50, 65" is the answer somebody browsing relays wants,
 * and a props table of five one-word rows is not.
 */
function relayDiscoveryCard(ev, opts) {
  const url = tagOf(ev, "d") || "";
  const nip11 = jsonContent(ev);
  const nips = tagsOf(ev, "N").map((t) => t[1]).filter(Boolean);
  const reqs = tagsOf(ev, "R").map((t) => t[1]).filter(Boolean);
  const types = tagsOf(ev, "T").map((t) => t[1]).filter(Boolean);
  const inner =
    titleHtml(opts, nip11.name || hostOf(url), 140) +
    (url ? `<div class="result-body"><span class="mono">${esc(url)}</span></div>` : "") +
    bodyHtml(opts, nip11.description || "", 300) +
    chipRow([...types, ...reqs, ...nips.map((n) => `NIP-${n}`)], opts);
  return shell(ev, opts, inner, [
    ["software", tagOf(ev, "s") ? esc(tagOf(ev, "s")) : null],
    ["network", tagOf(ev, "n") ? esc(tagOf(ev, "n")) : null],
    ["location", tagOf(ev, "l") ? esc(tagOf(ev, "l")) : null],
  ]);
}

/** "monitors relays every 1h" — the frequency being what makes its records mean anything. */
const monitorLine = (ev) => {
  const every = everyN(tagOf(ev, "frequency"));
  return `monitors relays${every ? ` every ${every}` : ""}`;
};

/**
 * 10166 — a monitor announcing itself: how often it checks, how long it waits,
 * and which checks it runs. The frequency is the field that decides whether a
 * negative record from this monitor means anything, so it is stated in words
 * rather than as a raw seconds count.
 */
function relayMonitorCard(ev, opts) {
  const checks = tagsOf(ev, "c").map((t) => t[1]).filter(Boolean);
  const kinds = tagsOf(ev, "k").map((t) => t[1]).filter(Boolean);
  const timeouts = tagsOf(ev, "timeout").map((t) => `${t[1]}: ${t[2]}ms`);
  const inner =
    `<div class="result-body">${esc(monitorLine(ev))}</div>` +
    chipRow([...checks, ...kinds.map((k) => `kind ${k}`)], opts) +
    bodyHtml(opts, ev.content, 300) +
    (timeouts.length && opts && opts.full ? relayRows(timeouts.map((t) => ({ url: t })), opts) : "");
  return shell(ev, opts, inner);
}

register([30166], relayDiscoveryCard);
register([10166], relayMonitorCard);

// The rows. A discovery record's only human-written words are in the NIP-11
// document it carries as JSON — so the row printed that document, in a search
// where every result was one. The host is the fallback, and it is the thing a
// reader recognises anyway.
registerRow([30166], (ev) => {
  const nip11 = jsonContent(ev);
  const url = tagOf(ev, "d") || "";
  return { name: nip11.name || hostOf(url), sub: nip11.description || url };
});
registerRow([10166], (ev) => ({ name: monitorLine(ev), sub: ev.content }));
