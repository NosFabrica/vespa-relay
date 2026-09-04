// NIP-66: relay discovery records and the monitors that publish them. This
// relay publishes both (RelayIdentity's key signs the announcement), so their
// cards explain its own output. A discovery record's fields are tags: the url
// is the `d`, the supported NIPs `N`, the software `s`. The NIP-11 document
// rides in `content` as JSON and is the only place a human-written name or
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
 * `T`/`R` the relay's type and requirements, shown as chips.
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

/** "monitors relays every 1h"; the frequency is what makes its records mean anything. */
const monitorLine = (ev) => {
  const every = everyN(tagOf(ev, "frequency"));
  return `monitors relays${every ? ` every ${every}` : ""}`;
};

/** 10166 — a monitor announcing itself: how often it checks, how long it waits, and which checks it runs. */
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

// A discovery record's only human-written words are in its NIP-11 JSON; the host is the fallback.
registerRow([30166], (ev) => {
  const nip11 = jsonContent(ev);
  const url = tagOf(ev, "d") || "";
  return { name: nip11.name || hostOf(url), sub: nip11.description || url };
});
registerRow([10166], (ev) => ({ name: monitorLine(ev), sub: ev.content }));
