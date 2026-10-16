// OpenAgents' public discovery records: a capability head (NIP-CAP 30180), a plugin's listing
// (NIP-EXT 30184) and one immutable release of it (NIP-EXT 3184). All three carry their text
// inside JSON content — no title or summary tag — so the generic floor would show the raw JSON;
// each card reads the body instead.
//
// What a record describes is a claim of its publisher's: nothing here says a capability works
// or that a release is safe to install, and the cards do not phrase it as if it did.

import { esc, clip } from "../shared/format.js";
import {
  register, registerRow, registerNamedPeople, shell, bodyHtml, titleHtml, chipRow, relayRows, personLink, eventHref,
  extLink, tagsOf, tagOf, jsonContent, oneLine, satsOf, fmtBytes,
} from "./base.js";

const HEX64 = /^[0-9a-f]{64}$/;

/** `<root>:<slug>` → the slug, the part a person reads. */
const slugOf = (pkg) => {
  const s = String(pkg || "");
  const i = s.indexOf(":");
  return i === 64 ? s.slice(65) : "";
};

/** `<publisher>:<package>/<component>` → `package/component`. */
const pathOf = (id) => {
  const s = String(id || "");
  return s.indexOf(":") === 64 && s.includes("/") ? s.slice(65) : "";
};

/** The `oa:<prefix>…` markers on the event, with the prefix taken off. */
const markers = (ev, prefix) =>
  tagsOf(ev, "t").map((t) => String(t[1] || "")).filter((v) => v.startsWith(prefix)).map((v) => v.slice(prefix.length));

// ---- 30184: a plugin's listing ---------------------------------------------

const listingOf = (ev) => {
  const c = jsonContent(ev);
  return {
    title: oneLine(c.title),
    description: typeof c.description === "string" ? c.description : "",
    slug: slugOf(c.package) || tagOf(ev, "d"),
    hidden: c.state === "hidden",
    release: c.release && HEX64.test(c.release.id || "") ? c.release : null,
  };
};

/** 30184 — a plugin: what its publisher calls it and the release it points at now. */
function listingCard(ev, opts) {
  const l = listingOf(ev);
  const inner =
    titleHtml(opts, l.title || l.slug) +
    bodyHtml(opts, l.description, 400) +
    (l.hidden ? `<div class="muted-note">hidden by its publisher</div>` : "");
  return shell(ev, opts, inner, [
    ["package", l.slug ? `<span class="mono">${esc(clip(l.slug, 64))}</span>` : null],
    ["current release", l.release
      ? `<a class="mono" href="${eventHref(l.release.id, { author: l.release.pubkey, kind: 3184 })}">${esc(l.release.id.slice(0, 12))}…</a>`
      : null],
  ]);
}

// ---- 3184: one release -------------------------------------------------------

const releaseOf = (ev) => {
  const c = jsonContent(ev);
  const manifest = c.manifest && typeof c.manifest === "object" ? c.manifest : {};
  return {
    slug: slugOf(c.package),
    version: oneLine(c.version),
    fee: satsOf(c.fee_msat),
    payout: oneLine(c.payout),
    digest: oneLine(manifest.digest),
    size: Number(manifest.size),
  };
};

/** What a release charges per call: "10 sats per call", or "free". */
const feeLine = (r) => (r.fee ? `${r.fee} sats per call` : "free");

/** 3184 — one immutable version of a plugin, bound to its manifest's exact bytes. */
function releaseCard(ev, opts) {
  const r = releaseOf(ev);
  const heading = [r.slug, r.version].filter(Boolean).join(" ");
  const inner =
    titleHtml(opts, heading || "plugin release") +
    `<div class="result-body">${esc(feeLine(r))}</div>`;
  return shell(ev, opts, inner, [
    ["package", r.slug ? `<span class="mono">${esc(clip(r.slug, 64))}</span>` : null],
    ["version", r.version ? esc(clip(r.version, 40)) : null],
    ["paid to", r.fee && r.payout ? `<span class="mono">${esc(clip(r.payout, 72))}</span>` : null],
    ["manifest", r.digest ? `<span class="mono">${esc(clip(r.digest, 24))}</span>` : null],
    ["manifest size", Number.isFinite(r.size) && r.size >= 0 ? esc(fmtBytes(r.size)) : null],
  ]);
}

// ---- 30180: a capability head ------------------------------------------------

const capabilityOf = (ev) => {
  const c = jsonContent(ev);
  const contract = c.binding_contract && typeof c.binding_contract === "object" ? c.binding_contract : {};
  const remote = contract.remote && typeof contract.remote === "object" ? contract.remote : {};
  const lanes = contract.service && Array.isArray(contract.service.lanes) ? contract.service.lanes : [];
  const workers = [remote, ...lanes]
    .filter((w) => w && HEX64.test(w.worker || ""))
    .map((w) => ({ worker: w.worker, relays: Array.isArray(w.relays) ? w.relays.map(oneLine).filter(Boolean) : [] }));
  const operations = [contract.operation, ...(Array.isArray(contract.operations) ? contract.operations : [])]
    .map(oneLine).filter(Boolean);
  return {
    path: pathOf(c.id) || pathOf(c.definition && c.definition.id) || tagOf(ev, "d"),
    summary: typeof c.summary === "string" ? c.summary : "",
    head: !c.id && !!c.definition,
    iface: oneLine(contract.interface),
    endpoint: oneLine(remote.endpoint),
    paid: Array.isArray(c.requires) && c.requires.includes("oa-x402-v1"),
    operations,
    workers,
  };
};

/** 30180 — operations a publisher says it serves, and where to reach them. */
function capabilityCard(ev, opts) {
  const c = capabilityOf(ev);
  const profile = markers(ev, "oa:profile:")[0] || "";
  const transports = markers(ev, "oa:transport:");
  const flags = [c.paid ? "paid (x402)" : "", c.head ? "definition published elsewhere" : "",
    tagsOf(ev, "t").some((t) => t[1] === "oa:test") ? "test listing" : "",
    tagsOf(ev, "t").some((t) => t[1] === "oa:dev") ? "dev listing" : ""].filter(Boolean);
  const relays = c.workers.flatMap((w) => w.relays);
  const inner =
    titleHtml(opts, c.path || "capability") +
    bodyHtml(opts, c.summary, 400) +
    chipRow(c.operations, opts) +
    (flags.length ? `<div class="muted-note">${esc(flags.join(" · "))}</div>` : "") +
    (opts && opts.full && relays.length ? relayRows([...new Set(relays)].map((url) => ({ url })), opts) : "");
  return shell(ev, opts, inner, [
    ["profile", profile ? esc(clip(profile, 32)) : null],
    ["transport", transports.length ? esc(transports.join(", ")) : null],
    ["interface", c.iface ? `<span class="mono">${esc(clip(c.iface, 72))}</span>` : null],
    ["served by", c.workers.length ? c.workers.map((w) => personLink(w.worker)).join(", ") : null],
    ["endpoint", c.endpoint ? extLink(c.endpoint) : null],
  ]);
}

register([30184], listingCard);
register([3184], releaseCard);
register([30180], capabilityCard);

// The workers a capability names are drawn as people, so the page loads their profiles first.
registerNamedPeople([30180], (ev) => [...new Set(capabilityOf(ev).workers.map((w) => w.worker))]);

// Each row leads with what a person searched for: the plugin's title, the release's package and
// version, the capability's path; the second line is what it does or charges.
registerRow([30184], (ev) => {
  const l = listingOf(ev);
  return { name: l.title || l.slug, sub: l.title ? l.description : "" };
});
registerRow([3184], (ev) => {
  const r = releaseOf(ev);
  return { name: [r.slug, r.version].filter(Boolean).join(" ") || "plugin release", sub: feeLine(r) };
});
registerRow([30180], (ev) => {
  const c = capabilityOf(ev);
  return { name: c.path || "capability", sub: c.summary };
});
