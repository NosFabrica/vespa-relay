// NIP-90 job requests: somebody asking a data vending machine to do a piece of work. Five kinds
// here, one shape — what is asked differs, how it is asked does not:
//
//   `i`      the inputs, `["i", <value>, <type>, <relay>, <marker>]`, one per input
//   `param`  the knobs, `["param", <name>, <value>…]`
//   `output` the mime type the requester wants back
//   `bid`    what they are willing to pay, in millisats
//   `p`      the machines they are asking, when they are asking particular ones
//
// Only the request half is here: the results (6050…) and the status events are the machine's
// side of the conversation and nothing mirrors them into this store.

import { esc, clip } from "../shared/format.js";
import {
  register, registerRow, shell, bodyHtml, chipRow, relayRows, faceStrip, noteHref, addrHref,
  tagOf, tagsOf, oneLine, plural, satsOf,
} from "./base.js";

const HEX64 = /^[0-9a-f]{64}$/;

/** What each kind asks for, in the words its own NIP-90 section uses. */
const ASKS = {
  5050: "asks for text",
  5100: "asks for an image",
  5250: "asks for speech",
  5302: "asks to search content",
  5303: "asks to search people",
};

/** The inputs, each with the type that says how to read it. */
const inputsOf = (ev) =>
  tagsOf(ev, "i").filter((t) => t[1]).map((t) => ({ value: oneLine(t[1]), type: oneLine(t[2]) }));

/** One input as a row: a type that names another event links to it, everything else is text. */
function inputRow(input) {
  const label = clip(input.value, 90);
  if (input.type === "event" && HEX64.test(input.value)) {
    return `<a class="mono" href="${noteHref(input.value)}">${esc(label)}</a>`;
  }
  if (input.type === "job" && HEX64.test(input.value)) {
    return `<a class="mono" href="${noteHref(input.value)}">${esc(label)}</a>`;
  }
  if (input.type === "url") return `<span class="mono">${esc(label)}</span>`;
  const href = input.value.includes(":") ? addrHref(input.value) : null;
  return href ? `<a href="${href}">${esc(label)}</a>` : `<span>${esc(label)}</span>`;
}

/** The knobs, as `name: value` chips — a param with no value says nothing worth a chip. */
const paramsOf = (ev) =>
  tagsOf(ev, "param").filter((t) => t[1] && t[2]).map((t) => `${oneLine(t[1])}: ${oneLine(t.slice(2).join(" "))}`);

/** 5050 / 5100 / 5250 / 5302 / 5303 — one job request. */
function jobRequestCard(ev, opts) {
  const inputs = inputsOf(ev);
  const bid = satsOf(tagOf(ev, "bid"));
  const machines = tagsOf(ev, "p").map((t) => t[1]).filter((pk) => HEX64.test(pk || ""));
  const relays = tagsOf(ev, "relays").flatMap((t) => t.slice(1)).map(oneLine).filter(Boolean);
  const full = opts && opts.full;
  const inner =
    `<div class="result-body">${esc(ASKS[ev.kind] || "asks for a job")}` +
    `${machines.length ? ` from ${esc(plural(machines.length, "machine"))}` : ""}</div>` +
    (bid ? `<div class="price-line">${esc(bid)} sats offered</div>` : "") +
    bodyHtml(opts, ev.content, 300) +
    (inputs.length
      ? `<ul class="ref-list">${inputs.map((i) => `<li>${inputRow(i)}${i.type ? ` <span class="muted-note">${esc(clip(i.type, 24))}</span>` : ""}</li>`).join("")}</ul>`
      : "") +
    chipRow(paramsOf(ev), opts) +
    faceStrip(machines, full ? 24 : 12) +
    (full && relays.length ? relayRows(relays.map((url) => ({ url })), opts) : "");
  return shell(ev, opts, inner, [
    ["wants back", tagOf(ev, "output") ? esc(clip(tagOf(ev, "output"), 40)) : null],
  ]);
}

register([5050, 5100, 5250, 5302, 5303], jobRequestCard);

// The ask leads; what was handed to the machine follows, since the content is often empty.
registerRow([5050, 5100, 5250, 5302, 5303], (ev) => ({
  name: ASKS[ev.kind] || "asks for a job",
  sub: [ev.content, inputsOf(ev).map((i) => i.value).join(" · ")].filter(Boolean).join(" · "),
}));
