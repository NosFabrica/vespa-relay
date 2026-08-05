// kind 0 — the profile card, rendered purely from the event, no server-side
// numbers. The one card with its own frame (big avatar header) instead of the
// shared byline shell.

import { esc } from "../shared/format.js";
import { npub, shortNpub } from "../shared/nip19.js";
import { displayName, parseProfile } from "../shared/profiles.js";
import { register, avatarHtml, badgeHtml, extLink, jsonHtml, propsHtml, keyHref, clipIf, clampCls } from "./base.js";

function profileCard(ev, opts) {
  const p = parseProfile(ev);
  const name = displayName(p) || shortNpub(ev.pubkey);
  const props = [];
  if (p.nip05) {
    props.push(["nip05",
      `<span class="nip05" data-addr="${esc(p.nip05)}" data-pk="${esc(ev.pubkey)}">` +
      `${esc(p.nip05)}<span class="n5chip checking" title="checking with the domain…">…</span></span>`]);
  }
  if (p.website) props.push(["website", extLink(p.website)]);
  if (p.lud16) props.push(["lightning", esc(p.lud16)]);
  // The pubkey row only when the profile carries no name. With one, the name
  // IS how the person is shown — the npub stays in the page URL and one click
  // away behind "json", instead of trailing every named human as a hex-shaped
  // string nobody reads.
  if (!displayName(p)) props.push(["pubkey", `<a class="mono" href="${keyHref(ev.pubkey)}" title="${esc(npub(ev.pubkey))}">${esc(shortNpub(ev.pubkey))}</a>`]);
  const about = clipIf(opts, p.about, 400);
  return `
    <article class="result${opts && opts.full ? " full" : ""}" data-id="${esc(ev.id)}">
      <div class="result-header">
        ${avatarHtml(p.picture, ev.pubkey)}
        <div class="who">
          <h2 class="result-name"><a href="${keyHref(ev.pubkey)}">${esc(name)}</a></h2>
          ${(p.name || "").trim() && (p.name || "").trim() !== name ? `<div class="result-display">${esc(p.name)}</div>` : ""}
        </div>
        ${badgeHtml(ev)}
      </div>
      ${about ? `<div class="result-body${clampCls(opts)}">${esc(about)}</div>` : ""}
      ${propsHtml(props)}
      ${jsonHtml(ev)}
    </article>`;
}

register([0], profileCard);
