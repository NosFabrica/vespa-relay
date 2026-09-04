// kind 0 — the profile card, rendered from the event alone. The one card
// with its own frame (big avatar header) instead of the shared byline shell.

import { esc } from "../shared/format.js";
import { npub, shortNpub } from "../shared/nip19.js";
import { displayName, parseProfile } from "../shared/profiles.js";
import { avatarHtml } from "../shared/avatar.js";
import { register, registerRow, badgeHtml, extLink, jsonHtml, propsHtml, provHtml, keyHref, selfHref, clipIf, clampCls } from "./base.js";

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
  // The pubkey row only when the profile carries no name; the npub is in the page url and under "json".
  if (!displayName(p)) props.push(["pubkey", `<a class="mono" href="${keyHref(ev.pubkey)}" title="${esc(npub(ev.pubkey))}">${esc(shortNpub(ev.pubkey))}</a>`]);
  const about = clipIf(opts, p.about, 400);
  // This frame is hand-rolled, so everything shell() does for a card is done
  // here too: the click target (the person's page, not this revision's id)
  // and the provenance row, which a spliced profile needs most.
  const href = opts && opts.full ? null : selfHref(ev);
  return `
    <article class="result${opts && opts.full ? " full" : ""}" data-id="${esc(ev.id)}"${href ? ` data-href="${href}"` : ""}>
      <div class="result-header">
        ${avatarHtml(p.picture, ev.pubkey, "xl")}
        <div class="who">
          <h2 class="result-name"><a href="${keyHref(ev.pubkey)}">${esc(name)}</a></h2>
          ${(p.name || "").trim() && (p.name || "").trim() !== name ? `<div class="result-display">${esc(p.name)}</div>` : ""}
        </div>
        ${badgeHtml(ev)}
      </div>
      ${provHtml(ev, opts)}
      ${about ? `<div class="result-body${clampCls(opts)}">${esc(about)}</div>` : ""}
      ${propsHtml(props)}
      ${jsonHtml(ev)}
    </article>`;
}

register([0], profileCard);
// `self`: the row's subject is its author, so the second line must not repeat
// the name. Named from the event, so a profile the cache has not learned yet
// still gets its own name rather than an npub.
registerRow([0], (ev) => {
  const p = parseProfile(ev);
  return { name: displayName(p) || shortNpub(ev.pubkey), sub: p.about, pic: p.picture, self: true };
});
