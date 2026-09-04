// The app-ecosystem stragglers: NIP-89 handlers and recommendations, software
// applications, and feeds. Handler events carry a kind-0-shaped JSON content,
// so they render as a mini profile of the app.

import { esc, titleOf, summaryOf } from "../shared/format.js";
import { avatarHtml } from "../shared/avatar.js";
import { register, registerRow, shell, bodyHtml, extLink, tagsOf, tagOf, jsonContent, clipIf, plural } from "./base.js";

/**
 * What an app calls itself, wherever it put it: a NIP-89 handler carries a
 * kind-0-shaped JSON content, and the same three fields also ride as tags for
 * the clients that write them there.
 */
const appOf = (ev) => {
  const c = jsonContent(ev);
  return {
    name: c.display_name || c.name || tagOf(ev, "name") || titleOf(ev),
    icon: c.picture || c.icon || tagOf(ev, "icon"),
    about: c.about || tagOf(ev, "description") || summaryOf(ev),
  };
};

/** 31990/32267 — an application, profile-shaped. */
function appCard(ev, opts) {
  const { name, icon, about } = appOf(ev);
  const inner = `
    <div class="result-main">
      ${icon ? avatarHtml(icon, ev.pubkey) : ""}
      <div class="text">
        ${name ? `<h2 class="result-title">${esc(clipIf(opts, name, 120))}</h2>` : ""}
        ${bodyHtml(opts, about || "", 400)}
      </div>
    </div>`;
  return shell(ev, opts, inner, [
    ["website", extLink(tagOf(ev, "web"))],
  ]);
}

/** "recommends 2 handlers for kind 30023", the whole of what a 31989 says. */
const recommendsLine = (ev) => {
  const forKind = tagOf(ev, "d");
  return `recommends ${plural(tagsOf(ev, "a").length, "handler")}${forKind ? ` for kind ${forKind}` : ""}`;
};

/** 31989 — a recommendation: which kind, handled by how many apps. */
function recommendationCard(ev, opts) {
  return shell(ev, opts, `<div class="result-body">${esc(recommendsLine(ev))}</div>`);
}

/** 31890 — a feed definition. */
function feedCard(ev, opts) {
  const inner =
    (titleOf(ev) ? `<h2 class="result-title">${esc(clipIf(opts, titleOf(ev), 140))}</h2>` : "") +
    bodyHtml(opts, summaryOf(ev) || ev.content, 300);
  return shell(ev, opts, inner);
}

register([31990, 32267], appCard);
register([31989], recommendationCard);
register([31890], feedCard);

// An app's JSON content is a profile in all but kind, so its row is one too.
registerRow([31990, 32267], (ev) => {
  const { name, icon, about } = appOf(ev);
  return { name, sub: about, pic: icon };
});
registerRow([31989], (ev) => ({ name: recommendsLine(ev) }));
registerRow([31890], (ev) => ({ name: titleOf(ev), sub: summaryOf(ev) || ev.content }));
