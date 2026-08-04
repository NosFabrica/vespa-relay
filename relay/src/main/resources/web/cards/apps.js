// The app-ecosystem stragglers: NIP-89 handlers and recommendations, software
// applications, and feeds. Handler events carry a kind-0-shaped JSON content,
// so they render as a mini profile of the app.

import { esc, titleOf, summaryOf } from "../shared/format.js";
import { register, shell, bodyHtml, avatarHtml, tagsOf, tagOf, jsonContent, clipIf } from "./base.js";

/** 31990/32267 — an application, profile-shaped. */
function appCard(ev, opts) {
  const c = jsonContent(ev);
  const name = c.display_name || c.name || tagOf(ev, "name") || titleOf(ev);
  const icon = c.picture || c.icon || tagOf(ev, "icon");
  const about = c.about || tagOf(ev, "description") || summaryOf(ev);
  const inner = `
    <div class="result-main">
      ${icon ? avatarHtml(icon, ev.pubkey) : ""}
      <div class="text">
        ${name ? `<h2 class="result-title">${esc(clipIf(opts, name, 120))}</h2>` : ""}
        ${bodyHtml(opts, about || "", 400)}
      </div>
    </div>`;
  return shell(ev, opts, inner, [
    ["website", tagOf(ev, "web") ? `<a href="${esc(tagOf(ev, "web"))}" target="_blank" rel="noopener noreferrer">${esc(tagOf(ev, "web"))}</a>` : null],
  ]);
}

/** 31989 — a recommendation: which kind, handled by how many apps. */
function recommendationCard(ev, opts) {
  const forKind = tagOf(ev, "d");
  const handlers = tagsOf(ev, "a").length;
  const inner = `<div class="result-body">recommends ${handlers} handler${handlers === 1 ? "" : "s"}${forKind ? ` for kind ${esc(forKind)}` : ""}</div>`;
  return shell(ev, opts, inner);
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
