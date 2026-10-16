// The relay's live panels on stats.html, drawn from [LiveCounts] as its answers arrive. Every
// number is one this page asked the relay for; an ask that has not answered is an em dash, never
// a zero.

import { ago, cardHead, el, fitTiles, fmt, isoOf } from "../shared/page.js";
import { PEOPLE_KINDS } from "./live.js";

/** Past this the newest-event tile is coloured. Not a verdict: a serve-only relay may be quiet. */
const STALE_CORPUS_SECONDS = 24 * 3600;

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
  const update = () => {
    card.replaceChildren();
    cardHead(card, "Right now", null, null);
    const row = el("div", "tiles");

    // Freshness is the headline: a relay whose mirror died keeps its event count.
    const newest = tile(live, "newest", "Newest event", "The newest created_at this relay holds, bounded at the present.", (t) => ago(t));
    const at = live.value("newest");
    if (at != null) {
      if (Math.floor(Date.now() / 1000) - at > STALE_CORPUS_SECONDS) {
        newest.v.classList.add("stale");
        newest.t.title =
          "The newest event this relay holds is over a day old. Either the mirror has stopped, or this " +
          "deployment serves without one — this page cannot tell which.";
      } else {
        // The ISO first; the refresh's failure note, when there is one, stays beside it.
        newest.t.title = [isoOf(at), live.get("newest").error ? newest.t.title : ""].filter(Boolean).join(" — ");
      }
    }
    row.appendChild(newest.t);
    row.appendChild(tile(live, "last.60", "Last minute", "Events created in the last 60 seconds that this relay holds now.").t);
    row.appendChild(tile(live, "last.3600", "Last hour", "Events created in the last hour that this relay holds now.").t);
    card.appendChild(fitTiles(row));

    const m = live.value("last.60");
    const facts = [];
    if (m != null) facts.push(`${perSecond(m)} over the last minute`);
    facts.push(`counted live over ${socketUrl}`);
    if (live.paused) facts.push("paused while this tab is hidden");
    card.appendChild(el("p", "card-sub", facts.join(" · ") + "."));
    if (live.connError) card.appendChild(el("p", "err", `Cannot reach this relay's websocket: ${live.connError}. Retrying.`));
  };
  return { card, update };
}

/** Totals of kinds that hold one event per author, so each is a count of people. */
function peoplePanel(live) {
  const card = el("div", "panel");
  const LABELS = {
    0: ["Profiles", "Pubkeys with a kind-0 profile. Replaceable: one per author."],
    3: ["Follow lists", "Pubkeys with a kind-3 follow list. Replaceable: one per author."],
    10002: ["Relay lists", "Pubkeys with a NIP-65 relay list. Replaceable: one per author."],
  };
  const update = () => {
    card.replaceChildren();
    cardHead(card, "People and trust", null, null);
    const row = el("div", "tiles");
    for (const k of PEOPLE_KINDS) row.appendChild(tile(live, `people.${k}`, ...LABELS[k]).t);
    card.appendChild(fitTiles(row));
  };
  return { card, update };
}

/**
 * The relay's live panels, in reading order, and one `update` that redraws them. Updates are
 * batched to a frame, since a page load answers several asks at once.
 */
export function liveCards(live, socketUrl) {
  const parts = [nowPanel(live, socketUrl), peoplePanel(live)];
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
