// Records of what somebody did or saw: a workout and the exercise it was built from, a bird
// sighting and the life list it goes into. None is defined by a merged NIP, all four are lax by
// construction — every tag optional, units guessed at where they are missing — so nothing here
// computes a value the publisher did not state.

import { esc, clip, titleOf, summaryOf } from "../shared/format.js";
import {
  register, registerRow, shell, titleHtml, bodyHtml, chipRow, extLink, safeUrl,
  tagOf, tagsOf, oneLine, fmtDuration, plural,
} from "./base.js";

// ---- 1301 / 33401, the workouts --------------------------------------------

/** A measure as it was published: the number, then the unit in its second slot. */
const measure = (ev, name, fallbackUnit = "") => {
  const tag = tagsOf(ev, name)[0];
  const value = oneLine(tag && tag[1]);
  if (!value) return "";
  const unit = oneLine(tag[2]) || fallbackUnit;
  return unit ? `${value} ${unit}` : value;
};

/**
 * A duration as published: `HH:MM:SS` from the clients that write it that way, a count of
 * seconds from the ones that do not. A value that is neither reads back as itself.
 */
function workoutDuration(ev) {
  const raw = oneLine(tagOf(ev, "duration"));
  if (!raw) return "";
  return /^\d+$/.test(raw) ? fmtDuration(raw) || raw : raw;
}

/** The line a workout is scanned for: how far, how long, how hard. */
const workoutStats = (ev) => [
  measure(ev, "distance", "km"),
  workoutDuration(ev),
  measure(ev, "elevation_gain", "m") && `↑ ${measure(ev, "elevation_gain", "m")}`,
  oneLine(tagOf(ev, "calories")) && `${oneLine(tagOf(ev, "calories"))} kcal`,
  oneLine(tagOf(ev, "steps")) && `${oneLine(tagOf(ev, "steps"))} steps`,
  oneLine(tagOf(ev, "avg_heart_rate")) && `${oneLine(tagOf(ev, "avg_heart_rate"))} bpm`,
].filter(Boolean).join(" · ");

/** The strength half, which a run never carries and a lift always does. */
const strengthStats = (ev) => [
  oneLine(tagOf(ev, "sets")) && `${oneLine(tagOf(ev, "sets"))} sets`,
  oneLine(tagOf(ev, "reps")) && `${oneLine(tagOf(ev, "reps"))} reps`,
  measure(ev, "weight", "kg"),
].filter(Boolean).join(" · ");

/** 1301 — a workout record: the stats line the app leads with, then the notes. */
function workoutCard(ev, opts) {
  const type = oneLine(tagOf(ev, "type"));
  const stats = [workoutStats(ev), strengthStats(ev)].filter(Boolean).join(" · ");
  const inner =
    titleHtml(opts, titleOf(ev), 140) +
    (type ? `<div class="result-body muted">${esc(clip(type, 60))}</div>` : "") +
    (stats ? `<div class="price-line">${esc(clip(stats, 200))}</div>` : "") +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner, [
    ["max heart rate", tagOf(ev, "max_heart_rate") ? esc(clip(tagOf(ev, "max_heart_rate"), 12)) : null],
    ["descent", measure(ev, "elevation_loss", "m") ? esc(measure(ev, "elevation_loss", "m")) : null],
    ["recorded by", tagOf(ev, "source") ? esc(clip(tagOf(ev, "source"), 40)) : null],
  ]);
}

/** 33401 — an exercise template: the definition a workout's sets point at. */
function exerciseCard(ev, opts) {
  const about = [tagOf(ev, "equipment"), tagOf(ev, "difficulty")].map(oneLine).filter(Boolean).join(" · ");
  const inner =
    titleHtml(opts, titleOf(ev), 140) +
    (about ? `<div class="result-body muted">${esc(clip(about, 80))}</div>` : "") +
    bodyHtml(opts, ev.content, 400);
  // `format` and `format_units` are one tag each carrying every column, not one tag per column.
  const columns = (name) => (tagsOf(ev, name)[0] || []).slice(1).map(oneLine).filter(Boolean);
  const measured = columns("format").map((c, i) => [c, columns("format_units")[i]].filter(Boolean).join(" in "));
  return shell(ev, opts, inner, [
    ["measured in", measured.length ? esc(clip(measured.join(" · "), 120)) : null],
  ]);
}

// ---- 2473 / 12473, the birds -----------------------------------------------

/**
 * The species a birdstar event names, paired with the reference that follows each one: `n` is
 * the scientific name and the next `i` is its Wikidata entity. Paired by position, because that
 * is how the app writes them and there is nothing else tying the two together.
 */
function speciesOf(ev) {
  const out = [];
  for (const t of (ev && ev.tags) || []) {
    if (!Array.isArray(t)) continue;
    if (t[0] === "n" && oneLine(t[1])) out.push({ name: oneLine(t[1]), ref: "" });
    else if (t[0] === "i" && oneLine(t[1]) && out.length && !out[out.length - 1].ref) {
      out[out.length - 1].ref = oneLine(t[1]);
    }
  }
  return out;
}

/** A species name, linked to its reference where the reference is a url this page would follow. */
const speciesRows = (species, opts) => {
  const shown = opts && opts.full ? species : species.slice(0, 12);
  const more = species.length - shown.length;
  if (!shown.length) return "";
  const row = (s) => (safeUrl(s.ref)
    ? `<li>${extLink(s.ref, s.name)}</li>`
    : `<li>${esc(clip(s.name, 80))}</li>`);
  return `<ul class="ref-list">${shown.map(row).join("")}` +
    `${more > 0 ? `<li class="muted-note">…and ${more} more</li>` : ""}</ul>`;
};

/** 2473 — one sighting. The `alt` is the publisher's own sentence for it, so it leads. */
function birdCard(ev, opts) {
  const species = speciesOf(ev);
  const first = species[0];
  const inner =
    titleHtml(opts, first ? first.name : summaryOf(ev), 140) +
    bodyHtml(opts, summaryOf(ev) && first ? summaryOf(ev) : "", 200, true) +
    bodyHtml(opts, ev.content, 200);
  return shell(ev, opts, inner, [
    ["species", first && safeUrl(first.ref) ? extLink(first.ref, "Wikidata") : null],
    // The `g` is a geohash: a place, at whatever precision the publisher chose to give.
    ["where", tagOf(ev, "g") ? `<span class="mono">${esc(clip(tagOf(ev, "g"), 24))}</span>` : null],
  ]);
}

/** 12473 — the life list: every species this author has logged, newest event replacing the last. */
function birdexCard(ev, opts) {
  const species = speciesOf(ev);
  const inner =
    `<div class="result-body">${esc(plural(species.length, "species", "species"))}</div>` +
    bodyHtml(opts, summaryOf(ev), 200, true) +
    speciesRows(species, opts);
  return shell(ev, opts, inner);
}

register([1301], workoutCard);
register([33401], exerciseCard);
register([2473], birdCard);
register([12473], birdexCard);

registerRow([1301], (ev) => ({
  name: titleOf(ev) || oneLine(tagOf(ev, "type")) || "a workout",
  sub: [workoutStats(ev), strengthStats(ev), ev.content].filter(Boolean).join(" · "),
}));
registerRow([33401], (ev) => ({
  name: titleOf(ev),
  sub: [[tagOf(ev, "equipment"), tagOf(ev, "difficulty")].map(oneLine).filter(Boolean).join(" · "), ev.content]
    .filter(Boolean).join(" · "),
}));
registerRow([2473], (ev) => {
  const first = speciesOf(ev)[0];
  return { name: first ? first.name : summaryOf(ev) || "a sighting", sub: first ? summaryOf(ev) : "" };
});
registerRow([12473], (ev) => {
  const species = speciesOf(ev);
  return {
    name: plural(species.length, "species", "species"),
    sub: species.slice(0, 6).map((s) => s.name).join(", "),
  };
});
