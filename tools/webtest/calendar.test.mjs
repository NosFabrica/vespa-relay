// The month arithmetic behind `since:`/`until:` — the half of the date picker
// that has no DOM in it.
//
// It is split out of searchfield.js for the reason query.js is: it is the part
// that can be checked without a browser, and it is the part with the traps. A
// grid of days looks like counting, and every assertion below is a way it is
// not — a February with four possible lengths, a week that starts on a
// different day per reader, and a local day that is 23 or 25 hours twice a
// year. None of those show up as a failure anywhere else; a wrong grid just
// quietly offers the wrong date under the reader's finger.
import assert from "assert";

const cal = await import(new URL("../../web/src/main/resources/web/shared/calendar.js", import.meta.url));
const { midnight, shiftDays, shiftMonths, sameDay, sameMonth, monthGrid, typedMonth, quickPicks, dowNames, dayLabel } = cal;

const D = (y, m, d) => new Date(y, m - 1, d);

// ---- the day, as local date FIELDS -----------------------------------------
//
// `+ n * 86400000` is the version that breaks, and it breaks twice a year in
// whichever timezone the reader is in. These run in the runner's zone on
// purpose: the suite is run under several (see the README of this directory's
// query test), and a shift that lands an hour out lands on a different DAY.

assert.strictEqual(midnight(new Date(2026, 7, 6, 23, 59, 59)).getHours(), 0, "midnight strips the clock");
assert.deepStrictEqual(
  [shiftDays(D(2026, 8, 6), 1), shiftDays(D(2026, 8, 6), -1)].map((d) => d.getDate()),
  [7, 5],
  "a day either way is a day",
);
assert.strictEqual(shiftDays(D(2026, 8, 31), 1).getMonth(), 8, "…and rolls into September");
assert.strictEqual(shiftDays(D(2026, 1, 1), -1).getFullYear(), 2025, "…and back over a new year");
// Every day of a year, stepped one at a time, must stay at midnight: this is
// what an hours-based shift cannot do across a DST boundary.
for (let d = D(2026, 1, 1), n = 0; n < 365; n++, d = shiftDays(d, 1)) {
  assert.strictEqual(d.getHours(), 0, `${d} is still midnight after ${n} steps`);
}

assert.strictEqual(shiftMonths(D(2026, 8, 6), 0).getDate(), 1, "a month is positioned by its 1st");
assert.strictEqual(shiftMonths(D(2026, 1, 31), 1).getMonth(), 1, "…so stepping from the 31st cannot skip February");
assert.strictEqual(shiftMonths(D(2026, 1, 15), -1).getFullYear(), 2025, "…and December is last year");

assert(sameDay(D(2026, 8, 6), new Date(2026, 7, 6, 13, 4)), "the same day at a different hour is the same day");
assert(!sameDay(D(2026, 8, 6), D(2027, 8, 6)), "…and the same date in another year is not");
assert(!sameMonth(D(2026, 8, 6), D(2027, 8, 6)), "nor the same month");
assert(!sameDay(null, D(2026, 8, 6)) && !sameMonth(D(2026, 8, 6), null), "nothing is never the same as something");

// ---- the grid ---------------------------------------------------------------

const TODAY = D(2026, 8, 6);

// February is the whole reason `new Date(y, m + 1, 0)` is used instead of a
// lookup table: four lengths, and the century rule nobody remembers.
assert.strictEqual(monthGrid(D(2026, 2, 1), TODAY).days.length, 28, "February 2026 has 28 days");
assert.strictEqual(monthGrid(D(2024, 2, 1), TODAY).days.length, 29, "…2024 has 29");
assert.strictEqual(monthGrid(D(1900, 2, 1), TODAY).days.length, 28, "…1900 has 28 — divisible by 100, not by 400");
assert.strictEqual(monthGrid(D(2000, 2, 1), TODAY).days.length, 29, "…2000 has 29 — divisible by 400");
assert.deepStrictEqual(
  [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].map((m) => monthGrid(D(2026, m, 1), TODAY).days.length),
  [31, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
  "and the other eleven are the lengths they have always been",
);

// The blanks before the 1st are what put a day under its own weekday. 1 August
// 2026 is a Saturday (getDay() 6), so the lead is 6 from a Sunday week and 5
// from a Monday one — and every rotation in between has to work, because the
// browser reads it off the reader's locale.
assert.strictEqual(D(2026, 8, 1).getDay(), 6, "1 August 2026 is a Saturday — the fixture this rests on");
assert.deepStrictEqual(
  [0, 1, 2, 3, 4, 5, 6].map((start) => monthGrid(D(2026, 8, 1), TODAY, start).lead),
  [6, 5, 4, 3, 2, 1, 0],
  "the lead blanks rotate with the week's first day",
);
// A month starting ON the week's first day needs no blanks at all — the case a
// `lead` that forgot its modulo would return 7 for, pushing the 1st a row down.
assert.strictEqual(monthGrid(D(2026, 2, 1), TODAY, 0).lead, 0, "1 February 2026 is a Sunday: no blanks from a Sunday week");
assert.strictEqual(monthGrid(D(2026, 6, 1), TODAY, 1).lead, 0, "1 June 2026 is a Monday: none from a Monday week");

const aug = monthGrid(D(2026, 8, 1), TODAY);
assert.strictEqual(aug.days[0].value, "2026-08-01", "a cell carries the token spelling of its day");
assert.strictEqual(aug.days[30].value, "2026-08-31", "…to the last of them");
assert.strictEqual(aug.days.filter((d) => d.today).length, 1, "exactly one day is today");
assert.strictEqual(aug.days.find((d) => d.today).value, "2026-08-06", "…and it is the one passed in");
assert.deepStrictEqual(
  [aug.days[4].ahead, aug.days[5].ahead, aug.days[6].ahead],
  [false, false, true],
  "ahead starts the day AFTER today — today is not in its own future",
);
// Today outside the shown month must not mark anything, or the grid would claim
// a day in March is today because the reader stepped back to March.
assert.strictEqual(monthGrid(D(2026, 3, 1), TODAY).days.some((d) => d.today), false, "another month has no today in it");
assert.strictEqual(monthGrid(D(2026, 3, 1), TODAY).days.every((d) => !d.ahead), true, "…and a past month is all past");
assert(monthGrid(D(2026, 3, 1), TODAY).label.includes("2026"), "the heading names the year, whatever the locale calls March");

// ---- typedMonth: following what the box says --------------------------------

assert.strictEqual(typedMonth("2026-08").getMonth(), 7, "a typed YYYY-MM moves the grid");
assert.strictEqual(typedMonth("2026-08-06").getMonth(), 7, "…and so does the prefix of a full date");
assert.strictEqual(typedMonth("2026-08").getDate(), 1, "…landing on the 1st, which is how a month is positioned");
assert.strictEqual(typedMonth("2026-1"), null, "a half-typed month names none");
assert.strictEqual(typedMonth("202"), null, "…and neither does a half-typed year");
assert.strictEqual(typedMonth(""), null, "…nor an empty partial, which is where the calendar opens");
assert.strictEqual(typedMonth("2026-13"), null, "there is no thirteenth month");
assert.strictEqual(typedMonth("2026-00"), null, "…nor a zeroth");
assert.strictEqual(typedMonth("0026-01"), null, "…and 0026 is not a year the constructor keeps");

// ---- the shortcuts ----------------------------------------------------------
//
// Absolute days, not durations: a saved URL reading `since:7d` would mean a
// different search every morning it was opened, which is not what a link is.

assert.deepStrictEqual(
  quickPicks("since", TODAY).map((p) => [p.label, p.value]),
  [["Today", "2026-08-06"], ["Last 7 days", "2026-07-31"], ["Last 30 days", "2026-07-08"], ["Last 90 days", "2026-05-09"]],
  "the since shortcuts are the starts of the windows they name",
);
// "Last 7 days" has to be seven days INCLUDING today, or the label is a lie:
// the window is `since` to now, and both ends count.
const week = quickPicks("since", TODAY).find((p) => p.label === "Last 7 days");
assert.strictEqual(shiftDays(D(2026, 7, 31), 6).getDate(), 6, "…and the seventh day of it is today");
assert.strictEqual(week.value, "2026-07-31", "…which is what makes it -6 and not -7");
assert.deepStrictEqual(
  quickPicks("until", TODAY).map((p) => p.value),
  ["2026-08-06", "2026-08-05", "2026-07-30", "2026-07-07"],
  "the until shortcuts are cutoffs, so they read backwards from today",
);
assert.deepStrictEqual(quickPicks("nonsense", TODAY), [], "a field with no shortcuts has none, rather than throwing");

// ---- the headings -----------------------------------------------------------

for (const start of [0, 1, 6]) {
  const names = dowNames(start);
  assert.strictEqual(names.length, 7, "seven columns, whichever day starts them");
  assert.strictEqual(new Set(names.map((n) => n.long)).size, 7, "…and seven distinct days in them");
  // Narrow names repeat (T is Tuesday and Thursday in English), which is
  // exactly why the long name is carried alongside for the hover.
  assert(names.every((n) => n.narrow && n.long), "both spellings, for the column and for the tooltip");
}
assert.strictEqual(
  dowNames(0)[0].long,
  dowNames(6)[1].long,
  "a rotation is a rotation: Sunday first, or Saturday first and Sunday second",
);
assert(dayLabel(D(2026, 8, 6)).includes("2026"), "a day label names its year — a search window across years must be readable");

console.log("calendar: months, leads, DST-safe day steps and the shortcuts they write");
