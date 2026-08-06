// A month, as arithmetic. The half of the date picker that has no DOM in it.
//
// Split out for the reason query.js is: this is the part that can be tested
// without a browser, and it is the part with the traps in it. A month grid
// looks like counting and is not —
//
//   - a local day is 23 or 25 hours twice a year, so stepping by days has to
//     be done in local date FIELDS and never by adding 86,400,000;
//   - February has four different lengths;
//   - the week starts on a different day depending on who is reading, and the
//     blanks before the 1st move with it;
//   - `new Date(y, m, 0)` is the last day of month `m - 1`, which is either an
//     elegant way to count a month's days or an off-by-one waiting to happen,
//     depending entirely on whether anything checks.
//
// tools/webtest/calendar.test.mjs checks. Everything here is a pure function of
// its arguments and the locale; the field renderer (searchfield.js) turns what
// comes back into buttons, and owns nothing about which days exist.
//
// Dates in and out are LOCAL and at midnight, matching what query.js's dayBound
// means by a day — the two have to agree or the square the reader clicks and
// the second the relay is asked would drift by a timezone.

import { ymd } from "./query.js";

/** The same day, at 00:00 local — the only shape the rest of this file passes. */
export const midnight = (d) => new Date(d.getFullYear(), d.getMonth(), d.getDate());

/**
 * `n` days later, by local date fields rather than by milliseconds.
 *
 * The distinction is the whole reason this is a function: `+ n * 86400000`
 * lands an hour out on the two days a year the clocks move, which is how a
 * "last 7 days" shortcut quietly becomes six days and 23 hours.
 */
export const shiftDays = (d, n) => new Date(d.getFullYear(), d.getMonth(), d.getDate() + n);

/** The 1st of the month `n` months away — the grid's own unit of position. */
export const shiftMonths = (d, n) => new Date(d.getFullYear(), d.getMonth() + n, 1);

export const sameMonth = (a, b) => !!a && !!b && a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth();
export const sameDay = (a, b) => sameMonth(a, b) && a.getDate() === b.getDate();

// The locale's answers, asked ONCE. Neither the language nor the week's first
// day changes while a page is open, and `toLocaleDateString` builds a fresh
// formatter on every call — measured at 0.077ms against 0.0015ms for a kept
// one, times the 46 calls a calendar render makes. That was 3.5ms of the 6.4ms
// an arrow-key repeat cost, which is most of a frame per keypress.
const fmt = (opts) => new Intl.DateTimeFormat(undefined, opts);
const DAY_FMT = fmt({ day: "numeric", month: "short", year: "numeric" });
const MONTH_FMT = fmt({ month: "long", year: "numeric" });

/** A day in the reader's own spelling — what a pill and a grid cell say out loud. */
export const dayLabel = (d) => DAY_FMT.format(d);
/** The month above the grid, likewise. */
export const monthLabel = (d) => MONTH_FMT.format(d);

/**
 * Which weekday a week starts on, 0 = Sunday.
 *
 * The reader's own locale where the browser will say (Chrome and Safari carry
 * Intl.Locale.weekInfo) and ISO Monday where it will not (Firefox, and Node,
 * which has no `navigator` at all). A calendar that starts the week on the
 * wrong day is not so much wrong as unreadable: the columns stop being where
 * the eye already put them.
 */
export function weekStart() {
  try {
    // weekInfo counts 1..7 from Monday; JS dates count 0..6 from Sunday.
    const first = new Intl.Locale(navigator.language).weekInfo?.firstDay;
    if (first) return first % 7;
  } catch (e) { /* no weekInfo here — ISO it is */ }
  return 1;
}

export const WEEK_START = weekStart();

/**
 * The seven column headings, in the order this locale's week runs.
 *
 * 4 January 2026 is a Sunday, so `4 + start + i` walks one week from whichever
 * day starts it. Narrow for the heading and long for the hover, because a
 * column headed "T" is two different days and the tooltip is where that gets
 * resolved without widening the grid.
 */
export const dowNames = (start = WEEK_START) =>
  Array.from({ length: 7 }, (_, i) => new Date(2026, 0, 4 + start + i)).map((d) => ({
    narrow: fmt({ weekday: "narrow" }).format(d),
    long: fmt({ weekday: "long" }).format(d),
  }));

export const DOW = dowNames();

/**
 * One month as the cells a seven-column grid draws.
 *
 *   { label, lead, days: [{ at, value, today, ahead }] }
 *
 * `lead` is how many blanks come before the 1st so it lands under its own
 * weekday — the grid is seven columns wide and a month that started on the
 * wrong one would misdate every square in it. `value` is the `YYYY-MM-DD` the
 * token is written as, so the renderer never re-derives it and cannot disagree
 * with query.js about which day a cell means.
 *
 * `today` is passed in rather than read: it makes this a pure function of its
 * arguments, and it is the only way a test can ask what the grid looks like on
 * a day that is not the day the test runs.
 */
export function monthGrid(month, today, start = WEEK_START) {
  const y = month.getFullYear();
  const m = month.getMonth();
  // Day 0 of the NEXT month is the last day of this one. Terse, and the reason
  // the test above pins February in a leap year and out of one.
  const count = new Date(y, m + 1, 0).getDate();
  return {
    label: monthLabel(new Date(y, m, 1)),
    lead: (new Date(y, m, 1).getDay() - start + 7) % 7,
    days: Array.from({ length: count }, (_, i) => {
      const at = new Date(y, m, i + 1);
      return { at, value: ymd(at), today: sameDay(at, today), ahead: at > today };
    }),
  };
}

/**
 * The month a half-typed date names, or null while it names none.
 *
 * `since:2026-01` should not leave the grid sitting on this month: the reader
 * has already said which one they mean, and making them arrow back to it is
 * the picker ignoring what is in the box. The DAY half is deliberately not
 * read — `2026-01-3` is not a day yet — and neither is a partial year, or the
 * grid would lurch through the year 2 on the way to 2026.
 */
export function typedMonth(partial) {
  const m = /^(\d{4})-(\d{2})/.exec(String(partial || ""));
  if (!m) return null;
  const [y, mo] = m.slice(1).map(Number);
  if (mo < 1 || mo > 12) return null;
  const at = new Date(y, mo - 1, 1);
  // The two-digit-year rule again (query.js's dayBound says it in full): `26`
  // is 1926, so a year that did not survive the constructor is not a year.
  return at.getFullYear() === y ? at : null;
}

/**
 * The shortcuts under the grid, as days rather than as durations.
 *
 * Different for the two prefixes because they answer different questions: a
 * `since` is the start of a window ("the last week"), an `until` is a cutoff
 * ("before last week"). Each resolves to an ABSOLUTE day, and what gets written
 * into the box is that day — a saved URL reading `since:7d` would mean a
 * different search every morning it was opened, which is not what a link is.
 */
const QUICK = {
  since: [["Today", 0], ["Last 7 days", -6], ["Last 30 days", -29], ["Last 90 days", -89]],
  until: [["Today", 0], ["Yesterday", -1], ["A week ago", -7], ["A month ago", -30]],
};

export const quickPicks = (field, today) =>
  (QUICK[field] || []).map(([label, off]) => ({ label, value: ymd(shiftDays(today, off)) }));
