// The web UI's test suite, plain node — no test framework, no dependencies,
// matching the pages under test. Run from anywhere:
//
//     node tools/webtest/run.mjs           # every module suite
//
// nip19.test.mjs   decoder vs an independently written test-side encoder
// query.test.mjs   the search box's from:/to:, since:/until: and #hashtag
//                  language — what the field draws and what the relay is asked,
//                  held to ONE tokenizer
// groups.test.mjs  which NIP-29 group the picker offers for a half-typed
//                  `group:` — and that a 10009 tag's url and a 39000's signing
//                  key are never folded into one row, because nothing joins them
// groupnames.test.mjs what the `group:` PILL may draw over an id — your own
//                  list's name first, the corpus's only where its hosts agree,
//                  and the hex id wherever they do not
// calendar.test.mjs the month arithmetic the date picker draws: lengths, leads,
//                  DST-safe day steps, and the days the shortcuts write
// cards.test.mjs   EVERY registered kind renders, preview and permalink —
//                  fails if a kind registers without a fixture, which is
//                  what keeps "covers all kinds" a checked claim. And the
//                  same for the search field's TYPE-AHEAD row, whose
//                  registry is held to the card registry's key set: without
//                  it a kind falls back to a ladder ending in raw content,
//                  which is how a channel's row read
//                  `{"about":"","name":"Test group","picture":""}` beside a
//                  card drawing that channel properly
// feed.test.mjs    the latest feed — that its ask carries no NIP-50 search
//                  string (an ordered feed is a plain NIP-01 read), that every
//                  kind chip picks the kinds it names and survives the URL,
//                  and that replies, future dates and duplicates never reach
//                  a card
// related.test.mjs what a git permalink shows UNDER its card — which filters
//                  answer "what else belongs to this", and the shape they come
//                  back as: a repository's lists newest-first, a thread's
//                  replies oldest-first, and the newest status as the verdict
// relay.test.mjs   the NIP-42 CLOSED auth-required -> auth -> resend wiring
// profiles.test.mjs a lookup caches "no profile" only off a COMPLETE read —
//                  the rule two separate caches have now got wrong
// parents.test.mjs which `e` tag a reply answers and who wrote it — NIP-10's
//                  rule, plus that same complete-read rule on its cache
// preload.test.mjs the modulepreload hints match the real import graph, so the
//                  module waterfall stays one round trip instead of three
// filters.test.mjs every control in the filters panel is counted on the button
//                  and carried in the URL — the two things that keep a filter
//                  behind a closed disclosure from being an invisible one
// help.test.mjs    the syntax sheet against the tokenizer, both ways: a prefix
//                  query.js lifts and the sheet never names, and a token the
//                  sheet names that query.js leaves in the query as WORDS —
//                  which is a help page that reads like an answer
// keynav.test.mjs  j/k/Enter walk the results — which presses are a move at
//                  all (never the ones typed INTO the search box, which is a
//                  contenteditable div) and where a move lands
// mirrors.test.mjs  the kind bound a count against this relay must carry —
//                   read off /stats.json, and REFUSED rather than guessed,
//                   since an unscoped count is the "35% mirroring" a complete
//                   mirror was drawing
// verdicts.test.mjs what a signed kind-30166 record claims — fold vs cleared
//                   told apart AFTER normalising, a verdict aged on its own
//                   clock rather than the record's, and the two verdicts read
//                   independently. Every case here is one the Kotlin reader got
//                   wrong first, asserted in the direction it failed
// readiness.test.mjs which link of the trust chain a signed-in reader is
//                  missing — the ordering (first unmet link wins, everything
//                  below it waits) and the three rules the panel must not
//                  break: an unfinished read is not an absence, a non-answer
//                  is not a zero, and no denominator is not a percentage
// avatar.test.mjs  the one face renderer — and every size it names has a row
//                  in index.html's --av table, since a missing row draws a
//                  face at no size at all and throws nothing
// sync.test.mjs   what the sync card DECIDES over the router's progress
//                  document — which legs are worth naming, what each bar is a
//                  proportion of, whether a health object is drawable. The
//                  only pins that code ever had were string greps for member
//                  names in stats.html, and a grep cannot see a wrong
//                  denominator: five bugs shipped behind one
// source.test.mjs  every file the page ships is TEXT — one NUL byte made git
//                  call searchfield.js binary, and four undiffable commits
//                  later one of them had deleted a function still being called
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";

let failed = 0;
for (const t of ["nip19.test.mjs", "query.test.mjs", "groups.test.mjs", "groupnames.test.mjs", "calendar.test.mjs", "feed.test.mjs", "cards.test.mjs", "related.test.mjs", "relay.test.mjs", "profiles.test.mjs", "parents.test.mjs", "preload.test.mjs", "filters.test.mjs", "help.test.mjs", "keynav.test.mjs", "avatar.test.mjs", "mirrors.test.mjs", "readiness.test.mjs", "verdicts.test.mjs", "sync.test.mjs", "source.test.mjs"]) {
  const r = spawnSync(process.execPath, [fileURLToPath(new URL(t, import.meta.url))], { stdio: "inherit" });
  if (r.status !== 0) failed++;
}
process.exit(failed ? 1 : 0);
