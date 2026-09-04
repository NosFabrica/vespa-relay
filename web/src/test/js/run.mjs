// The web UI's test suite, plain node — no test framework, no dependencies,
// matching the pages under test. Run from anywhere:
//
//     ./gradlew :web:jsTest                  # …and `build` depends on it
//     node web/src/test/js/run.mjs           # every module suite, directly
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
// paging.test.mjs  the results view's pager — which prefix each page asks the
//                  relay for (there is no offset in NIP-50, so a page is a
//                  longer ask cut locally), that the ask in front of the reader
//                  is still ONE page with the three ahead fetched behind it,
//                  that a widened answer never renumbers the page on screen,
//                  and that the two ways a list ends — the corpus running out
//                  and this page declining to follow a ranking deeper — are
//                  told apart
// related.test.mjs what a git permalink shows UNDER its card — which filters
//                  answer "what else belongs to this", and the shape they come
//                  back as: a repository's lists newest-first, a thread's
//                  replies oldest-first, and the newest status as the verdict
// relay.test.mjs   the NIP-42 CLOSED auth-required -> auth -> resend wiring
// lens.test.mjs    what a read says about whose eyes it is read through —
//                  the `observer:`/`include:spam` declaration the relay now
//                  requires of every unauthenticated REQ, and the near-misses
//                  (a bech32 observer, `include:spammy`) that declare nothing
// profiles.test.mjs a lookup caches "no profile" only off a COMPLETE read —
//                  the rule two separate caches have now got wrong
// parents.test.mjs which `e` tag a reply answers and who wrote it — NIP-10's
//                  rule, plus that same complete-read rule on its cache
// provenance.test.mjs WHY an event is in a page — that two lists sharing a
//                  title collapse to one pill with a count rather than two
//                  identical chips, that a language label is not provenance,
//                  that a delegated source and an open one never share a tone,
//                  that a pill whose target is not on screen is not drawn, and
//                  that the whole row is independent of the page's ORDER — a
//                  member is placed by the confidence its list expressed, so it
//                  no longer arrives behind the pointer that named it
// providers.test.mjs whose word a reader took — the kind-10040 parse, and
//                  specifically the BARE `30392` shape that NIP-85's own tag
//                  parser has never matched, so a reader delegating lists that
//                  way resolves to nobody and draws no list pill at all
// pointers.test.mjs the follow-up read behind that row, now that a `kinds:[0]`
//                  search answers with the profiles and not the lists, labels
//                  and assertions that found them: that every declaration
//                  filter carries `authors` — the trust gate, moved off the
//                  relay and onto the client that lost it — that a kind
//                  delegated to nobody is not asked for openly, and that a
//                  pointer arriving BOTH ways is still one record
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
// processors.test.mjs the status pages' glossary lookup — that a member named
//                  after something on Object.prototype comes back as nothing
//                  rather than as a function assigned to an element's title.
//                  Several call sites look up a key the DOCUMENT chose
// paths.test.mjs   every asset reference the markup makes is DOCUMENT-relative
//                  and resolves — the thing that lets one status page be
//                  served behind `/sync/` as well as at a host root, and the
//                  one where a mistake half-loads (the relay at that root
//                  answers 200 with its own copy) instead of 404ing
// source.test.mjs  every file the page ships is TEXT — one NUL byte made git
//                  call searchfield.js binary, and four undiffable commits
//                  later one of them had deleted a function still being called
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";

let failed = 0;
for (const t of ["nip19.test.mjs", "query.test.mjs", "groups.test.mjs", "groupnames.test.mjs", "calendar.test.mjs", "feed.test.mjs", "paging.test.mjs", "asks.test.mjs", "cards.test.mjs", "provenance.test.mjs", "providers.test.mjs", "pointers.test.mjs", "related.test.mjs", "relay.test.mjs", "lens.test.mjs", "profiles.test.mjs", "parents.test.mjs", "preload.test.mjs", "filters.test.mjs", "help.test.mjs", "keynav.test.mjs", "avatar.test.mjs", "mirrors.test.mjs", "readiness.test.mjs", "verdicts.test.mjs", "sync.test.mjs", "processors.test.mjs", "paths.test.mjs", "source.test.mjs"]) {
  const r = spawnSync(process.execPath, [fileURLToPath(new URL(t, import.meta.url))], { stdio: "inherit" });
  if (r.status !== 0) failed++;
}
process.exit(failed ? 1 : 0);
