// The web UI's test suite, plain node — no test framework, no dependencies,
// matching the pages under test. Run from anywhere:
//
//     node tools/webtest/run.mjs           # every module suite
//
// nip19.test.mjs   decoder vs an independently written test-side encoder
// query.test.mjs   the search box's from:/to:, since:/until: and #hashtag
//                  language — what the field draws and what the relay is asked,
//                  held to ONE tokenizer
// calendar.test.mjs the month arithmetic the date picker draws: lengths, leads,
//                  DST-safe day steps, and the days the shortcuts write
// cards.test.mjs   EVERY registered kind renders, preview and permalink —
//                  fails if a kind registers without a fixture, which is
//                  what keeps "covers all kinds" a checked claim
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
// avatar.test.mjs  the one face renderer — and every size it names has a row
//                  in index.html's --av table, since a missing row draws a
//                  face at no size at all and throws nothing
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";

let failed = 0;
for (const t of ["nip19.test.mjs", "query.test.mjs", "calendar.test.mjs", "cards.test.mjs", "relay.test.mjs", "profiles.test.mjs", "parents.test.mjs", "preload.test.mjs", "filters.test.mjs", "avatar.test.mjs"]) {
  const r = spawnSync(process.execPath, [fileURLToPath(new URL(t, import.meta.url))], { stdio: "inherit" });
  if (r.status !== 0) failed++;
}
process.exit(failed ? 1 : 0);
