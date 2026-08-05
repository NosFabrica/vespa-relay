// The web UI's test suite, plain node — no test framework, no dependencies,
// matching the pages under test. Run from anywhere:
//
//     node tools/webtest/run.mjs           # every module suite
//
// nip19.test.mjs   decoder vs an independently written test-side encoder
// query.test.mjs   the search box's from:/to: language — what the field draws
//                  and what the relay is asked, held to ONE tokenizer
// cards.test.mjs   EVERY registered kind renders, preview and permalink —
//                  fails if a kind registers without a fixture, which is
//                  what keeps "covers all kinds" a checked claim
// relay.test.mjs   the NIP-42 CLOSED auth-required -> auth -> resend wiring
// profiles.test.mjs a lookup caches "no profile" only off a COMPLETE read —
//                  the rule two separate caches have now got wrong
// preload.test.mjs the modulepreload hints match the real import graph, so the
//                  module waterfall stays one round trip instead of three
// avatar.test.mjs  the one face renderer — and every size it names has a row
//                  in index.html's --av table, since a missing row draws a
//                  face at no size at all and throws nothing
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";

let failed = 0;
for (const t of ["nip19.test.mjs", "query.test.mjs", "cards.test.mjs", "relay.test.mjs", "profiles.test.mjs", "preload.test.mjs", "avatar.test.mjs"]) {
  const r = spawnSync(process.execPath, [fileURLToPath(new URL(t, import.meta.url))], { stdio: "inherit" });
  if (r.status !== 0) failed++;
}
process.exit(failed ? 1 : 0);
