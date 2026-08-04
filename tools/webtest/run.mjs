// The web UI's test suite, plain node — no test framework, no dependencies,
// matching the pages under test. Run from anywhere:
//
//     node tools/webtest/run.mjs           # the three module suites
//     node tools/webtest/navwalk.mjs       # + the browser walkthrough
//                                          #   (needs playwright + chromium)
//
// nip19.test.mjs   decoder vs an independently written test-side encoder
// cards.test.mjs   EVERY registered kind renders, preview and permalink —
//                  fails if a kind registers without a fixture, which is
//                  what keeps "covers all kinds" a checked claim
// relay.test.mjs   the NIP-42 CLOSED auth-required -> auth -> resend wiring
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";

let failed = 0;
for (const t of ["nip19.test.mjs", "cards.test.mjs", "relay.test.mjs"]) {
  const r = spawnSync(process.execPath, [fileURLToPath(new URL(t, import.meta.url))], { stdio: "inherit" });
  if (r.status !== 0) failed++;
}
process.exit(failed ? 1 : 0);
