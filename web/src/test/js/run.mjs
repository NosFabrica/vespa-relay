// The web UI's test suite: plain node, no framework, no dependencies, like the
// pages under test. Each *.test.mjs opens with what it covers. Run from anywhere:
//
//     ./gradlew :web:jsTest                  # `build` depends on it
//     node web/src/test/js/run.mjs           # every module suite, directly
import { spawnSync } from "child_process";
import { fileURLToPath } from "url";

let failed = 0;
for (const t of ["nip19.test.mjs", "query.test.mjs", "groups.test.mjs", "groupnames.test.mjs", "calendar.test.mjs", "feed.test.mjs", "paging.test.mjs", "asks.test.mjs", "cards.test.mjs", "provenance.test.mjs", "providers.test.mjs", "pointers.test.mjs", "related.test.mjs", "relay.test.mjs", "lens.test.mjs", "profiles.test.mjs", "parents.test.mjs", "preload.test.mjs", "filters.test.mjs", "help.test.mjs", "keynav.test.mjs", "avatar.test.mjs", "mirrors.test.mjs", "readiness.test.mjs", "verdicts.test.mjs", "sync.test.mjs", "processors.test.mjs", "paths.test.mjs", "source.test.mjs"]) {
  const r = spawnSync(process.execPath, [fileURLToPath(new URL(t, import.meta.url))], { stdio: "inherit" });
  if (r.status !== 0) failed++;
}
process.exit(failed ? 1 : 0);
