// Which schema a stats document is judged against: a wrong answer prints a false "written for
// schema N" warning under a page that is reading its document correctly.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { schemaFor } from "../../main/resources/web/shared/statspage.js";

const ok = (name) => console.log(`  ✓ ${name}`);

/** What stats.html mounts with: the relay's document is versioned apart from the other two. */
const versions = { relay: 3, sync: 1, monitor: 1 };

const source = (path) => readFileSync(new URL(`../../../../${path}`, import.meta.url), "utf8");

{
  // The relay's counters tier lands first, carrying the mirror's manifest as `sync` and no `corpus`.
  const countersOnly = { schema: 3, relay: "wss://relay.example/", countedAs: "anonymous", sync: { status: "ok" } };
  assert.equal(schemaFor(countersOnly, versions), versions.relay, "the relay's counters tier was read as a mirror document");
  const both = { ...countersOnly, corpus: { status: "ok" } };
  assert.equal(schemaFor(both, versions), versions.relay);
  ok("the relay's document is the relay's before and after its charts tier lands");
}

{
  assert.equal(schemaFor({ schema: 1, sync: { status: "ok" } }, versions), versions.sync);
  assert.equal(schemaFor({ schema: 1, relay: "wss://relay.example/", monitor: { status: "ok" } }, versions), versions.monitor);
  assert.equal(schemaFor({ schema: 1 }, versions), versions.relay, "an unrecognised document falls back to the strictest");
  ok("the mirror's and the monitor's documents are their own");
}

{
  // The rule above leans on which publisher names a relay at top level; the Kotlin is the contract.
  assert.match(source("relay/src/main/kotlin/com/nosfabrica/vespa/relay/maintenance/StatsRollup.kt"), /put\("relay", relayUrl\)/);
  assert.doesNotMatch(source("sync/src/main/kotlin/com/nosfabrica/vespa/relay/status/SyncStatus.kt"), /put\("relay"/);
  ok("the relay's rollup names its relay and the mirror's status never does");
}

{
  // The page, the rollup and this suite name one relay schema between them.
  const html = source("web/src/main/resources/stats.html");
  assert.match(html, new RegExp(`const SCHEMA = ${versions.relay};`), "stats.html and this suite disagree on the relay schema");
  assert.match(
    source("relay/src/main/kotlin/com/nosfabrica/vespa/relay/maintenance/StatsRollup.kt"),
    new RegExp(`const val SCHEMA_VERSION = ${versions.relay}\\b`),
    "the rollup and this suite disagree on the relay schema",
  );
  // Only the relay's page counts its store over a socket: the mirror's and the monitor's have none to count.
  assert.match(html, /const isRelayDoc = \(doc\) => !!doc\.relay && !doc\.monitor;/);
  ok("only the relay's document mounts the live panels, and every reader agrees on its schema");
}
