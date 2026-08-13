// What a signed kind-30166 record CLAIMS — the tag semantics behind the
// monitor-verdicts panel.
//
// This is the half worth testing because it is the half that can be wrong
// silently. Every failure here draws a confident, well-formatted, wrong answer:
// a cleared verdict shown as a fold onto itself, a month-old verdict shown as
// current, a stability answer invisible because the url was never folded. Each
// of those is a bug the Kotlin reader made first, and each is asserted below in
// the direction it failed.
import assert from "node:assert/strict";
import {
  MONITOR_KIND, hostOf, sameUrl, readRecord, isCurrent, groupByHost, summarise, TTL_SECONDS,
} from "../../relay/src/main/resources/web/shared/verdicts.js";

const ok = (name) => console.log(`  ✓ ${name}`);

const NOW = 1786630000;
/** A record in the shape `RelayAliasRecord.edit` signs. */
const rec = (url, tags, at = NOW) => ({
  id: url + JSON.stringify(tags),
  kind: MONITOR_KIND,
  pubkey: "4391".padEnd(64, "0"),
  created_at: at,
  tags: [["d", url], ...tags],
});
const sameAs = (target, evidence, at) => ["same-as", target, evidence, String(at)];
const consistent = (v, evidence, at) => ["self-consistent", v, evidence, String(at)];

// ---- the host, which is what decides a GROUP -------------------------------
{
  assert.equal(hostOf("wss://nos.lol/"), "nos.lol");
  assert.equal(hostOf("wss://nos.lol/cipher-zulu"), "nos.lol");
  assert.equal(hostOf("wss://NOS.LOL:443/beacon"), "nos.lol", "port and case are not part of the host");
  assert.equal(hostOf("ws://nos.lol"), "nos.lol", "the scheme is not either — a host serving both is one host");
  // The colons inside an IPv6 literal are not the port separator. Grouping this
  // wrong would split one host into as many groups as it has urls, and the
  // panel would draw every one of them as unfoldable.
  assert.equal(hostOf("wss://[2001:db8::1]:443/alpha"), "[2001:db8::1]");
  assert.equal(hostOf("wss://[2001:db8::1]/alpha"), "[2001:db8::1]");
  ok("the host is the hostname alone — no port, no path, no scheme");
}

// ---- fold vs cleared, which are told apart AFTER normalising ---------------
{
  // The cleared form points at the record's OWN url. Compared by string,
  // `wss://nos.lol` vs `wss://nos.lol/` reads as a FOLD of a url onto itself —
  // which is a duplicate the panel would report and the router does not have.
  const r = readRecord(rec("wss://nos.lol/", [sameAs("wss://nos.lol", "500 newest events, best 4 shared", NOW)]));
  assert.equal(r.cleared, true, "a trailing slash must not turn a cleared verdict into a fold");
  assert.equal(r.fold, null);
  assert.equal(sameUrl("wss://nos.lol", "wss://nos.lol/"), true);
  assert.equal(sameUrl("wss://NOS.LOL/", "wss://nos.lol"), true, "the authority is case-insensitive");
  assert.equal(sameUrl("wss://nos.lol/alpha", "wss://nos.lol/"), false, "a path is not nothing");
  assert.equal(sameUrl("wss://nos.lol/Inbox", "wss://nos.lol/inbox"), false, "…and a path is opaque, so its case is kept");
  ok("cleared and folded are told apart after normalising, never by string");
}

{
  const r = readRecord(rec("wss://nos.lol/cipher-zulu", [sameAs("wss://nos.lol/", "500 newest events, 498 shared with wss://nos.lol/", NOW)]));
  assert.equal(r.fold, "wss://nos.lol/");
  assert.equal(r.cleared, false);
  assert.equal(r.foldEvidence, "500 newest events, 498 shared with wss://nos.lol/",
    "the evidence is the point of the panel — it is what says WHY, and it is the monitor's own sentence");
  ok("a fold names the url it folded onto and carries its evidence");
}

// ---- the two verdicts are read INDEPENDENTLY -------------------------------
{
  // A url may carry a fold, a stability answer, both or neither. An early
  // return for a missing `same-as` hid every stability verdict on a url that
  // was never folded — the exact bug the Kotlin reader had.
  const r = readRecord(rec("wss://flaky.example/", [consistent("false", "203 + 179 events at a 7d anchor, 128 shared -> 0.715", NOW)]));
  assert.equal(r.stable, false);
  assert.equal(r.fold, null);
  assert.equal(r.cleared, false, "no same-as tag is NOT a cleared verdict — it is no verdict at all");
  const both = readRecord(rec("wss://x.example/a", [sameAs("wss://x.example/", "e", NOW), consistent("true", "s", NOW)]));
  assert.equal(both.fold, "wss://x.example/");
  assert.equal(both.stable, true);
  ok("a stability answer is read whether or not the url was ever folded");
}

{
  // An answer this reader does not recognise is not a verdict. Guessing
  // "unstable" would draw a relay as refused from the fan-out on a tag we
  // cannot read.
  assert.equal(readRecord(rec("wss://x.example/", [consistent("maybe", "", NOW)])).stable, null);
  ok("an unrecognised stability value is ignored, not guessed at");
}

// ---- the verdict's clock, which is not the record's ------------------------
{
  // Kind 30166 is addressable and shared: quartz's monitor rewrites the record
  // every time this client connects to the relay, carrying our tags forward. So
  // `created_at` tracks the last time we TALKED to it, not the last time we
  // MEASURED it — and reading the record's clock made half these verdicts
  // immortal on the Kotlin side. The tag carries its own.
  const stale = NOW - TTL_SECONDS - 1;
  const r = readRecord(rec("wss://busy.example/a", [sameAs("wss://busy.example/", "e", stale)], NOW));
  assert.equal(r.foldMeasuredAt, stale, "the record was rewritten a second ago; the measurement was not");
  assert.equal(isCurrent(r.foldMeasuredAt, NOW), false);
  assert.equal(isCurrent(NOW - TTL_SECONDS + 60, NOW), true);
  ok("a verdict ages on when it was MEASURED, not on the record it rides on");
}

{
  // Records written before the measured-at element existed fall back to the
  // event's clock, which is the only honest reading available for them.
  const r = readRecord(rec("wss://old.example/a", [["same-as", "wss://old.example/", "e"]], NOW - 10));
  assert.equal(r.foldMeasuredAt, NOW - 10);
  ok("a tag with no measured-at falls back to the record's clock");
}

// ---- grouping, which is the whole point ------------------------------------
{
  const events = [
    rec("wss://h.example/", [sameAs("wss://h.example/", "500 newest events, best 6 shared with 2 compared endpoint(s) on this host", NOW)]),
    rec("wss://h.example/alpha", [sameAs("wss://h.example/", "500 newest events, 500 shared", NOW)]),
    rec("wss://h.example/beta", [sameAs("wss://h.example/", "500 newest events, 499 shared", NOW)]),
    // Same host, verdict aged out: the router would re-probe this today, so the
    // panel must not draw it as folded.
    rec("wss://h.example/gamma", [sameAs("wss://h.example/", "e", NOW - TTL_SECONDS - 1)]),
    // A record with no verdict tag at all — quartz's passive monitor writing
    // reachability. Counted as silent, never as a verdict.
    rec("wss://h.example/delta", [["n", "clearnet"], ["rtt-open", "120"]]),
    rec("wss://other.example/", [consistent("true", "s", NOW)]),
  ];
  const [first, second] = groupByHost(events, NOW);
  assert.equal(first.host, "h.example", "the host with the most to explain comes first");
  assert.equal(first.urls.length, 5);
  assert.equal(first.folded, 2, "only verdicts the router would still act on");
  assert.equal(first.cleared, 1);
  assert.equal(first.expired, 1);
  assert.equal(first.survivors, 3, "the cleared url, the expired one and the silent one are all still dialled");
  assert.equal(first.urls[0].url, "wss://h.example/", "shortest first puts the pathless url where it belongs");
  assert.equal(second.host, "other.example");

  const sum = summarise([first, second], NOW);
  assert.equal(sum.hosts, 2);
  assert.equal(sum.urls, 6);
  assert.equal(sum.folded, 2);
  assert.equal(sum.expired, 1);
  assert.equal(sum.silent, 1, "a record with no verdict tag separates 'the fold is not running' from 'nothing is'");
  assert.equal(sum.stable, 1);
  ok("urls group by host, and an expired verdict is not a fold");
}

{
  // 30166 is addressable, so a paged read can serve two versions of one
  // address. The newest wins, or a stale copy draws over the current verdict.
  const events = [
    rec("wss://h.example/a", [sameAs("wss://h.example/", "old", NOW - 500)], NOW - 500),
    rec("wss://h.example/a", [sameAs("wss://h.example/", "new", NOW)], NOW),
  ];
  const [group] = groupByHost(events, NOW);
  const real = group.urls.filter((u) => !u.synthetic);
  assert.equal(real.length, 1, "one address is one row");
  assert.equal(real[0].foldEvidence, "new");
  ok("two versions of one address collapse to the newest");
}

// ---- the survivor, which usually has no record of its own ------------------
{
  // `RelayAliases.learn` clears — publishes a verdict about — only a leader
  // that nothing folded onto. So the very url a group collapses TO is the one
  // url with no `same-as` to find, and a group read purely from records drew
  // "23 urls · 0 dialled" for a host that is dialled exactly once. Measured on
  // the live store: `articles.layer3.news`, 23 folds, survivor absent.
  const events = [
    rec("wss://a.example/alpha", [sameAs("wss://a.example/lantern", "e", NOW)]),
    rec("wss://a.example/beta", [sameAs("wss://a.example/lantern", "e", NOW)]),
  ];
  const [group] = groupByHost(events, NOW);
  assert.equal(group.urls.length, 3, "the url the folds point at is on the list");
  assert.equal(group.inferred, 1);
  assert.equal(group.survivors, 1, "…and it is what makes the host's dialled count 1 rather than 0");
  const survivor = group.urls.find((u) => u.synthetic);
  assert.equal(survivor.url, "wss://a.example/lantern");
  assert.equal(group.urls[0], survivor, "the survivor leads its own group");
  // Inferred, not stated. It carries no verdict BY CONSTRUCTION, so counting it
  // as silent would inflate the one number meaning "nothing has looked at this".
  const sum = summarise([group], NOW);
  assert.equal(sum.silent, 0);
  assert.equal(sum.inferred, 1);
  ok("a survivor with no record of its own is inferred from the folds that point at it");
}

{
  // Only when the target really is on this host. A hand-edited or malformed
  // record can point anywhere, and inventing a row for it would put one host's
  // url inside another host's group.
  const [group] = groupByHost([rec("wss://a.example/x", [sameAs("wss://elsewhere.example/", "e", NOW)])], NOW);
  assert.equal(group.urls.length, 1, "a fold pointing off-host synthesises nothing");
  assert.equal(group.inferred, undefined);
  // An EXPIRED fold points at nothing the router acts on, so its target is not
  // a survivor to draw either.
  const stale = groupByHost([rec("wss://b.example/x", [sameAs("wss://b.example/", "e", NOW - TTL_SECONDS - 1)])], NOW);
  assert.equal(stale[0].urls.length, 1);
  ok("nothing is synthesised for an off-host target or an expired fold");
}

{
  assert.deepEqual(groupByHost([], NOW), []);
  assert.deepEqual(summarise([], NOW), { hosts: 0, urls: 0, folded: 0, cleared: 0, expired: 0, silent: 0, stable: 0, unstable: 0, inferred: 0 });
  // A record with no `d` tag is not addressed at anything and cannot be drawn.
  assert.equal(readRecord({ tags: [["same-as", "wss://x.example/"]], created_at: NOW }), null);
  ok("an empty store and a record with no d tag are both handled, not thrown on");
}
