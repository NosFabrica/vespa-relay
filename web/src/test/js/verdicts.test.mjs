// What a signed kind-30166 record claims: the tag semantics behind the monitor-verdicts panel.
import assert from "node:assert/strict";
import {
  MONITOR_KIND, hostOf, sameUrl, readRecord, isCurrent, groupByHost, summarise, walkRecords, TTL_SECONDS,
  FOLD_EPOCH, CONSISTENCY_EPOCH, FITNESS_EPOCH, FITNESS_NAMESPACE, PRIME, NETWORK_TOR,
} from "../../main/resources/web/shared/verdicts.js";

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
// A fixture without an epoch is a record from an older build, a case of its own below.
const sameAs = (target, evidence, at, epoch = FOLD_EPOCH) => ["same-as", target, evidence, String(at), epoch];
const consistent = (v, evidence, at, epoch = CONSISTENCY_EPOCH) => ["self-consistent", v, evidence, String(at), epoch];
// NIP-32 shape: the namespace takes index 2, so evidence starts at 3.
const grade = (value, evidence, at, epoch = FITNESS_EPOCH) => ["l", value, FITNESS_NAMESPACE, evidence, String(at), epoch];

// ---- the host, which decides a group ---------------------------------------
{
  assert.equal(hostOf("wss://nos.lol/"), "nos.lol");
  assert.equal(hostOf("wss://nos.lol/cipher-zulu"), "nos.lol");
  assert.equal(hostOf("wss://NOS.LOL:443/beacon"), "nos.lol", "port and case are not part of the host");
  assert.equal(hostOf("ws://nos.lol"), "nos.lol", "the scheme is not either — a host serving both is one host");
  // The colons inside an IPv6 literal are not the port separator.
  assert.equal(hostOf("wss://[2001:db8::1]:443/alpha"), "[2001:db8::1]");
  assert.equal(hostOf("wss://[2001:db8::1]/alpha"), "[2001:db8::1]");
  ok("the host is the hostname alone — no port, no path, no scheme");
}

// ---- fold vs cleared, told apart after normalising -------------------------
{
  // The cleared form points at the record's own url, compared after normalising.
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

// ---- the two verdicts are read independently ------------------------------
{
  // An early return for a missing same-as would hide the stability verdict on a url never folded.
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
  // Guessing "unstable" would draw a relay as refused on a tag this reader cannot read.
  assert.equal(readRecord(rec("wss://x.example/", [consistent("maybe", "", NOW)])).stable, null);
  ok("an unrecognised stability value is ignored, not guessed at");
}

// ---- the verdict's clock, which is not the record's ------------------------
{
  // created_at is rewritten on every connect; the tag carries when the verdict was measured.
  const stale = NOW - TTL_SECONDS - 1;
  const r = readRecord(rec("wss://busy.example/a", [sameAs("wss://busy.example/", "e", stale)], NOW));
  assert.equal(r.foldMeasuredAt, stale, "the record was rewritten a second ago; the measurement was not");
  assert.equal(isCurrent(r.foldMeasuredAt, NOW, r.foldEpoch, FOLD_EPOCH), false);
  assert.equal(isCurrent(NOW - TTL_SECONDS + 60, NOW, FOLD_EPOCH, FOLD_EPOCH), true);
  ok("a verdict ages on when it was MEASURED, not on the record it rides on");
}

{
  // An unstamped verdict must not borrow the record's clock, which is rewritten on every connect.
  const r = readRecord(rec("wss://old.example/a", [["same-as", "wss://old.example/", "e"]], NOW - 10));
  assert.equal(r.foldMeasuredAt, null, "an unstamped verdict must not borrow the record's clock");
  assert.equal(isCurrent(r.foldMeasuredAt, NOW, r.foldEpoch, FOLD_EPOCH), false);
  assert.equal(r.foldEvidence, "e", "…and it is still READ — the panel shows what the record says, expired or not");
  ok("a tag with no measured-at is stale, not dated from the record");
}

// ---- the rules a verdict was measured under --------------------------------
{
  // A verdict measured under superseded rules is inside the TTL and still not acted on.
  const old = readRecord(rec("wss://x.example/a", [sameAs("wss://x.example/", "e", NOW, "1")]));
  assert.equal(old.fold, "wss://x.example/", "the record still SAYS this — being superseded is not being unreadable");
  assert.equal(old.foldEpoch, "1");
  assert.equal(isCurrent(old.foldMeasuredAt, NOW, old.foldEpoch, FOLD_EPOCH), false,
    "a verdict measured seconds ago under old rules is inside the TTL and still not acted on");

  // The host counts it as expired, the number that says a re-measure is outstanding.
  const [group] = groupByHost([rec("wss://x.example/a", [sameAs("wss://x.example/", "e", NOW, "1")])], NOW);
  assert.equal(group.folded, 0);
  assert.equal(group.expired, 1);
  ok("a verdict from superseded rules reads as expired, whatever its age");
}

{
  // The cleared half expires too, or it falls out of every counter on the page.
  const self = (url, at, epoch) => rec(url, [sameAs(url, "500 newest events, best 2 shared", at, epoch)]);
  const [aged] = groupByHost([self("wss://a.example/x", NOW - TTL_SECONDS - 1)], NOW);
  assert.equal(aged.cleared, 0, "a cleared verdict past its TTL is not a verdict the router acts on");
  assert.equal(aged.expired, 1);

  const [superseded] = groupByHost([self("wss://b.example/x", NOW, "1")], NOW);
  assert.equal(superseded.cleared, 0);
  assert.equal(superseded.expired, 1);
  assert.equal(summarise([aged, superseded], NOW).silent, 0, "a retired verdict is not the same as never having looked");
  ok("a cleared verdict is counted as expired when the router has retired it");
}

// ---- grouping, which is the whole point ------------------------------------
{
  const events = [
    rec("wss://h.example/", [sameAs("wss://h.example/", "500 newest events, best 6 shared with 2 compared endpoint(s) on this host", NOW)]),
    rec("wss://h.example/alpha", [sameAs("wss://h.example/", "500 newest events, 500 shared", NOW)]),
    rec("wss://h.example/beta", [sameAs("wss://h.example/", "500 newest events, 499 shared", NOW)]),
    // Aged out: the router would re-probe this today, so it is not a fold.
    rec("wss://h.example/gamma", [sameAs("wss://h.example/", "e", NOW - TTL_SECONDS - 1)]),
    // No verdict tag at all: quartz's passive monitor writing reachability.
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
  // A refusal past its TTL or epoch is re-measured by the router, so it stops counting here too.
  const events = [
    rec("wss://flaky.example/", [consistent("false", "e", NOW)]),
    rec("wss://aged.example/", [consistent("false", "e", NOW - TTL_SECONDS - 1)]),
    rec("wss://ruled.example/", [consistent("false", "e", NOW, "0")]),
    rec("wss://steady.example/", [consistent("true", "s", NOW - TTL_SECONDS - 1)]),
  ];
  const groups = groupByHost(events, NOW);
  const sum = summarise(groups, NOW);
  assert.equal(sum.unstable, 1, "only the refusal the router still acts on may count as a refusal");
  assert.equal(sum.stable, 0, "an aged-out 'consistent' is re-measured, not trusted");
  const urls = groups.flatMap((g) => g.urls);
  assert.equal(urls.find((u) => u.url === "wss://flaky.example/").stableCurrent, true);
  assert.equal(urls.find((u) => u.url === "wss://aged.example/").stableCurrent, false,
    "…and the row says so, so the card can draw the verdict struck rather than hidden");
  assert.equal(urls.find((u) => u.url === "wss://ruled.example/").stableCurrent, false,
    "a refusal measured under superseded rules is a reading of a different rule, not a standing refusal");
  ok("a stability verdict past its TTL or epoch stops counting, and the row can say why");
}

{
  // 30166 is addressable, so a paged read can serve two versions of one address.
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
  // The url a group collapses to usually has no record of its own.
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
  // Inferred, so it carries no verdict by construction and is not silent.
  const sum = summarise([group], NOW);
  assert.equal(sum.silent, 0);
  assert.equal(sum.inferred, 1);
  ok("a survivor with no record of its own is inferred from the folds that point at it");
}

{
  // A malformed record can point anywhere; a row for it would put one host's url in another's group.
  const [group] = groupByHost([rec("wss://a.example/x", [sameAs("wss://elsewhere.example/", "e", NOW)])], NOW);
  assert.equal(group.urls.length, 1, "a fold pointing off-host synthesises nothing");
  assert.equal(group.inferred, undefined);
  // An expired fold points at nothing the router acts on.
  const stale = groupByHost([rec("wss://b.example/x", [sameAs("wss://b.example/", "e", NOW - TTL_SECONDS - 1)])], NOW);
  assert.equal(stale[0].urls.length, 1);
  ok("nothing is synthesised for an off-host target or an expired fold");
}

{
  assert.deepEqual(groupByHost([], NOW), []);
  assert.deepEqual(summarise([], NOW), {
    hosts: 0, urls: 0, folded: 0, cleared: 0, expired: 0, silent: 0, stable: 0, unstable: 0, inferred: 0, graded: 0, prime: 0,
    primeTor: 0,
  });
  // A record with no `d` tag is not addressed at anything and cannot be drawn.
  assert.equal(readRecord({ tags: [["same-as", "wss://x.example/"]], created_at: NOW }), null);
  ok("an empty store and a record with no d tag are both handled, not thrown on");
}

// ---- the fitness grade, which is a NIP-32 label ----------------------------
{
  // `s` is the relay's software to every monitor on the network; the grade rides a NIP-32 label.
  const r = readRecord({
    created_at: NOW,
    tags: [
      ["d", "wss://good.example/"],
      grade(PRIME, "answered 20 events at a settled anchor", NOW),
      ["L", FITNESS_NAMESPACE],
      ["s", "git+https://github.com/hoytech/strfry.git"],
    ],
  });
  assert.equal(r.grade, PRIME);
  assert.equal(r.software, "git+https://github.com/hoytech/strfry.git");
  assert.equal(r.gradeEvidence, "answered 20 events at a settled anchor");
  assert.equal(r.gradeMeasuredAt, NOW, "the grade's clock sits one place right of the fold's");
  assert.equal(r.extra, 0, "`l` and `L` are the panel's own tags, not unknown ones");
  ok("the grade is read off the label and the software off `s`, each as itself");
}

{
  // `l` is shared ground: nostr.watch labels the same relay with its country and ASN.
  const r = readRecord({
    created_at: NOW,
    tags: [["d", "wss://foreign.example/"], ["l", "CA", "countryCode"], ["l", "prime", "somebody.else"]],
  });
  assert.equal(r.grade, null, "another namespace's label is not our grade, whatever it says");
  ok("a foreign monitor's label under another namespace is not read as a grade");
}

{
  // The grade expires on both rules, like the other two verdicts.
  const aged = groupByHost([rec("wss://a.example/", [grade(PRIME, "e", NOW - TTL_SECONDS - 1)])], NOW);
  assert.equal(aged[0].prime, 0, "a grade past its TTL admits nothing");
  assert.equal(aged[0].graded, 0);
  const wrongEpoch = groupByHost([rec("wss://b.example/", [grade(PRIME, "e", NOW, "0")])], NOW);
  assert.equal(wrongEpoch[0].prime, 0, "a grade from superseded rules admits nothing either");
  const good = groupByHost([rec("wss://c.example/", [grade(PRIME, "e", NOW)])], NOW);
  assert.equal(good[0].prime, 1);
  assert.equal(good[0].graded, 1);
  // A refusal is graded but not prime, the distinction the two tiles carry.
  const dead = groupByHost([rec("wss://d.example/", [grade("dead", "no TCP answer", NOW)])], NOW);
  assert.equal(dead[0].graded, 1);
  assert.equal(dead[0].prime, 0);
  ok("a grade admits only inside its TTL and its epoch, and only `prime` admits");
}

// ---- prime, split by the transport it was measured over --------------------
{
  // A narrowing of `prime` and nothing else: same TTL, same epoch, same grade.
  const onion = `wss://${"v".repeat(56)}.onion/`;
  const sum = summarise(groupByHost([
    rec(onion, [grade(PRIME, "e", NOW), ["n", NETWORK_TOR]]),
    rec("wss://routed.example/", [grade(PRIME, "e", NOW), ["n", NETWORK_TOR]]),
    rec("wss://plain.example/", [grade(PRIME, "e", NOW), ["n", "clearnet"]]),
    // Graded over a circuit and refused: not in a roster, so not in this tile.
    rec("wss://dead.example/", [grade("dead", "no answer through the circuit", NOW), ["n", NETWORK_TOR]]),
    // Prime over Tor a month ago, which is a url the router is re-measuring.
    rec("wss://aged.example/", [grade(PRIME, "e", NOW - TTL_SECONDS - 1), ["n", NETWORK_TOR]]),
  ], NOW), NOW);
  assert.equal(sum.prime, 3);
  assert.equal(sum.primeTor, 2, "a clearnet prime is not counted, and a routed clearnet host is");
  assert.ok(sum.primeTor <= sum.prime, "the split can never exceed what it splits");
  ok("`prime on Tor` counts the current prime grades whose measurement went over a circuit");
}

{
  // An onion address has no other transport, and older records carry no `n`.
  const bare = `wss://${"w".repeat(56)}.onion/`;
  const sum = summarise(groupByHost([rec(bare, [grade(PRIME, "e", NOW)])], NOW), NOW);
  assert.equal(sum.primeTor, 1);
  // The reverse is not inferred: a clearnet host with no `n` is not a claim of Tor.
  const quiet = summarise(groupByHost([rec("wss://quiet.example/", [grade(PRIME, "e", NOW)])], NOW), NOW);
  assert.equal(quiet.primeTor, 0, "silence about the transport is not a claim of one");
  ok("an onion with no network tag counts as Tor; a clearnet host with none does not");
}

{
  // `silent` must look past the fold and the stability tag: most urls are graded alone.
  const graded = summarise(groupByHost([rec("wss://a.example/", [grade("dead", "no TCP answer", NOW)])], NOW), NOW);
  assert.equal(graded.silent, 0, "the monitor looked at this url and wrote down what it found");
  assert.equal(graded.graded, 1);
  const nothing = summarise(groupByHost([rec("wss://b.example/", [["n", "clearnet"]])], NOW), NOW);
  assert.equal(nothing.silent, 1, "a record with no verdict of any kind is still silent");
  ok("a url carrying a grade is not counted as one the monitor has never looked at");
}

// ---- the rest of the record, which several writers share -------------------
{
  // Other writers put `n`, `rtt-*` and `R` on the same record; the panel draws them so a writer
  // that rebuilds the record and drops them is visible.
  const r = readRecord({
    created_at: NOW,
    pubkey: "a".repeat(64),
    content: '{"name":"x"}',
    tags: [
      ["d", "wss://gated.example/"],
      ["n", "clearnet"],
      ["R", "auth"],
      ["R", "payment"],
      ["rtt-open", "875"],
      ["rtt-read", "897"],
      ["s", "git+https://github.com/hoytech/strfry.git"],
      ["something-new", "42"],
      ["same-as", "wss://gated.example/", "e", String(NOW)],
    ],
  });
  assert.equal(r.network, "clearnet");
  assert.deepEqual(r.requirements, ["auth", "payment"], "a relay can be both auth-gated and paid");
  assert.equal(r.rttOpen, "875");
  assert.equal(r.rttRead, "897");
  assert.equal(r.software, "git+https://github.com/hoytech/strfry.git", "`s` is the relay's software, which is what it means everywhere else");
  assert.equal(r.hasDoc, true);
  assert.equal(r.cleared, true, "reading the metadata does not disturb the verdict");
  // Counted, never dropped: an unknown tag must look different from nothing.
  assert.equal(r.extra, 1);
  ok("the tags other writers put on the record are read, and unknown ones are counted");
}

{
  // The clobbered shape: our verdict and nothing else, every metadata field absent.
  const r = readRecord(rec("wss://lonely.example/a", [sameAs("wss://lonely.example/", "e", NOW)]));
  assert.equal(r.network, undefined);
  assert.deepEqual(r.requirements, []);
  assert.equal(r.extra, 0);
  assert.equal(r.hasDoc, false);
  ok("a record carrying only our own tag reads as exactly that");
}

{
  // A synthesised survivor must carry every field a read record does.
  const [group] = groupByHost([rec("wss://s.example/a", [sameAs("wss://s.example/b", "e", NOW)])], NOW);
  const survivor = group.urls.find((u) => u.synthetic);
  const real = readRecord(rec("wss://s.example/a", [sameAs("wss://s.example/b", "e", NOW)]));
  for (const key of Object.keys(real)) {
    assert.ok(key in survivor, `a synthesised survivor is missing "${key}", which the renderer reads on every row`);
  }
  assert.deepEqual(survivor.requirements, []);
  ok("a synthesised survivor carries every field a read record does");
}

// ---- the walk, and a run of records longer than a page --------------------
{
  // Holds `total` records, hands back the newest `limit` at or below `until`, and stamps `run`
  // of them with one created_at, as a flushed batch is.
  const relay = (total, run, at = 1000) => {
    const all = [];
    for (let i = 0; i < total; i++) all.push({ id: "e" + i, created_at: i < run ? at : at - 1 - i });
    all.sort((a, b) => b.created_at - a.created_at || a.id.localeCompare(b.id));
    let asks = 0;
    return {
      asks: () => asks,
      ask: async (limit, until) => {
        asks++;
        return all.filter((e) => until == null || e.created_at <= until).slice(0, limit);
      },
    };
  };

  // A run longer than the page: a fixed page returns the same records and the cursor cannot move.
  const stuck = relay(900, 600);
  const fixed = await walkRecords({ ask: stuck.ask, pageSize: 500, maxPage: 500 });
  assert.ok(fixed.events.length < 900, "the fixture does not reproduce the run this exists for");
  assert.equal(fixed.complete, false, "a walk that could not get past a run must NOT call itself complete");

  // The same relay, allowed to grow its page.
  const grown = relay(900, 600);
  const full = await walkRecords({ ask: grown.ask, pageSize: 500, maxPage: 5000 });
  assert.equal(full.events.length, 900, "growing the page past the run is what recovers the missing records");
  assert.equal(full.complete, true);
  assert.ok(full.grew > 0, "it should have had to grow at least once");
  ok("a page that is entirely one second grows until it spans two, instead of stepping over the rest");
}

{
  // The ordinary case pays nothing: no run longer than a page, no growth.
  const all = [];
  for (let i = 0; i < 1200; i++) all.push({ id: "x" + i, created_at: 9000 - i });
  const ask = async (limit, until) => all.filter((e) => until == null || e.created_at <= until).slice(0, limit);
  const walk = await walkRecords({ ask, pageSize: 500, maxPage: 5000 });
  assert.equal(walk.events.length, 1200);
  assert.equal(walk.complete, true);
  assert.equal(walk.grew, 0, "a store with no same-second run must never grow its ask");
  ok("a store without same-second runs is walked at the base page size");
}

{
  // Nothing at all, and a relay that answers every ask with the same page.
  assert.deepEqual((await walkRecords({ ask: async () => [] })).events, []);
  assert.equal((await walkRecords({ ask: async () => [] })).complete, true);
  const same = [{ id: "a", created_at: 5 }, { id: "b", created_at: 4 }];
  const spin = await walkRecords({ ask: async () => same, pageSize: 500, maxPage: 500, maxPages: 50 });
  assert.equal(spin.events.length, 2, "a relay that will not walk backwards is given up on, not spun on");
  assert.ok(spin.pages < 50);
  ok("an empty store completes, and a relay that repeats itself is abandoned");
}

{
  // A page the timeout cut (`complete: false`) is not an exhausted store, and the cursor must
  // not step below it.
  const cut = (arr) => { arr.complete = false; return arr; };

  const timedOut = await walkRecords({ ask: async () => cut([]) });
  assert.equal(timedOut.complete, false, "an empty page the timeout cut must not read as an exhausted store");

  const untils = [];
  const page = [{ id: "a", created_at: 500 }, { id: "b", created_at: 400 }];
  const walk = await walkRecords({
    ask: async (limit, until) => { untils.push(until); return cut(page.filter((e) => until == null || e.created_at <= until)); },
    pageSize: 500,
    maxPage: 500,
    maxPages: 10,
  });
  assert.equal(walk.complete, false);
  assert.ok(!untils.some((u) => u != null && u < 400), `the cursor must never step below a cut page: ${untils}`);
  ok("a page the timeout cut ends the walk as partial, with the cursor held above what it withheld");
}
